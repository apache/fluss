/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Verifies bounded synchronous reading, yielding and reader ownership. */
final class FlussPageSourceTest {
    private static final FlussTableHandle HANDLE =
            new FlussTableHandle("sales", "events", "Sales", "Events", 42, 1, 1, 0);
    private static final TablePath PATH = TablePath.of("Sales", "Events");
    private static final TableBucket BUCKET = new TableBucket(42, 0);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private final FlussClientManager clients = mock(FlussClientManager.class);
    private final Table table = mock(Table.class);
    private final Scan scan = mock(Scan.class);
    private final LogScanner scanner = mock(LogScanner.class);
    private FlussPageSource source;

    @BeforeEach
    void setUp() {
        when(clients.openTable(PATH)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo(42));
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(scanner);
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            source.close();
        }
    }

    @Test
    void testFirstPageIsReadSynchronouslyOnCallingThread() throws Exception {
        Thread caller = Thread.currentThread();
        when(scanner.poll(POLL_TIMEOUT))
                .thenAnswer(
                        invocation -> {
                            assertThat(Thread.currentThread()).isSameAs(caller);
                            return records(5, 6, 6);
                        });
        doAnswer(
                        invocation -> {
                            assertThat(Thread.currentThread()).isSameAs(caller);
                            return null;
                        })
                .when(scanner)
                .close();
        source = create(5, 6, false);
        assertThat(BIGINT.getLong(source.getNextSourcePage().getBlock(0), 0)).isEqualTo(5);
        assertThat(source.isFinished()).isTrue();
        verify(scanner).subscribe(0, 5);
        verify(scanner).close();
        verify(table).close();
    }

    @Test
    void testDefaultIsBlockedDoesNotInitializeReader() {
        source = create(5, 6, false);
        assertThat(source.isBlocked().isDone()).isTrue();
        assertThat(source.isFinished()).isFalse();
        verifyNoInteractions(clients);
    }

    @Test
    void testCrossPageBatchAndStopBoundary() throws Exception {
        when(scanner.poll(POLL_TIMEOUT)).thenReturn(records(5, 2057, 2057));
        source = create(5, 2055, false);
        List<Long> rows = new ArrayList<>();
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            assertThat(page).isNotNull();
            assertThat(page.getPositionCount()).isLessThanOrEqualTo(1024);
            for (int i = 0; i < page.getPositionCount(); i++) {
                rows.add(BIGINT.getLong(page.getBlock(0), i));
            }
        }
        assertThat(rows).hasSize(2050);
        assertThat(rows.get(0)).isEqualTo(5L);
        assertThat(rows.get(rows.size() - 1)).isEqualTo(2054L);
        assertThat(source.getCompletedPositions()).hasValue(2050);
        // Input byte accounting excludes fetched records beyond the stopping boundary.
        assertThat(source.getCompletedBytes()).isEqualTo(2050L * 8);
        verify(scanner).poll(POLL_TIMEOUT);
        source.close();
        verify(scanner).close();
        verify(table).close();
    }

    @Test
    void testOnePollPerCallAndProgressOnlyCompletion() {
        when(scanner.poll(POLL_TIMEOUT))
                .thenReturn(
                        ScanRecords.EMPTY,
                        ScanRecords.EMPTY,
                        new ScanRecords(
                                Collections.emptyMap(), Collections.singletonMap(BUCKET, 20L)));
        source = create(5, 20, false);
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isFalse();
        verify(scanner).poll(POLL_TIMEOUT);
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isFalse();
        verify(scanner, times(2)).poll(POLL_TIMEOUT);
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isTrue();
        verify(scanner, times(3)).poll(POLL_TIMEOUT);
    }

    @Test
    void testZeroColumnsCountActualRowsWithOffsetGaps() {
        List<ScanRecord> rows = Arrays.asList(record(5), record(9));
        when(scanner.poll(POLL_TIMEOUT))
                .thenReturn(
                        new ScanRecords(
                                Collections.singletonMap(BUCKET, rows),
                                Collections.singletonMap(BUCKET, 10L)));
        source = create(5, 10, true);
        SourcePage page = source.getNextSourcePage();
        assertThat(page.getChannelCount()).isZero();
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    void testFinalRecordCanCompleteWithoutProgressMetadata() {
        when(scanner.poll(POLL_TIMEOUT))
                .thenReturn(
                        new ScanRecords(
                                Collections.singletonMap(
                                        BUCKET, Collections.singletonList(record(5)))));
        source = create(5, 6, false);
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(1);
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    void testSmallBatchDoesNotImplyEndOfRange() {
        when(scanner.poll(POLL_TIMEOUT)).thenReturn(records(5, 6, 6), records(6, 8, 8));
        source = create(5, 8, false);
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(1);
        assertThat(source.isFinished()).isFalse();
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(2);
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    void testRecordBeforeStartFailsAndClosesReader() throws Exception {
        when(scanner.poll(POLL_TIMEOUT)).thenReturn(records(4, 6, 6));
        source = create(5, 6, false);
        assertThatThrownBy(source::getNextSourcePage)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("bucket 0")
                .hasMessageContaining("4");
        verify(scanner).close();
        verify(table).close();
    }

    @Test
    void testEmptyRangeDoesNotOpenTable() {
        source = create(5, 5, false);
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getNextSourcePage()).isNull();
        verifyNoInteractions(clients);
    }

    @Test
    void testEarlyCloseDiscardsRemainingBatchAndClosesOnce() throws Exception {
        when(scanner.poll(POLL_TIMEOUT)).thenReturn(records(0, 2050, 2050));
        source = create(0, 2050, false);
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(1024);
        assertThat(source.isFinished()).isFalse();
        source.close();
        source.close();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isTrue();
        verify(scanner).poll(POLL_TIMEOUT);
        verify(scanner).close();
        verify(table).close();
        verify(scanner, never()).wakeup();
    }

    @Test
    void testCloseBeforeInitializationHasNoClientInteraction() {
        source = create(0, 1, false);
        source.close();
        source.close();
        assertThat(source.getNextSourcePage()).isNull();
        verifyNoInteractions(clients, table, scanner);
    }

    @Test
    void testOpenedTableIdentityIsValidatedBeforeScannerCreation() throws Exception {
        when(table.getTableInfo()).thenReturn(tableInfo(43));
        source = create(0, 10, false);
        assertThatThrownBy(source::getNextSourcePage)
                .hasMessageContaining("changed during query planning");
        verify(table, never()).newScan();
        verify(table).close();
    }

    @Test
    void testInitializationFailureClosesTable() throws Exception {
        RuntimeException failure = new IllegalStateException("scanner initialization failed");
        when(scan.createLogScanner()).thenThrow(failure);
        source = create(0, 10, false);
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        verify(table).close();
    }

    @Test
    void testSubscribeFailureClosesBothResources() throws Exception {
        RuntimeException failure = new IllegalStateException("subscription failed");
        doThrow(failure).when(scanner).subscribe(0, 0);
        source = create(0, 10, false);
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        verify(scanner).close();
        verify(table).close();
    }

    @Test
    void testPollFailurePreservesCleanupFailure() throws Exception {
        RuntimeException readFailure = new IllegalStateException("read failed");
        IOException closeFailure = new IOException("scanner close failed");
        when(scanner.poll(POLL_TIMEOUT)).thenThrow(readFailure);
        doThrow(closeFailure).when(scanner).close();
        source = create(0, 10, false);
        assertThatThrownBy(source::getNextSourcePage).isSameAs(readFailure);
        assertThat(readFailure.getSuppressed()).hasSize(1);
        assertThat(readFailure.getSuppressed()[0]).hasCause(closeFailure);
        verify(table).close();
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    void testCloseFailurePreservesAllCleanupFailuresAndIsIdempotent() throws Exception {
        MemoryContext memory = mock(MemoryContext.class);
        when(scanner.poll(POLL_TIMEOUT)).thenReturn(ScanRecords.EMPTY);
        source =
                new FlussPageSource(
                        clients, HANDLE, new FlussSplit(0, 0, 10), Collections.emptyList(), memory);
        source.getNextSourcePage();
        IOException scannerFailure = new IOException("scanner close failed");
        IOException tableFailure = new IOException("table close failed");
        RuntimeException memoryFailure = new IllegalStateException("memory release failed");
        doThrow(scannerFailure).when(scanner).close();
        doThrow(tableFailure).when(table).close();
        doThrow(memoryFailure).when(memory).setBytes(0);
        assertThatThrownBy(source::close).hasCause(scannerFailure);
        assertThat(scannerFailure.getSuppressed()).containsExactly(tableFailure, memoryFailure);
        source.close();
        InOrder order = inOrder(scanner, table);
        order.verify(scanner).close();
        order.verify(table).close();
    }

    @Test
    void testMemoryFailureStillReleasesResources() throws Exception {
        MemoryContext memory = mock(MemoryContext.class);
        RuntimeException failure = new IllegalStateException("reservation denied");
        doAnswer(
                        invocation -> {
                            if ((long) invocation.getArgument(0) > 0) {
                                throw failure;
                            }
                            return null;
                        })
                .when(memory)
                .setBytes(anyLong());
        when(scanner.poll(POLL_TIMEOUT)).thenReturn(records(0, 1, 1));
        source =
                new FlussPageSource(
                        clients, HANDLE, new FlussSplit(0, 0, 1), Collections.emptyList(), memory);
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        verify(scanner).close();
        verify(table).close();
        verify(memory).setBytes(0);
    }

    @Test
    void testMemoryAccountingUsesCallingThreadAndClearsAfterLastPage() {
        MemoryContext memory = mock(MemoryContext.class);
        Thread caller = Thread.currentThread();
        List<Long> reservations = new ArrayList<>();
        doAnswer(
                        invocation -> {
                            assertThat(Thread.currentThread()).isSameAs(caller);
                            reservations.add(invocation.getArgument(0));
                            return null;
                        })
                .when(memory)
                .setBytes(anyLong());
        when(scanner.poll(POLL_TIMEOUT)).thenReturn(records(0, 1, 1));
        source =
                new FlussPageSource(
                        clients, HANDLE, new FlussSplit(0, 0, 1), Collections.emptyList(), memory);
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(1);
        assertThat(reservations).anyMatch(bytes -> bytes > 0);
        assertThat(reservations.get(reservations.size() - 1)).isZero();
    }

    private FlussPageSource create(long start, long stop, boolean zeroColumns) {
        return new FlussPageSource(
                clients,
                HANDLE,
                new FlussSplit(0, start, stop),
                zeroColumns
                        ? Collections.emptyList()
                        : Collections.singletonList(new FlussColumnHandle("id", 0)),
                MemoryContext.NO_LIMIT);
    }

    private static TableInfo tableInfo(long tableId) {
        return TableInfo.of(
                PATH,
                tableId,
                1,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.BIGINT()).build())
                        .distributedBy(1)
                        .build(),
                null,
                0,
                0);
    }

    private static ScanRecords records(long start, long stop, long progress) {
        List<ScanRecord> rows = new ArrayList<>();
        for (long offset = start; offset < stop; offset++) {
            rows.add(record(offset));
        }
        return new ScanRecords(
                Collections.singletonMap(BUCKET, rows), Collections.singletonMap(BUCKET, progress));
    }

    private static ScanRecord record(long offset) {
        return new ScanRecord(42, 1, offset, 0, ChangeType.INSERT, GenericRow.of(offset), 8);
    }
}
