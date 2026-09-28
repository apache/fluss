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
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

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
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Verifies shared page assembly, worker validation and resource ownership using a KV reader. */
final class FlussPageSourceTest {
    private static final FlussTableHandle HANDLE =
            new FlussTableHandle("sales", "users", "Sales", "Users", 42, 1);
    private static final TablePath PATH = TablePath.of("Sales", "Users");
    private static final TableBucket BUCKET = new TableBucket(42, 0);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private final FlussClientManager clients = mock(FlussClientManager.class);
    private final Table table = mock(Table.class);
    private final Scan scan = mock(Scan.class);
    private final BatchScanner scanner = mock(BatchScanner.class);
    private FlussPageSource source;

    @BeforeEach
    void setUp() {
        when(clients.openTable(PATH)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo(42, 1, 1, 0, true));
        when(table.newScan()).thenReturn(scan);
        when(scan.createBatchScanner(BUCKET)).thenReturn(scanner);
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            source.close();
        }
    }

    @Test
    void testLazyInitializationAndEarlyClose() {
        source = create();
        assertThat(source.isBlocked().isDone()).isTrue();
        assertThat(source.isFinished()).isFalse();
        source.close();
        source.close();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isTrue();
        verifyNoInteractions(clients, table, scan, scanner);
    }

    @Test
    void testEmptyBatchYieldsButNullFinishes() throws Exception {
        CloseableIterator<InternalRow> empty = rows(0);
        when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(empty).thenReturn(null);
        source = create();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isFalse();
        verify(scanner).pollBatch(POLL_TIMEOUT);
        verify(empty).close();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isTrue();
        verify(scanner, times(2)).pollBatch(POLL_TIMEOUT);
        verify(scanner).close();
        verify(table).close();
    }

    @Test
    void testBatchSpansPagesWithoutAdditionalPoll() throws Exception {
        CloseableIterator<InternalRow> batch = rows(2050);
        when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(batch).thenReturn(null);
        source = create();
        List<Long> values = new ArrayList<>();
        for (int size : new int[] {1024, 1024, 2}) {
            SourcePage page = source.getNextSourcePage();
            assertThat(page.getPositionCount()).isEqualTo(size);
            for (int i = 0; i < size; i++) {
                values.add(BIGINT.getLong(page.getBlock(0), i));
            }
            verify(scanner).pollBatch(POLL_TIMEOUT);
            assertThat(source.isFinished()).isFalse();
        }
        assertThat(values).hasSize(2050);
        for (int i = 0; i < values.size(); i++) {
            assertThat(values.get(i)).isEqualTo((long) i);
        }
        verify(batch).close();
        assertThat(source.getCompletedPositions()).hasValue(2050);
        assertThat(source.getCompletedBytes()).isZero();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isTrue();
        verify(scan).createBatchScanner(BUCKET);
        verifyNoMoreInteractions(scan);
    }

    @Test
    void testLogBatchSpansPagesAndCompletesAtStop() throws Exception {
        LogScanner logScanner = mock(LogScanner.class);
        when(table.getTableInfo()).thenReturn(tableInfo(42, 1, 1, 0, false));
        when(scan.createLogScanner()).thenReturn(logScanner);
        List<ScanRecord> records = new ArrayList<>();
        for (int i = 0; i < 2052; i++) {
            records.add(new ScanRecord(42, 1, i, 0, ChangeType.INSERT, GenericRow.of((long) i), 8));
        }
        when(logScanner.poll(POLL_TIMEOUT))
                .thenReturn(new ScanRecords(Collections.singletonMap(BUCKET, records)));
        source =
                create(
                        FlussSplit.forLog(new FlussBucketHandle(42, Optional.empty(), 0), 0, 2050),
                        Collections.singletonList(new FlussColumnHandle("id", 0)),
                        MemoryContext.NO_LIMIT);
        int position = 0;
        for (int size : new int[] {1024, 1024, 2}) {
            SourcePage page = source.getNextSourcePage();
            assertThat(page.getPositionCount()).isEqualTo(size);
            for (int i = 0; i < size; i++) {
                assertThat(BIGINT.getLong(page.getBlock(0), i)).isEqualTo(position++);
            }
        }
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getCompletedPositions()).hasValue(2050);
        assertThat(source.getCompletedBytes()).isEqualTo(2050L * 8);
        verify(logScanner).poll(POLL_TIMEOUT);
        verify(logScanner).close();
        verify(table).close();
    }

    @Test
    void testZeroColumnsRetainRealRowCount() throws Exception {
        when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(rows(3)).thenReturn(null);
        source =
                create(
                        FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                        Collections.emptyList(),
                        MemoryContext.NO_LIMIT);
        SourcePage page = source.getNextSourcePage();
        assertThat(page.getChannelCount()).isZero();
        assertThat(page.getPositionCount()).isEqualTo(3);
        assertThat(source.isFinished()).isFalse();
    }

    @Test
    void testReorderedAndNullColumns() throws Exception {
        when(scanner.pollBatch(POLL_TIMEOUT))
                .thenReturn(
                        spy(
                                CloseableIterator.wrap(
                                        Arrays.<InternalRow>asList(
                                                        GenericRow.of(1L, null),
                                                        GenericRow.of(2L, 7L))
                                                .iterator())));
        source =
                create(
                        FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                        Arrays.asList(
                                new FlussColumnHandle("value", 1), new FlussColumnHandle("id", 0)),
                        MemoryContext.NO_LIMIT);
        SourcePage page = source.getNextSourcePage();
        assertThat(page.getBlock(0).isNull(0)).isTrue();
        assertThat(BIGINT.getLong(page.getBlock(0), 1)).isEqualTo(7);
        assertThat(BIGINT.getLong(page.getBlock(1), 0)).isEqualTo(1);
    }

    @Test
    void testIdentityAndScanTypeCheckedBeforeScannerCreation() throws Exception {
        for (TableInfo info :
                Arrays.asList(
                        tableInfo(43, 1, 1, 0, true),
                        tableInfo(42, 2, 1, 0, true),
                        tableInfo(42, 1, 1, 0, false))) {
            when(table.getTableInfo()).thenReturn(info);
            source = create();
            assertThatThrownBy(source::getNextSourcePage).isInstanceOf(TrinoException.class);
            assertThat(source.isFinished()).isTrue();
        }
        verify(table, times(3)).close();
        verifyNoInteractions(scan, scanner);
    }

    @Test
    void testInvalidColumnFailsBeforeScannerCreation() throws Exception {
        source =
                create(
                        FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                        Collections.singletonList(new FlussColumnHandle("wrong", 0)),
                        MemoryContext.NO_LIMIT);
        assertThatThrownBy(source::getNextSourcePage).hasMessageContaining("schema");
        verify(table).close();
        verifyNoInteractions(scan, scanner);
    }

    @Test
    void testMetadataFailureClosesTable() throws Exception {
        RuntimeException failure = new IllegalStateException("metadata failed");
        when(table.getTableInfo()).thenThrow(failure);
        source = create();
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        verify(table).close();
        verifyNoInteractions(scan, scanner);
    }

    @Test
    void testWorkerRejectsUnsupportedCapabilitiesBeforeScannerCreation() throws Exception {
        for (boolean partitioned : new boolean[] {true, false}) {
            TableDescriptor.Builder descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("id", DataTypes.BIGINT())
                                            .column("value", DataTypes.BIGINT())
                                            .primaryKey("id", "value")
                                            .build())
                            .distributedBy(1);
            if (partitioned) {
                descriptor.partitionedBy("value");
            } else {
                descriptor.property("table.datalake.enabled", "true");
            }
            when(table.getTableInfo())
                    .thenReturn(TableInfo.of(PATH, 42, 1, descriptor.build(), null, 0, 0));
            source = create();
            assertThatThrownBy(source::getNextSourcePage).isInstanceOf(TrinoException.class);
        }
        verify(table, times(2)).close();
        verifyNoInteractions(scan, scanner);
    }

    @Test
    void testInitializationFailureClosesAcquiredTable() throws Exception {
        RuntimeException failure = new IllegalStateException("create failed");
        when(scan.createBatchScanner(BUCKET)).thenThrow(failure);
        source = create();
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        verify(table).close();
        verifyNoInteractions(scanner);
    }

    @Test
    void testOpenFailureDoesNotRetry() {
        RuntimeException failure = new IllegalStateException("open failed");
        when(clients.openTable(PATH)).thenThrow(failure);
        source = create();
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        assertThat(source.getNextSourcePage()).isNull();
        verify(clients).openTable(PATH);
        verifyNoInteractions(table, scanner);
    }

    @Test
    void testPollIOExceptionIncludesBucketAndPreservesCleanup() throws Exception {
        IOException failure = new IOException("snapshot expired");
        IOException cleanup = new IOException("close failed");
        when(scanner.pollBatch(POLL_TIMEOUT)).thenThrow(failure);
        doThrow(cleanup).when(scanner).close();
        source = create();
        assertThatThrownBy(source::getNextSourcePage)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("bucketId=0")
                .hasCause(failure)
                .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
        assertThat(source.getReadTimeNanos()).isGreaterThan(0);
        assertThat(source.getNextSourcePage()).isNull();
        verify(table).close();
        verify(scanner).pollBatch(POLL_TIMEOUT);
    }

    @Test
    void testIteratorFailuresCloseAllResources() throws Exception {
        for (boolean failHasNext : new boolean[] {true, false}) {
            CloseableIterator<InternalRow> batch = rows(1);
            RuntimeException failure = new IllegalStateException("iterator failed");
            if (failHasNext) {
                when(batch.hasNext()).thenThrow(failure);
            } else {
                doThrow(failure).when(batch).next();
            }
            when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(batch);
            source = create();
            assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
            verify(batch).close();
        }
        verify(scanner, times(2)).close();
        verify(table, times(2)).close();
    }

    @Test
    void testDecodeFailurePreservesIteratorCloseFailure() throws Exception {
        InternalRow row = mock(InternalRow.class);
        RuntimeException failure = new IllegalStateException("decode failed");
        RuntimeException cleanup = new IllegalStateException("iterator close failed");
        when(row.isNullAt(0)).thenThrow(failure);
        CloseableIterator<InternalRow> batch =
                spy(CloseableIterator.wrap(Arrays.asList(row, row).iterator()));
        doThrow(cleanup).when(batch).close();
        when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(batch);
        source = create();
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed()[0]).hasCause(cleanup);
        verify(scanner).close();
        verify(table).close();
    }

    @Test
    void testExhaustedIteratorCloseFailureIsNotRetried() throws Exception {
        CloseableIterator<InternalRow> batch = rows(1);
        RuntimeException failure = new IllegalStateException("iterator close failed");
        doThrow(failure).when(batch).close();
        when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(batch);
        source = create();
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        source.close();
        verify(batch).close();
        verify(scanner).close();
        verify(table).close();
        assertThat(source.getCompletedPositions()).hasValue(0);
    }

    @Test
    void testEarlyCloseOrderAndSuppressedFailures() throws Exception {
        CloseableIterator<InternalRow> batch = rows(2050);
        MemoryContext memory = mock(MemoryContext.class);
        when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(batch);
        source =
                create(
                        FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                        Collections.emptyList(),
                        memory);
        source.getNextSourcePage();
        RuntimeException iteratorFailure = new IllegalStateException("iterator close failed");
        IOException scannerFailure = new IOException("scanner close failed");
        IOException tableFailure = new IOException("table close failed");
        RuntimeException memoryFailure = new IllegalStateException("memory release failed");
        doThrow(iteratorFailure).when(batch).close();
        doThrow(scannerFailure).when(scanner).close();
        doThrow(tableFailure).when(table).close();
        doThrow(memoryFailure).when(memory).setBytes(0);
        assertThatThrownBy(source::close).hasCause(iteratorFailure);
        assertThat(iteratorFailure.getSuppressed())
                .containsExactly(scannerFailure, tableFailure, memoryFailure);
        source.close();
        InOrder order = inOrder(batch, scanner, table, memory);
        order.verify(batch).close();
        order.verify(scanner).close();
        order.verify(table).close();
        order.verify(memory, atLeastOnce()).setBytes(0);
        assertThat(source.getNextSourcePage()).isNull();
    }

    @Test
    void testCloseErrorStillReleasesOtherResources() throws Exception {
        CloseableIterator<InternalRow> batch = rows(2050);
        MemoryContext memory = mock(MemoryContext.class);
        when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(batch);
        source =
                create(
                        FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                        Collections.emptyList(),
                        memory);
        source.getNextSourcePage();
        AssertionError failure = new AssertionError("iterator close error");
        doThrow(failure).when(batch).close();
        assertThatThrownBy(source::close).isSameAs(failure);
        verify(scanner).close();
        verify(table).close();
        verify(memory, atLeastOnce()).setBytes(0);
        source.close();
        verify(batch).close();
    }

    @Test
    void testMemoryFailureClosesRemainingIterator() throws Exception {
        CloseableIterator<InternalRow> batch = rows(2050);
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
        when(scanner.pollBatch(POLL_TIMEOUT)).thenReturn(batch);
        source =
                create(
                        FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                        Collections.singletonList(new FlussColumnHandle("id", 0)),
                        memory);
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        verify(batch).close();
        verify(scanner).close();
        verify(table).close();
        verify(memory, atLeastOnce()).setBytes(0);
        assertThat(source.getCompletedPositions()).hasValue(0);
    }

    @Test
    void testPollingAndMemoryStayOnCallingThread() throws Exception {
        Thread caller = Thread.currentThread();
        MemoryContext memory = mock(MemoryContext.class);
        List<Long> reservations = new ArrayList<>();
        doAnswer(
                        invocation -> {
                            assertThat(Thread.currentThread()).isSameAs(caller);
                            reservations.add(invocation.getArgument(0));
                            return null;
                        })
                .when(memory)
                .setBytes(anyLong());
        when(scanner.pollBatch(POLL_TIMEOUT))
                .thenAnswer(
                        invocation -> {
                            assertThat(Thread.currentThread()).isSameAs(caller);
                            return rows(1);
                        });
        source =
                create(
                        FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                        Collections.singletonList(new FlussColumnHandle("id", 0)),
                        memory);
        source.getNextSourcePage();
        assertThat(reservations).anyMatch(value -> value > 0);
        assertThat(reservations.get(reservations.size() - 1)).isZero();
        assertThat(source.getCompletedBytes()).isZero();
    }

    private FlussPageSource create() {
        return create(
                FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                Collections.singletonList(new FlussColumnHandle("id", 0)),
                MemoryContext.NO_LIMIT);
    }

    private FlussPageSource create(
            FlussSplit split, List<FlussColumnHandle> columns, MemoryContext memory) {
        return new FlussPageSource(clients, HANDLE, split, columns, memory);
    }

    private static TableInfo tableInfo(
            long id, int schemaId, int bucketCount, int bucketEpoch, boolean primaryKey) {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("value", DataTypes.BIGINT());
        if (primaryKey) {
            schema.primaryKey("id");
        }
        return TableInfo.of(
                PATH,
                id,
                schemaId,
                TableDescriptor.builder().schema(schema.build()).distributedBy(bucketCount).build(),
                null,
                0,
                0,
                bucketEpoch);
    }

    private static CloseableIterator<InternalRow> rows(int count) {
        List<InternalRow> values = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            values.add(GenericRow.of(i, i));
        }
        return spy(CloseableIterator.wrap(values.iterator()));
    }
}
