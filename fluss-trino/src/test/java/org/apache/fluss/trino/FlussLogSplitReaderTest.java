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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.trino.FlussSplitReader.PollResult.AVAILABLE;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.FINISHED;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.YIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Verifies log boundaries, scanner progress and physical bucket subscription. */
final class FlussLogSplitReaderTest {
    private static final Duration TIMEOUT = Duration.ofMillis(100);
    private static final FlussBucketHandle BUCKET = new FlussBucketHandle(42, Optional.empty(), 0);
    private final Table table = mock(Table.class);
    private final Scan scan = mock(Scan.class);
    private final LogScanner scanner = mock(LogScanner.class);

    @BeforeEach
    void setUp() {
        when(table.newScan()).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(scanner);
    }

    @Test
    void testEmptyRangeDoesNotCreateScanner() throws Exception {
        try (FlussLogSplitReader reader = create(1000, 1000)) {
            assertThat(reader.isFinished()).isTrue();
            assertThat(reader.poll(TIMEOUT)).isEqualTo(FINISHED);
            assertThat(reader.hasNext()).isFalse();
        }
        verifyNoInteractions(table, scan, scanner);
    }

    @Test
    void testStopsAtExclusiveBoundaryAndCountsOnlyConsumedBytes() throws Exception {
        when(scanner.poll(TIMEOUT)).thenReturn(records(5, 9, 10, 11));
        try (FlussLogSplitReader reader = create(5, 10)) {
            assertThat(reader.poll(TIMEOUT)).isEqualTo(AVAILABLE);
            assertThat(reader.getRetainedSizeInBytes()).isGreaterThan(0);
            assertThatThrownBy(() -> reader.poll(TIMEOUT))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(reader.next().getLong(0)).isEqualTo(5);
            assertThat(reader.next().getLong(0)).isEqualTo(9);
            assertThat(reader.hasNext()).isFalse();
            assertThat(reader.isFinished()).isTrue();
            assertThat(reader.getCompletedBytes()).isEqualTo(16);
            assertThat(reader.getRetainedSizeInBytes()).isZero();
            assertThat(reader.poll(TIMEOUT)).isEqualTo(FINISHED);
        }
        verify(scanner).subscribe(0, 5);
        verify(scanner).poll(TIMEOUT);
        verify(scanner).close();
    }

    @Test
    void testSmallBatchYieldsUntilProgressReachesStop() throws Exception {
        when(scanner.poll(TIMEOUT))
                .thenReturn(
                        records(5),
                        ScanRecords.EMPTY,
                        new ScanRecords(
                                Collections.emptyMap(),
                                Collections.singletonMap(BUCKET.toTableBucket(), 10L)));
        try (FlussLogSplitReader reader = create(5, 10)) {
            assertThat(reader.poll(TIMEOUT)).isEqualTo(AVAILABLE);
            reader.next();
            assertThat(reader.isFinished()).isFalse();
            assertThat(reader.poll(TIMEOUT)).isEqualTo(YIELD);
            assertThat(reader.poll(TIMEOUT)).isEqualTo(FINISHED);
        }
    }

    @Test
    void testRecordBeforeStartFails() throws Exception {
        when(scanner.poll(TIMEOUT)).thenReturn(records(4));
        try (FlussLogSplitReader reader = create(5, 10)) {
            assertThatThrownBy(() -> reader.poll(TIMEOUT))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("precedes");
        }
    }

    @Test
    void testPartitionedSubscription() throws Exception {
        FlussBucketHandle partition = new FlussBucketHandle(42, Optional.of(7L), 2);
        when(scanner.poll(TIMEOUT))
                .thenReturn(
                        new ScanRecords(
                                Collections.singletonMap(
                                        new TableBucket(42, 7L, 2),
                                        Collections.singletonList(record(5)))));
        try (FlussLogSplitReader reader =
                new FlussLogSplitReader(table, partition, new FlussLogRange(5, 6))) {
            assertThat(reader.poll(TIMEOUT)).isEqualTo(AVAILABLE);
            assertThat(reader.next().getLong(0)).isEqualTo(5);
            assertThat(reader.isFinished()).isTrue();
        }
        verify(scanner).subscribe(7L, 2, 5L);
    }

    @Test
    void testSubscribeFailurePreservesCleanupFailure() throws Exception {
        RuntimeException failure = new IllegalStateException("subscribe failed");
        IOException cleanup = new IOException("close failed");
        doThrow(failure).when(scanner).subscribe(0, 5);
        doThrow(cleanup).when(scanner).close();
        assertThatThrownBy(() -> create(5, 10)).isSameAs(failure);
        assertThat(failure.getSuppressed()).containsExactly(cleanup);
    }

    @Test
    void testPollFailureHasReadErrorContext() throws Exception {
        RuntimeException failure = new IllegalStateException("poll failed");
        when(scanner.poll(TIMEOUT)).thenThrow(failure);
        try (FlussLogSplitReader reader = create(5, 10)) {
            assertThatThrownBy(() -> reader.poll(TIMEOUT))
                    .isInstanceOf(TrinoException.class)
                    .hasCause(failure)
                    .hasMessageContaining("bucketId=0");
        }
    }

    @Test
    void testEarlyCloseClearsBufferAndIsIdempotent() throws Exception {
        when(scanner.poll(TIMEOUT)).thenReturn(records(5, 6));
        FlussLogSplitReader reader = create(5, 10);
        reader.poll(TIMEOUT);
        reader.close();
        reader.close();
        assertThat(reader.hasNext()).isFalse();
        assertThat(reader.isFinished()).isTrue();
        assertThat(reader.getRetainedSizeInBytes()).isZero();
        verify(scanner).close();
    }

    private FlussLogSplitReader create(long start, long stop) {
        return new FlussLogSplitReader(table, BUCKET, new FlussLogRange(start, stop));
    }

    private static ScanRecords records(long... offsets) {
        return new ScanRecords(
                Collections.singletonMap(
                        BUCKET.toTableBucket(),
                        Arrays.stream(offsets)
                                .mapToObj(FlussLogSplitReaderTest::record)
                                .collect(java.util.stream.Collectors.toList())));
    }

    private static ScanRecord record(long offset) {
        return new ScanRecord(42, 1, offset, 0, ChangeType.INSERT, GenericRow.of(offset), 8);
    }
}
