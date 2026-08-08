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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.flink.utils.TestLogScanner;
import org.apache.fluss.flink.utils.TestUpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link UndoRecoveryExecutor}.
 *
 * <p>Tests verify: (1) streaming execution with futures, (2) multi-bucket recovery, (3) exception
 * propagation.
 */
class UndoRecoveryExecutorTest {

    private static final RowType ROW_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT());
    private static final List<String> PRIMARY_KEY_COLUMNS = Collections.singletonList("f0");
    private static final long TABLE_ID = 1L;

    private KeyEncoder keyEncoder;
    private TestUpsertWriter mockWriter;
    private TestLogScanner mockScanner;
    private UndoComputer undoComputer;
    private UndoRecoveryExecutor executor;

    // Short per-bucket idle timeout for testing (~300ms instead of 1 hour)
    private static final long TEST_MAX_IDLE_TIME_MS = 300;

    @BeforeEach
    void setUp() {
        keyEncoder = KeyEncoder.of(ROW_TYPE, PRIMARY_KEY_COLUMNS, null);
        mockWriter = new TestUpsertWriter();
        mockScanner = new TestLogScanner();
        undoComputer = new UndoComputer(keyEncoder, mockWriter);
        // Use short timeout for testing
        executor =
                new UndoRecoveryExecutor(
                        mockScanner, mockWriter, undoComputer, TEST_MAX_IDLE_TIME_MS);
    }

    /**
     * Test multi-bucket recovery with mixed ChangeTypes and key deduplication.
     *
     * <p>Validates: Requirements 3.3 - All futures complete after execute.
     */
    @Test
    void testMultiBucketRecoveryWithDeduplication() throws Exception {
        TableBucket bucket0 = new TableBucket(TABLE_ID, 0);
        TableBucket bucket1 = new TableBucket(TABLE_ID, 1);

        BucketRecoveryContext ctx0 = new BucketRecoveryContext(bucket0, 0L, 4L);

        BucketRecoveryContext ctx1 = new BucketRecoveryContext(bucket1, 0L, 3L);

        // Bucket0: INSERT(key=1), UPDATE_BEFORE(key=1, dup), INSERT(key=2), DELETE(key=3)
        mockScanner.setRecordsForBucket(
                bucket0,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "a", 100)),
                        new ScanRecord(1L, 0L, ChangeType.UPDATE_BEFORE, row(1, "b", 200)),
                        new ScanRecord(2L, 0L, ChangeType.INSERT, row(2, "c", 300)),
                        new ScanRecord(3L, 0L, ChangeType.DELETE, row(3, "d", 400))));

        // Bucket1: DELETE(key=10), UPDATE_AFTER(key=11, skip), INSERT(key=12)
        mockScanner.setRecordsForBucket(
                bucket1,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.DELETE, row(10, "e", 500)),
                        new ScanRecord(1L, 0L, ChangeType.UPDATE_AFTER, row(11, "f", 600)),
                        new ScanRecord(2L, 0L, ChangeType.INSERT, row(12, "g", 700))));

        executor.execute(Arrays.asList(ctx0, ctx1));

        // Bucket0: 3 unique keys (key=1 deduplicated)
        // - key=1: INSERT → delete
        // - key=2: INSERT → delete
        // - key=3: DELETE → upsert
        assertThat(ctx0.getProcessedKeys()).hasSize(3);

        // Bucket1: 2 unique keys (UPDATE_AFTER skipped)
        // - key=10: DELETE → upsert
        // - key=12: INSERT → delete
        assertThat(ctx1.getProcessedKeys()).hasSize(2);

        // Total: 3 deletes (key=1,2,12), 2 upserts (key=3,10)
        assertThat(mockWriter.getDeleteCount()).isEqualTo(3);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(2);
        assertThat(mockWriter.isFlushCalled()).isTrue();
    }

    /**
     * Test that recovery is skipped when checkpoint offset >= target offset.
     *
     * <p>Covers three scenarios for {@link BucketRecoveryContext#needsRecovery()}:
     *
     * <ul>
     *   <li>checkpointOffset == logEndOffset → no recovery needed
     *   <li>checkpointOffset > logEndOffset → no recovery needed (checkpoint ahead)
     *   <li>checkpointOffset < logEndOffset → needs recovery
     * </ul>
     *
     * <p>Validates: No unnecessary work when no recovery needed.
     */
    @Test
    void testNoRecoveryNeededSkipsExecution() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);

        // Verify needsRecovery() for the three scenarios
        assertThat(new BucketRecoveryContext(bucket, 5L, 5L).needsRecovery())
                .as("checkpointOffset == logEndOffset → no recovery")
                .isFalse();
        assertThat(new BucketRecoveryContext(bucket, 10L, 5L).needsRecovery())
                .as("checkpointOffset > logEndOffset → no recovery")
                .isFalse();
        assertThat(new BucketRecoveryContext(bucket, 0L, 1L).needsRecovery())
                .as("checkpointOffset < logEndOffset → needs recovery")
                .isTrue();

        // Execute with no-recovery context and verify nothing happens
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 5L, 5L);
        executor.execute(Collections.singletonList(ctx));

        assertThat(mockWriter.getDeleteCount()).isEqualTo(0);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
        assertThat(mockWriter.isFlushCalled()).isFalse();
    }

    /**
     * Test exception propagation from writer failures.
     *
     * <p>Validates: Requirements 7.1, 7.2 - Exception propagation.
     */
    @Test
    void testExceptionPropagationFromWriter() {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);

        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 2L);

        mockScanner.setRecordsForBucket(
                bucket,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "a", 100)),
                        new ScanRecord(1L, 0L, ChangeType.INSERT, row(2, "b", 200))));

        mockWriter.setShouldFail(true);

        assertThatThrownBy(() -> executor.execute(Collections.singletonList(ctx)))
                .hasCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated write failure");
    }

    /**
     * Test that exception is thrown after a bucket makes no progress for the configured timeout.
     *
     * <p>Validates: Undo recovery timeout triggers a retryable exception. Note: This test uses a
     * mock scanner that always returns empty, so it will hit the timeout quickly in test
     * environment. In production, the timeout is 1 hour.
     */
    @Test
    void testFatalExceptionOnMaxEmptyPolls() {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);

        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 2L);

        // Configure scanner to always return empty (simulating network issues or server problems)
        mockScanner.setAlwaysReturnEmpty(true);

        // The test will timeout based on this bucket's idle time. The mock scanner does not block,
        // so it reaches the short test timeout quickly.
        assertThatThrownBy(() -> executor.execute(Collections.singletonList(ctx)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Undo recovery timed out")
                .hasMessageContaining("bucket=" + bucket)
                .hasMessageContaining("baseline=0")
                .hasMessageContaining("target=2")
                .hasMessageContaining("position=0")
                .hasMessageContaining("idle");
    }

    /**
     * Test multi-poll scenario where records are returned in batches.
     *
     * <p>This tests realistic LogScanner behavior where records are returned incrementally across
     * multiple poll() calls, ensuring the executor correctly handles partial results.
     */
    @Test
    void testMultiPollBatchProcessing() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);

        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 6L);

        // Configure 6 records but return only 2 per poll
        mockScanner.setRecordsForBucket(
                bucket,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "a", 100)),
                        new ScanRecord(1L, 0L, ChangeType.INSERT, row(2, "b", 200)),
                        new ScanRecord(2L, 0L, ChangeType.DELETE, row(3, "c", 300)),
                        new ScanRecord(3L, 0L, ChangeType.UPDATE_BEFORE, row(4, "d", 400)),
                        new ScanRecord(4L, 0L, ChangeType.UPDATE_AFTER, row(4, "e", 500)),
                        new ScanRecord(5L, 0L, ChangeType.INSERT, row(5, "f", 600))));
        mockScanner.setBatchSize(2);

        executor.execute(Collections.singletonList(ctx));

        // Should process all 6 records across 3 polls
        // key=1: INSERT → delete
        // key=2: INSERT → delete
        // key=3: DELETE → upsert
        // key=4: UPDATE_BEFORE → upsert (UPDATE_AFTER skipped)
        // key=5: INSERT → delete
        assertThat(ctx.getProcessedKeys()).hasSize(5);
        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(6);
        assertThat(mockWriter.getDeleteCount()).isEqualTo(3); // keys 1, 2, 5
        assertThat(mockWriter.getUpsertCount()).isEqualTo(2); // keys 3, 4
        assertThat(ctx.isComplete()).isTrue();
    }

    /**
     * Test single record recovery with checkpointOffset=0, logEndOffset=1.
     *
     * <p>This is a boundary case where the recovery range contains exactly one record. The executor
     * must read and undo that record before completion. Completion is determined by {@code
     * scanner.position(bucket) >= logEndOffset}.
     */
    @Test
    void testSingleRecordRecoveryOffByOneFix() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);

        // checkpointOffset=0, logEndOffset=1: exactly 1 record to recover
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 1L);

        // Scanner returns 1 INSERT record at offset 0
        mockScanner.setRecordsForBucket(
                bucket,
                Collections.singletonList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "a", 100))));

        executor.execute(Collections.singletonList(ctx));

        // The single record should have been processed
        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(1);

        // INSERT record should be undone with a delete
        assertThat(mockWriter.getDeleteCount()).isEqualTo(1);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
        assertThat(mockWriter.isFlushCalled()).isTrue();
    }

    @Test
    void testEmptyBatchOnlyRangeCompletesFromScannerPosition() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 3L);
        LogScanner scanner = new SinglePollLogScanner(bucket, Collections.emptyList(), 3L);
        UndoRecoveryExecutor boundaryExecutor =
                new UndoRecoveryExecutor(scanner, mockWriter, undoComputer, TEST_MAX_IDLE_TIME_MS);

        // One production poll can consume offsets [0, 3) without yielding user records.
        boundaryExecutor.execute(Collections.singletonList(ctx));

        assertThat(ctx.isComplete()).isTrue();
        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(0);
        assertThat(mockWriter.getDeleteCount()).isEqualTo(0);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
    }

    @Test
    void testTrailingEmptyBatchRangeCompletesFromScannerPosition() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 3L);
        LogScanner scanner =
                new SinglePollLogScanner(
                        bucket,
                        Collections.singletonList(
                                new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "a", 100))),
                        3L);
        UndoRecoveryExecutor boundaryExecutor =
                new UndoRecoveryExecutor(scanner, mockWriter, undoComputer, TEST_MAX_IDLE_TIME_MS);
        // The same poll returns the record at offset 0 and consumes trailing empty batches [1, 3).
        boundaryExecutor.execute(Collections.singletonList(ctx));

        assertThat(ctx.isComplete()).isTrue();
        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(1);
        assertThat(mockWriter.getDeleteCount()).isEqualTo(1);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
    }

    @Test
    void testRecentProgressIsNotRejectedByTotalElapsedTime() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 2L);
        ProgressThenEmptyLogScanner scanner = new ProgressThenEmptyLogScanner(bucket);
        TestUpsertWriter writer = new TestUpsertWriter();
        UndoComputer computer = new UndoComputer(keyEncoder, writer);
        UndoRecoveryExecutor progressExecutor =
                new UndoRecoveryExecutor(scanner, writer, computer, 100L);

        progressExecutor.execute(Collections.singletonList(ctx));

        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(2);
        assertThat(writer.getDeleteCount()).isEqualTo(2);
        assertThat(writer.getUpsertCount()).isEqualTo(0);
    }

    @Test
    void testDoesNotProcessRecordAtOrBeyondTargetLeo() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 2L);

        // Offset 1 is represented by an empty log batch. The record at offset 2 belongs to writes
        // after the recovery target and must never be undone.
        mockScanner.setRecordsForBucket(
                bucket,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "before-target", 100)),
                        new ScanRecord(2L, 0L, ChangeType.INSERT, row(2, "at-target", 200))));

        executor.execute(Collections.singletonList(ctx));

        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(1);
        assertThat(mockWriter.getDeleteCount()).isEqualTo(1);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
    }

    @Test
    void testEachBucketHasIndependentIdleTimeout() {
        TableBucket progressingBucket = new TableBucket(TABLE_ID, 0);
        TableBucket stalledBucket = new TableBucket(TABLE_ID, 1);
        BucketRecoveryContext progressingContext =
                new BucketRecoveryContext(progressingBucket, 0L, 2L);
        BucketRecoveryContext stalledContext = new BucketRecoveryContext(stalledBucket, 0L, 1L);
        OneBucketProgressLogScanner scanner =
                new OneBucketProgressLogScanner(progressingBucket, stalledBucket);
        TestUpsertWriter writer = new TestUpsertWriter();
        UndoRecoveryExecutor idleExecutor =
                new UndoRecoveryExecutor(
                        scanner, writer, new UndoComputer(keyEncoder, writer), 50L);

        assertThatThrownBy(
                        () ->
                                idleExecutor.execute(
                                        Arrays.asList(progressingContext, stalledContext)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Undo recovery timed out")
                .hasMessageContaining("bucket=" + stalledBucket)
                .hasMessageContaining("baseline=0")
                .hasMessageContaining("target=1")
                .hasMessageContaining("position=0")
                .hasMessageContaining("idle");

        assertThat(progressingContext.getTotalRecordsProcessed())
                .as("progress in one bucket must not reset another bucket's idle timer")
                .isEqualTo(1);
    }

    @Test
    void testNullPositionForActiveBucketFails() {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 1L);
        TestLogScanner scanner =
                new TestLogScanner() {
                    @Override
                    public Long position(TableBucket ignored) {
                        return null;
                    }
                };
        scanner.setRecordsForBucket(
                bucket,
                Collections.singletonList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "value", 100))));
        TestUpsertWriter writer = new TestUpsertWriter();
        UndoRecoveryExecutor nullPositionExecutor =
                new UndoRecoveryExecutor(
                        scanner,
                        writer,
                        new UndoComputer(keyEncoder, writer),
                        TEST_MAX_IDLE_TIME_MS);

        assertThatThrownBy(() -> nullPositionExecutor.execute(Collections.singletonList(ctx)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("active bucket")
                .hasMessageContaining(bucket.toString())
                .hasMessageContaining("position");
    }

    /** Returns one complete, externally observable poll result for a single bucket. */
    private static class SinglePollLogScanner implements LogScanner {
        private final TableBucket bucket;
        private final List<ScanRecord> records;
        private final long consumedUpToOffset;
        private Long position;
        private boolean returned;

        private SinglePollLogScanner(
                TableBucket bucket, List<ScanRecord> records, long consumedUpToOffset) {
            this.bucket = bucket;
            this.records = records;
            this.consumedUpToOffset = consumedUpToOffset;
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            if (position == null) {
                throw new IllegalStateException("LogScanner is not subscribed to the bucket");
            }
            if (returned) {
                return ScanRecords.EMPTY;
            }
            returned = true;
            position = consumedUpToOffset;
            return new ScanRecords(
                    Collections.singletonMap(bucket, records),
                    Collections.singletonMap(bucket, consumedUpToOffset));
        }

        @Override
        public void subscribe(int bucketId, long offset) {
            if (bucket.getPartitionId() != null || bucket.getBucket() != bucketId) {
                throw new IllegalArgumentException("Unexpected bucket " + bucketId);
            }
            position = offset;
        }

        @Override
        public void subscribe(long partitionId, int bucketId, long offset) {
            throw new UnsupportedOperationException("This scanner uses a non-partitioned bucket");
        }

        @Override
        public Long position(TableBucket tableBucket) {
            return bucket.equals(tableBucket) ? position : null;
        }

        @Override
        public void unsubscribe(long partitionId, int bucketId) {
            throw new UnsupportedOperationException("This scanner uses a non-partitioned bucket");
        }

        @Override
        public void unsubscribe(int bucketId) {
            if (bucket.getBucket() == bucketId) {
                position = null;
            }
        }

        @Override
        public void wakeup() {}

        @Override
        public void close() {}
    }

    private static class ProgressThenEmptyLogScanner implements LogScanner {
        private final TableBucket bucket;
        private int pollCount;
        private Long position;

        private ProgressThenEmptyLogScanner(TableBucket bucket) {
            this.bucket = bucket;
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            pollCount++;
            if (pollCount == 1) {
                sleep(250L);
                position = 1L;
                return recordsAt(0L, 1);
            }
            if (pollCount == 2) {
                return ScanRecords.EMPTY;
            }
            if (pollCount == 3) {
                position = 2L;
                return recordsAt(1L, 2);
            }
            return ScanRecords.EMPTY;
        }

        @Override
        public void subscribe(int bucket, long offset) {
            position = offset;
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            position = offset;
        }

        @Override
        public Long position(TableBucket tableBucket) {
            return bucket.equals(tableBucket) ? position : null;
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {
            position = null;
        }

        @Override
        public void unsubscribe(int bucket) {
            position = null;
        }

        @Override
        public void wakeup() {}

        @Override
        public void close() {}

        private ScanRecords recordsAt(long offset, int key) {
            Map<TableBucket, List<ScanRecord>> records = new HashMap<>();
            records.put(
                    bucket,
                    Collections.singletonList(
                            new ScanRecord(
                                    offset,
                                    0L,
                                    ChangeType.INSERT,
                                    row(key, "value-" + key, key * 100))));
            return new ScanRecords(records);
        }

        private static void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while scripting scanner progress", e);
            }
        }
    }

    private static class OneBucketProgressLogScanner implements LogScanner {
        private final TableBucket progressingBucket;
        private final TableBucket stalledBucket;
        private final Map<TableBucket, Long> positions = new HashMap<>();
        private int pollCount;

        private OneBucketProgressLogScanner(
                TableBucket progressingBucket, TableBucket stalledBucket) {
            this.progressingBucket = progressingBucket;
            this.stalledBucket = stalledBucket;
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            pollCount++;
            if (pollCount == 1) {
                ProgressThenEmptyLogScanner.sleep(75L);
                positions.put(progressingBucket, 1L);
                Map<TableBucket, List<ScanRecord>> records = new HashMap<>();
                records.put(
                        progressingBucket,
                        Collections.singletonList(
                                new ScanRecord(
                                        0L, 0L, ChangeType.INSERT, row(1, "progress", 100))));
                return new ScanRecords(records);
            }
            return ScanRecords.EMPTY;
        }

        @Override
        public void subscribe(int bucket, long offset) {
            if (progressingBucket.getBucket() == bucket) {
                positions.put(progressingBucket, offset);
            } else if (stalledBucket.getBucket() == bucket) {
                positions.put(stalledBucket, offset);
            }
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            throw new UnsupportedOperationException("This fixture uses non-partitioned buckets");
        }

        @Override
        public Long position(TableBucket bucket) {
            return positions.get(bucket);
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {
            throw new UnsupportedOperationException("This fixture uses non-partitioned buckets");
        }

        @Override
        public void unsubscribe(int bucket) {
            if (progressingBucket.getBucket() == bucket) {
                positions.remove(progressingBucket);
            } else if (stalledBucket.getBucket() == bucket) {
                positions.remove(stalledBucket);
            }
        }

        @Override
        public void wakeup() {}

        @Override
        public void close() {}
    }
}
