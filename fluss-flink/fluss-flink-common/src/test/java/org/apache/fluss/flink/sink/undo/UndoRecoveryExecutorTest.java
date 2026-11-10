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
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.flink.utils.TestLogScanner;
import org.apache.fluss.flink.utils.TestUpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    // Short timeout value for testing (total ~300ms instead of 1 hour)
    private static final long TEST_MAX_TOTAL_WAIT_TIME_MS = 300;

    @BeforeEach
    void setUp() {
        keyEncoder = KeyEncoder.of(ROW_TYPE, PRIMARY_KEY_COLUMNS, null);
        mockWriter = new TestUpsertWriter();
        mockScanner = new TestLogScanner();
        undoComputer = new UndoComputer(keyEncoder, mockWriter);
        // Use short timeout for testing
        executor =
                new UndoRecoveryExecutor(
                        mockScanner, mockWriter, undoComputer, TEST_MAX_TOTAL_WAIT_TIME_MS);
    }

    @Test
    void testFilteredRecoveryIsolatesBucketKeysAndCompletesFromEmptyPollProgress()
            throws Exception {
        AtomicInteger encodedKeys = new AtomicInteger();
        KeyEncoder countingEncoder =
                record -> {
                    encodedKeys.incrementAndGet();
                    return keyEncoder.encodeKey(record);
                };
        undoComputer = new UndoComputer(countingEncoder, mockWriter);
        TableBucket bucket0 = new TableBucket(TABLE_ID, 0);
        TableBucket bucket1 = new TableBucket(TABLE_ID, 1);
        BucketRecoveryContext ctx0 = new BucketRecoveryContext(bucket0, 0L, 3L);
        BucketRecoveryContext ctx1 = new BucketRecoveryContext(bucket1, 3L, 5L);
        ScanRecord first0 =
                new ScanRecord(0L, 0L, ChangeType.UPDATE_BEFORE, row(1, "bucket0", 100));
        ScanRecord first1 =
                new ScanRecord(3L, 0L, ChangeType.UPDATE_BEFORE, row(1, "bucket1", 200));
        ScanRecord duplicate0 =
                new ScanRecord(1L, 0L, ChangeType.UPDATE_BEFORE, row(1, "duplicate", 300));
        ScanRecord end0 = new ScanRecord(3L, 0L, ChangeType.UPDATE_BEFORE, row(2, "outside", 400));
        ScanRecord remaining0 =
                new ScanRecord(2L, 0L, ChangeType.UPDATE_BEFORE, row(1, "duplicate", 500));
        ScanRecord after1 = new ScanRecord(4L, 0L, ChangeType.UPDATE_AFTER, row(2, "after", 600));
        LogScannerImpl scanner = mock(LogScannerImpl.class);
        when(scanner.pollFiltered(any(Duration.class), any()))
                .thenAnswer(
                        invocation -> {
                            BiPredicate<TableBucket, LogRecord> filter = invocation.getArgument(1);
                            assertThat(filter.test(bucket0, first0)).isTrue();
                            assertThat(filter.test(bucket0, duplicate0)).isFalse();
                            assertThat(filter.test(bucket0, end0)).isFalse();
                            assertThat(filter.test(bucket1, first1)).isTrue();
                            assertThat(filter.test(new TableBucket(TABLE_ID, 2), first0)).isFalse();
                            assertThat(ctx0.getProcessedKeys()).isEmpty();
                            assertThat(ctx1.getProcessedKeys()).isEmpty();
                            Map<TableBucket, List<ScanRecord>> selected = new HashMap<>();
                            selected.put(
                                    bucket0,
                                    Collections.singletonList(
                                            new ScanRecord(
                                                    0L,
                                                    0L,
                                                    ChangeType.UPDATE_BEFORE,
                                                    row(1, "bucket0", 100))));
                            selected.put(bucket1, Collections.singletonList(first1));
                            Map<TableBucket, Long> progress = new HashMap<>();
                            progress.put(bucket0, 2L);
                            progress.put(bucket1, 4L);
                            return new ScanRecords(selected, progress);
                        })
                .thenAnswer(
                        invocation -> {
                            BiPredicate<TableBucket, LogRecord> filter = invocation.getArgument(1);
                            assertThat(filter.test(bucket0, remaining0)).isFalse();
                            assertThat(filter.test(bucket1, after1)).isFalse();
                            Map<TableBucket, Long> progress = new HashMap<>();
                            progress.put(bucket0, 3L);
                            progress.put(bucket1, 5L);
                            return new ScanRecords(Collections.emptyMap(), progress);
                        })
                .thenThrow(
                        new AssertionError("Recovery polled after consuming both bucket ranges"));

        new UndoRecoveryExecutor(scanner, mockWriter, undoComputer, TEST_MAX_TOTAL_WAIT_TIME_MS)
                .execute(Arrays.asList(ctx0, ctx1));

        assertThat(mockWriter.getAllUpsertedRows())
                .containsExactly(first0.getRow(), first1.getRow());
        assertThat(mockWriter.getDeleteCount()).isZero();
        assertThat(mockWriter.isFlushCalled()).isTrue();
        assertThat(ctx0.isComplete()).isTrue();
        assertThat(ctx1.isComplete()).isTrue();
        assertThat(ctx0.getProcessedKeys()).isEmpty();
        assertThat(ctx1.getProcessedKeys()).isEmpty();
        assertThat(ctx0.getTotalUndoOperations()).isEqualTo(1);
        assertThat(ctx1.getTotalUndoOperations()).isEqualTo(1);
        assertThat(encodedKeys.get()).isEqualTo(4);
        verify(scanner, times(2)).pollFiltered(any(Duration.class), any());
        verify(scanner).unsubscribe(0);
        verify(scanner).unsubscribe(1);
    }

    @Test
    void testCompletedBucketReleasesKeysWhileOtherBucketsAreStillRecovering() throws Exception {
        TableBucket firstBucket = new TableBucket(TABLE_ID, 0);
        TableBucket secondBucket = new TableBucket(TABLE_ID, 1);
        BucketRecoveryContext first = new BucketRecoveryContext(firstBucket, 0L, 1L);
        BucketRecoveryContext second = new BucketRecoveryContext(secondBucket, 0L, 2L);
        TestLogScanner scanner =
                new TestLogScanner() {
                    private int polls;

                    @Override
                    public ScanRecords poll(Duration timeout) {
                        if (++polls == 2) {
                            assertThat(first.isComplete()).isTrue();
                            assertThat(first.getProcessedKeys()).isEmpty();
                            assertThat(first.getTotalUndoOperations()).isEqualTo(1);
                            assertThat(second.getProcessedKeys()).hasSize(1);
                        }
                        return super.poll(timeout);
                    }
                };
        scanner.setBatchSize(1);
        scanner.setRecordsForBucket(
                firstBucket,
                Collections.singletonList(
                        new ScanRecord(0L, 0L, ChangeType.DELETE, row(1, "first", 1))));
        scanner.setRecordsForBucket(
                secondBucket,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.DELETE, row(2, "second", 2)),
                        new ScanRecord(1L, 0L, ChangeType.DELETE, row(3, "third", 3))));

        new UndoRecoveryExecutor(scanner, mockWriter, undoComputer)
                .execute(Arrays.asList(first, second));

        assertThat(second.getTotalUndoOperations()).isEqualTo(2);
        assertThat(second.getProcessedKeys()).isEmpty();
    }

    @Test
    void testLargeRecoveryFlushesBoundedWindowsAndWaitsForOffsetCallbacks() throws Exception {
        int recordCount = 10000;
        BufferedUpsertWriter writer = new BufferedUpsertWriter(false);
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        setLargeRecoveryRecords(bucket, recordCount);
        BucketRecoveryContext context = new BucketRecoveryContext(bucket, 0L, recordCount);

        Map<TableBucket, Long> offsets =
                new UndoRecoveryExecutor(mockScanner, writer, new UndoComputer(keyEncoder, writer))
                        .execute(Collections.singletonList(context));

        assertThat(writer.flushCount).isGreaterThan(1);
        assertThat(writer.maxBuffered).isLessThan(recordCount);
        assertThat(writer.pending).isEmpty();
        assertThat(offsets).containsEntry(bucket, recordCount + 1000L);
        assertThat(context.getTotalUndoOperations()).isEqualTo(recordCount);
    }

    @Test
    void testFailedAcknowledgementStopsBeforeSubmittingTheRemainingWindows() {
        int recordCount = 10000;
        BufferedUpsertWriter writer = new BufferedUpsertWriter(true);
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        setLargeRecoveryRecords(bucket, recordCount);

        assertThatThrownBy(
                        () ->
                                new UndoRecoveryExecutor(
                                                mockScanner,
                                                writer,
                                                new UndoComputer(keyEncoder, writer))
                                        .execute(
                                                Collections.singletonList(
                                                        new BucketRecoveryContext(
                                                                bucket, 0L, recordCount))))
                .hasRootCauseMessage("injected acknowledgement failure");
        assertThat(writer.submitted).isLessThan(recordCount);
    }

    @Test
    void testBucketAdmissionWaitsForCompletionAndReleasesKeys() throws Exception {
        for (int limit : Arrays.asList(1, 2, 32)) {
            List<BucketRecoveryContext> contexts = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                TableBucket bucket =
                        i % 2 == 0
                                ? new TableBucket(TABLE_ID, i)
                                : new TableBucket(TABLE_ID, 100L + i, i);
                contexts.add(new BucketRecoveryContext(bucket, 0L, 1L));
            }
            BufferedUpsertWriter writer = new BufferedUpsertWriter(false);
            AdmissionScanner scanner = new AdmissionScanner(contexts, writer);
            Map<TableBucket, Long> recovered =
                    new UndoRecoveryExecutor(
                                    scanner, writer, new UndoComputer(keyEncoder, writer), limit, 3)
                            .execute(contexts);

            assertThat(scanner.maxActive).isEqualTo(limit);
            assertThat(scanner.admitted).isEqualTo(40);
            assertThat(scanner.active).isEmpty();
            assertThat(recovered).hasSize(40);
            assertThat(writer.submitted).isEqualTo(40);
            assertThat(writer.pending).isEmpty();
            for (BucketRecoveryContext context : contexts) {
                assertThat(context.isComplete()).isTrue();
                assertThat(context.getProcessedKeys()).isEmpty();
                assertThat(context.getTotalUndoOperations()).isEqualTo(1);
                assertThat(recovered.get(context.getBucket())).isGreaterThanOrEqualTo(1L);
            }
        }
    }

    @Test
    void testFailedWriteDoesNotAdmitQueuedBuckets() {
        List<BucketRecoveryContext> contexts =
                Arrays.asList(
                        new BucketRecoveryContext(new TableBucket(TABLE_ID, 0), 0L, 1L),
                        new BucketRecoveryContext(new TableBucket(TABLE_ID, 1), 0L, 1L));
        BufferedUpsertWriter writer = new BufferedUpsertWriter(true);
        AdmissionScanner scanner = new AdmissionScanner(contexts, writer);
        assertThatThrownBy(
                        () ->
                                new UndoRecoveryExecutor(
                                                scanner,
                                                writer,
                                                new UndoComputer(keyEncoder, writer),
                                                1,
                                                3)
                                        .execute(contexts))
                .hasRootCauseMessage("injected acknowledgement failure");
        assertThat(scanner.admitted).isEqualTo(1);
        assertThat(writer.submitted).isEqualTo(1);
    }

    @Test
    void testWriteWindowIsConfigurable() throws Exception {
        for (int window : Arrays.asList(1, 3, 1024)) {
            TableBucket bucket = new TableBucket(TABLE_ID, 0);
            setLargeRecoveryRecords(bucket, 2050);
            BufferedUpsertWriter writer = new BufferedUpsertWriter(false);
            new UndoRecoveryExecutor(
                            mockScanner, writer, new UndoComputer(keyEncoder, writer), 1, window)
                    .execute(
                            Collections.singletonList(
                                    new BucketRecoveryContext(bucket, 0L, 2050L)));
            assertThat(writer.maxBuffered).isEqualTo(window);
            assertThat(writer.submitted).isEqualTo(2050);
            assertThat(writer.pending).isEmpty();
        }
    }

    @Test
    void testRecoveryLimitsMustBePositive() {
        for (int invalid : Arrays.asList(0, -1)) {
            assertThatThrownBy(
                            () ->
                                    new UndoRecoveryExecutor(
                                            mockScanner, mockWriter, undoComputer, invalid, 1024))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sink.undo-recovery.max-concurrent-buckets");
            assertThatThrownBy(
                            () ->
                                    new UndoRecoveryExecutor(
                                            mockScanner, mockWriter, undoComputer, 32, invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sink.undo-recovery.max-pending-writes");
        }
    }

    private static class AdmissionScanner extends TestLogScanner {
        private final Map<TableBucket, BucketRecoveryContext> contexts = new HashMap<>();
        private final Map<TableBucket, Boolean> active = new LinkedHashMap<>();
        private final BufferedUpsertWriter writer;
        private int maxActive;
        private int admitted;

        private AdmissionScanner(
                List<BucketRecoveryContext> contexts, BufferedUpsertWriter writer) {
            for (BucketRecoveryContext context : contexts) {
                this.contexts.put(context.getBucket(), context);
            }
            this.writer = writer;
        }

        @Override
        public void subscribe(int bucket, long offset) {
            admit(new TableBucket(TABLE_ID, bucket), offset);
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            admit(new TableBucket(TABLE_ID, partitionId, bucket), offset);
        }

        private void admit(TableBucket bucket, long offset) {
            assertThat(offset).isEqualTo(contexts.get(bucket).getCheckpointOffset());
            assertThat(writer.pending).isEmpty();
            for (BucketRecoveryContext context : contexts.values()) {
                if (context.isComplete()) {
                    assertThat(context.getProcessedKeys()).isEmpty();
                }
            }
            active.put(bucket, false);
            admitted++;
            maxActive = Math.max(maxActive, active.size());
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            TableBucket bucket = active.keySet().iterator().next();
            assertThat(active.get(bucket)).isFalse();
            active.put(bucket, true);
            ScanRecord record =
                    new ScanRecord(0L, 0L, ChangeType.DELETE, row(bucket.getBucket(), "value", 1));
            return new ScanRecords(
                    Collections.singletonMap(bucket, Collections.singletonList(record)),
                    Collections.singletonMap(bucket, 1L));
        }

        @Override
        public void unsubscribe(int bucket) {
            active.remove(new TableBucket(TABLE_ID, bucket));
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {
            active.remove(new TableBucket(TABLE_ID, partitionId, bucket));
        }
    }

    private void setLargeRecoveryRecords(TableBucket bucket, int count) {
        List<ScanRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new ScanRecord(i, 0L, ChangeType.DELETE, row(i, "value", i)));
        }
        mockScanner.setRecordsForBucket(bucket, records);
    }

    private static class BufferedUpsertWriter extends TestUpsertWriter {
        private final List<CompletableFuture<UpsertResult>> pending = new ArrayList<>();
        private final boolean failFirst;
        private int submitted;
        private int maxBuffered;
        private int flushCount;

        private BufferedUpsertWriter(boolean failFirst) {
            this.failFirst = failFirst;
        }

        @Override
        public CompletableFuture<UpsertResult> upsert(InternalRow row) {
            CompletableFuture<UpsertResult> future = new CompletableFuture<>();
            pending.add(future);
            submitted++;
            maxBuffered = Math.max(maxBuffered, pending.size());
            return future;
        }

        @Override
        public void flush() {
            flushCount++;
            int first = submitted - pending.size();
            for (int i = pending.size() - 1; i >= 0; i--) {
                if (failFirst && first + i == 0) {
                    pending.get(i)
                            .completeExceptionally(
                                    new IllegalStateException("injected acknowledgement failure"));
                } else {
                    pending.get(i)
                            .complete(
                                    new UpsertResult(
                                            new TableBucket(TABLE_ID, 0), first + i + 1001L));
                }
            }
            pending.clear();
        }
    }

    @Test
    void testRecoveredOffsetWaitsForOutOfOrderAcknowledgements() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        CompletableFuture<UpsertResult> first = new CompletableFuture<>();
        CompletableFuture<UpsertResult> second = new CompletableFuture<>();
        TestUpsertWriter writer =
                new TestUpsertWriter() {
                    @Override
                    public CompletableFuture<UpsertResult> upsert(InternalRow record) {
                        return record.getInt(0) == 1 ? first : second;
                    }

                    @Override
                    public void flush() {
                        second.complete(new UpsertResult(bucket, 20L));
                        first.complete(new UpsertResult(bucket, 10L));
                    }
                };
        mockScanner.setRecordsForBucket(
                bucket,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.DELETE, row(1, "a", 100)),
                        new ScanRecord(1L, 0L, ChangeType.DELETE, row(2, "b", 200))));
        UndoRecoveryExecutor recovery =
                new UndoRecoveryExecutor(mockScanner, writer, new UndoComputer(keyEncoder, writer));

        Map<TableBucket, Long> recovered =
                recovery.execute(
                        Collections.singletonList(new BucketRecoveryContext(bucket, 0L, 2L)));

        assertThat(recovered).containsOnlyKeys(bucket).containsEntry(bucket, 20L);
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
        assertThat(ctx0.getProcessedKeys()).isEmpty();
        assertThat(ctx0.getTotalUndoOperations()).isEqualTo(3);

        // Bucket1: 2 unique keys (UPDATE_AFTER skipped)
        // - key=10: DELETE → upsert
        // - key=12: INSERT → delete
        assertThat(ctx1.getProcessedKeys()).isEmpty();
        assertThat(ctx1.getTotalUndoOperations()).isEqualTo(2);

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
     * Test that exception is thrown after max total wait time.
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

        // The test will timeout based on total wait time, but since we're using a mock
        // scanner with no actual delay, it will fail quickly
        assertThatThrownBy(() -> executor.execute(Collections.singletonList(ctx)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Undo recovery timed out")
                .hasMessageContaining("minutes of waiting")
                .hasMessageContaining("still incomplete")
                .hasMessageContaining("restart and retry");
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
        assertThat(ctx.getProcessedKeys()).isEmpty();
        assertThat(ctx.getTotalUndoOperations()).isEqualTo(5);
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
    void testEmptyBatchOnlyRangeCompletesFromPollProgress() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 3L);
        mockScanner.setConsumedUpToOffset(bucket, 3L);

        assertThat(executor.execute(Collections.singletonList(ctx))).containsEntry(bucket, 3L);

        assertThat(ctx.isComplete()).isTrue();
        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(0);
        assertThat(mockWriter.getDeleteCount()).isEqualTo(0);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
    }

    @Test
    void testTrailingEmptyBatchRangeCompletesFromPollProgress() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 3L);
        mockScanner.setRecordsForBucket(
                bucket,
                Collections.singletonList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "a", 100))));
        mockScanner.setConsumedUpToOffset(bucket, 3L);

        executor.execute(Collections.singletonList(ctx));

        assertThat(ctx.isComplete()).isTrue();
        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(1);
        assertThat(mockWriter.getDeleteCount()).isEqualTo(1);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
    }

    @Test
    void testDoesNotProcessRecordAtOrBeyondTargetLeo() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L, 2L);
        mockScanner.setRecordsForBucket(
                bucket,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "before-target", 100)),
                        new ScanRecord(2L, 0L, ChangeType.INSERT, row(2, "at-target", 200))));
        mockScanner.setConsumedUpToOffset(bucket, 3L);

        executor.execute(Collections.singletonList(ctx));

        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(1);
        assertThat(mockWriter.getDeleteCount()).isEqualTo(1);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
    }
}
