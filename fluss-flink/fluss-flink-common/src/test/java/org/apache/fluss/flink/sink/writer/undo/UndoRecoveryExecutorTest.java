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

package org.apache.fluss.flink.sink.writer.undo;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.utils.TestLogScanner;
import org.apache.fluss.flink.utils.TestUpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        assertThat(ctx0.isComplete()).isTrue();
        assertThat(ctx1.isComplete()).isTrue();
    }

    /**
     * Test that recovery is skipped when checkpoint offset >= target offset.
     *
     * <p>Validates: No unnecessary work when no recovery needed.
     */
    @Test
    void testNoRecoveryNeededSkipsExecution() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);

        // Checkpoint offset (5) >= log end offset (5) → no recovery needed
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
        assertThat(ctx.getProcessedKeys()).hasSize(5);
        assertThat(ctx.getTotalRecordsProcessed()).isEqualTo(6);
        assertThat(mockWriter.getDeleteCount()).isEqualTo(3); // keys 1, 2, 5
        assertThat(mockWriter.getUpsertCount()).isEqualTo(2); // keys 3, 4
        assertThat(ctx.isComplete()).isTrue();
    }
}
