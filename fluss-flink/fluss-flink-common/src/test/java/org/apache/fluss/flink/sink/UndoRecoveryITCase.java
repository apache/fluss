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

package org.apache.fluss.flink.sink;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ProducerOffsetsResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.testutils.CountingSource;
import org.apache.fluss.flink.sink.testutils.FailingCountingSource;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Undo Recovery functionality using Aggregation Merge Engine.
 *
 * <p>This test suite verifies undo recovery in two categories:
 *
 * <h3>Checkpoint Failover Tests (use FailingCountingSource)</h3>
 *
 * <p>These tests simulate real job failures by injecting exceptions and relying on Flink's
 * automatic checkpoint-based recovery. All failover tests use the default producerId (Flink Job ID)
 * to match production usage:
 *
 * <ul>
 *   <li><b>Checkpoint Failover Recovery</b>: {@link #testCheckpointFailoverRecovery()} - Tests
 *       recovery when failure occurs after checkpoint completion
 *   <li><b>Producer Offset Recovery</b>: {@link #testProducerOffsetRecoveryWithFailover()} - Tests
 *       recovery when failure occurs before any checkpoint
 *   <li><b>Multiple Keys Recovery</b>: {@link #testUndoRecoveryMultipleKeys()} - Tests recovery
 *       across multiple keys after checkpoint
 * </ul>
 *
 * <h3>Savepoint Rescale Tests (use CountingSource)</h3>
 *
 * <p>These tests verify undo recovery when parallelism changes during savepoint restore. Savepoints
 * are required for rescaling because checkpoints are tied to specific parallelism:
 *
 * <ul>
 *   <li><b>Rescale Up</b>: {@link #testRescaleUp()} - Parallelism 1 → 2
 *   <li><b>Rescale Down</b>: {@link #testRescaleDown()} - Parallelism 2 → 1
 * </ul>
 *
 * <p><b>Note:</b> Rescale tests continue to use savepoints because Flink checkpoints are bound to
 * specific parallelism and cannot be used for rescaling. Savepoints provide the portable state
 * format needed for parallelism changes.
 *
 * <p>Tests use bounded sources for precise record tracking and exact expected value calculations
 * with {@code isEqualTo()} assertions.
 */
abstract class UndoRecoveryITCase {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryITCase.class);

    protected static final String CATALOG_NAME = "testcatalog";
    protected static final String DEFAULT_DB = "fluss";
    protected static final int DEFAULT_BUCKET_NUM = 1;
    protected static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
    protected static final long VALUE_PER_RECORD = 10L;

    @TempDir public static File checkpointDir;
    @TempDir public static File savepointDir;
    @TempDir public static File failureMarkerDir;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new org.apache.fluss.config.Configuration()
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                                    .set(
                                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS,
                                            Integer.MAX_VALUE))
                    .setNumOfTabletServers(3)
                    .build();

    protected org.apache.fluss.config.Configuration clientConf;
    protected Connection conn;
    protected Admin admin;
    protected MiniClusterWithClientResource miniCluster;
    protected String bootstrapServers;

    @BeforeEach
    protected void beforeEach() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        miniCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(getFileBasedCheckpointsConfig())
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .build());
        miniCluster.before();
    }

    @AfterEach
    protected void afterEach() throws Exception {
        if (miniCluster != null) {
            miniCluster.after();
            miniCluster = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    // ==================== Test Methods ====================

    /**
     * Tests checkpoint-based failover recovery.
     *
     * <p>This test verifies that undo recovery works correctly when a job fails and automatically
     * recovers from a checkpoint. The test uses FailingCountingSource to inject a failure at a
     * controlled point.
     *
     * <p>Pattern:
     *
     * <ol>
     *   <li>Phase 1: Write 10 records, checkpoint completes
     *   <li>Phase 2: Failure is triggered (simulating crash)
     *   <li>Phase 3: Auto-recover from checkpoint, undo any uncommitted writes, write 3 new records
     * </ol>
     *
     * <p>Expected result: Final sum = (10 + 3) * VALUE_PER_RECORD = 130 (Any writes after the
     * checkpoint are undone during recovery)
     */
    @Test
    void testCheckpointFailoverRecovery() throws Exception {
        String tableName = "undo_checkpoint_failover_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        initTableEnvironment(null, false).executeSql(createAggTableDDL(tableName));

        // Configure: 10 records before failure, 3 records after recovery
        int recordsBeforeFailure = 10;
        int recordsAfterFailure = 3;

        // Start job with FailingCountingSource (default producerId = Flink Job ID)
        JobClient job =
                startFailoverJob(
                        tablePath, null, recordsBeforeFailure, recordsAfterFailure, 1, false);

        // Wait for checkpoint to complete before failure triggers
        waitForCheckpoint(job.getJobID());

        // Wait for final sum after recovery
        // Expected: (10 + 3) * VALUE_PER_RECORD = 130
        long expectedFinalSum = (recordsBeforeFailure + recordsAfterFailure) * VALUE_PER_RECORD;
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(tablePath, 1L);
                    assertThat(sum)
                            .as("Final sum after checkpoint failover recovery")
                            .isEqualTo(expectedFinalSum);
                });

        // Cancel the job after verification
        job.cancel().get();
        waitForJobTermination(job, DEFAULT_TIMEOUT);

        // Final verification
        Long finalSum = lookupSum(tablePath, 1L);
        LOG.info("Final sum: {}, expected: {}", finalSum, expectedFinalSum);
        assertThat(finalSum)
                .as(
                        "Final sum should equal (recordsBeforeFailure + recordsAfterFailure) * VALUE_PER_RECORD")
                .isEqualTo(expectedFinalSum);
    }

    /** Tests checkpoint-based failover recovery through the SQL/Table API sink path. */
    @Test
    void testCheckpointFailoverRecoverySQLJob() throws Exception {
        String tableName = "undo_checkpoint_failover_sql_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        // Configure: 10 records before failure, 3 records after recovery
        int recordsBeforeFailure = 10;
        int recordsAfterFailure = 3;

        // Start job with FailingCountingSource (default producerId = Flink Job ID)
        TableResult tableResult =
                startFailoverSqlJob(
                        tablePath,
                        createAggTableDDL(tableName),
                        recordsBeforeFailure,
                        recordsAfterFailure,
                        1,
                        false);

        JobClient job =
                tableResult
                        .getJobClient()
                        .orElseThrow(() -> new RuntimeException("JobClient not available"));

        // Wait for checkpoint to complete before failure triggers
        waitForCheckpoint(job.getJobID());

        // Wait for final sum after recovery
        // Expected: (10 + 3) * VALUE_PER_RECORD = 130
        long expectedFinalSum = (recordsBeforeFailure + recordsAfterFailure) * VALUE_PER_RECORD;
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(tablePath, 1L);
                    assertThat(sum)
                            .as("Final sum after checkpoint failover recovery")
                            .isEqualTo(expectedFinalSum);
                });

        // Cancel the job after verification
        job.cancel().get();
        waitForJobTermination(job, DEFAULT_TIMEOUT);

        // Final verification
        Long finalSum = lookupSum(tablePath, 1L);
        LOG.info("Final sum: {}, expected: {}", finalSum, expectedFinalSum);
        assertThat(finalSum)
                .as(
                        "Final sum should equal (recordsBeforeFailure + recordsAfterFailure) * VALUE_PER_RECORD")
                .isEqualTo(expectedFinalSum);
    }

    /**
     * Tests undo recovery with multiple keys using checkpoint-based failover.
     *
     * <p>This test verifies that undo recovery works correctly across multiple keys when a job
     * fails and automatically recovers from a checkpoint. The test uses FailingCountingSource in
     * multi-key mode to emit records for keys 1, 2, 3.
     *
     * <p>Pattern:
     *
     * <ol>
     *   <li>Phase 1: Write 10 records per key (30 total), checkpoint completes
     *   <li>Phase 2: Failure is triggered (simulating crash)
     *   <li>Phase 3: Auto-recover from checkpoint, undo any uncommitted writes, write 3 records per
     *       key
     * </ol>
     *
     * <p>Expected result: Each key's final sum = (10 + 3) * VALUE_PER_RECORD = 130
     */
    @Test
    void testUndoRecoveryMultipleKeys() throws Exception {
        String tableName = "undo_multi_keys_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        initTableEnvironment(null, false).executeSql(createAggTableDDL(tableName));

        // Configure: 10 records per key before failure, 3 records per key after recovery
        int recordsBeforeFailurePerKey = 10;
        int recordsAfterFailurePerKey = 3;

        // Start job with FailingCountingSource in multi-key mode (default producerId = Flink Job
        // ID)
        JobClient job =
                startFailoverJob(
                        tablePath,
                        null,
                        recordsBeforeFailurePerKey,
                        recordsAfterFailurePerKey,
                        1,
                        true); // multiKey = true

        // Wait for checkpoint to complete before failure triggers
        waitForCheckpoint(job.getJobID());

        // Wait for final sums after recovery for all 3 keys
        // Expected per key: (10 + 3) * VALUE_PER_RECORD = 130
        long expectedFinalSumPerKey =
                (recordsBeforeFailurePerKey + recordsAfterFailurePerKey) * VALUE_PER_RECORD;
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long[] sums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
                    for (int i = 0; i < 3; i++) {
                        assertThat(sums[i])
                                .as(
                                        "Key "
                                                + (i + 1)
                                                + " final sum after checkpoint failover recovery")
                                .isEqualTo(expectedFinalSumPerKey);
                    }
                });

        // Cancel the job after verification
        job.cancel().get();
        waitForJobTermination(job, DEFAULT_TIMEOUT);

        // Final verification
        Long[] finalSums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
        LOG.info(
                "Final sums: key1={}, key2={}, key3={}, expected={}",
                finalSums[0],
                finalSums[1],
                finalSums[2],
                expectedFinalSumPerKey);

        for (int i = 0; i < 3; i++) {
            assertThat(finalSums[i])
                    .as("Key " + (i + 1) + " final sum")
                    .isEqualTo(expectedFinalSumPerKey);
        }
    }

    /**
     * Tests producer offset recovery when failure occurs before any checkpoint completes.
     *
     * <p>This test verifies that undo recovery works even without checkpoint state, using the
     * producer offset stored on the server side. The test uses FailingCountingSource configured to
     * fail quickly (after just 2 records) before a checkpoint can complete.
     *
     * <p>Pattern:
     *
     * <ol>
     *   <li>Write 2 records (failure triggers before checkpoint)
     *   <li>Failure is triggered
     *   <li>Auto-recover from fresh state (no checkpoint to restore from)
     *   <li>Producer offset recovery undoes the 2 pre-failure records
     *   <li>Source emits total of 7 records (2+5), but only 7 are persisted after undo
     * </ol>
     *
     * <p>Expected result: Final sum = 7 * VALUE_PER_RECORD = 70
     *
     * <p>Note: Even without checkpoint state, Fluss can perform undo recovery using the producer
     * offset stored on the server. The pre-failure writes ARE undone because the producer ID allows
     * the server to identify uncommitted writes from the previous run.
     */
    @Test
    void testProducerOffsetRecoveryWithFailover() throws Exception {
        String tableName = "undo_producer_offset_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        initTableEnvironment(null, false).executeSql(createAggTableDDL(tableName));

        // Configure to fail quickly before checkpoint completes
        int recordsBeforeFailure = 2;
        int recordsAfterFailure = 5;

        // Start job with FailingCountingSource (default producerId = Flink Job ID)
        JobClient job =
                startFailoverJob(
                        tablePath, null, recordsBeforeFailure, recordsAfterFailure, 1, false);

        // Wait for final sum after recovery
        // Expected: 7 * VALUE_PER_RECORD = 70
        // Pre-failure writes ARE undone via producer offset recovery
        long expectedFinalSum = (recordsBeforeFailure + recordsAfterFailure) * VALUE_PER_RECORD;
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(tablePath, 1L);
                    assertThat(sum)
                            .as("Final sum after producer offset recovery")
                            .isEqualTo(expectedFinalSum);
                });

        // Cancel the job after verification
        job.cancel().get();
        waitForJobTermination(job, DEFAULT_TIMEOUT);

        // Final verification
        Long finalSum = lookupSum(tablePath, 1L);
        LOG.info("Final sum: {}, expected: {}", finalSum, expectedFinalSum);
        assertThat(finalSum)
                .as(
                        "Final sum should equal (recordsBeforeFailure + recordsAfterFailure) * VALUE_PER_RECORD")
                .isEqualTo(expectedFinalSum);
    }

    /** Tests Rescale Up - undo recovery when parallelism increases (1 -> 2). */
    @Test
    void testRescaleUp() throws Exception {
        runThreePhaseMultiBucketUndoTest(
                "undo_rescale_up",
                2, // buckets
                1, // phase1 & phase2 parallelism
                2, // phase3 parallelism (scale up)
                new int[] {10, 5, 3});
    }

    /** Tests Rescale Down - undo recovery when parallelism decreases (2 -> 1). */
    @Test
    void testRescaleDown() throws Exception {
        runThreePhaseMultiBucketUndoTest(
                "undo_rescale_down",
                2, // buckets
                2, // phase1 & phase2 parallelism
                1, // phase3 parallelism (scale down)
                new int[] {10, 5, 3});
    }

    /**
     * Tests that undo recovery works through the SQL/Table API sink path.
     *
     * <p>The SQL path creates the sink via {@code FlinkTableSink.getSinkRuntimeProvider()} → {@code
     * getFlinkSink()}, which is a different code path from the DataStream API's {@code
     * FlussSinkBuilder}. This test verifies that the SQL path correctly configures and executes
     * undo recovery.
     *
     * <p>Pattern:
     *
     * <ol>
     *   <li>Phase 1 (DataStream API): Write dirty data using CountingSource with a specific
     *       producerId, then cancel without checkpoint — leaving uncommitted writes
     *   <li>Phase 2 (SQL INSERT): Execute SQL INSERT with the same producerId via {@code
     *       sink.producer-id} table option — the SQL sink's UndoRecoveryOperator should undo the
     *       dirty data from Phase 1
     *   <li>Verify: Final result contains only the SQL INSERT data (dirty data undone)
     * </ol>
     */
    @Test
    void testSqlInsertWithUndoRecovery() throws Exception {
        String tableName = "undo_sql_test_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "test-producer-sql-" + System.currentTimeMillis();

        // Create table with sink.producer-id option (needed for SQL INSERT in Phase 2)
        initTableEnvironment(null, false)
                .executeSql(
                        String.format(
                                "CREATE TABLE `%s`.`%s` ("
                                        + "  id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,"
                                        + "  sum_val BIGINT"
                                        + ") WITH ("
                                        + "  'bucket.num' = '1',"
                                        + "  'table.merge-engine' = 'aggregation',"
                                        + "  'fields.sum_val.agg' = 'sum',"
                                        + "  'sink.producer-id' = '%s'"
                                        + ")",
                                DEFAULT_DB, tableName, producerId));

        // Phase 1: Write dirty data via DataStream API with the same producerId, cancel without
        // checkpoint
        JobClient dirtyJob = startBoundedJob(tablePath, producerId, null, 5, 1, false);

        // Wait for dirty data to be written
        long dirtySum = 5 * VALUE_PER_RECORD; // 50
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(tablePath, 1L);
                    assertThat(sum).as("Dirty data sum").isEqualTo(dirtySum);
                });

        // Cancel without checkpoint — leaves uncommitted writes
        dirtyJob.cancel().get();
        waitForJobTermination(dirtyJob, DEFAULT_TIMEOUT);

        // Verify dirty data is still visible
        Long sumAfterCancel = lookupSum(tablePath, 1L);
        LOG.info("Sum after dirty job cancel: {} (dirty, uncommitted)", sumAfterCancel);
        assertThat(sumAfterCancel).as("Dirty data should still be visible").isEqualTo(dirtySum);

        // Phase 2: SQL INSERT with the same producerId — triggers undo recovery via SQL path
        StreamTableEnvironment tEnv = initTableEnvironment(null, true);

        // SQL INSERT writes new data — undo recovery should undo the dirty Phase 1 data
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s`.`%s` VALUES (1, 100), (1, 200)",
                                DEFAULT_DB, tableName))
                .await();

        // Verify: final sum = only SQL INSERT data (dirty data undone)
        // If undo recovery didn't work, sum would be 50 + 300 = 350
        // With undo recovery: 300 (dirty 50 undone, then 100 + 200 written)
        long expectedSum = 300L;
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(tablePath, 1L);
                    assertThat(sum)
                            .as(
                                    "Final sum should be SQL INSERT data only (dirty data undone by SQL sink's undo recovery)")
                            .isEqualTo(expectedSum);
                });

        // Verify producer offsets cleaned up
        verifyProducerOffsetsCleanedUp(producerId);
    }

    /**
     * Tests that producer offsets are cleaned up via endInput() for bounded jobs without
     * checkpointing.
     *
     * <p>This verifies the cleanup path added in Comment #12: when a bounded job finishes (source
     * reaches end of input) without any checkpoint completing, {@code endInput()} calls {@code
     * deleteProducerOffsetsIfNeeded()} to ensure producer offsets don't remain in ZK.
     */
    @Test
    void testBoundedJobCleansUpProducerOffsetsWithoutCheckpoint() throws Exception {
        String tableName = "undo_bounded_cleanup_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "test-producer-bounded-" + System.currentTimeMillis();

        // Create table via SQL DDL
        StreamTableEnvironment tEnv = initTableEnvironment(null, false);
        tEnv.executeSql(createAggTableDDL(tableName));

        // Use SQL INSERT which is inherently bounded — no checkpointing needed.
        // endInput() is called when the bounded source finishes.
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE `%s`.`%s_with_pid` ("
                                + "  id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,"
                                + "  sum_val BIGINT"
                                + ") WITH ("
                                + "  'bucket.num' = '1',"
                                + "  'table.merge-engine' = 'aggregation',"
                                + "  'fields.sum_val.agg' = 'sum',"
                                + "  'sink.producer-id' = '%s'"
                                + ")",
                        DEFAULT_DB, tableName, producerId));

        // Execute bounded SQL INSERT (no checkpointing enabled in initTableEnvironment)
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s`.`%s_with_pid` VALUES (1, 10), (1, 20), (1, 30)",
                                DEFAULT_DB, tableName))
                .await();

        // Verify data written
        TablePath actualPath = TablePath.of(DEFAULT_DB, tableName + "_with_pid");
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long sum = lookupSum(actualPath, 1L);
                    assertThat(sum).as("Sum after bounded write").isEqualTo(60L);
                });

        // Verify producer offsets cleaned up via endInput() path
        verifyProducerOffsetsCleanedUp(producerId);
    }

    // ==================== Reusable Test Patterns ====================

    /**
     * Runs a three-phase undo recovery test with multiple buckets and multiple keys.
     *
     * <p>This test verifies that undo recovery works correctly across different buckets when
     * parallelism changes. It uses multiple keys (1, 2, 3) to ensure data is distributed across
     * buckets and verifies each key's sum independently.
     *
     * <p>Pattern: Phase1 (write + savepoint) -> Phase2 (write + cancel) -> Phase3 (restore + undo +
     * write)
     */
    private void runThreePhaseMultiBucketUndoTest(
            String tablePrefix,
            int buckets,
            int phase12Parallelism,
            int phase3Parallelism,
            int[] recordsPerPhase)
            throws Exception {

        String tableName = tablePrefix + "_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String producerId = "test-producer-" + tablePrefix + "-" + System.currentTimeMillis();

        LOG.info(
                "Test: {} with producerId: {}, buckets: {}, parallelism: {} -> {}",
                tablePrefix,
                producerId,
                buckets,
                phase12Parallelism,
                phase3Parallelism);

        initTableEnvironment(null, false).executeSql(createAggTableDDL(tableName, buckets));

        // Phase 1: Write records for multiple keys and take savepoint
        JobClient phase1Job =
                startBoundedJob(
                        tablePath, producerId, null, recordsPerPhase[0], phase12Parallelism, true);

        // Wait for data to be written before checkpoint (verify all 3 keys)
        long expectedSumPerKey = recordsPerPhase[0] * VALUE_PER_RECORD;
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long[] sums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
                    for (int i = 0; i < 3; i++) {
                        assertThat(sums[i])
                                .as("Key " + (i + 1) + " sum before savepoint")
                                .isEqualTo(expectedSumPerKey);
                    }
                });

        waitForCheckpoint(phase1Job.getJobID());

        Long[] sumsAtSavepoint = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
        LOG.info(
                "Sums at savepoint: key1={}, key2={}, key3={}",
                sumsAtSavepoint[0],
                sumsAtSavepoint[1],
                sumsAtSavepoint[2]);

        String savepointPath =
                phase1Job
                        .stopWithSavepoint(
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get(60, TimeUnit.SECONDS);
        waitForJobTermination(phase1Job, DEFAULT_TIMEOUT);
        Long[] sumsAfterPhase1 = lookupSumsForKeys(tablePath, 1L, 2L, 3L);

        // Phase 2: Write more records (will be undone), then cancel
        JobClient phase2Job =
                startBoundedJob(
                        tablePath,
                        producerId,
                        savepointPath,
                        recordsPerPhase[1],
                        phase12Parallelism,
                        true);

        // Wait for Phase 2 data to be written (verify all 3 keys)
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long[] sums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
                    for (int i = 0; i < 3; i++) {
                        long expected =
                                sumsAfterPhase1[i] + (recordsPerPhase[1] * VALUE_PER_RECORD);
                        assertThat(sums[i])
                                .as("Key " + (i + 1) + " sum after Phase 2 write")
                                .isEqualTo(expected);
                    }
                });

        phase2Job.cancel().get();
        waitForJobTermination(phase2Job, DEFAULT_TIMEOUT);

        Long[] sumsAfterPhase2 = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
        LOG.info(
                "Sums after Phase 2: key1={}, key2={}, key3={}",
                sumsAfterPhase2[0],
                sumsAfterPhase2[1],
                sumsAfterPhase2[2]);

        // Phase 3: Restore from savepoint with different parallelism - triggers undo recovery
        JobClient phase3Job =
                startBoundedJob(
                        tablePath,
                        producerId,
                        savepointPath,
                        recordsPerPhase[2],
                        phase3Parallelism,
                        true);

        // Wait for Phase 3 data to be written (after undo, verify all 3 keys)
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    Long[] sums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
                    for (int i = 0; i < 3; i++) {
                        long expected =
                                sumsAfterPhase1[i] + (recordsPerPhase[2] * VALUE_PER_RECORD);
                        assertThat(sums[i])
                                .as("Key " + (i + 1) + " final sum after undo")
                                .isEqualTo(expected);
                    }
                });

        // Verify producer offsets have been cleaned up from ZK after checkpoint (Comment #8)
        waitForCheckpoint(phase3Job.getJobID());
        verifyProducerOffsetsCleanedUp(producerId);

        phase3Job.cancel().get();
        waitForJobTermination(phase3Job, DEFAULT_TIMEOUT);

        // Verify: Phase 2 data undone for all keys, Phase 3 data present
        Long[] finalSums = lookupSumsForKeys(tablePath, 1L, 2L, 3L);
        LOG.info(
                "Final sums: key1={}, key2={}, key3={} (parallelism changed from {} to {})",
                finalSums[0],
                finalSums[1],
                finalSums[2],
                phase12Parallelism,
                phase3Parallelism);

        for (int i = 0; i < 3; i++) {
            long expected = sumsAfterPhase1[i] + (recordsPerPhase[2] * VALUE_PER_RECORD);
            assertThat(finalSums[i])
                    .as(
                            "Key "
                                    + (i + 1)
                                    + " final sum (phase2 undone, parallelism: "
                                    + phase12Parallelism
                                    + " -> "
                                    + phase3Parallelism
                                    + ")")
                    .isEqualTo(expected);
        }
    }

    // ==================== Helper Methods ====================

    private String createAggTableDDL(String tableName) {
        return createAggTableDDL(tableName, DEFAULT_BUCKET_NUM);
    }

    private String createAggTableDDL(String tableName, int bucketNum) {
        return String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "  id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,"
                        + "  sum_val BIGINT"
                        + ") WITH ("
                        + "  'bucket.num' = '%d',"
                        + "  'table.merge-engine' = 'aggregation',"
                        + "  'fields.sum_val.agg' = 'sum'"
                        + ")",
                DEFAULT_DB, tableName, bucketNum);
    }

    /**
     * Unified method to start a bounded streaming job.
     *
     * @param tablePath target table
     * @param producerId producer ID for undo recovery, or null to use default (Flink Job ID)
     * @param savepointPath savepoint to restore from, or null
     * @param maxRecords max records to emit (per key if multiKey)
     * @param parallelism job parallelism
     * @param multiKey if true, emit to keys 1,2,3 in round-robin
     */
    private JobClient startBoundedJob(
            TablePath tablePath,
            @Nullable String producerId,
            @Nullable String savepointPath,
            int maxRecords,
            int parallelism,
            boolean multiKey)
            throws Exception {

        Configuration conf = new Configuration();
        if (savepointPath != null) {
            conf.setString("execution.savepoint.path", savepointPath);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        env.enableCheckpointing(1000);

        CountingSource source =
                multiKey
                        ? CountingSource.multiKey(VALUE_PER_RECORD, maxRecords)
                        : CountingSource.singleKey(1L, VALUE_PER_RECORD, maxRecords);

        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "counting-source");

        FlussSinkBuilder<RowData> sinkBuilder =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(tablePath.getDatabaseName())
                        .setTable(tablePath.getTableName())
                        .setSerializationSchema(new RowDataSerializationSchema(false, true));
        if (producerId != null) {
            sinkBuilder.setProducerId(producerId);
        }

        stream.sinkTo(sinkBuilder.build()).name("Fluss Sink");

        String jobName = multiKey ? "Multi-Key Undo Test" : "Undo Test (p=" + parallelism + ")";
        return env.executeAsync(jobName);
    }

    /**
     * Starts a job with FailingCountingSource for checkpoint-based failover testing.
     *
     * <p>This method configures a job that will:
     *
     * <ol>
     *   <li>Emit {@code recordsBeforeFailure} records
     *   <li>Trigger a failure (RuntimeException)
     *   <li>Auto-recover from checkpoint and emit {@code recordsAfterFailure} records
     * </ol>
     *
     * @param tablePath target table
     * @param producerId producer ID for undo recovery, or null to use default (Flink Job ID)
     * @param recordsBeforeFailure records to emit before triggering failure
     * @param recordsAfterFailure records to emit after recovery
     * @param parallelism job parallelism
     * @param multiKey if true, emit to keys 1,2,3 in round-robin
     * @return JobClient for monitoring the job
     */
    private JobClient startFailoverJob(
            TablePath tablePath,
            @Nullable String producerId,
            int recordsBeforeFailure,
            int recordsAfterFailure,
            int parallelism,
            boolean multiKey)
            throws Exception {

        Configuration conf = new Configuration();
        // No savepoint path - this is for checkpoint-based failover

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        env.enableCheckpointing(1000);

        FailingCountingSource source =
                multiKey
                        ? FailingCountingSource.multiKey(
                                failureMarkerDir,
                                VALUE_PER_RECORD,
                                recordsBeforeFailure,
                                recordsAfterFailure)
                        : FailingCountingSource.singleKey(
                                failureMarkerDir,
                                1L,
                                VALUE_PER_RECORD,
                                recordsBeforeFailure,
                                recordsAfterFailure);

        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "failing-counting-source");

        FlussSink<RowData> sink;
        FlussSinkBuilder<RowData> sinkBuilder =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(tablePath.getDatabaseName())
                        .setTable(tablePath.getTableName())
                        .setSerializationSchema(new RowDataSerializationSchema(false, true));
        if (producerId != null) {
            sinkBuilder.setProducerId(producerId);
        }
        sink = sinkBuilder.build();

        stream.sinkTo(sink).name("Fluss Sink");

        String jobName =
                multiKey ? "Multi-Key Failover Test" : "Failover Test (p=" + parallelism + ")";
        return env.executeAsync(jobName);
    }

    /**
     * Starts a job with FailingCountingSource for checkpoint-based failover testing.
     *
     * <p>This method configures a job that will:
     *
     * <ol>
     *   <li>Emit {@code recordsBeforeFailure} records
     *   <li>Trigger a failure (RuntimeException)
     *   <li>Auto-recover from checkpoint and emit {@code recordsAfterFailure} records
     * </ol>
     *
     * @param tablePath target table
     * @param recordsBeforeFailure records to emit before triggering failure
     * @param recordsAfterFailure records to emit after recovery
     * @param parallelism job parallelism
     * @param multiKey if true, emit to keys 1,2,3 in round-robin
     * @return JobClient for monitoring the job
     */
    private TableResult startFailoverSqlJob(
            TablePath tablePath,
            String ddl,
            int recordsBeforeFailure,
            int recordsAfterFailure,
            int parallelism,
            boolean multiKey)
            throws Exception {

        Configuration conf = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        env.enableCheckpointing(1000);

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("USE CATALOG " + CATALOG_NAME);

        FailingCountingSource source =
                multiKey
                        ? FailingCountingSource.multiKey(
                                failureMarkerDir,
                                VALUE_PER_RECORD,
                                recordsBeforeFailure,
                                recordsAfterFailure)
                        : FailingCountingSource.singleKey(
                                failureMarkerDir,
                                1L,
                                VALUE_PER_RECORD,
                                recordsBeforeFailure,
                                recordsAfterFailure);

        DataStreamSource<RowData> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "failing-counting-source",
                        source.getProducedType());

        tEnv.createTemporaryView(
                "source_table",
                stream,
                Schema.newBuilder()
                        .column("key", DataTypes.BIGINT())
                        .column("value", DataTypes.BIGINT())
                        .build());

        tEnv.executeSql(ddl).await();

        return tEnv.executeSql(
                "INSERT INTO `"
                        + tablePath.getDatabaseName()
                        + "`.`"
                        + tablePath.getTableName()
                        + "` SELECT * FROM source_table");
    }

    /**
     * Verifies that producer offsets have been cleaned up from ZK.
     *
     * <p>After a successful checkpoint, the UndoRecoveryOperator deletes producer offsets from ZK.
     * This method retries the check to account for async cleanup timing.
     *
     * @param producerId the producer ID to check
     */
    private void verifyProducerOffsetsCleanedUp(String producerId) {
        retry(
                DEFAULT_TIMEOUT,
                () -> {
                    ProducerOffsetsResult result = admin.getProducerOffsets(producerId).get();
                    assertThat(result)
                            .as(
                                    "Producer offsets for '"
                                            + producerId
                                            + "' should be cleaned up from ZK")
                            .isNull();
                });
        LOG.info("Verified producer offsets cleaned up for {}", producerId);
    }

    @Nullable
    private Long lookupSum(TablePath tablePath, Long key) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(key)).get().getSingletonRow();
            return result == null ? null : result.getLong(1);
        }
    }

    private Long[] lookupSumsForKeys(TablePath tablePath, Long... keys) throws Exception {
        Long[] sums = new Long[keys.length];
        for (int i = 0; i < keys.length; i++) {
            sums[i] = lookupSum(tablePath, keys[i]);
        }
        return sums;
    }

    protected StreamTableEnvironment initTableEnvironment(
            @Nullable String savepointPath, boolean enableCheckpointing) {
        Configuration conf = new Configuration();
        if (savepointPath != null) {
            conf.setString("execution.savepoint.path", savepointPath);
        }

        StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        execEnv.setParallelism(1);
        if (enableCheckpointing) {
            execEnv.enableCheckpointing(1000);
        }

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("USE CATALOG " + CATALOG_NAME);

        return tEnv;
    }

    protected static Configuration getFileBasedCheckpointsConfig() {
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

        // Restart strategy for automatic recovery from checkpoints
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));

        return config;
    }

    protected void waitForJobTermination(JobClient jobClient, Duration timeout) {
        waitUntil(
                () -> {
                    try {
                        return jobClient.getJobStatus().get().isTerminalState();
                    } catch (Exception e) {
                        return true;
                    }
                },
                timeout,
                "Timeout waiting for job termination");
    }

    protected void waitForCheckpoint(JobID jobId) {
        String jobIdStr = jobId.toHexString();
        waitUntil(
                () -> {
                    File jobCheckpointDir = new File(checkpointDir, jobIdStr);
                    if (!jobCheckpointDir.exists()) {
                        return false;
                    }
                    File[] checkpoints =
                            jobCheckpointDir.listFiles(
                                    f -> f.isDirectory() && f.getName().startsWith("chk-"));
                    return checkpoints != null && checkpoints.length > 0;
                },
                DEFAULT_TIMEOUT,
                "Timeout waiting for checkpoint for job " + jobIdStr);
    }
}
