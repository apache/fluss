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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidRecordException;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.KvRecordTestUtils.V2RecordEntry;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MutationType;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.LogRecordsAssert.assertThatLogRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link KvTablet} with {@link MergeMode} support.
 *
 * <p>These tests verify that OVERWRITE mode correctly bypasses the merge engine and directly
 * replaces values, which is essential for undo recovery scenarios.
 */
class KvTabletMergeModeTest {

    private static final short SCHEMA_ID = 1;

    private final Configuration conf = new Configuration();
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private TestingSchemaGetter schemaGetter;
    private LogTablet logTablet;
    private KvTablet kvTablet;

    // Schema with aggregation functions for testing
    private static final Schema AGG_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                    .column("max_val", DataTypes.INT(), AggFunctions.MAX())
                    .column("name", DataTypes.STRING(), AggFunctions.LAST_VALUE())
                    .primaryKey("id")
                    .build();

    private static final RowType AGG_ROW_TYPE = AGG_SCHEMA.getRowType();

    // Schema with only retract-safe functions (SUM, LAST_VALUE) for retract tests
    private static final Schema RETRACT_SAFE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                    .column("name", DataTypes.STRING(), AggFunctions.LAST_VALUE())
                    .primaryKey("id")
                    .build();

    private static final RowType RETRACT_SAFE_ROW_TYPE = RETRACT_SAFE_SCHEMA.getRowType();

    private static final Schema SUM_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("total", DataTypes.BIGINT(), AggFunctions.SUM())
                    .primaryKey("id")
                    .build();
    private static final RowType SUM_ROW_TYPE = SUM_SCHEMA.getRowType();
    private static final KvRecordTestUtils.KvRecordFactory SUM_KV_RECORD_FACTORY =
            KvRecordTestUtils.KvRecordFactory.of(SUM_ROW_TYPE);
    private static final KvRecordTestUtils.KvRecordFactory RETRACT_KV_RECORD_FACTORY =
            KvRecordTestUtils.KvRecordFactory.of(RETRACT_SAFE_ROW_TYPE);

    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(AGG_ROW_TYPE);

    @BeforeEach
    void setUp() throws Exception {
        initTablets(AGG_SCHEMA);
    }

    /** Reinitialize tablets with a different schema. Closes existing tablets first. */
    private void initTablets(Schema schema) throws Exception {
        initTablets(schema, Collections.emptyMap());
    }

    /**
     * Reinitialize tablets with a different schema and extra config. Closes existing tablets first.
     */
    private void initTablets(Schema schema, Map<String, String> extraConfig) throws Exception {
        if (kvTablet != null) {
            kvTablet.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }

        Map<String, String> config = new HashMap<>();
        config.put("table.merge-engine", "aggregation");
        config.putAll(extraConfig);

        TablePath tablePath = TablePath.of("testDb", "test_merge_mode");
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));

        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir,
                        physicalTablePath.getDatabaseName(),
                        0L,
                        physicalTablePath.getTableName());
        logTablet =
                LogTablet.create(
                        physicalTablePath,
                        logTabletDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        new FlussScheduler(1),
                        LogFormat.ARROW,
                        1,
                        true,
                        SystemClock.getInstance(),
                        true);

        TableBucket tableBucket = logTablet.getTableBucket();
        TableConfig tableConf = new TableConfig(Configuration.fromMap(config));
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        tablePath,
                        new TableConfig(new Configuration()),
                        new TestingSequenceGeneratorFactory());

        kvTablet =
                KvTablet.create(
                        physicalTablePath,
                        tableBucket,
                        logTablet,
                        tmpKvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED,
                        rowMerger,
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        tableConf.getChangelogImage(),
                        KvManager.getDefaultRateLimiter(),
                        autoIncrementManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (kvTablet != null) {
            kvTablet.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }
    }

    // ==================== DEFAULT Mode Tests ====================

    @Test
    void testDefaultModeAppliesMergeEngine() throws Exception {
        // Insert initial record: id=1, count=10, max_val=100, name="Alice"
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Update with DEFAULT mode: count should be summed, max_val should take max
        // id=1, count=5, max_val=150, name="Bob"
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, 150, "Bob"}));
        kvTablet.putAsLeader(batch2, null, MergeMode.DEFAULT);

        // Verify CDC log shows aggregated values
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"}, // before
                                new Object[] {
                                    1, 15L, 150, "Bob"
                                } // after: count=10+5, max=max(100,150)
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== OVERWRITE Mode Tests ====================

    @Test
    void testOverwriteModeBypassesMergeEngine() throws Exception {
        // Insert initial record with DEFAULT mode
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Update with OVERWRITE mode: values should be directly replaced, not aggregated
        // id=1, count=5, max_val=50, name="Bob"
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 5L, 50, "Bob"}));
        kvTablet.putAsLeader(batch2, null, MergeMode.OVERWRITE);

        // Verify CDC log shows directly replaced values (not aggregated)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"}, // before
                                new Object[] {
                                    1, 5L, 50, "Bob"
                                } // after: directly replaced, NOT aggregated
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);

        // Key assertion: count=5 (not 15), max_val=50 (not 100)
        // This proves OVERWRITE bypassed the merge engine
    }

    @Test
    void testOverwriteModeForUndoRecoveryScenario() throws Exception {
        // Simulate a typical undo recovery scenario:
        // 1. Initial state: id=1, count=100, max_val=500, name="Original"
        // 2. After some operations: id=1, count=150, max_val=600, name="Updated"
        // 3. Undo recovery needs to restore to: id=1, count=100, max_val=500, name="Original"

        // Step 1: Insert initial record
        KvRecordBatch initialBatch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, 500, "Original"}));
        kvTablet.putAsLeader(initialBatch, null, MergeMode.DEFAULT);

        // Step 2: Simulate some aggregation operations
        KvRecordBatch updateBatch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 50L, 600, "Updated"}));
        kvTablet.putAsLeader(updateBatch, null, MergeMode.DEFAULT);

        long beforeUndoOffset = logTablet.localLogEndOffset();

        // Step 3: Undo recovery - restore to original state using OVERWRITE mode
        KvRecordBatch undoBatch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, 500, "Original"}));
        kvTablet.putAsLeader(undoBatch, null, MergeMode.OVERWRITE);

        // Verify the undo operation produced correct CDC log
        LogRecords actualLogRecords = readLogRecords(beforeUndoOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        beforeUndoOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                // Before: the aggregated state (count=150, max_val=600)
                                new Object[] {1, 150L, 600, "Updated"},
                                // After: restored to original (count=100, max_val=500)
                                new Object[] {1, 100L, 500, "Original"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testOverwriteModeWithNewKey() throws Exception {
        // OVERWRITE mode with a new key should behave like INSERT
        KvRecordBatch batch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch, null, MergeMode.OVERWRITE);

        LogRecords actualLogRecords = readLogRecords(0);
        MemoryLogRecords expectedLogs =
                logRecords(
                        0L,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(new Object[] {1, 10L, 100, "Alice"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testOverwriteModeWithDelete() throws Exception {
        // Insert initial record
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Delete with OVERWRITE mode
        KvRecordBatch deleteBatch =
                kvRecordBatchFactory.ofRecords(kvRecordFactory.ofRecord("k1".getBytes(), null));
        kvTablet.putAsLeader(deleteBatch, null, MergeMode.OVERWRITE);

        // Verify DELETE is produced
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Collections.singletonList(ChangeType.DELETE),
                        Collections.singletonList(new Object[] {1, 10L, 100, "Alice"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testMixedMergeModeOperations() throws Exception {
        // Test interleaved DEFAULT and OVERWRITE operations

        // 1. Insert with DEFAULT
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "v1"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        // 2. Update with DEFAULT (should aggregate)
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 5L, 150, "v2"}));
        kvTablet.putAsLeader(batch2, null, MergeMode.DEFAULT);

        long afterDefaultOffset = logTablet.localLogEndOffset();

        // 3. Overwrite with specific value
        KvRecordBatch batch3 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 20L, 80, "v3"}));
        kvTablet.putAsLeader(batch3, null, MergeMode.OVERWRITE);

        long afterOverwriteOffset = logTablet.localLogEndOffset();

        // 4. Continue with DEFAULT (should aggregate from overwritten value)
        KvRecordBatch batch4 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 200, "v4"}));
        kvTablet.putAsLeader(batch4, null, MergeMode.DEFAULT);

        // Verify the final aggregation is based on overwritten value
        LogRecords actualLogRecords = readLogRecords(afterOverwriteOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        afterOverwriteOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                // Before: overwritten value
                                new Object[] {1, 20L, 80, "v3"},
                                // After: aggregated from overwritten value
                                // count=20+10=30, max_val=max(80,200)=200
                                new Object[] {1, 30L, 200, "v4"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testOverwriteModeWithPartialUpdate() throws Exception {
        // Insert initial record
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Partial update with OVERWRITE mode (only update id and count columns)
        int[] targetColumns = new int[] {0, 1}; // id and count
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, null, null}));
        kvTablet.putAsLeader(batch2, targetColumns, MergeMode.OVERWRITE);

        // Verify partial update with OVERWRITE: count should be replaced (not aggregated)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"}, // before
                                new Object[] {
                                    1, 5L, 100, "Alice"
                                } // after: count replaced, others unchanged
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testOverwriteModeWithMultipleKeys() throws Exception {
        // Insert multiple records
        List<KvRecord> initialRecords =
                Arrays.asList(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}),
                        kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, 20L, 200, "Bob"}));
        KvRecordBatch batch1 = kvRecordBatchFactory.ofRecords(initialRecords);
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Overwrite multiple keys in single batch
        List<KvRecord> overwriteRecords =
                Arrays.asList(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, 50, "Alice2"}),
                        kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, 8L, 80, "Bob2"}));
        KvRecordBatch batch2 = kvRecordBatchFactory.ofRecords(overwriteRecords);
        kvTablet.putAsLeader(batch2, null, MergeMode.OVERWRITE);

        // Verify both keys are overwritten (not aggregated)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"},
                                new Object[] {1, 5L, 50, "Alice2"}, // k1 overwritten
                                new Object[] {2, 20L, 200, "Bob"},
                                new Object[] {2, 8L, 80, "Bob2"} // k2 overwritten
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== Per-Record MutationType RETRACT Tests (V2 format) ====================

    @Test
    void testRetractPairedWithUpsertOnExistingKey() throws Exception {
        initTablets(RETRACT_SAFE_SCHEMA);

        // Insert initial state: key=k1, count=100, name="init"
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        RETRACT_KV_RECORD_FACTORY.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, "init"}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Send V2 batch: RETRACT(20, "old") + UPSERT(30, "new") for same key
        // count: 100-20+30=110, name: retract "old" then upsert "new" → "new"
        List<V2RecordEntry> entries =
                Arrays.asList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 20L, "old"})),
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 30L, "new"})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify UB(old) + UA(new) CDC entries
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        RETRACT_SAFE_ROW_TYPE,
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 100L, "init"}, new Object[] {1, 110L, "new"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(RETRACT_SAFE_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractUnpairedOnExistingKey() throws Exception {
        initTablets(RETRACT_SAFE_SCHEMA);

        // Initial state: key=k1, count=100, name="init"
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        RETRACT_KV_RECORD_FACTORY.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, "init"}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Send V2 batch: single RETRACT(20, "old") — no following upsert
        // count: 100-20=80, name: retract "old" → null (last_value retract semantics)
        List<V2RecordEntry> entries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 20L, "old"})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify UB(old) + UA(intermediate) CDC entries
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        RETRACT_SAFE_ROW_TYPE,
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(new Object[] {1, 100L, "init"}, new Object[] {1, 80L, null}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(RETRACT_SAFE_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractOnNonExistentKey() throws Exception {
        initTablets(RETRACT_SAFE_SCHEMA);

        long startOffset = logTablet.localLogEndOffset();

        // Send V2 batch: RETRACT on a key that doesn't exist — should be skipped
        List<V2RecordEntry> entries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 10L, "ghost"})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify no CDC records generated
        LogRecords logRecords = readLogRecords(startOffset);
        int totalRecordCount = 0;
        for (LogRecordBatch logBatch : logRecords.batches()) {
            totalRecordCount += logBatch.getRecordCount();
        }
        assertThat(totalRecordCount)
                .as("No changelog records should be emitted for retract on non-existent key")
                .isEqualTo(0);

        // Verify the key is still absent from the KV store (no spurious write)
        assertThat(kvTablet.getKvPreWriteBuffer().get(Key.of("k1".getBytes()))).isNull();
    }

    @Test
    void testRetractIndependentLastValueProducesNullIntermediate() throws Exception {
        // Schema where the only value column is LAST_VALUE.
        // LAST_VALUE.retract() always returns null for the column value, so an unpaired
        // RETRACT produces an intermediate row with all non-PK fields null.
        // With the zombie row fix, this is detected as a fully-retracted row and
        // treated as a deletion. With DeleteBehavior.ALLOW, it produces a DELETE CDC entry.
        Schema lastValueOnlySchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING(), AggFunctions.LAST_VALUE())
                        .primaryKey("id")
                        .build();
        Map<String, String> allowDelete = new HashMap<>();
        allowDelete.put("table.delete.behavior", "allow");
        initTablets(lastValueOnlySchema, allowDelete);

        RowType lastValueRowType = lastValueOnlySchema.getRowType();
        KvRecordTestUtils.KvRecordFactory lastValueKvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(lastValueRowType);

        // Insert initial state: key=k1, name="hello"
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        lastValueKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, "hello"}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Send V2 batch: single unpaired RETRACT(k1, "hello")
        // LAST_VALUE.retract("hello") → null column value → all non-PK fields null → row removed
        List<V2RecordEntry> entries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                lastValueKvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, "hello"})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify CDC: DELETE(1, "hello") — zombie row is removed
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        lastValueRowType,
                        endOffset,
                        Collections.singletonList(ChangeType.DELETE),
                        Collections.singletonList(new Object[] {1, "hello"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(lastValueRowType)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractRejectedForNonAggregationTable() throws Exception {
        // Create a table with first_row merge engine (non-aggregation)
        Schema defaultSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Map<String, String> firstRowConfig = new HashMap<>();
        firstRowConfig.put("table.merge-engine", "first_row");
        initTablets(defaultSchema, firstRowConfig);

        RowType defaultRowType = defaultSchema.getRowType();
        KvRecordTestUtils.KvRecordFactory defaultKvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(defaultRowType);

        // Send a V2 RETRACT record to a non-aggregation table
        List<V2RecordEntry> entries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                defaultKvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, "old"})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);

        assertThatThrownBy(() -> kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2))
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("RETRACT")
                .hasMessageContaining("aggregation");
    }

    @Test
    void testRetractRejectedForNonRetractableFunction() throws Exception {
        // AGG_SCHEMA has MAX which does not support retract
        // Send a V2 RETRACT record — should be rejected
        KvRecordTestUtils.KvRecordFactory aggKvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(AGG_ROW_TYPE);

        // Insert initial record first
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        aggKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        List<V2RecordEntry> entries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                aggKvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 5L, 50, "old"})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);

        assertThatThrownBy(() -> kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2))
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("RETRACT")
                .hasMessageContaining("retract-safe");
    }

    @Test
    void testMixedUpsertAndRetractInSameBatch() throws Exception {
        initTablets(RETRACT_SAFE_SCHEMA);

        // Insert initial state for k1
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        RETRACT_KV_RECORD_FACTORY.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, "init"}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Mixed batch: UPSERT(k2) + RETRACT(k1) + UPSERT(k1)
        // k2 is new → INSERT
        // k1: retract(20,"old") + upsert(30,"new") → 100-20+30=110
        List<V2RecordEntry> entries =
                Arrays.asList(
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k2".getBytes(), new Object[] {2, 50L, "Bob"})),
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 20L, "old"})),
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 30L, "new"})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify: INSERT(k2) + UB(k1,old) + UA(k1,new)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        RETRACT_SAFE_ROW_TYPE,
                        endOffset,
                        Arrays.asList(
                                ChangeType.INSERT,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {2, 50L, "Bob"},
                                new Object[] {1, 100L, "init"},
                                new Object[] {1, 110L, "new"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(RETRACT_SAFE_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractMultiplePairsSameKeyInOneBatch() throws Exception {
        initTablets(RETRACT_SAFE_SCHEMA);

        // Initial state: key=k1, count=100, name="init"
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        RETRACT_KV_RECORD_FACTORY.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, "init"}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Send 2 retract+upsert pairs for same key in one V2 batch:
        // Pair 1: RETRACT(20, "old1") + UPSERT(30, "new1")
        //   count: 100-20+30=110, name: "new1"
        // Pair 2: RETRACT(10, "old2") + UPSERT(50, "new2")
        //   count: 110-10+50=150, name: "new2"
        List<V2RecordEntry> entries =
                Arrays.asList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 20L, "old1"})),
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 30L, "new1"})),
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 10L, "old2"})),
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 50L, "new2"})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify changelog shows the transitions
        LogRecords actualLogRecords = readLogRecords(endOffset);

        // Pair 1 produces: UB(100,"init") + UA(110,"new1")
        // Pair 2 produces: UB(110,"new1") + UA(150,"new2")
        MemoryLogRecords expectedLogs =
                logRecords(
                        RETRACT_SAFE_ROW_TYPE,
                        endOffset,
                        Arrays.asList(
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 100L, "init"},
                                new Object[] {1, 110L, "new1"},
                                new Object[] {1, 110L, "new1"},
                                new Object[] {1, 150L, "new2"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(RETRACT_SAFE_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractToZeroThenRescue() throws Exception {
        // Schema uses SUM for count. Initial state: key=k1, total=10
        initTablets(SUM_SCHEMA);

        // Initial state: key=k1, total=10
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        SUM_KV_RECORD_FACTORY.ofRecord("k1".getBytes(), new Object[] {1, 10L}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // V2 batch: RETRACT(10) + UPSERT(30)
        // Retract: 10-10=0 (intermediate)
        // Upsert: 0+30=30
        List<V2RecordEntry> entries =
                Arrays.asList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                SUM_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 10L})),
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                SUM_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 30L})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify key is NOT deleted and final value is 30
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        SUM_ROW_TYPE,
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L}, // before
                                new Object[] {1, 30L} // after: rescued from zero
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(SUM_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractNoChangeOptimization() throws Exception {
        // Use a simple SUM schema
        initTablets(SUM_SCHEMA);

        // Initial state: key=k1, total=10
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        SUM_KV_RECORD_FACTORY.ofRecord("k1".getBytes(), new Object[] {1, 10L}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // V2 batch: RETRACT(5) + UPSERT(5) → 10-5+5=10 (no change)
        List<V2RecordEntry> entries =
                Arrays.asList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                SUM_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 5L})),
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                SUM_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 5L})));
        KvRecordBatch retractBatch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT, (short) 2);

        // Verify no changelog is emitted (the newValue.equals(oldValue) optimization).
        LogRecords logRecords = readLogRecords(endOffset);
        int totalRecordCount = 0;
        for (LogRecordBatch logBatch : logRecords.batches()) {
            totalRecordCount += logBatch.getRecordCount();
        }
        assertThat(totalRecordCount)
                .as("No changelog records should be emitted when retract+upsert produces no change")
                .isEqualTo(0);
    }

    @Test
    void testConsecutiveRetractsThenUpsertOptimization() throws Exception {
        // Verify that when a batch contains RETRACT → RETRACT → UPSERT for the same key,
        // the second RETRACT gets its own peek-ahead and merges with the UPSERT,
        // producing fewer CDC entries than fully independent processing.
        initTablets(SUM_SCHEMA);

        // Initial state: key=k1, total=100
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        SUM_KV_RECORD_FACTORY.ofRecord("k1".getBytes(), new Object[] {1, 100L}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // V2 batch: RETRACT(20) → RETRACT(20) → UPSERT(30) for same key
        // Expected processing:
        //   1st RETRACT peeks, sees 2nd RETRACT (not UPSERT) → independent: 100→80
        //     CDC: UB(100) + UA(80)
        //   2nd RETRACT peeks, sees UPSERT → merged: 80-20+30=90
        //     CDC: UB(80) + UA(90)
        // Total: 4 CDC entries (not 6 as would happen without the optimization)
        List<V2RecordEntry> entries =
                Arrays.asList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                SUM_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 20L})),
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                SUM_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 20L})),
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                SUM_KV_RECORD_FACTORY.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 30L})));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify changelog: 4 entries total
        // 1st RETRACT (independent): UB(100) + UA(80)
        // 2nd RETRACT+UPSERT (merged): UB(80) + UA(90)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        SUM_ROW_TYPE,
                        endOffset,
                        Arrays.asList(
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 100L},
                                new Object[] {1, 80L},
                                new Object[] {1, 80L},
                                new Object[] {1, 90L}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(SUM_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== Default MergeMode Tests ====================

    @Test
    void testDefaultMergeModeIsDefault() throws Exception {
        // Insert initial record using default (no mergeMode parameter)
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null); // Using overload without mergeMode

        long endOffset = logTablet.localLogEndOffset();

        // Update using default (should aggregate)
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, 150, "Bob"}));
        kvTablet.putAsLeader(batch2, null); // Using overload without mergeMode

        // Verify aggregation happened (proving default is DEFAULT)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"},
                                new Object[] {
                                    1, 15L, 150, "Bob"
                                } // count=10+5=15, max=max(100,150)=150
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== V2 Format Validation Tests ====================

    @Test
    void testV2UpsertWithNullRowThrows() throws Exception {
        // Build a V2 batch with UPSERT mutation type but null row value
        KvRecordTestUtils.KvRecordFactory aggKvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(AGG_ROW_TYPE);

        List<V2RecordEntry> entries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                aggKvRecordFactory.ofRecord("k1".getBytes(), null)));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);

        assertThatThrownBy(() -> kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2))
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("V2 UPSERT record must carry a non-null row value.");
    }

    @Test
    void testV2RetractWithNullRowThrows() throws Exception {
        initTablets(RETRACT_SAFE_SCHEMA);

        List<V2RecordEntry> entries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                RETRACT_KV_RECORD_FACTORY.ofRecord("k1".getBytes(), null)));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);

        assertThatThrownBy(() -> kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2))
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("RETRACT record must carry a non-null row value.");
    }

    @Test
    void testV2DeleteOnAggregationTable() throws Exception {
        // Verify that V2 DELETE behaves the same as V0 delete on an aggregation table.
        // Insert initial record, then send a V2 batch with MutationType.DELETE.
        // Must use DeleteBehavior.ALLOW so the delete is not silently dropped.
        Map<String, String> allowDelete = new HashMap<>();
        allowDelete.put("table.delete.behavior", "allow");
        initTablets(RETRACT_SAFE_SCHEMA, allowDelete);

        // Insert initial state: key=k1, count=100, name="init"
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        RETRACT_KV_RECORD_FACTORY.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, "init"}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Send V2 batch with DELETE mutation type (null row value)
        List<V2RecordEntry> entries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.DELETE,
                                RETRACT_KV_RECORD_FACTORY.ofRecord("k1".getBytes(), null)));
        KvRecordBatch batch = kvRecordBatchFactory.ofV2Records(entries);
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT, (short) 2);

        // Verify DELETE CDC is produced — same behavior as V0 delete
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        RETRACT_SAFE_ROW_TYPE,
                        endOffset,
                        Collections.singletonList(ChangeType.DELETE),
                        Collections.singletonList(new Object[] {1, 100L, "init"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(RETRACT_SAFE_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== Partial Update + Retract Tests ====================

    @Test
    void testRetractWithPartialUpdateTargetColumns() throws Exception {
        // Schema with two SUM columns to verify partial update targets only one
        Schema twoSumSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("col_a", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("col_b", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();
        initTablets(twoSumSchema);

        RowType twoSumRowType = twoSumSchema.getRowType();
        KvRecordTestUtils.KvRecordFactory twoSumKvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(twoSumRowType);

        // Step 1: Insert initial state: id=1, col_a=100, col_b=200
        KvRecordBatch initBatch =
                kvRecordBatchFactory.ofRecords(
                        twoSumKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, 200L}));
        kvTablet.putAsLeader(initBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Step 2: V2 RETRACT targeting only col_a (targetColumns = {0, 1} = id + col_a)
        // retract col_a=30 → col_a: 100-30=70, col_b unchanged (200)
        int[] targetColumns = new int[] {0, 1}; // id and col_a
        List<V2RecordEntry> retractEntries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.RETRACT,
                                twoSumKvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 30L, null})));
        KvRecordBatch retractBatch = kvRecordBatchFactory.ofV2Records(retractEntries);
        kvTablet.putAsLeader(retractBatch, targetColumns, MergeMode.DEFAULT, (short) 2);

        long afterRetractOffset = logTablet.localLogEndOffset();

        // Verify CDC after retract: UB(100,200) + UA(70,200)
        LogRecords retractCdc = readLogRecords(endOffset);
        MemoryLogRecords expectedRetractLogs =
                logRecords(
                        twoSumRowType,
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(new Object[] {1, 100L, 200L}, new Object[] {1, 70L, 200L}));

        assertThatLogRecords(retractCdc)
                .withSchema(twoSumRowType)
                .assertCheckSum(true)
                .isEqualTo(expectedRetractLogs);

        // Step 3: V2 UPSERT targeting only col_a (upsert col_a=50)
        // col_a: 70+50=120, col_b unchanged (200)
        List<V2RecordEntry> upsertEntries =
                Collections.singletonList(
                        V2RecordEntry.of(
                                MutationType.UPSERT,
                                twoSumKvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, 50L, null})));
        KvRecordBatch upsertBatch = kvRecordBatchFactory.ofV2Records(upsertEntries);
        kvTablet.putAsLeader(upsertBatch, targetColumns, MergeMode.DEFAULT, (short) 2);

        // Verify CDC after upsert: UB(70,200) + UA(120,200)
        LogRecords upsertCdc = readLogRecords(afterRetractOffset);
        MemoryLogRecords expectedUpsertLogs =
                logRecords(
                        twoSumRowType,
                        afterRetractOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(new Object[] {1, 70L, 200L}, new Object[] {1, 120L, 200L}));

        assertThatLogRecords(upsertCdc)
                .withSchema(twoSumRowType)
                .assertCheckSum(true)
                .isEqualTo(expectedUpsertLogs);
    }

    // ==================== Helper Methods ====================

    private LogRecords readLogRecords(long startOffset) throws Exception {
        return logTablet
                .read(startOffset, Integer.MAX_VALUE, FetchIsolation.LOG_END, false, null)
                .getRecords();
    }

    private MemoryLogRecords logRecords(
            RowType rowType, long baseOffset, List<ChangeType> changeTypes, List<Object[]> rows)
            throws Exception {
        return logRecords(rowType, SCHEMA_ID, baseOffset, changeTypes, rows);
    }

    private MemoryLogRecords logRecords(
            RowType rowType,
            short schemaId,
            long baseOffset,
            List<ChangeType> changeTypes,
            List<Object[]> rows)
            throws Exception {
        return createBasicMemoryLogRecords(
                rowType,
                schemaId,
                baseOffset,
                -1L,
                CURRENT_LOG_MAGIC_VALUE,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                changeTypes,
                rows,
                LogFormat.ARROW,
                DEFAULT_COMPRESSION);
    }

    private MemoryLogRecords logRecords(
            long baseOffset, List<ChangeType> changeTypes, List<Object[]> rows) throws Exception {
        return logRecords(AGG_ROW_TYPE, baseOffset, changeTypes, rows);
    }
}
