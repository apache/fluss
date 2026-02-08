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
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
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
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.LogRecordsAssert.assertThatLogRecords;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link KvTablet} schema evolution handling.
 *
 * <p>Verifies that server-side row conversion (via column IDs) correctly handles partial updates
 * when the incoming batch or stored KV rows use an older schema.
 */
class KvTabletSchemaEvolutionTest {

    private static final short SCHEMA_ID_V0 = 0;
    private static final short SCHEMA_ID_V1 = 1;

    // Schema v0: {a INT PK, b STRING, c STRING}
    private static final Schema SCHEMA_V0 =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("c", DataTypes.STRING())
                    .primaryKey("a")
                    .build();

    // Schema v1: ADD COLUMN d STRING (nullable)
    // {a INT PK, b STRING, c STRING, d STRING}
    private static final Schema SCHEMA_V1 =
            Schema.newBuilder().fromSchema(SCHEMA_V0).column("d", DataTypes.STRING()).build();

    private static final RowType ROW_TYPE_V0 = SCHEMA_V0.getRowType();
    private static final RowType ROW_TYPE_V1 = SCHEMA_V1.getRowType();

    private final Configuration conf = new Configuration();

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private TestingSchemaGetter schemaGetter;
    private LogTablet logTablet;
    private KvTablet kvTablet;

    @BeforeEach
    void setUp() throws Exception {
        TablePath tablePath = TablePath.of("testDb", "test_schema_evolution");
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        // Start with schema v0
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(SCHEMA_V0, SCHEMA_ID_V0));

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
        TableConfig tableConf = new TableConfig(new Configuration());
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter, tablePath, tableConf, new TestingSequenceGeneratorFactory());

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

    @Test
    void testConvertTargetColumns_droppedColumnIsFiltered() {
        // Build a target schema that doesn't contain column c (id=2).
        // This simulates a DROP COLUMN scenario.
        Schema schemaWithoutC =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .build();

        // Positions [0, 2] in v0 = columns a(id=0) and c(id=2)
        // Column c(id=2) does not exist in schemaWithoutC (which has ids 0, 1)
        // → c should be silently dropped from the result
        int[] positions = {0, 2};
        int[] result = KvTablet.convertTargetColumns(positions, SCHEMA_V0, schemaWithoutC);
        // Only column a(id=0) survives → mapped to position 0 in schemaWithoutC
        assertThat(result).containsExactly(0);
    }

    @Test
    void testPartialUpdateAfterAddColumn() throws Exception {
        KvRecordTestUtils.KvRecordFactory recordFactoryV0 =
                KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE_V0);
        KvRecordTestUtils.KvRecordBatchFactory batchFactoryV0 =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID_V0);

        // Step 1: Insert full row with schema v0: {a=1, b="b_val", c="c_val"}
        KvRecordBatch insertBatch =
                batchFactoryV0.ofRecords(
                        recordFactoryV0.ofRecord(
                                "k1".getBytes(), new Object[] {1, "b_val", "c_val"}));
        kvTablet.putAsLeader(insertBatch, null);

        // Step 2: Evolve schema to v1 (ADD COLUMN d)
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(SCHEMA_V1, SCHEMA_ID_V1));

        long beforePartialUpdate = logTablet.localLogEndOffset();

        // Step 3: Send partial update with schemaId=0, targeting columns [0, 1] (a, b in v0)
        // Row values: {a=1, b="new_b", c=null} (c is not targeted so its value doesn't matter)
        int[] targetColumns = {0, 1};
        KvRecordBatch partialBatch =
                batchFactoryV0.ofRecords(
                        recordFactoryV0.ofRecord("k1".getBytes(), new Object[] {1, "new_b", null}));
        kvTablet.putAsLeader(partialBatch, targetColumns);

        // Step 4: Verify result: {a=1, b="new_b", c="c_val", d=null}
        // - b updated to "new_b"
        // - c preserved from old row
        // - d is null (new column, not present in old or new data)
        LogRecords actualLogRecords = readLogRecords(beforePartialUpdate);
        MemoryLogRecords expectedLogs =
                logRecords(
                        ROW_TYPE_V1,
                        SCHEMA_ID_V1,
                        beforePartialUpdate,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {
                                    1, "b_val", "c_val", null
                                }, // before (old row aligned)
                                new Object[] {1, "new_b", "c_val", null} // after (b updated)
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(ROW_TYPE_V1)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testDeleteAfterAddColumn() throws Exception {
        KvRecordTestUtils.KvRecordFactory recordFactoryV0 =
                KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE_V0);
        KvRecordTestUtils.KvRecordBatchFactory batchFactoryV0 =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID_V0);

        // Step 1: Insert row with schema v0
        KvRecordBatch insertBatch =
                batchFactoryV0.ofRecords(
                        recordFactoryV0.ofRecord(
                                "k1".getBytes(), new Object[] {1, "b_val", "c_val"}));
        kvTablet.putAsLeader(insertBatch, null);

        // Step 2: Evolve schema
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(SCHEMA_V1, SCHEMA_ID_V1));

        long beforeDelete = logTablet.localLogEndOffset();

        // Step 3: Delete with schemaId=0
        KvRecordBatch deleteBatch =
                batchFactoryV0.ofRecords(recordFactoryV0.ofRecord("k1".getBytes(), null));
        kvTablet.putAsLeader(deleteBatch, null);

        // Step 4: Verify DELETE log uses aligned (v1) row
        LogRecords actualLogRecords = readLogRecords(beforeDelete);
        MemoryLogRecords expectedLogs =
                logRecords(
                        ROW_TYPE_V1,
                        SCHEMA_ID_V1,
                        beforeDelete,
                        Collections.singletonList(ChangeType.DELETE),
                        Collections.singletonList(new Object[] {1, "b_val", "c_val", null}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(ROW_TYPE_V1)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== Helper Methods ====================

    private LogRecords readLogRecords(long startOffset) throws Exception {
        return logTablet
                .read(startOffset, Integer.MAX_VALUE, FetchIsolation.LOG_END, false, null)
                .getRecords();
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
}
