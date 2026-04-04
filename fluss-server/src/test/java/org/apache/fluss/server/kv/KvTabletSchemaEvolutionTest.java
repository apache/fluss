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
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.LogRecordsAssert.assertThatLogRecords;

/** Tests for {@link KvTablet} partial update/delete after schema evolution (ADD COLUMN). */
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
    void testPartialUpdateAfterAddColumn() throws Exception {
        KvRecordTestUtils.KvRecordFactory recordFactoryV0 =
                KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE_V0);
        KvRecordTestUtils.KvRecordBatchFactory batchFactoryV0 =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID_V0);

        // Insert {a=1, b="b_val", c="c_val"} with schema v0.
        KvRecordBatch insertBatch =
                batchFactoryV0.ofRecords(
                        recordFactoryV0.ofRecord(
                                "k1".getBytes(), new Object[] {1, "b_val", "c_val"}));
        kvTablet.putAsLeader(insertBatch, null);

        // Evolve schema to v1 (ADD COLUMN d).
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(SCHEMA_V1, SCHEMA_ID_V1));

        long beforePartialUpdate = logTablet.localLogEndOffset();

        // Partial update targeting columns [a, b] with old schemaId.
        int[] targetColumns = {0, 1};
        KvRecordBatch partialBatch =
                batchFactoryV0.ofRecords(
                        recordFactoryV0.ofRecord("k1".getBytes(), new Object[] {1, "new_b", null}));
        kvTablet.putAsLeader(partialBatch, targetColumns);

        // Expect {a=1, b="new_b", c="c_val", d=null}.
        LogRecords actualLogRecords = readLogRecords(beforePartialUpdate);
        MemoryLogRecords expectedLogs =
                logRecords(
                        ROW_TYPE_V1,
                        SCHEMA_ID_V1,
                        beforePartialUpdate,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, "b_val", "c_val", null},
                                new Object[] {1, "new_b", "c_val", null}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(ROW_TYPE_V1)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testPartialDeleteAfterAddColumn() throws Exception {
        KvRecordTestUtils.KvRecordFactory recordFactoryV0 =
                KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE_V0);
        KvRecordTestUtils.KvRecordBatchFactory batchFactoryV0 =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID_V0);

        // Insert {a=1, b="b_val", c="c_val"} with schema v0.
        KvRecordBatch insertBatch =
                batchFactoryV0.ofRecords(
                        recordFactoryV0.ofRecord(
                                "k1".getBytes(), new Object[] {1, "b_val", "c_val"}));
        kvTablet.putAsLeader(insertBatch, null);

        // Evolve schema to v1 (ADD COLUMN d).
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(SCHEMA_V1, SCHEMA_ID_V1));

        long beforePartialDelete = logTablet.localLogEndOffset();

        // Partial delete targeting columns [a, b] with old schemaId.
        int[] targetColumns = {0, 1};
        KvRecordBatch deleteBatch =
                batchFactoryV0.ofRecords(recordFactoryV0.ofRecord("k1".getBytes(), null));
        kvTablet.putAsLeader(deleteBatch, targetColumns);

        // b (target) is nulled, c (non-target) preserved, d (new column) is null.
        // Since c is non-null, this is a partial delete (UPDATE, not full DELETE).
        LogRecords actualLogRecords = readLogRecords(beforePartialDelete);
        MemoryLogRecords expectedLogs =
                logRecords(
                        ROW_TYPE_V1,
                        SCHEMA_ID_V1,
                        beforePartialDelete,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, "b_val", "c_val", null},
                                new Object[] {1, null, "c_val", null}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(ROW_TYPE_V1)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

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
