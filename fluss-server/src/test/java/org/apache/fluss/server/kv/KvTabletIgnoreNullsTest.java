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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.DeletionDisabledException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.memory.TestingMemorySegmentPool;
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
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.protocol.MergeMode;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.LogRecordsAssert.assertThatLogRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for null-preserving upserts, materialized changelogs, and explicit overwrites. */
class KvTabletIgnoreNullsTest {

    private static final short SCHEMA_ID = 1;

    private final Configuration conf = new Configuration();
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private TestingSchemaGetter schemaGetter;
    private LogTablet logTablet;
    private KvTablet kvTablet;

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("value", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final RowType ROW_TYPE = SCHEMA.getRowType();

    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE);

    private void initLogTabletAndKvTablet(Map<String, String> config) throws Exception {

        TablePath tablePath = TablePath.of("testDb", "test_ignore_nulls");
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(SCHEMA, SCHEMA_ID));

        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir,
                        physicalTablePath.getDatabaseName(),
                        0L,
                        physicalTablePath.getTableName());
        logTablet =
                LogTablet.create(
                        tempLogDir,
                        physicalTablePath,
                        logTabletDir,
                        conf,
                        new AtomicBoolean(
                                conf.get(ConfigOptions.LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED)),
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

    @ParameterizedTest
    @ValueSource(strings = {"FULL", "WAL"})
    void testRequestScopedNullSkippingAndDefaultAssignments(String image) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("table.changelog.image", image);
        initLogTabletAndKvTablet(config);
        KvRecord first = kvRecordFactory.ofRecord("k1", new Object[] {1, "value"});
        KvRecord nullValue = kvRecordFactory.ofRecord("k1", new Object[] {1, null});
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(first, nullValue), null, MergeMode.IGNORE_NULLS);
        assertThat(
                        new ValueDecoder(schemaGetter, KvFormat.COMPACTED)
                                .decodeValue(
                                        kvTablet.multiGetFromBufferOrKv(
                                                        Collections.singletonList("k1".getBytes()))
                                                .get(0)
                                                .toByteArray())
                                .row
                                .getString(1)
                                .toString())
                .isEqualTo("value");
        if (image.equals("FULL")) {
            checkEqual(
                    readLogRecords(0),
                    Collections.singletonList(
                            logRecords(
                                    0L,
                                    Arrays.asList(
                                            ChangeType.INSERT,
                                            ChangeType.UPDATE_BEFORE,
                                            ChangeType.UPDATE_AFTER),
                                    Arrays.asList(
                                            new Object[] {1, "value"},
                                            new Object[] {1, "value"},
                                            new Object[] {1, "value"}))));
        } else {
            checkEqual(
                    readLogRecords(0),
                    Collections.singletonList(
                            logRecords(
                                    0L,
                                    Arrays.asList(ChangeType.INSERT, ChangeType.UPDATE_AFTER),
                                    Arrays.asList(
                                            new Object[] {1, "value"},
                                            new Object[] {1, "value"}))));
        }
        long offset = logTablet.localLogEndOffset();
        kvTablet.putAsLeader(kvRecordBatchFactory.ofRecords(nullValue), null, MergeMode.DEFAULT);
        assertThat(
                        new ValueDecoder(schemaGetter, KvFormat.COMPACTED)
                                .decodeValue(
                                        kvTablet.multiGetFromBufferOrKv(
                                                        Collections.singletonList("k1".getBytes()))
                                                .get(0)
                                                .toByteArray())
                                .row
                                .isNullAt(1))
                .isTrue();
        List<ChangeType> changes =
                image.equals("FULL")
                        ? Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER)
                        : Collections.singletonList(ChangeType.UPDATE_AFTER);
        List<Object[]> rows =
                image.equals("FULL")
                        ? Arrays.asList(new Object[] {1, "value"}, new Object[] {1, null})
                        : Collections.singletonList(new Object[] {1, null});
        checkEqual(
                readLogRecords(offset),
                Collections.singletonList(logRecords(offset, changes, rows)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ignore", "disable"})
    void testIgnoreNullsRespectsDeleteBehavior(String behavior) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("table.delete.behavior", behavior);
        initLogTabletAndKvTablet(config);
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "value"})),
                null);
        KvRecordBatch deletion =
                kvRecordBatchFactory.ofRecords(kvRecordFactory.ofRecord("k1", null));
        if (behavior.equals("disable")) {
            assertThatThrownBy(() -> kvTablet.putAsLeader(deletion, null, MergeMode.IGNORE_NULLS))
                    .isInstanceOf(DeletionDisabledException.class);
        } else {
            kvTablet.putAsLeader(deletion, null, MergeMode.IGNORE_NULLS);
        }
        assertThat(
                        new ValueDecoder(schemaGetter, KvFormat.COMPACTED)
                                .decodeValue(
                                        kvTablet.multiGetFromBufferOrKv(
                                                        Collections.singletonList("k1".getBytes()))
                                                .get(0)
                                                .toByteArray())
                                .row
                                .getString(1)
                                .toString())
                .isEqualTo("value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"first_row", "versioned", "aggregation"})
    void testRejectIgnoreNullsForOtherEngines(String engine) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("table.merge-engine", engine);
        if (engine.equals("versioned")) {
            config.put("table.merge-engine.versioned.ver-column", "id");
        }
        initLogTabletAndKvTablet(config);
        assertThatThrownBy(
                        () ->
                                kvTablet.putAsLeader(
                                        kvRecordBatchFactory.ofRecords(
                                                kvRecordFactory.ofRecord(
                                                        "k1", new Object[] {1, "value"})),
                                        null,
                                        MergeMode.IGNORE_NULLS))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("only supported for the default merge engine");
    }

    private void checkEqual(LogRecords actual, List<MemoryLogRecords> expected) {
        assertThatLogRecords(actual)
                .withSchema(ROW_TYPE)
                .withSchemaGetter(schemaGetter)
                .assertCheckSum(true)
                .isEqualTo(expected.get(0));
    }

    // ==================== Helper Methods ====================

    private LogRecords readLogRecords(long startOffset) throws Exception {
        return logTablet
                .read(startOffset, Integer.MAX_VALUE, FetchIsolation.LOG_END, false, null, null)
                .getRecords();
    }

    private MemoryLogRecords logRecords(
            long baseOffset, List<ChangeType> changeTypes, List<Object[]> rows) throws Exception {
        return createBasicMemoryLogRecords(
                ROW_TYPE,
                SCHEMA_ID,
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
