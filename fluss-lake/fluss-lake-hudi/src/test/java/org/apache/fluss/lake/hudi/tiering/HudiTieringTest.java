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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.hudi.HudiLakeCatalog;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadata;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.watermark.WatermarkExtractor;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.compact.FlinkCompactionConfig;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for tiering Fluss records to Hudi via {@link HudiLakeTieringFactory}. */
class HudiTieringTest {

    private static final String HUDI_CONF_PREFIX = "hudi.";
    private static final String DATA_INSTANT = "20260624000200000";
    private static final String COMPACTION_INSTANT = "20260624000300000";

    @TempDir private File tempWarehouseDir;

    private Configuration hudiConfig;
    private HudiLakeCatalog hudiLakeCatalog;
    private HudiLakeTieringFactory hudiLakeTieringFactory;

    @BeforeEach
    void beforeEach() {
        hudiConfig = new Configuration();
        hudiConfig.setString("catalog.path", tempWarehouseDir.toURI().toString());
        hudiConfig.setString("mode", "dfs");
        hudiLakeCatalog = new HudiLakeCatalog(hudiConfig);
        hudiLakeTieringFactory = new HudiLakeTieringFactory(hudiConfig);
    }

    @AfterEach
    void afterEach() {
        if (hudiLakeCatalog != null) {
            hudiLakeCatalog.close();
        }
    }

    @Test
    void testWriteAndCommitLogTable() throws Exception {
        TablePath tablePath = TablePath.of("hudi", "test_write_and_commit_log_table");
        TableDescriptor tableDescriptor = createLogTableDescriptor();
        hudiLakeCatalog.createTable(
                tablePath, tableDescriptor, new TestingLakeCatalogContext(tableDescriptor));
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 0, tableDescriptor, null, 1L, 1L);

        HudiWriteResult writeResult;
        try (LakeWriter<HudiWriteResult> writer =
                hudiLakeTieringFactory.createLakeWriter(
                        new TestingWriterInitContext(
                                tablePath, new TableBucket(1L, 0), tableInfo, 0, 0L))) {
            writer.write(logRecord(0L, 1, "v1"));
            writer.write(logRecord(1L, 2, "v2"));
            writeResult = writer.complete();
        }

        long committedSnapshotId;
        try (LakeCommitter<HudiWriteResult, HudiCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(committer.getMissingLakeSnapshot(null)).isNull();

            HudiCommittable committable =
                    committer.toCommittable(Collections.singletonList(writeResult));
            committedSnapshotId =
                    committer
                            .commit(
                                    committable,
                                    Collections.singletonMap(
                                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                            "fluss-offset-file"))
                            .getCommittedSnapshotId();
        }

        try (LakeCommitter<HudiWriteResult, HudiCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot missingLakeSnapshot =
                    committer.getMissingLakeSnapshot(committedSnapshotId - 1);

            assertThat(missingLakeSnapshot).isNotNull();
            assertThat(missingLakeSnapshot.getLakeSnapshotId()).isEqualTo(committedSnapshotId);
            assertThat(missingLakeSnapshot.getSnapshotProperties())
                    .containsEntry("commit-user", FLUSS_LAKE_TIERING_COMMIT_USER)
                    .containsEntry(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "fluss-offset-file");
            assertThat(committer.getMissingLakeSnapshot(committedSnapshotId)).isNull();
        }
    }

    @Test
    void testRejectMissingWriteStatsOnCommit() throws Exception {
        TablePath tablePath = TablePath.of("hudi", "test_reject_missing_write_stats_on_commit");
        TableDescriptor tableDescriptor = createLogTableDescriptor();
        hudiLakeCatalog.createTable(
                tablePath, tableDescriptor, new TestingLakeCatalogContext(tableDescriptor));
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 0, tableDescriptor, null, 1L, 1L);

        try (LakeCommitter<HudiWriteResult, HudiCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            HudiCommittable committable =
                    new HudiCommittable(
                            Collections.emptyMap(),
                            Collections.singletonMap(
                                    "20260623000100000",
                                    new HudiWriteStats(
                                            Collections.singletonList(writeStat()), 0L)));

            assertThatThrownBy(() -> committer.commit(committable, Collections.emptyMap()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Hudi write stats must contain exactly one instant");
        }
    }

    @Test
    void testCommitReturnsDataInstantAndSchedulesCompaction() throws Exception {
        TestingCommitterContext context = createTestingCommitterContext();
        HudiCommittable committable = committableWithCompaction();
        whenDataCommitSucceeds(context);
        when(context.compactionService.commitCompaction(
                        eq(committable.getCompactionWriteStats()), anyMap()))
                .thenReturn(COMPACTION_INSTANT);

        LakeCommitResult commitResult =
                context.committer.commit(committable, Collections.emptyMap());

        assertThat(commitResult.getCommittedSnapshotId()).isEqualTo(Long.parseLong(DATA_INSTANT));
        InOrder inOrder =
                inOrder(context.writeClient, context.ckpMetadata, context.compactionService);
        inOrder.verify(context.writeClient)
                .commitStats(eq(DATA_INSTANT), anyList(), any(Option.class), eq("commit"));
        inOrder.verify(context.ckpMetadata).commitInstant(DATA_INSTANT);
        inOrder.verify(context.compactionService)
                .commitCompaction(eq(committable.getCompactionWriteStats()), anyMap());
        inOrder.verify(context.compactionService).scheduleCompaction();
        inOrder.verify(context.compactionService).markSelectedCompactionsInflight();
    }

    @Test
    void testCommitFailsWhenCompactionCommitFails() throws Exception {
        TestingCommitterContext context = createTestingCommitterContext();
        HudiCommittable committable = committableWithCompaction();
        whenDataCommitSucceeds(context);
        doThrow(new IOException("compaction failed"))
                .when(context.compactionService)
                .commitCompaction(eq(committable.getCompactionWriteStats()), anyMap());

        assertThatThrownBy(() -> context.committer.commit(committable, Collections.emptyMap()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("compaction failed");
        verify(context.compactionService, never()).scheduleCompaction();
        verify(context.compactionService, never()).markSelectedCompactionsInflight();
    }

    @Test
    void testScheduleCompactionFailureDoesNotFailCommit() throws Exception {
        TestingCommitterContext context = createTestingCommitterContext();
        HudiCommittable committable = committableWithCompaction();
        whenDataCommitSucceeds(context);
        when(context.compactionService.commitCompaction(
                        eq(committable.getCompactionWriteStats()), anyMap()))
                .thenReturn(COMPACTION_INSTANT);
        doThrow(new IOException("schedule failed"))
                .when(context.compactionService)
                .scheduleCompaction();

        LakeCommitResult commitResult =
                context.committer.commit(committable, Collections.emptyMap());

        assertThat(commitResult.getCommittedSnapshotId()).isEqualTo(Long.parseLong(DATA_INSTANT));
        verify(context.compactionService, never()).markSelectedCompactionsInflight();
    }

    @Test
    void testAbortRollsBackCompactionBeforeDataInstant() throws Exception {
        TestingCommitterContext context = createTestingCommitterContext();
        when(context.writeClient.getHoodieTable())
                .thenThrow(new RuntimeException("rollback failed"));
        when(context.writeClient.rollback(DATA_INSTANT)).thenReturn(true);

        assertThatThrownBy(() -> context.committer.abort(committableWithCompaction()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(COMPACTION_INSTANT);
        InOrder inOrder = inOrder(context.metaClient, context.writeClient, context.ckpMetadata);
        inOrder.verify(context.metaClient).reloadActiveTimeline();
        inOrder.verify(context.writeClient).getHoodieTable();
        inOrder.verify(context.writeClient).rollback(DATA_INSTANT);
        inOrder.verify(context.ckpMetadata).abortInstant(DATA_INSTANT);
    }

    @Test
    void testFlinkCompactionConfigMapping() {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.set(FlinkOptions.PATH, "/tmp/hudi/table");
        config.set(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, FlinkCompactionConfig.NUM_OR_TIME);
        config.set(FlinkOptions.ARCHIVE_MAX_COMMITS, 30);
        config.set(FlinkOptions.ARCHIVE_MIN_COMMITS, 20);
        config.set(FlinkOptions.CLEAN_POLICY, "KEEP_LATEST_COMMITS");
        config.set(FlinkOptions.CLEAN_RETAIN_COMMITS, 10);
        config.set(FlinkOptions.CLEAN_RETAIN_HOURS, 24);
        config.set(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS, 5);
        config.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 2);
        config.set(FlinkOptions.COMPACTION_DELTA_SECONDS, 120);
        config.set(FlinkOptions.COMPACTION_MAX_MEMORY, 256);
        config.set(FlinkOptions.COMPACTION_TARGET_IO, 1024L);
        config.set(FlinkOptions.COMPACTION_TASKS, 3);
        config.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
        config.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, true);

        FlinkCompactionConfig compactionConfig =
                HudiCompactionService.toFlinkCompactionConfig(config);

        assertThat(compactionConfig.path).isEqualTo("/tmp/hudi/table");
        assertThat(compactionConfig.compactionTriggerStrategy)
                .isEqualTo(FlinkCompactionConfig.NUM_OR_TIME);
        assertThat(compactionConfig.archiveMaxCommits).isEqualTo(30);
        assertThat(compactionConfig.archiveMinCommits).isEqualTo(20);
        assertThat(compactionConfig.cleanPolicy).isEqualTo("KEEP_LATEST_COMMITS");
        assertThat(compactionConfig.cleanRetainCommits).isEqualTo(10);
        assertThat(compactionConfig.cleanRetainHours).isEqualTo(24);
        assertThat(compactionConfig.cleanRetainFileVersions).isEqualTo(5);
        assertThat(compactionConfig.compactionDeltaCommits).isEqualTo(2);
        assertThat(compactionConfig.compactionDeltaSeconds).isEqualTo(120);
        assertThat(compactionConfig.compactionMaxMemory).isEqualTo(256);
        assertThat(compactionConfig.compactionTargetIo).isEqualTo(1024L);
        assertThat(compactionConfig.compactionTasks).isEqualTo(3);
        assertThat(compactionConfig.cleanAsyncEnable).isTrue();
        assertThat(compactionConfig.schedule).isTrue();
        assertThat(compactionConfig.compactionPlanSelectStrategy)
                .isEqualTo(CompactionPlanStrategy.NUM_INSTANTS);
        assertThat(compactionConfig.maxNumCompactionPlans).isEqualTo(1);
    }

    private LakeCommitter<HudiWriteResult, HudiCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        return hudiLakeTieringFactory.createLakeCommitter(
                new TestingCommitterInitContext(tablePath, tableInfo));
    }

    private static TestingCommitterContext createTestingCommitterContext() {
        HudiWriteTableInfo hudiTableInfo = mock(HudiWriteTableInfo.class);
        HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
        HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
        CkpMetadata ckpMetadata = mock(CkpMetadata.class);
        HudiCompactionService compactionService = mock(HudiCompactionService.class);
        org.apache.flink.configuration.Configuration flinkConfig =
                new org.apache.flink.configuration.Configuration();
        flinkConfig.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, true);

        when(hudiTableInfo.getWriteClient()).thenReturn(writeClient);
        when(hudiTableInfo.getMetaClient()).thenReturn(metaClient);
        when(hudiTableInfo.getFlinkConfig()).thenReturn(flinkConfig);
        when(hudiTableInfo.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
        when(hudiTableInfo.getTablePath()).thenReturn(TablePath.of("hudi", "mock_table"));
        when(metaClient.getCommitActionType()).thenReturn("commit");
        return new TestingCommitterContext(
                new HudiLakeCommitter(hudiTableInfo, ckpMetadata, compactionService),
                writeClient,
                metaClient,
                ckpMetadata,
                compactionService);
    }

    private static void whenDataCommitSucceeds(TestingCommitterContext context) {
        when(context.writeClient.commitStats(
                        eq(DATA_INSTANT), anyList(), any(Option.class), eq("commit")))
                .thenReturn(true);
    }

    private static HudiCommittable committableWithCompaction() {
        Map<String, HudiWriteStats> writeStats = new HashMap<>();
        writeStats.put(
                DATA_INSTANT, new HudiWriteStats(Collections.singletonList(writeStat()), 0L));
        Map<String, HudiWriteStats> compactionWriteStats = new HashMap<>();
        compactionWriteStats.put(
                COMPACTION_INSTANT, new HudiWriteStats(Collections.singletonList(writeStat()), 0L));
        return new HudiCommittable(writeStats, compactionWriteStats);
    }

    private static TableDescriptor createLogTableDescriptor() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .build())
                .distributedBy(1, "id")
                .customProperty(HUDI_CONF_PREFIX + FlinkOptions.RECORD_KEY_FIELD.key(), "id")
                .customProperty(HUDI_CONF_PREFIX + "precombine.field", "name")
                .build();
    }

    private static LogRecord logRecord(long offset, int id, String name) {
        GenericRow row = new GenericRow(2);
        row.setField(0, id);
        row.setField(1, BinaryString.fromString(name));
        return new GenericRecord(offset, 1_000L + offset, ChangeType.APPEND_ONLY, row);
    }

    private static HoodieWriteStat writeStat() {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setFileId("file-1");
        writeStat.setPartitionPath("");
        writeStat.setPath("file-1.parquet");
        return writeStat;
    }

    private static class TestingCommitterContext {

        private final HudiLakeCommitter committer;
        private final HoodieFlinkWriteClient writeClient;
        private final HoodieTableMetaClient metaClient;
        private final CkpMetadata ckpMetadata;
        private final HudiCompactionService compactionService;

        private TestingCommitterContext(
                HudiLakeCommitter committer,
                HoodieFlinkWriteClient writeClient,
                HoodieTableMetaClient metaClient,
                CkpMetadata ckpMetadata,
                HudiCompactionService compactionService) {
            this.committer = committer;
            this.writeClient = writeClient;
            this.metaClient = metaClient;
            this.ckpMetadata = ckpMetadata;
            this.compactionService = compactionService;
        }
    }

    private static class TestingWriterInitContext implements WriterInitContext {

        private final TablePath tablePath;
        private final TableBucket tableBucket;
        private final TableInfo tableInfo;
        private final int splitIndex;
        private final long tieringRoundTimestamp;

        private TestingWriterInitContext(
                TablePath tablePath,
                TableBucket tableBucket,
                TableInfo tableInfo,
                int splitIndex,
                long tieringRoundTimestamp) {
            this.tablePath = tablePath;
            this.tableBucket = tableBucket;
            this.tableInfo = tableInfo;
            this.splitIndex = splitIndex;
            this.tieringRoundTimestamp = tieringRoundTimestamp;
        }

        @Override
        public TablePath tablePath() {
            return tablePath;
        }

        @Override
        public TableBucket tableBucket() {
            return tableBucket;
        }

        @Nullable
        @Override
        public String partition() {
            return null;
        }

        @Override
        public TableInfo tableInfo() {
            return tableInfo;
        }

        @Override
        public int splitIndex() {
            return splitIndex;
        }

        @Override
        public long tieringRoundTimestamp() {
            return tieringRoundTimestamp;
        }

        @Nullable
        @Override
        public WatermarkExtractor watermarkExtractor() {
            return null;
        }
    }

    private static class TestingCommitterInitContext implements CommitterInitContext {

        private final TablePath tablePath;
        private final TableInfo tableInfo;

        private TestingCommitterInitContext(TablePath tablePath, TableInfo tableInfo) {
            this.tablePath = tablePath;
            this.tableInfo = tableInfo;
        }

        @Override
        public TablePath tablePath() {
            return tablePath;
        }

        @Override
        public TableInfo tableInfo() {
            return tableInfo;
        }

        @Override
        public Configuration lakeTieringConfig() {
            return new Configuration();
        }

        @Override
        public Configuration flussClientConfig() {
            return new Configuration();
        }
    }
}
