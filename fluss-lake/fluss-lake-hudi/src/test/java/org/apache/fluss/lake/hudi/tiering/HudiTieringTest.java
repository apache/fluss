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
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.hudi.HudiLakeCatalog;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
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

import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.configuration.FlinkOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for tiering Fluss records to Hudi via {@link HudiLakeTieringFactory}. */
class HudiTieringTest {

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
    void testLatestCommittedInstantNeverGoesBackwards() {
        assertThat(HudiLakeCommitter.getLatestCommittedInstant("20260624000200000", null))
                .isEqualTo("20260624000200000");
        assertThat(
                        HudiLakeCommitter.getLatestCommittedInstant(
                                "20260624000200000", "20260624000100000"))
                .isEqualTo("20260624000200000");
        assertThat(
                        HudiLakeCommitter.getLatestCommittedInstant(
                                "20260624000200000", "20260624000300000"))
                .isEqualTo("20260624000300000");
    }

    private LakeCommitter<HudiWriteResult, HudiCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        return hudiLakeTieringFactory.createLakeCommitter(
                new TestingCommitterInitContext(tablePath, tableInfo));
    }

    private static TableDescriptor createLogTableDescriptor() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .build())
                .distributedBy(1, "id")
                .customProperty("hudi." + FlinkOptions.RECORD_KEY_FIELD.key(), "id")
                .customProperty("hudi.precombine.field", "name")
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
