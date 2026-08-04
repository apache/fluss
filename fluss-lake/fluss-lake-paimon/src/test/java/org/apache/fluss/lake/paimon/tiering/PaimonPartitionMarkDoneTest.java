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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.paimon.tiering.PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** The UT for partition mark-done during tiering to Paimon. */
class PaimonPartitionMarkDoneTest {

    private static final String DATABASE = "paimon";
    private static final String IDLE_TIME_KEY = "partition.idle-time-to-done";
    private static final String TIME_INTERVAL_KEY = "partition.time-interval";

    private @TempDir File tempWarehouseDir;
    private PaimonLakeTieringFactory paimonLakeTieringFactory;
    private Catalog paimonCatalog;

    @BeforeEach
    void beforeEach() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toString());
        paimonLakeTieringFactory = new PaimonLakeTieringFactory(configuration);
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(configuration.toMap())));
    }

    @Test
    void testMarkDoneLifecycle() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_lifecycle");
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath, false);

        // first data commit: cold start along with the commit, all time-parsable partitions
        // pending; the illegal partition 'px' (no time can be extracted) is dropped without
        // marking done, same as Paimon
        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01", "2024-01-02", "px");
        MarkDoneState state = getMarkDoneState(tablePath, snapshot1);
        assertThat(state.isInitialized()).isTrue();
        assertThat(state.getPendingPartitions()).containsOnlyKeys("2024-01-01", "2024-01-02");

        // empty round: idle partitions are marked done via a properties-only snapshot which
        // carries over previous offsets
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot maintenanceSnapshot = lakeCommitter.commitMarkDoneMaintenance();
            assertThat(maintenanceSnapshot).isNotNull();
            assertThat(maintenanceSnapshot.getLakeSnapshotId()).isEqualTo(snapshot1 + 1);
            assertThat(maintenanceSnapshot.getSnapshotProperties())
                    .containsEntry(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets");
            assertThat(getSnapshotProperties(tablePath, snapshot1 + 1))
                    .isEqualTo(maintenanceSnapshot.getSnapshotProperties());
        }
        MarkDoneState state2 = getMarkDoneState(tablePath, snapshot1 + 1);
        assertThat(state2.isInitialized()).isTrue();
        assertThat(state2.getPendingPartitions()).isEmpty();
        // the default success-file action wrote _SUCCESS files
        assertThat(successFile(tablePath, "2024-01-01")).exists();
        assertThat(successFile(tablePath, "2024-01-02")).exists();
        assertThat(successFile(tablePath, "px")).doesNotExist();

        // another empty round: state unchanged, no snapshot created
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(lakeCommitter.commitMarkDoneMaintenance()).isNull();
        }

        // late data for 2024-01-01: re-added to pending and marked done again later
        long snapshot3 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        assertThat(getMarkDoneState(tablePath, snapshot3).getPendingPartitions())
                .containsOnlyKeys("2024-01-01");

        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(lakeCommitter.commitMarkDoneMaintenance()).isNotNull();
        }
        assertThat(getMarkDoneState(tablePath, snapshot3 + 1).getPendingPartitions()).isEmpty();
        assertThat(successFile(tablePath, "2024-01-01")).exists();
    }

    @Test
    void testColdStartBackfill() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_cold_start");
        // first tier data without the mark-done option
        createPaimonTable(tablePath, Collections.singletonMap(TIME_INTERVAL_KEY, "1 d"));
        TableInfo tableInfo = tableInfo(tablePath, false);
        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01", "2024-01-02");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);

        // then enable mark-done: the cold start backfills the existing idle partitions
        Thread.sleep(50);
        paimonCatalog.alterTable(
                toPaimon(tablePath),
                Collections.singletonList(SchemaChange.setOption(IDLE_TIME_KEY, "1 ms")),
                false);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(lakeCommitter.commitMarkDoneMaintenance()).isNotNull();
        }
        MarkDoneState state = getMarkDoneState(tablePath, snapshot1 + 1);
        assertThat(state.isInitialized()).isTrue();
        assertThat(state.getPendingPartitions()).isEmpty();
        assertThat(successFile(tablePath, "2024-01-01")).exists();
        assertThat(successFile(tablePath, "2024-01-02")).exists();
    }

    @Test
    void testPartitionEndTimeGuardsFuturePartition() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_partition_end_time");
        createPaimonTable(tablePath, Collections.singletonMap(IDLE_TIME_KEY, "1 ms"));
        // auto-partitioned by day: partition end time is derived from the partition name
        TableInfo tableInfo = tableInfo(tablePath, true);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "20200101", "99991231");
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(lakeCommitter.commitMarkDoneMaintenance()).isNotNull();
        }
        // the ancient partition is done, the future partition is guarded by its end time
        MarkDoneState state = getMarkDoneState(tablePath, snapshot1 + 1);
        assertThat(state.getPendingPartitions()).containsOnlyKeys("99991231");
        assertThat(successFile(tablePath, "20200101")).exists();
        assertThat(successFile(tablePath, "99991231")).doesNotExist();
    }

    @Test
    void testStateJsonSerde() {
        // round trip
        Map<String, Long> pending = new HashMap<>();
        pending.put("20240101", 1234L);
        pending.put("region$2024", -1L);
        MarkDoneState state = new MarkDoneState(true, pending);
        assertThat(MarkDoneStateJsonSerde.fromJson(MarkDoneStateJsonSerde.toJson(state)))
                .isEqualTo(state);

        // missing fields fall back to defaults, unknown fields are ignored
        assertThat(MarkDoneStateJsonSerde.fromJson("{}")).isEqualTo(MarkDoneState.empty());
        state =
                MarkDoneStateJsonSerde.fromJson(
                        "{\"initialized\":true,\"pending\":{\"p1\":100},\"unknown\":\"x\"}");
        assertThat(state.isInitialized()).isTrue();
        assertThat(state.getPendingPartitions()).containsEntry("p1", 100L);
    }

    private static Map<String, String> markDoneOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(IDLE_TIME_KEY, "1 ms");
        options.put(TIME_INTERVAL_KEY, "1 d");
        return options;
    }

    private TableInfo tableInfo(TablePath tablePath, boolean autoPartition) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(
                                org.apache.fluss.metadata.Schema.newBuilder()
                                        .column("c1", org.apache.fluss.types.DataTypes.INT())
                                        .column("c2", org.apache.fluss.types.DataTypes.STRING())
                                        .column("c3", org.apache.fluss.types.DataTypes.STRING())
                                        .build())
                        .partitionedBy("c3")
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true);
        if (autoPartition) {
            builder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.DAY);
        }
        return TableInfo.of(tablePath, 0, 1, builder.build(), DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    /** Writes one record to each given partition and commits, returns the snapshot id. */
    private long writeAndCommit(TablePath tablePath, TableInfo tableInfo, String... partitions)
            throws Exception {
        List<PaimonWriteResult> writeResults = new ArrayList<>();
        long partitionId = 1;
        for (String partition : partitions) {
            try (LakeWriter<PaimonWriteResult> lakeWriter =
                    createLakeWriter(tablePath, partition, partitionId++, tableInfo)) {
                GenericRow row = new GenericRow(3);
                row.setField(0, 1);
                row.setField(1, BinaryString.fromString("v1"));
                row.setField(2, BinaryString.fromString(partition));
                lakeWriter.write(
                        new GenericRecord(
                                0, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row));
                writeResults.add(lakeWriter.complete());
            }
        }
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            PaimonCommittable committable = lakeCommitter.toCommittable(writeResults);
            return lakeCommitter
                    .commit(
                            committable,
                            Collections.singletonMap(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets"))
                    .getCommittedSnapshotId();
        }
    }

    private MarkDoneState getMarkDoneState(TablePath tablePath, long snapshotId) throws Exception {
        Map<String, String> properties = getSnapshotProperties(tablePath, snapshotId);
        assertThat(properties).containsKey(MARK_DONE_STATE_PROPERTY);
        return MarkDoneStateJsonSerde.fromJson(properties.get(MARK_DONE_STATE_PROPERTY));
    }

    private Map<String, String> getSnapshotProperties(TablePath tablePath, long snapshotId)
            throws Exception {
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        return fileStoreTable.snapshotManager().snapshot(snapshotId).properties();
    }

    private File successFile(TablePath tablePath, String partition) {
        return new File(
                tempWarehouseDir,
                String.format(
                        "%s.db/%s/c3=%s/_SUCCESS",
                        tablePath.getDatabaseName(), tablePath.getTableName(), partition));
    }

    private void createPaimonTable(TablePath tablePath, Map<String, String> options)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("c1", org.apache.paimon.types.DataTypes.INT())
                        .column("c2", org.apache.paimon.types.DataTypes.STRING())
                        .column("c3", org.apache.paimon.types.DataTypes.STRING())
                        .partitionKeys("c3")
                        .options(options);
        builder.column(BUCKET_COLUMN_NAME, org.apache.paimon.types.DataTypes.INT());
        builder.column(OFFSET_COLUMN_NAME, org.apache.paimon.types.DataTypes.BIGINT());
        builder.column(
                TIMESTAMP_COLUMN_NAME, org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS());
        builder.option(
                CoreOptions.COMMIT_CALLBACKS.key(),
                PaimonLakeCommitter.PaimonCommitCallback.class.getName());
        paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
        paimonCatalog.createTable(toPaimon(tablePath), builder.build(), true);
    }

    private LakeWriter<PaimonWriteResult> createLakeWriter(
            TablePath tablePath, @Nullable String partition, Long partitionId, TableInfo tableInfo)
            throws IOException {
        return paimonLakeTieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(0, partitionId, 0);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return partition;
                    }

                    @Override
                    public TableInfo tableInfo() {
                        return tableInfo;
                    }
                });
    }

    private LakeCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        return paimonLakeTieringFactory.createLakeCommitter(
                new CommitterInitContext() {
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
                });
    }
}
