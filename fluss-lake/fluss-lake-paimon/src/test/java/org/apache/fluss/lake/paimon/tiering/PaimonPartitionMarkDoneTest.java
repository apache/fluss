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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.paimon.tiering.markdone.PartitionMarkDoneState;
import org.apache.fluss.lake.paimon.tiering.markdone.PartitionMarkDoneStateJsonSerde;
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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.tiering.markdone.PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The UT for partition mark-done during tiering to Paimon. */
class PaimonPartitionMarkDoneTest {

    private static final String DATABASE = "paimon";
    private static final String IDLE_TIME_KEY = "partition.idle-time-to-done";
    private static final String TIME_INTERVAL_KEY = "partition.time-interval";
    private static final String TIMESTAMP_FORMATTER_KEY = "partition.timestamp-formatter";

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

    @AfterEach
    void afterEach() throws Exception {
        paimonCatalog.close();
    }

    private void setPaimonOptions(TablePath tablePath, Map<String, String> options)
            throws Exception {
        List<SchemaChange> changes = new ArrayList<>();
        options.forEach((key, value) -> changes.add(SchemaChange.setOption(key, value)));
        paimonCatalog.alterTable(toPaimon(tablePath), changes, false);
    }

    @Test
    void testMarkDoneLifecycle() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_lifecycle");
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath);

        // first data commit: cold start along with the commit, all time-parsable partitions
        // pending; the illegal partition 'px' (no time can be extracted) is dropped without
        // marking done, same as Paimon
        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01", "2024-01-02", "px");
        PartitionMarkDoneState state = getMarkDoneState(tablePath, snapshot1);
        assertThat(state.isInitialized()).isTrue();
        assertThat(state.getTrackedPartitionLastUpdateTimes())
                .containsOnlyKeys("2024-01-01", "2024-01-02");

        // empty round: idle partitions are marked done via a properties-only snapshot which
        // carries a freshly prepared offsets file
        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            LakeCommitResult maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-2");
            assertThat(maintenanceSnapshot).isNotNull();
            assertThat(maintenanceSnapshot.getCommittedSnapshotId()).isEqualTo(snapshot1 + 1);
            assertThat(maintenanceSnapshot.committedIsReadable()).isTrue();
            assertThat(getSnapshotProperties(tablePath, snapshot1 + 1))
                    .containsEntry(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets-2");
        }
        PartitionMarkDoneState state2 = getMarkDoneState(tablePath, snapshot1 + 1);
        assertThat(state2.isInitialized()).isTrue();
        assertThat(state2.getTrackedPartitionLastUpdateTimes()).isEmpty();
        // the default success-file action wrote _SUCCESS files
        assertThat(successFile(tablePath, "2024-01-01")).exists();
        assertThat(successFile(tablePath, "2024-01-02")).exists();
        assertThat(successFile(tablePath, "px")).doesNotExist();

        // another empty round: state unchanged, no snapshot created
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-3")).isNull();
        }

        // late data for 2024-01-01: re-added to pending and marked done again later
        long snapshot3 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        assertThat(getMarkDoneState(tablePath, snapshot3).getTrackedPartitionLastUpdateTimes())
                .containsOnlyKeys("2024-01-01");

        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-4")).isNotNull();
        }
        assertThat(getMarkDoneState(tablePath, snapshot3 + 1).getTrackedPartitionLastUpdateTimes())
                .isEmpty();
        assertThat(successFile(tablePath, "2024-01-01")).exists();
    }

    @Test
    void testColdStartBackfill() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_cold_start");
        // First tier data with mark-done disabled.
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo = tableInfo(tablePath);
        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01", "2024-01-02");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);

        // Enabling the lake table options backfills existing idle partitions.
        setPaimonOptions(tablePath, markDoneOptions());
        Thread.sleep(50);
        TableInfo enabledTableInfo = tableInfo(tablePath);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, enabledTableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNotNull();
        }
        PartitionMarkDoneState state = getMarkDoneState(tablePath, snapshot1 + 1);
        assertThat(state.isInitialized()).isTrue();
        assertThat(state.getTrackedPartitionLastUpdateTimes()).isEmpty();
        assertThat(successFile(tablePath, "2024-01-01")).exists();
        assertThat(successFile(tablePath, "2024-01-02")).exists();
    }

    @Test
    void testLakeTableOptionsAreAuthoritative() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_paimon_side_options");
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath);

        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(
                            committer.preparePartitionMarkDone(
                                    committer.toCommittable(Collections.emptyList())))
                    .isTrue();
        }
        long snapshot = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        assertThat(getMarkDoneState(tablePath, snapshot).getTrackedPartitionLastUpdateTimes())
                .containsOnlyKeys("2024-01-01");

        // Stale Fluss custom properties cannot override the lake table.
        TableInfo staleTableInfo =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder()
                                .customProperty("paimon." + IDLE_TIME_KEY, "1 ms")
                                .customProperty("paimon." + TIME_INTERVAL_KEY, "1 d")
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);
        paimonCatalog.alterTable(
                toPaimon(tablePath),
                Collections.singletonList(SchemaChange.removeOption(IDLE_TIME_KEY)),
                false);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> committer =
                createLakeCommitter(tablePath, staleTableInfo)) {
            assertThat(
                            committer.preparePartitionMarkDone(
                                    committer.toCommittable(Collections.emptyList())))
                    .isFalse();
        }
    }

    @Test
    void testAutoPartitionWithoutPaimonIntervalDisablesMarkDone() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_auto_partition_no_interval");
        createPaimonTable(tablePath, Collections.singletonMap(IDLE_TIME_KEY, "1 ms"));
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder()
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);

        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(
                            committer.preparePartitionMarkDone(
                                    committer.toCommittable(Collections.emptyList())))
                    .isFalse();
        }
        long snapshot = writeAndCommit(tablePath, tableInfo, "20240101");
        assertThat(getSnapshotProperties(tablePath, snapshot))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);
    }

    @Test
    void testPartitionExpirationDoesNotBreakMarkDoneState() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_partition_expiration");
        // partition expiration configured on the Paimon table: Paimon appends an OVERWRITE
        // snapshot with the same commit user and null properties right after our commit
        Map<String, String> paimonOptions = markDoneOptions();
        paimonOptions.put("partition.expiration-time", "1 d");
        paimonOptions.put("partition.expiration-check-interval", "10 min");
        paimonOptions.put("partition.timestamp-formatter", "yyyy-MM-dd");
        createPaimonTable(tablePath, paimonOptions);
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder()
                                .customProperty("paimon." + IDLE_TIME_KEY, "1 ms")
                                .customProperty("paimon." + TIME_INTERVAL_KEY, "1 d")
                                // enable snapshot auto-expiration so the committer runs the
                                // partition expiration check on every commit
                                .property(ConfigOptions.TABLE_DATALAKE_AUTO_EXPIRE_SNAPSHOT, true)
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);

        // '2020-01-01' is long expired for partition expiration, '9999-12-31' is alive
        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2020-01-01", "9999-12-31");

        // the returned snapshot is the latest physical one, i.e. the expiration OVERWRITE
        // snapshot appended within the same commit call, so Fluss reads don't resurrect the
        // expired partitions; the offsets & state live on the data snapshot before it
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        assertThat(fileStoreTable.snapshotManager().latestSnapshotId()).isEqualTo(snapshot1);
        assertThat(getSnapshotProperties(tablePath, snapshot1)).isNull();

        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            // missing recovery pairs the latest snapshot id with the properties of the round
            CommittedLakeSnapshot missing = lakeCommitter.getMissingLakeSnapshot(null);
            assertThat(missing).isNotNull();
            assertThat(missing.getLakeSnapshotId()).isEqualTo(snapshot1);
            assertThat(missing.getSnapshotProperties())
                    .containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)
                    .containsKey(MARK_DONE_STATE_PROPERTY);
            assertThat(lakeCommitter.getMissingLakeSnapshot(snapshot1)).isNull();

            // the mark-done state must still be found instead of restarting from cold start
            LakeCommitResult maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-2");
            assertThat(maintenanceSnapshot).isNotNull();
            PartitionMarkDoneState state =
                    getMarkDoneState(tablePath, maintenanceSnapshot.getCommittedSnapshotId());
            assertThat(state.isInitialized()).isTrue();
            assertThat(state.getTrackedPartitionLastUpdateTimes()).containsOnlyKeys("9999-12-31");
            assertThat(successFile(tablePath, "2020-01-01")).exists();
        }
    }

    @Test
    void testMaintenanceTailLookback() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_maintenance_tail");
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01", "9999-12-31");

        // A maintenance tail may have holes when snapshots expire during lookup.
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        for (int i = 0; i < 3; i++) {
            try (TableCommitImpl truncateCommit =
                    fileStoreTable.newCommit(FLUSS_LAKE_TIERING_COMMIT_USER)) {
                truncateCommit.truncatePartitions(
                        Collections.singletonList(Collections.singletonMap("c3", "2024-01-01")));
            }
        }

        fileStoreTable.snapshotManager().deleteSnapshot(snapshot1 + 1);
        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            // the properties of the round are found behind the full-length tail
            CommittedLakeSnapshot missing = lakeCommitter.getMissingLakeSnapshot(snapshot1);
            assertThat(missing).isNotNull();
            assertThat(missing.getLakeSnapshotId()).isEqualTo(snapshot1 + 3);
            assertThat(missing.getSnapshotProperties())
                    .containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)
                    .containsKey(MARK_DONE_STATE_PROPERTY);

            // so is the mark-done state
            LakeCommitResult maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-2");
            assertThat(maintenanceSnapshot).isNotNull();
            PartitionMarkDoneState state =
                    getMarkDoneState(tablePath, maintenanceSnapshot.getCommittedSnapshotId());
            assertThat(state.isInitialized()).isTrue();
            assertThat(state.getTrackedPartitionLastUpdateTimes()).containsOnlyKeys("9999-12-31");
        }
    }

    @Test
    void testZeroFileDonePartitionStillMarkedDone() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_zero_file_partition");
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01");

        // empty the partition (like a PK partition whose data was fully deleted and
        // compacted): it disappears from the partition entries though it legitimately existed
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        try (TableCommitImpl truncateCommit = fileStoreTable.newCommit("test-truncate")) {
            truncateCommit.truncatePartitions(
                    Collections.singletonList(Collections.singletonMap("c3", "2024-01-01")));
        }
        assertThat(fileStoreTable.newSnapshotReader().partitionEntries()).isEmpty();

        // the partition is still marked done although it holds zero files
        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            LakeCommitResult maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-2");
            assertThat(maintenanceSnapshot).isNotNull();
            assertThat(
                            getMarkDoneState(
                                            tablePath, maintenanceSnapshot.getCommittedSnapshotId())
                                    .getTrackedPartitionLastUpdateTimes())
                    .isEmpty();
        }
        assertThat(successFile(tablePath, "2024-01-01")).exists();
    }

    @Test
    void testLegacySnapshotWithoutPropertiesFailsFast() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_legacy_snapshot");
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath);

        writeAndCommit(tablePath, tableInfo, "2024-01-01");

        // simulate a legacy (v0.7) Fluss data commit: same commit user, APPEND kind, no
        // properties at all
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        try (TableCommitImpl legacyCommit =
                fileStoreTable.newCommit(FLUSS_LAKE_TIERING_COMMIT_USER)) {
            legacyCommit.ignoreEmptyCommit(false);
            legacyCommit.commit(new ManifestCommittable(COMMIT_IDENTIFIER));
        }

        // the legacy snapshot can't be registered to Fluss (no offsets recorded), the
        // missing-snapshot check must fail fast instead of silently re-tiering the data
        // that the legacy snapshot already holds
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThatThrownBy(() -> lakeCommitter.getMissingLakeSnapshot(null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to load committed lake snapshot properties");
        }
    }

    @Test
    void testDisabledByJobLevelSwitch() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_job_level_switch");
        createPaimonTable(tablePath, markDoneOptions());
        // The lake table opts in, but the job-level switch stays off.
        TableInfo tableInfo = tableInfo(tablePath);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, new Configuration(), "2024-01-01");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);
        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo, new Configuration())) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNull();
        }
        assertThat(successFile(tablePath, "2024-01-01")).doesNotExist();
    }

    @Test
    void testBadRestoredStateDoesNotFailDataCommit() throws Exception {
        Map<String, Long> trackedPartitions = new HashMap<>();
        trackedPartitions.put("a$b", 1L);
        trackedPartitions.put("2023-12-31", 1L);
        Map<String, String> badStates = new LinkedHashMap<>();
        badStates.put(
                "test_mark_done_illegal_state",
                PartitionMarkDoneStateJsonSerde.toJson(
                        new PartitionMarkDoneState(true, trackedPartitions)));
        badStates.put("test_mark_done_corrupt_state", "corrupt-json");
        badStates.put(
                "test_mark_done_old_state",
                "{\"version\":1,\"initialized\":true,\"pending\":{\"2023-12-31\":1}}");
        badStates.put(
                "test_mark_done_unversioned_state",
                "{\"initialized\":true,\"pending\":{\"2023-12-31\":1}}");

        for (Map.Entry<String, String> badState : badStates.entrySet()) {
            TablePath tablePath = TablePath.of(DATABASE, badState.getKey());
            createPaimonTable(tablePath, markDoneOptions());
            TableInfo tableInfo = tableInfo(tablePath);
            // Seed a historical partition without running mark-done before restoring the state.
            writeAndCommit(tablePath, tableInfo, new Configuration(), "2023-12-31");
            assertThat(successFile(tablePath, "2023-12-31")).doesNotExist();

            FileStoreTable fileStoreTable =
                    (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
            try (TableCommitImpl stateCommit =
                    fileStoreTable.newCommit(FLUSS_LAKE_TIERING_COMMIT_USER)) {
                stateCommit.ignoreEmptyCommit(false);
                ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
                committable.addProperty(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets");
                committable.addProperty(MARK_DONE_STATE_PROPERTY, badState.getValue());
                stateCommit.commit(committable);
            }
            assertThat(
                            getSnapshotProperties(
                                    tablePath, fileStoreTable.snapshotManager().latestSnapshotId()))
                    .containsEntry(MARK_DONE_STATE_PROPERTY, badState.getValue());

            Thread.sleep(50);
            long snapshot2 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
            assertThat(successFile(tablePath, "2023-12-31")).exists();
            assertThat(getSnapshotProperties(tablePath, snapshot2).get(MARK_DONE_STATE_PROPERTY))
                    .contains("\"trackedPartitionLastUpdateTimes\":")
                    .doesNotContain("\"pending\"");
            PartitionMarkDoneState state = getMarkDoneState(tablePath, snapshot2);
            assertThat(state.isInitialized()).isTrue();
            assertThat(state.getTrackedPartitionLastUpdateTimes()).containsOnlyKeys("2024-01-01");

            // the healed state works: the next round marks the partition done
            Thread.sleep(50);
            try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                    createLakeCommitter(tablePath, tableInfo)) {
                assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNotNull();
            }
            assertThat(successFile(tablePath, "2024-01-01")).exists();
        }
    }

    @Test
    void testUnsupportedStateVersionSkipsMarkDone() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_unsupported_version");
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath);
        String stateJson =
                "{\"version\":2,\"initialized\":true,"
                        + "\"trackedPartitionLastUpdateTimes\":{\"2024-01-01\":1},"
                        + "\"futureState\":{\"epoch\":7}}";
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        try (TableCommitImpl stateCommit =
                fileStoreTable.newCommit(FLUSS_LAKE_TIERING_COMMIT_USER)) {
            stateCommit.ignoreEmptyCommit(false);
            ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
            committable.addProperty(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets");
            committable.addProperty(MARK_DONE_STATE_PROPERTY, stateJson);
            stateCommit.commit(committable);
        }
        long snapshotId = fileStoreTable.snapshotManager().latestSnapshotId();

        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNull();
        }
        assertThat(fileStoreTable.snapshotManager().latestSnapshotId()).isEqualTo(snapshotId);

        long dataSnapshotId = writeAndCommit(tablePath, tableInfo, "2024-01-02");
        assertThat(dataSnapshotId).isEqualTo(snapshotId + 1);
        assertThat(fileStoreTable.snapshotManager().snapshot(dataSnapshotId).totalRecordCount())
                .isEqualTo(1);
        assertThat(getSnapshotProperties(tablePath, dataSnapshotId))
                .containsEntry(MARK_DONE_STATE_PROPERTY, stateJson);
        assertThat(successFile(tablePath, "2024-01-01")).doesNotExist();
    }

    @ParameterizedTest
    @CsvSource({
        "partition.idle-time-to-done, not-a-duration",
        "partition.time-interval, ",
        "partition.mark-done-action, unknown",
        "partition.mark-done-action, custom",
        "partition.mark-done-action.mode, watermark"
    })
    void testInvalidConfigDisablesMarkDone(String option, String value) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_invalid_config");
        Map<String, String> options = markDoneOptions();
        if (value == null) {
            options.remove(option);
        } else {
            options.put(option, value);
        }
        createPaimonTable(tablePath, options);
        TableInfo tableInfo = tableInfo(tablePath);
        long snapshot = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        assertThat(getSnapshotProperties(tablePath, snapshot))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(committer, "offsets-2")).isNull();
        }
    }

    @Test
    void testInvalidFormatterDisablesMarkDoneAndRecoversByColdStart() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_invalid_formatter");
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath);

        writeAndCommit(tablePath, tableInfo, "2024-01-01");

        // an invalid formatter syntax disables mark-done as a whole instead of draining the
        // pending set partition by partition; the data commit still succeeds without state
        setPaimonOptions(tablePath, Collections.singletonMap(TIMESTAMP_FORMATTER_KEY, "{invalid}"));
        long snapshot2 = writeAndCommit(tablePath, tableInfo, "2024-01-02");
        assertThat(getSnapshotProperties(tablePath, snapshot2))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);

        // Once the formatter is fixed, cold start recovers all live partitions.
        setPaimonOptions(
                tablePath, Collections.singletonMap(TIMESTAMP_FORMATTER_KEY, "yyyy-MM-dd"));
        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-3")).isNotNull();
        }
        assertThat(successFile(tablePath, "2024-01-01")).exists();
        assertThat(successFile(tablePath, "2024-01-02")).exists();
    }

    @Test
    void testFailedActionRetriedNextRound() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_flaky_action");
        Map<String, String> options = markDoneOptions();
        options.put("partition.mark-done-action", "custom");
        options.put("partition.mark-done-action.custom.class", FlakyMarkDoneAction.class.getName());
        createPaimonTable(tablePath, options);
        TableInfo tableInfo = tableInfo(tablePath);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        PartitionMarkDoneState state1 = getMarkDoneState(tablePath, snapshot1);
        assertThat(state1.getTrackedPartitionLastUpdateTimes()).containsOnlyKeys("2024-01-01");

        // the action fails in the data round tiering a new partition: the round doesn't
        // fail, the failed partition stays pending with its original last update time and
        // the new partition is tracked
        FlakyMarkDoneAction.remainingFailures.set(1);
        FlakyMarkDoneAction.invocations.set(0);
        Thread.sleep(50);
        long snapshot2 = writeAndCommit(tablePath, tableInfo, "2024-01-02");
        assertThat(FlakyMarkDoneAction.invocations.get()).isEqualTo(1);
        assertThat(getMarkDoneState(tablePath, snapshot2).getTrackedPartitionLastUpdateTimes())
                .containsOnlyKeys("2024-01-01", "2024-01-02")
                .containsEntry(
                        "2024-01-01",
                        state1.getTrackedPartitionLastUpdateTimes().get("2024-01-01"));

        // the next round retries the action and marks both idle partitions done
        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            LakeCommitResult maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-3");
            assertThat(maintenanceSnapshot).isNotNull();
            assertThat(
                            getMarkDoneState(
                                            tablePath, maintenanceSnapshot.getCommittedSnapshotId())
                                    .getTrackedPartitionLastUpdateTimes())
                    .isEmpty();
        }
        assertThat(FlakyMarkDoneAction.invocations.get()).isEqualTo(3);
    }

    @Test
    void testPreparationAndCommitAreSeparate() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_prepare");
        Map<String, String> options = markDoneOptions();
        options.put("partition.mark-done-action", "custom");
        options.put("partition.mark-done-action.custom.class", FlakyMarkDoneAction.class.getName());
        createPaimonTable(tablePath, options);
        FlakyMarkDoneAction.remainingFailures.set(0);
        FlakyMarkDoneAction.invocations.set(0);
        TableInfo tableInfo = tableInfo(tablePath);
        FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        long snapshot = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        Thread.sleep(50);
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            lakeCommitter.commit(
                    lakeCommitter.toCommittable(Collections.emptyList()),
                    Collections.singletonMap(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets-2"));
            assertThat(FlakyMarkDoneAction.invocations.get()).isZero();
            assertThat(getSnapshotProperties(tablePath, snapshot + 1))
                    .doesNotContainKey(MARK_DONE_STATE_PROPERTY);

            PaimonCommittable committable = lakeCommitter.toCommittable(Collections.emptyList());
            assertThat(lakeCommitter.preparePartitionMarkDone(committable)).isTrue();
            assertThat(FlakyMarkDoneAction.invocations.get()).isEqualTo(1);
            assertThat(committable.manifestCommittable().properties())
                    .containsKey(MARK_DONE_STATE_PROPERTY)
                    .doesNotContainKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot + 1);

            LakeCommitResult result =
                    lakeCommitter.commit(
                            committable,
                            Collections.singletonMap(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets-3"));
            assertThat(result.getCommittedSnapshotId()).isEqualTo(snapshot + 2);
            assertThat(FlakyMarkDoneAction.invocations.get()).isEqualTo(1);
            assertThat(
                            getMarkDoneState(tablePath, result.getCommittedSnapshotId())
                                    .getTrackedPartitionLastUpdateTimes())
                    .isEmpty();
            PaimonCommittable unchanged = lakeCommitter.toCommittable(Collections.emptyList());
            assertThat(lakeCommitter.preparePartitionMarkDone(unchanged)).isFalse();
            assertThat(unchanged.manifestCommittable().properties())
                    .containsEntry(
                            MARK_DONE_STATE_PROPERTY,
                            committable
                                    .manifestCommittable()
                                    .properties()
                                    .get(MARK_DONE_STATE_PROPERTY));
            assertThat(FlakyMarkDoneAction.invocations.get()).isEqualTo(1);
            assertThat(table.snapshotManager().latestSnapshotId())
                    .isEqualTo(result.getCommittedSnapshotId());
        }
    }

    /** A custom mark-done action failing on demand to verify the next-round retry. */
    public static class FlakyMarkDoneAction implements PartitionMarkDoneAction {

        private static final AtomicInteger remainingFailures = new AtomicInteger();
        private static final AtomicInteger invocations = new AtomicInteger();

        @Override
        public void markDone(String partition) {
            invocations.incrementAndGet();
            if (remainingFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new RuntimeException("injected mark-done failure");
            }
        }

        @Override
        public void close() {}
    }

    private static LakeCommitResult commitMarkDoneMaintenance(
            PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter,
            String offsetsPath)
            throws IOException {
        PaimonCommittable committable = lakeCommitter.toCommittable(Collections.emptyList());
        return lakeCommitter.preparePartitionMarkDone(committable)
                ? lakeCommitter.commit(
                        committable,
                        Collections.singletonMap(
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsetsPath))
                : null;
    }

    private static Map<String, String> markDoneOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(IDLE_TIME_KEY, "1 ms");
        options.put(TIME_INTERVAL_KEY, "1 d");
        return options;
    }

    private TableInfo tableInfo(TablePath tablePath) {
        return TableInfo.of(
                tablePath, 0, 1, newTableBuilder().build(), DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private TableDescriptor.Builder newTableBuilder() {
        return TableDescriptor.builder()
                .schema(
                        org.apache.fluss.metadata.Schema.newBuilder()
                                .column("c1", org.apache.fluss.types.DataTypes.INT())
                                .column("c2", org.apache.fluss.types.DataTypes.STRING())
                                .column("c3", org.apache.fluss.types.DataTypes.STRING())
                                .build())
                .partitionedBy("c3")
                .distributedBy(1)
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true);
    }

    /** Writes one record to each given partition and commits, returns the snapshot id. */
    private long writeAndCommit(TablePath tablePath, TableInfo tableInfo, String... partitions)
            throws Exception {
        return writeAndCommit(tablePath, tableInfo, enabledLakeTieringConfig(), partitions);
    }

    private long writeAndCommit(
            TablePath tablePath,
            TableInfo tableInfo,
            Configuration lakeTieringConfig,
            String... partitions)
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
        try (PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo, lakeTieringConfig)) {
            PaimonCommittable committable = lakeCommitter.toCommittable(writeResults);
            lakeCommitter.preparePartitionMarkDone(committable);
            return lakeCommitter
                    .commit(
                            committable,
                            Collections.singletonMap(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets"))
                    .getCommittedSnapshotId();
        }
    }

    private PartitionMarkDoneState getMarkDoneState(TablePath tablePath, long snapshotId)
            throws Exception {
        Map<String, String> properties = getSnapshotProperties(tablePath, snapshotId);
        assertThat(properties).containsKey(MARK_DONE_STATE_PROPERTY);
        return PartitionMarkDoneStateJsonSerde.fromJson(properties.get(MARK_DONE_STATE_PROPERTY));
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

                    @Override
                    public int bucketCount() {
                        return tableInfo.getNumBuckets();
                    }
                });
    }

    /** A job-level tiering config with the mark-done switch (disabled by default) enabled. */
    private static Configuration enabledLakeTieringConfig() {
        Configuration lakeTieringConfig = new Configuration();
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED, true);
        return lakeTieringConfig;
    }

    private PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        return createLakeCommitter(tablePath, tableInfo, enabledLakeTieringConfig());
    }

    private PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo, Configuration lakeTieringConfig)
            throws IOException {
        return paimonLakeTieringFactory.createLakeCommitter(
                committerContext(tablePath, tableInfo, lakeTieringConfig));
    }

    private CommitterInitContext committerContext(
            TablePath tablePath, TableInfo tableInfo, Configuration lakeTieringConfig) {
        return new CommitterInitContext() {
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
                return lakeTieringConfig;
            }

            @Override
            public Configuration flussClientConfig() {
                return new Configuration();
            }
        };
    }
}
