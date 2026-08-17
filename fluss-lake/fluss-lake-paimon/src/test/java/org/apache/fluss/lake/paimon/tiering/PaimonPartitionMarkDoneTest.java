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
import org.apache.fluss.lake.committer.PartitionMarkDoneMaintainer;
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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
import static org.apache.fluss.lake.paimon.tiering.PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY;
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
        // the mark-done options are carried in the Fluss custom properties only
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo = markDoneTableInfo(tablePath, false);

        // first data commit: cold start along with the commit, all time-parsable partitions
        // pending; the illegal partition 'px' (no time can be extracted) is dropped without
        // marking done, same as Paimon
        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01", "2024-01-02", "px");
        MarkDoneState state = getMarkDoneState(tablePath, snapshot1);
        assertThat(state.isInitialized()).isTrue();
        assertThat(state.getPendingPartitions()).containsOnlyKeys("2024-01-01", "2024-01-02");

        // empty round: idle partitions are marked done via a properties-only snapshot which
        // carries a freshly prepared offsets file
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-2");
            assertThat(maintenanceSnapshot).isNotNull();
            assertThat(maintenanceSnapshot.getLakeSnapshotId()).isEqualTo(snapshot1 + 1);
            // the maintenance snapshot carries its own offsets file, not the previous one
            assertThat(maintenanceSnapshot.getSnapshotProperties())
                    .containsEntry(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets-2");
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
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-3")).isNull();
        }

        // late data for 2024-01-01: re-added to pending and marked done again later
        long snapshot3 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        assertThat(getMarkDoneState(tablePath, snapshot3).getPendingPartitions())
                .containsOnlyKeys("2024-01-01");

        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-4")).isNotNull();
        }
        assertThat(getMarkDoneState(tablePath, snapshot3 + 1).getPendingPartitions()).isEmpty();
        assertThat(successFile(tablePath, "2024-01-01")).exists();
    }

    @Test
    void testColdStartBackfill() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_cold_start");
        // first tier data without the mark-done custom properties
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo = tableInfo(tablePath, false);
        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01", "2024-01-02");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);

        // then enable mark-done via the Fluss custom properties: the cold start backfills the
        // existing idle partitions
        Thread.sleep(50);
        TableInfo enabledTableInfo = markDoneTableInfo(tablePath, false);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, enabledTableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNotNull();
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
        createPaimonTable(tablePath, Collections.emptyMap());
        // auto-partitioned by day: partition end time is derived from the partition name
        TableInfo tableInfo = markDoneTableInfo(tablePath, true);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "20200101", "99991231");
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNotNull();
        }
        // the ancient partition is done, the future partition is guarded by its end time
        MarkDoneState state = getMarkDoneState(tablePath, snapshot1 + 1);
        assertThat(state.getPendingPartitions()).containsOnlyKeys("99991231");
        assertThat(successFile(tablePath, "20200101")).exists();
        assertThat(successFile(tablePath, "99991231")).doesNotExist();
    }

    @Test
    void testCustomQuarterFormat() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_custom_quarter_format");
        createPaimonTable(tablePath, Collections.emptyMap());
        // auto-partitioned by quarter with the custom time format yyyy-'Q'Q
        TableInfo tableInfo = quarterTableInfo(tablePath);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-Q3", "9999-Q4");
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNotNull();
        }
        // the elapsed quarter is marked done; the future quarter must stay pending instead of
        // being dropped as unparsable (regression: Q2-Q4 used to conflict with the month
        // default while resolving the custom quarter format)
        MarkDoneState state = getMarkDoneState(tablePath, snapshot1 + 1);
        assertThat(state.getPendingPartitions()).containsOnlyKeys("9999-Q4");
        assertThat(successFile(tablePath, "2024-Q3")).exists();
        assertThat(successFile(tablePath, "9999-Q4")).doesNotExist();
    }

    @Test
    void testDisabledWithoutTimeInterval() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_disabled_without_interval");
        // non auto-partitioned table with only the idle custom property: the partition end
        // time can never be derived, so mark-done must be disabled instead of accumulating
        // unbounded pending state and triggering useless maintenance rounds
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder(false)
                                .customProperty("paimon." + IDLE_TIME_KEY, "1 ms")
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);

        assertThat(paimonLakeTieringFactory.isPartitionMarkDoneEnabled(tableInfo)).isFalse();

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNull();
        }
    }

    @Test
    void testPaimonSideOptionsNotHonored() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_paimon_side_options");
        // mark-done options configured on the Paimon table directly are deliberately not
        // honored: the Fluss custom properties are the single source of truth
        createPaimonTable(tablePath, markDoneOptions());
        TableInfo tableInfo = tableInfo(tablePath, false);

        assertThat(paimonLakeTieringFactory.isPartitionMarkDoneEnabled(tableInfo)).isFalse();
        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);

        // while the switch in the Fluss custom properties works without any lake access
        // (the Paimon table of the path doesn't even exist yet)
        TablePath notCreatedTablePath = TablePath.of(DATABASE, "test_mark_done_enabler_props");
        assertThat(
                        paimonLakeTieringFactory.isPartitionMarkDoneEnabled(
                                markDoneTableInfo(notCreatedTablePath, false)))
                .isTrue();
    }

    @Test
    void testPartitionExpirationDoesNotBreakMarkDoneState() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_partition_expiration");
        // partition expiration configured on the Paimon table: Paimon appends an OVERWRITE
        // snapshot with the same commit user and null properties right after our commit
        Map<String, String> paimonOptions = new HashMap<>();
        paimonOptions.put("partition.expiration-time", "1 d");
        paimonOptions.put("partition.expiration-check-interval", "10 min");
        paimonOptions.put("partition.timestamp-formatter", "yyyy-MM-dd");
        createPaimonTable(tablePath, paimonOptions);
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder(false)
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
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
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
            CommittedLakeSnapshot maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-2");
            assertThat(maintenanceSnapshot).isNotNull();
            MarkDoneState state =
                    getMarkDoneState(tablePath, maintenanceSnapshot.getLakeSnapshotId());
            assertThat(state.isInitialized()).isTrue();
            assertThat(state.getPendingPartitions()).containsOnlyKeys("9999-12-31");
            assertThat(successFile(tablePath, "2020-01-01")).exists();
        }
    }

    @Test
    void testMaintenanceTailLookback() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_maintenance_tail");
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo = markDoneTableInfo(tablePath, false);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01", "9999-12-31");

        // simulate a batched partition expiration tail (e.g. partition.expiration-max-num=100
        // with batch size 1): 100 OVERWRITE snapshots with the same commit user and no
        // properties, crossed by the unbounded walk-back
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        for (int i = 0; i < 100; i++) {
            try (TableCommitImpl truncateCommit =
                    fileStoreTable.newCommit(FLUSS_LAKE_TIERING_COMMIT_USER)) {
                truncateCommit.truncatePartitions(
                        Collections.singletonList(Collections.singletonMap("c3", "2024-01-01")));
            }
        }

        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            // the properties of the round are found behind the full-length tail
            CommittedLakeSnapshot missing = lakeCommitter.getMissingLakeSnapshot(snapshot1);
            assertThat(missing).isNotNull();
            assertThat(missing.getLakeSnapshotId()).isEqualTo(snapshot1 + 100);
            assertThat(missing.getSnapshotProperties())
                    .containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)
                    .containsKey(MARK_DONE_STATE_PROPERTY);

            // so is the mark-done state
            CommittedLakeSnapshot maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-2");
            assertThat(maintenanceSnapshot).isNotNull();
            MarkDoneState state =
                    getMarkDoneState(tablePath, maintenanceSnapshot.getLakeSnapshotId());
            assertThat(state.isInitialized()).isTrue();
            assertThat(state.getPendingPartitions()).containsOnlyKeys("9999-12-31");
        }
    }

    @Test
    void testZeroFileDonePartitionStillMarkedDone() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_zero_file_partition");
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo = markDoneTableInfo(tablePath, false);

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
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-2");
            assertThat(maintenanceSnapshot).isNotNull();
            assertThat(
                            getMarkDoneState(tablePath, maintenanceSnapshot.getLakeSnapshotId())
                                    .getPendingPartitions())
                    .isEmpty();
        }
        assertThat(successFile(tablePath, "2024-01-01")).exists();
    }

    @Test
    void testActionConfigFromCustomProperties() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_action_from_props");
        // the Paimon table itself carries a broken action config (custom action without the
        // class): it must be ignored since the action config is also read from the Fluss
        // custom properties only, falling back to the default success-file action
        createPaimonTable(
                tablePath, Collections.singletonMap("partition.mark-done-action", "custom"));
        TableInfo tableInfo = markDoneTableInfo(tablePath, false);

        writeAndCommit(tablePath, tableInfo, "2024-01-01");
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNotNull();
        }
        assertThat(successFile(tablePath, "2024-01-01")).exists();
    }

    @Test
    void testWatermarkModeRejected() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_watermark_mode");
        createPaimonTable(tablePath, Collections.emptyMap());
        // the watermark mode is not supported yet: it must be rejected with a warning instead
        // of being silently degraded to the process-time judgment, which would trigger the
        // done actions ahead of the user-configured watermark boundary
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder(false)
                                .customProperty("paimon." + IDLE_TIME_KEY, "1 ms")
                                .customProperty("paimon." + TIME_INTERVAL_KEY, "1 d")
                                .customProperty(
                                        "paimon.partition.mark-done-action.mode", "watermark")
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);

        assertThat(paimonLakeTieringFactory.isPartitionMarkDoneEnabled(tableInfo)).isFalse();

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNull();
        }
        assertThat(successFile(tablePath, "2024-01-01")).doesNotExist();
    }

    @Test
    void testLegacySnapshotWithoutPropertiesFailsFast() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_legacy_snapshot");
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo = tableInfo(tablePath, false);

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
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThatThrownBy(() -> lakeCommitter.getMissingLakeSnapshot(null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to load committed lake snapshot properties");
        }
    }

    @Test
    void testDisabledByJobLevelSwitch() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_job_level_switch");
        createPaimonTable(tablePath, Collections.emptyMap());
        // the table opts in via its custom properties but the job-level switch stays off
        // (the default): no state is written and no maintenance happens
        TableInfo tableInfo = markDoneTableInfo(tablePath, false);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, new Configuration(), "2024-01-01");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo, new Configuration())) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNull();
        }
        assertThat(successFile(tablePath, "2024-01-01")).doesNotExist();
    }

    @Test
    void testBadRestoredStateDoesNotFailDataCommit() throws Exception {
        // a structurally illegal partition name ('a$b' can't be resolved against the single
        // partition key) is dropped by the trigger; a syntactically or type-wise corrupt
        // state JSON falls back to a cold-start re-initialization
        Map<String, String> badStates = new LinkedHashMap<>();
        badStates.put(
                "test_mark_done_illegal_state",
                MarkDoneStateJsonSerde.toJson(
                        new MarkDoneState(true, Collections.singletonMap("a$b", 1L))));
        badStates.put("test_mark_done_corrupt_state", "corrupt-json");
        badStates.put(
                "test_mark_done_bad_time_state",
                "{\"initialized\":true,\"pending\":{\"p\":\"bad\"}}");
        badStates.put("test_mark_done_bad_pending_state", "{\"initialized\":true,\"pending\":[]}");

        for (Map.Entry<String, String> badState : badStates.entrySet()) {
            TablePath tablePath = TablePath.of(DATABASE, badState.getKey());
            createPaimonTable(tablePath, Collections.emptyMap());
            TableInfo tableInfo = markDoneTableInfo(tablePath, false);

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

            // the data commit succeeds, the bad state is healed and the tiered partition
            // is tracked in the new state
            long snapshot2 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
            MarkDoneState state = getMarkDoneState(tablePath, snapshot2);
            assertThat(state.isInitialized()).isTrue();
            assertThat(state.getPendingPartitions()).containsOnlyKeys("2024-01-01");

            // the healed state works: the next round marks the partition done
            Thread.sleep(50);
            try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                    createLakeCommitter(tablePath, tableInfo)) {
                assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-2")).isNotNull();
            }
            assertThat(successFile(tablePath, "2024-01-01")).exists();
        }
    }

    @Test
    void testInvalidConfigDisablesMarkDone() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_invalid_config");
        createPaimonTable(tablePath, Collections.emptyMap());

        // an invalid idle duration is rejected by the cheap switch
        TableInfo invalidDuration =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder(false)
                                .customProperty("paimon." + IDLE_TIME_KEY, "not-a-duration")
                                .customProperty("paimon." + TIME_INTERVAL_KEY, "1 d")
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);
        assertThat(paimonLakeTieringFactory.isPartitionMarkDoneEnabled(invalidDuration)).isFalse();

        // a custom action without its class passes the switch but only disables mark-done
        // in the committer instead of failing the committer creation
        TableInfo invalidAction =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder(false)
                                .customProperty("paimon." + IDLE_TIME_KEY, "1 ms")
                                .customProperty("paimon." + TIME_INTERVAL_KEY, "1 d")
                                .customProperty("paimon.partition.mark-done-action", "custom")
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);
        assertThat(paimonLakeTieringFactory.isPartitionMarkDoneEnabled(invalidAction)).isTrue();
        long snapshot1 = writeAndCommit(tablePath, invalidAction, "2024-01-01");
        assertThat(getSnapshotProperties(tablePath, snapshot1))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);
    }

    @Test
    void testInvalidFormatterDisablesMarkDoneAndRecoversByColdStart() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_invalid_formatter");
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo = markDoneTableInfo(tablePath, false);

        writeAndCommit(tablePath, tableInfo, "2024-01-01");

        // an invalid formatter syntax disables mark-done as a whole instead of draining the
        // pending set partition by partition; the data commit still succeeds without state
        TableInfo invalidFormatter =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder(false)
                                .customProperty("paimon." + IDLE_TIME_KEY, "1 ms")
                                .customProperty("paimon." + TIME_INTERVAL_KEY, "1 d")
                                .customProperty("paimon.partition.timestamp-formatter", "{invalid}")
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);
        long snapshot2 = writeAndCommit(tablePath, invalidFormatter, "2024-01-02");
        assertThat(getSnapshotProperties(tablePath, snapshot2))
                .doesNotContainKey(MARK_DONE_STATE_PROPERTY);

        // once the formatter is fixed, cold start recovers all live partitions
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThat(commitMarkDoneMaintenance(lakeCommitter, "offsets-3")).isNotNull();
        }
        assertThat(successFile(tablePath, "2024-01-01")).exists();
        assertThat(successFile(tablePath, "2024-01-02")).exists();
    }

    @Test
    void testFailedActionRetriedNextRound() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_flaky_action");
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        0,
                        1,
                        newTableBuilder(false)
                                .customProperty("paimon." + IDLE_TIME_KEY, "1 ms")
                                .customProperty("paimon." + TIME_INTERVAL_KEY, "1 d")
                                .customProperty("paimon.partition.mark-done-action", "custom")
                                .customProperty(
                                        "paimon.partition.mark-done-action.custom.class",
                                        FlakyMarkDoneAction.class.getName())
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        MarkDoneState state1 = getMarkDoneState(tablePath, snapshot1);
        assertThat(state1.getPendingPartitions()).containsOnlyKeys("2024-01-01");

        // the action fails in the data round tiering a new partition: the round doesn't
        // fail, the failed partition stays pending with its original last update time and
        // the new partition is tracked
        FlakyMarkDoneAction.remainingFailures.set(1);
        FlakyMarkDoneAction.invocations.set(0);
        Thread.sleep(50);
        long snapshot2 = writeAndCommit(tablePath, tableInfo, "2024-01-02");
        assertThat(FlakyMarkDoneAction.invocations.get()).isEqualTo(1);
        assertThat(getMarkDoneState(tablePath, snapshot2).getPendingPartitions())
                .containsOnlyKeys("2024-01-01", "2024-01-02")
                .containsEntry("2024-01-01", state1.getPendingPartitions().get("2024-01-01"));

        // the next round retries the action and marks both idle partitions done
        Thread.sleep(50);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot maintenanceSnapshot =
                    commitMarkDoneMaintenance(lakeCommitter, "offsets-3");
            assertThat(maintenanceSnapshot).isNotNull();
            assertThat(
                            getMarkDoneState(tablePath, maintenanceSnapshot.getLakeSnapshotId())
                                    .getPendingPartitions())
                    .isEmpty();
        }
        assertThat(FlakyMarkDoneAction.invocations.get()).isEqualTo(3);
    }

    @Test
    void testMaintenanceOffsetsFailurePropagates() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "test_mark_done_offsets_failure");
        createPaimonTable(tablePath, Collections.emptyMap());
        TableInfo tableInfo = markDoneTableInfo(tablePath, false);

        long snapshot1 = writeAndCommit(tablePath, tableInfo, "2024-01-01");
        Thread.sleep(50);

        // a failure preparing the offsets file involves snapshot consistency and must
        // propagate instead of being swallowed as a mark-done failure
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            assertThatThrownBy(
                            () ->
                                    ((PartitionMarkDoneMaintainer) lakeCommitter)
                                            .commitMarkDoneMaintenance(
                                                    () -> {
                                                        throw new IOException("injected");
                                                    }))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("injected");
        }
        // no maintenance snapshot was committed
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        assertThat(fileStoreTable.snapshotManager().latestSnapshotId()).isEqualTo(snapshot1);
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

    @Test
    void testStateJsonSerde() {
        // round trip
        Map<String, Long> pending = new HashMap<>();
        pending.put("20240101", 1234L);
        pending.put("2024-01-02", -1L);
        MarkDoneState state = new MarkDoneState(true, pending);
        int stateHashCode = state.hashCode();
        String stateJson = MarkDoneStateJsonSerde.toJson(state);
        pending.clear();
        assertThat(state.getPendingPartitions()).hasSize(2);
        assertThat(state.hashCode()).isEqualTo(stateHashCode);
        assertThat(MarkDoneStateJsonSerde.toJson(state)).isEqualTo(stateJson);
        assertThat(MarkDoneStateJsonSerde.fromJson(stateJson)).isEqualTo(state);

        // missing fields fall back to defaults, unknown fields are ignored
        assertThat(MarkDoneStateJsonSerde.fromJson("{}")).isEqualTo(MarkDoneState.empty());
        state =
                MarkDoneStateJsonSerde.fromJson(
                        "{\"initialized\":true,\"pending\":{\"p1\":100},\"unknown\":\"x\"}");
        assertThat(state.isInitialized()).isTrue();
        assertThat(state.getPendingPartitions()).containsEntry("p1", 100L);

        // wrongly typed fields are rejected as corrupt instead of silently coerced
        assertThatThrownBy(() -> MarkDoneStateJsonSerde.fromJson("{\"initialized\":\"x\"}"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MarkDoneStateJsonSerde.fromJson("{\"pending\":[]}"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MarkDoneStateJsonSerde.fromJson("{\"pending\":{\"p\":\"bad\"}}"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static CommittedLakeSnapshot commitMarkDoneMaintenance(
            LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter, String offsetsPath)
            throws IOException {
        return ((PartitionMarkDoneMaintainer) lakeCommitter)
                .commitMarkDoneMaintenance(() -> offsetsPath);
    }

    private static Map<String, String> markDoneOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(IDLE_TIME_KEY, "1 ms");
        options.put(TIME_INTERVAL_KEY, "1 d");
        return options;
    }

    private TableInfo tableInfo(TablePath tablePath, boolean autoPartition) {
        return TableInfo.of(
                tablePath,
                0,
                1,
                newTableBuilder(autoPartition).build(),
                DEFAULT_REMOTE_DATA_DIR,
                1L,
                1L);
    }

    /** A table info carrying the mark-done options in the Fluss custom properties. */
    private TableInfo markDoneTableInfo(TablePath tablePath, boolean autoPartition) {
        TableDescriptor.Builder builder =
                newTableBuilder(autoPartition).customProperty("paimon." + IDLE_TIME_KEY, "1 ms");
        if (!autoPartition) {
            builder.customProperty("paimon." + TIME_INTERVAL_KEY, "1 d");
        }
        return TableInfo.of(tablePath, 0, 1, builder.build(), DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private TableInfo quarterTableInfo(TablePath tablePath) {
        TableDescriptor.Builder builder =
                newTableBuilder(true)
                        .customProperty("paimon." + IDLE_TIME_KEY, "1 ms")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.QUARTER)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy-'Q'Q");
        return TableInfo.of(tablePath, 0, 1, builder.build(), DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private TableDescriptor.Builder newTableBuilder(boolean autoPartition) {
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
        return builder;
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
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo, lakeTieringConfig)) {
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

    /** A job-level tiering config with the mark-done switch (disabled by default) enabled. */
    private static Configuration enabledLakeTieringConfig() {
        Configuration lakeTieringConfig = new Configuration();
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED, true);
        return lakeTieringConfig;
    }

    private LakeCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        return createLakeCommitter(tablePath, tableInfo, enabledLakeTieringConfig());
    }

    private LakeCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo, Configuration lakeTieringConfig)
            throws IOException {
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
                        return lakeTieringConfig;
                    }

                    @Override
                    public Configuration flussClientConfig() {
                        return new Configuration();
                    }
                });
    }
}
