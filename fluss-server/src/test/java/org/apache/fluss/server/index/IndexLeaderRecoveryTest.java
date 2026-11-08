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

package org.apache.fluss.server.index;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests index replication startup, snapshot recovery and leader lifecycle. */
class IndexLeaderRecoveryTest extends ReplicaTestBase {

    private static final long INDEXED_TABLE_ID = 710001L;
    private static final long PLAIN_TABLE_ID = 710002L;
    private static final long INDEX_TABLE_ID = 710003L;
    private static final String INDEX_NAME = "idx_b";
    private static final TablePath INDEXED_TABLE_PATH =
            TablePath.of("test_db_1", "fail_stop_index_main_pk");
    private static final TablePath PLAIN_TABLE_PATH =
            TablePath.of("test_db_1", "fail_stop_plain_pk");
    private static final long SNAPSHOT_ID = 0L;
    private static final long TRUNCATED_LOG_START_OFFSET = 100L;
    private static final int FOLLOWER_LEADER_EPOCH = INITIAL_LEADER_EPOCH;

    private static Stream<Arguments> invalidProgress() {
        return Stream.of(
                Arguments.of(null, false),
                Arguments.of(-1L, false),
                Arguments.of(null, true),
                Arguments.of(-1L, true));
    }

    @ParameterizedTest
    @MethodSource("invalidProgress")
    void testInvalidSnapshotProgressBlocksLeadership(Long progress, boolean truncateLog)
            throws Exception {
        registerMainTable(INDEXED_TABLE_PATH, INDEXED_TABLE_ID, indexedSchema());
        TableBucket tableBucket = new TableBucket(INDEXED_TABLE_ID, 0);
        injectLatestSnapshot(
                tableBucket, progress, progress, truncateLog ? TRUNCATED_LOG_START_OFFSET : 0L);
        if (truncateLog) {
            makeLogStartUnreachableFromZero(INDEXED_TABLE_PATH, tableBucket);
        }

        for (int attempt = 1; attempt <= 3; attempt++) {
            NotifyLeaderAndIsrResultForBucket result =
                    tryBecomeLeader(
                            tableBucket, INDEXED_TABLE_PATH, FOLLOWER_LEADER_EPOCH + attempt);
            assertThat(result.failed()).isTrue();
            assertNotLeaderAndNoIndexProgress(tableBucket);
        }
    }

    /**
     * Drives the bucket through the state a follower reaches once the leader has reclaimed its
     * early segments: the local log restarts at a later offset, so the range {@code [0, HW)} can no
     * longer be replayed.
     */
    private void makeLogStartUnreachableFromZero(TablePath tablePath, TableBucket tableBucket) {
        int remoteLeaderId = TABLET_SERVER_ID + 1;
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, remoteLeaderId);
        NotifyLeaderAndIsrData followerData =
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(tablePath),
                        tableBucket,
                        replicas,
                        new LeaderAndIsr(
                                remoteLeaderId,
                                FOLLOWER_LEADER_EPOCH,
                                replicas,
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                FOLLOWER_LEADER_EPOCH));
        List<NotifyLeaderAndIsrResultForBucket> results = new ArrayList<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(followerData),
                results::addAll);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).succeeded()).isTrue();

        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        replica.truncateFullyAndStartAt(TRUNCATED_LOG_START_OFFSET);
        assertThat(replica.getLogStartOffset()).isEqualTo(TRUNCATED_LOG_START_OFFSET);
    }

    @Test
    void testRestoresDistinctSyncAndAllIndexProgressBeforeReplay() throws Exception {
        Schema schema =
                Schema.newBuilder().fromSchema(indexedSchema()).index("idx_sync", "b").build();
        // Only the source metadata is registered. Without index-table metadata the supervisor
        // cannot start WAL replay, so progress here must come from the persisted snapshot.
        registerMainTable(INDEXED_TABLE_PATH, INDEXED_TABLE_ID, schema);
        TableBucket tableBucket = new TableBucket(INDEXED_TABLE_ID, 0);
        injectLatestSnapshot(tableBucket, 80L, 90L, TRUNCATED_LOG_START_OFFSET);
        makeLogStartUnreachableFromZero(INDEXED_TABLE_PATH, tableBucket);

        NotifyLeaderAndIsrResultForBucket result =
                tryBecomeLeader(tableBucket, INDEXED_TABLE_PATH, FOLLOWER_LEADER_EPOCH + 1);
        assertThat(result.getErrorMessage()).isNull();
        assertThat(result.succeeded()).isTrue();
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        assertThat(replica.getAllIndexPushedOffset()).isEqualTo(80L);
        assertThat(replica.getSyncIndexPushedOffset()).isEqualTo(90L);
        assertThat(replica.getClientLogHighWatermark()).isEqualTo(90L);
        // Metadata is still absent: visibility and recovery backlog must remain observable
        // even though no replicator has started.
        assertThat(indexGauge(MetricNames.INDEX_REPLICATION_MAX_LAG_OFFSETS)).isEqualTo(20L);
        assertThat(indexGauge(MetricNames.INDEX_REPLICATION_MAX_SYNC_LAG_OFFSETS)).isEqualTo(10L);
        manualClock.advanceTime(500, TimeUnit.MILLISECONDS);
        assertThat(indexGauge(MetricNames.INDEX_REPLICATION_MAX_NO_PROGRESS_TIME_MS))
                .isEqualTo(500L);
        assertThat(replicaManager.getServerMetricGroup().indexReplicationRetries().getCount())
                .isEqualTo(1);

        int remoteLeaderId = TABLET_SERVER_ID + 1;
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, remoteLeaderId);
        replica.makeFollower(
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(INDEXED_TABLE_PATH),
                        tableBucket,
                        replicas,
                        new LeaderAndIsr(
                                remoteLeaderId,
                                FOLLOWER_LEADER_EPOCH + 2,
                                replicas,
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                FOLLOWER_LEADER_EPOCH + 2)));
        assertThat(indexGauge(MetricNames.INDEX_REPLICATION_MAX_LAG_OFFSETS)).isEqualTo(0L);
        assertThat(indexGauge(MetricNames.INDEX_REPLICATION_MAX_NO_PROGRESS_TIME_MS)).isEqualTo(0L);
    }

    @Test
    void testDeferredIndexReplicationResumesAndReleasesResourcesOnShutdown() throws Exception {
        // Missing index-table metadata must defer replication without blocking leadership.
        registerMainTable(INDEXED_TABLE_PATH, INDEXED_TABLE_ID, indexedSchema());

        makeKvTableAsLeader(INDEXED_TABLE_ID, INDEXED_TABLE_PATH, 0);
        TableBucket sourceBucket = new TableBucket(INDEXED_TABLE_ID, 0);
        IndexSendBuffer sendBuffer = replicaManager.getIndexReplicationManager().sendBuffer();

        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.buckets()).isEmpty();

        // Deliver the missing metadata through the production update path.
        Replica mainReplica = replicaManager.getReplicaOrException(sourceBucket);
        TableInfo mainTableInfo = mainReplica.getTableInfo();
        Schema.Index index = mainTableInfo.getSchema().getIndexes().get(0);
        TablePath indexTablePath =
                TablePath.of(
                        INDEXED_TABLE_PATH.getDatabaseName(),
                        IndexTableUtils.indexTableName(
                                INDEXED_TABLE_PATH.getTableName(), index.getIndexName()));
        TableDescriptor indexDescriptor =
                IndexTableDescriptorFactory.derive(
                        mainTableInfo.toTableDescriptor(), mainTableInfo.getTableId(), index);
        zkClient.registerTable(
                indexTablePath,
                TableRegistration.newTable(
                        INDEX_TABLE_ID, DEFAULT_REMOTE_DATA_DIR, indexDescriptor));
        zkClient.registerFirstSchema(indexTablePath, indexDescriptor.getSchema());
        long now = System.currentTimeMillis();
        TableInfo indexTableInfo =
                TableInfo.of(
                        indexTablePath,
                        INDEX_TABLE_ID,
                        1,
                        indexDescriptor,
                        DEFAULT_REMOTE_DATA_DIR,
                        now,
                        now);
        List<BucketMetadata> indexBuckets = new ArrayList<>();
        for (int bucket = 0; bucket < indexTableInfo.getNumBuckets(); bucket++) {
            indexBuckets.add(new BucketMetadata(bucket, null, null, Collections.emptyList()));
        }
        replicaManager.maybeUpdateMetadataCache(
                INITIAL_COORDINATOR_EPOCH,
                new ClusterMetadata(
                        coordinatorServerInfo(),
                        aliveTabletServerInfos(),
                        Collections.singletonList(new TableMetadata(indexTableInfo, indexBuckets)),
                        Collections.emptyList()));

        // Keep the window queued so shutdown must release the source's undelivered payload.
        sendBuffer.setReadyBucketListener(bucket -> {});

        CompletableFuture<List<PutKvResultForBucket>> putFuture = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(sourceBucket, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                MergeMode.DEFAULT,
                (short) 1,
                putFuture::complete);
        assertThat(putFuture.get()).hasSize(1);

        retry(
                Duration.ofSeconds(30),
                () -> assertThat(sendBuffer.pendingBytes()).isGreaterThan(0L));

        replicaManager.shutdown();

        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.buckets()).isEmpty();
    }

    private Object indexGauge(String name) {
        return ((Gauge<?>) replicaManager.getServerMetricGroup().getMetrics().get(name)).getValue();
    }

    /** Ordinary KV tables do not require index progress in their snapshots. */
    @Test
    void testMissingIndexPushedOffsetAllowsLeadershipWithoutSecondaryIndexes() throws Exception {
        registerMainTable(PLAIN_TABLE_PATH, PLAIN_TABLE_ID, plainSchema());
        TableBucket tableBucket = new TableBucket(PLAIN_TABLE_ID, 0);
        injectLatestSnapshot(tableBucket, null, null, TRUNCATED_LOG_START_OFFSET);

        makeLogStartUnreachableFromZero(PLAIN_TABLE_PATH, tableBucket);

        NotifyLeaderAndIsrResultForBucket result =
                tryBecomeLeader(tableBucket, PLAIN_TABLE_PATH, FOLLOWER_LEADER_EPOCH + 1);
        assertThat(result.getErrorMessage()).isNull();
        assertThat(result.succeeded()).isTrue();

        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        assertThat(replica.isLeader()).isTrue();
        assertThat(replica.getKvTablet()).isNotNull();
    }

    /**
     * Asserts the bucket neither serves as leader nor pushed any index data, i.e. recovery did not
     * degrade into replaying from some later offset.
     */
    private void assertNotLeaderAndNoIndexProgress(TableBucket tableBucket) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        assertThat(replica.isLeader()).isFalse();
        assertThat(replica.getKvTablet()).isNull();
        assertThat(replicaManager.getIndexReplicationManager().sendBuffer().pendingBytes())
                .isZero();
        assertThat(replicaManager.getIndexReplicationManager().sendBuffer().buckets()).isEmpty();
    }

    private void registerMainTable(TablePath tablePath, long tableId, Schema schema)
            throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1");
        registerTableInZkClient(
                tablePath, schema, tableId, Collections.singletonList("a"), properties);
    }

    /**
     * Publishes a completed snapshot as the latest snapshot of the given bucket through the same
     * ZooKeeper handle store that the server reads on leader promotion.
     */
    private void injectLatestSnapshot(
            TableBucket tableBucket,
            Long indexPushedOffset,
            Long syncIndexPushedOffset,
            long snapshotLogOffset)
            throws Exception {
        FsPath snapshotLocation =
                new FsPath(
                        tempDir.getAbsolutePath()
                                + "/injected-kv-snapshots/"
                                + tableBucket.getTableId()
                                + "-"
                                + tableBucket.getBucket()
                                + "-"
                                + SNAPSHOT_ID);
        CompletedSnapshot snapshot =
                new CompletedSnapshot(
                        tableBucket,
                        SNAPSHOT_ID,
                        snapshotLocation,
                        KvSnapshotHandle.create(
                                Collections.emptyList(), Collections.emptyList(), 0L),
                        snapshotLogOffset,
                        null,
                        indexPushedOffset,
                        syncIndexPushedOffset,
                        null);

        FsPath metadataPath = snapshot.getMetadataFilePath();
        FileSystem fileSystem = metadataPath.getFileSystem();
        try (FSDataOutputStream out =
                fileSystem.create(metadataPath, FileSystem.WriteMode.OVERWRITE)) {
            out.write(CompletedSnapshotJsonSerde.toJson(snapshot));
        }

        zkClient.registerTableBucketSnapshot(
                tableBucket,
                new BucketSnapshot(SNAPSHOT_ID, snapshot.getLogOffset(), metadataPath.toString()));
    }

    /** Drives the production leader-promotion entry point and returns the result of the bucket. */
    private NotifyLeaderAndIsrResultForBucket tryBecomeLeader(
            TableBucket tableBucket, TablePath tablePath, int leaderEpoch) {
        NotifyLeaderAndIsrData data =
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(tablePath),
                        tableBucket,
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                leaderEpoch,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                // use leader epoch as bucket epoch
                                leaderEpoch));
        List<NotifyLeaderAndIsrResultForBucket> results = new ArrayList<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH, Collections.singletonList(data), results::addAll);
        assertThat(results).hasSize(1);
        return results.get(0);
    }

    private static Schema indexedSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .index(
                        INDEX_NAME,
                        IndexType.SECONDARY,
                        Collections.singletonList("b"),
                        IndexVisibility.ASYNC,
                        3)
                .build();
    }

    private static Schema plainSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .build();
    }

    private static ServerInfo coordinatorServerInfo() {
        return new ServerInfo(
                0,
                null,
                Endpoint.fromListenersString("CLIENT://localhost:1234"),
                ServerType.COORDINATOR);
    }

    private static Set<ServerInfo> aliveTabletServerInfos() {
        return new HashSet<>(
                Arrays.asList(
                        new ServerInfo(
                                TABLET_SERVER_ID,
                                "rack1",
                                Endpoint.fromListenersString("CLIENT://localhost:90"),
                                ServerType.TABLET_SERVER),
                        new ServerInfo(
                                2,
                                "rack2",
                                Endpoint.fromListenersString("CLIENT://localhost:91"),
                                ServerType.TABLET_SERVER),
                        new ServerInfo(
                                3,
                                "rack3",
                                Endpoint.fromListenersString("CLIENT://localhost:92"),
                                ServerType.TABLET_SERVER)));
    }
}
