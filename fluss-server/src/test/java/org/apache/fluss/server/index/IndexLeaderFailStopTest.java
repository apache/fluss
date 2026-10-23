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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies FIP-38 §6.1: a bucket of a table with secondary indexes must permanently refuse to
 * become leader when its latest KV snapshot carries no usable {@code indexPushedOffset}.
 */
class IndexLeaderFailStopTest extends ReplicaTestBase {

    private static final long INDEXED_TABLE_ID = 710001L;
    private static final long PLAIN_TABLE_ID = 710002L;
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
        injectLatestSnapshot(tableBucket, progress, truncateLog ? TRUNCATED_LOG_START_OFFSET : 0L);
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

    /** Ordinary KV tables do not require index progress in their snapshots. */
    @Test
    void testMissingIndexPushedOffsetAllowsLeadershipWithoutSecondaryIndexes() throws Exception {
        registerMainTable(PLAIN_TABLE_PATH, PLAIN_TABLE_ID, plainSchema());
        TableBucket tableBucket = new TableBucket(PLAIN_TABLE_ID, 0);
        injectLatestSnapshot(tableBucket, null, TRUNCATED_LOG_START_OFFSET);

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
        assertThat(replicaManager.getIndexSendBuffer().pendingBytes(tableBucket)).isZero();
        assertThat(replicaManager.getIndexSendBuffer().buckets()).isEmpty();
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
            TableBucket tableBucket, Long indexPushedOffset, long snapshotLogOffset)
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
}
