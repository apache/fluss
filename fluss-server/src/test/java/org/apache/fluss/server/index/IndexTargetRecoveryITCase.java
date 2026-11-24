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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutIndexResponse;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.replica.fetcher.InitialFetchStatus;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.utils.ByteArraySlice;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.server.index.TestingIndexTable.INDEX_NAME;
import static org.apache.fluss.server.index.TestingIndexTable.deleteRequest;
import static org.apache.fluss.server.index.TestingIndexTable.indexEntry;
import static org.apache.fluss.server.index.TestingIndexTable.upsertRequest;
import static org.apache.fluss.server.index.TestingIndexTable.writeSource;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Joint KV, application progress, local WAL, and remote WAL recovery coverage for Index Tables. */
class IndexTargetRecoveryITCase {

    private static final String DB = "test_db";
    private static final String MAIN_TABLE = "index_target_recovery";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    @Test
    void testLiveFollowerReplaysCommittedSnapshotToLogEndBeforePromotion() throws Exception {
        TablePath mainPath = TablePath.of(DB, MAIN_TABLE);
        Schema mainSchema = TestingIndexTable.indexedSchema(IndexVisibility.SYNC, 1);
        TableDescriptor mainDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(1, "a").build();
        TestingIndexTable table =
                new TestingIndexTable(
                        FLUSS_CLUSTER_EXTENSION, mainPath, mainDescriptor, INDEX_NAME);
        long mainTableId = table.mainTableId;
        long indexTableId = table.indexTableId;
        TableBucket indexBucket = new TableBucket(indexTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(indexBucket);
        TableBucket mainBucket = new TableBucket(mainTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(mainBucket);
        Replica mainLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        int mainLeaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);
        TabletServerGateway mainGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(mainLeaderId);
        IndexReplicationEncoder indexEncoder = table.indexEncoder(mainBucket);

        int originalLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexBucket);
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Optional<TableAssignment> assignment = zkClient.getTableAssignment(indexTableId);
        assertThat(assignment).isPresent();
        List<Integer> replicas = assignment.get().getBucketAssignment(0).getReplicas();
        int followerToPromote =
                replicas.stream()
                        .filter(id -> id != originalLeader)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        ReplicaManager followerManager =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(followerToPromote).getReplicaManager();
        followerManager
                .getReplicaFetcherManager()
                .removeFetcherForBuckets(Collections.singleton(indexBucket));
        Replica pausedFollower = followerManager.getReplicaOrException(indexBucket);
        long pausedFollowerOffset = pausedFollower.getLogTablet().localLogEndOffset();

        writeSource(mainGateway, upsertRequest(mainBucket, 1, "before-update"));
        writeSource(mainGateway, upsertRequest(mainBucket, 2, "removed-after-snapshot"));

        writeSource(mainGateway, upsertRequest(mainBucket, 4, "snapshot-only"));
        long kvSnapshotOffset =
                FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(indexBucket).getLogOffset();
        assertThat(pausedFollowerOffset).isLessThan(kvSnapshotOffset);

        Replica originalReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(indexBucket);
        writeSource(mainGateway, upsertRequest(mainBucket, 1, "after-update"));
        originalReplica.getLogTablet().roll(Optional.empty());
        writeSource(mainGateway, deleteRequest(mainBucket, 2));
        originalReplica.getLogTablet().roll(Optional.empty());
        writeSource(mainGateway, upsertRequest(mainBucket, 3, "inserted-after-snapshot"));
        originalReplica.getLogTablet().roll(Optional.empty());

        long expectedFence = mainLeader.getSyncIndexPushedOffset();

        LogTablet originalLog = originalReplica.getLogTablet();
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(indexBucket);
        waitUntil(
                () ->
                        originalLog.canFetchFromRemoteLog(pausedFollowerOffset)
                                && originalLog.localLogStartOffset() > pausedFollowerOffset,
                Duration.ofMinutes(2),
                "wait for target WAL beyond the live follower to be tiered and deleted");

        followerManager
                .getReplicaFetcherManager()
                .addFetcherForBuckets(
                        Collections.singletonMap(
                                indexBucket,
                                new InitialFetchStatus(
                                        indexTableId,
                                        table.indexPath,
                                        originalLeader,
                                        pausedFollowerOffset)));
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaExpandToIsr(indexBucket, followerToPromote);
        Replica recoveredReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(indexBucket, followerToPromote);
        assertThat(recoveredReplica.getLogTablet().localLogStartOffset())
                .isGreaterThan(pausedFollowerOffset);

        LeaderAndIsr current = FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(indexBucket);
        LeaderAndIsr promoted =
                new LeaderAndIsr(
                        followerToPromote,
                        current.leaderEpoch() + 1,
                        current.isr(),
                        Collections.emptyList(),
                        current.coordinatorEpoch(),
                        current.bucketEpoch() + 1);
        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                followerToPromote, table.indexPath, indexBucket, promoted, replicas);
        waitUntil(
                recoveredReplica::isLeader,
                Duration.ofMinutes(3),
                "wait for production promotion and synchronous KV replay");

        assertThat(
                        recoveredReplica
                                .lookups(
                                        Collections.singletonList(
                                                indexEntry(indexEncoder, 4, "snapshot-only").key()))
                                .get(0))
                .isNotNull();
        byte[] oldUpdatedKey = indexEntry(indexEncoder, 1, "before-update").key();
        byte[] updatedKey = indexEntry(indexEncoder, 1, "after-update").key();
        byte[] deletedKey = indexEntry(indexEncoder, 2, "removed-after-snapshot").key();
        byte[] insertedKey = indexEntry(indexEncoder, 3, "inserted-after-snapshot").key();
        List<ByteArraySlice> recoveredValues =
                recoveredReplica.lookups(
                        Arrays.asList(oldUpdatedKey, updatedKey, deletedKey, insertedKey));
        assertThat(recoveredValues.get(0)).isNull();
        assertThat(recoveredValues.get(1)).isNotNull();
        assertThat(recoveredValues.get(2)).isNull();
        assertThat(recoveredValues.get(3)).isNotNull();

        byte[] insertedValueBefore = recoveredValues.get(3).toByteArray();
        long walEndBefore = recoveredReplica.getLogTablet().localLogEndOffset();

        TabletServerGateway promotedGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(followerToPromote);
        PutIndexResponse replay =
                promotedGateway
                        .putIndex(
                                table.mutationRequest(
                                        indexEncoder,
                                        mainBucket,
                                        0,
                                        expectedFence,
                                        insertedKey,
                                        indexEntry(indexEncoder, 3, "inserted-after-snapshot")
                                                .value()
                                                .copy()))
                        .get();
        assertThat(replay.getBucketsRespsList())
                .singleElement()
                .satisfies(bucket -> assertThat(bucket.hasErrorCode()).isFalse());

        assertThat(recoveredReplica.getLogTablet().localLogEndOffset()).isEqualTo(walEndBefore);
        assertThat(
                        recoveredReplica
                                .lookups(Collections.singletonList(insertedKey))
                                .get(0)
                                .toByteArray())
                .isEqualTo(insertedValueBefore);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_BUCKET_NUMBER, 1);
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("100b"));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.setInt(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS, 1);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        return conf;
    }
}
