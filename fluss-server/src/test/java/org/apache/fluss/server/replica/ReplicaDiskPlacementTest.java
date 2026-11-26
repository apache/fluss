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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.entity.StopReplicaData;
import org.apache.fluss.server.entity.StopReplicaResultForBucket;
import org.apache.fluss.server.testutils.ServerTestTags;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.testutils.common.ScheduledTask;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/** Tests local placement of primary-key replicas across multiple data directories. */
@Tag(ServerTestTags.JBOD_MULTI_DIR_TAG)
class ReplicaDiskPlacementTest extends ReplicaTestBase {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "disk_placement");
    private static final PhysicalTablePath PHYSICAL_PATH = PhysicalTablePath.of(TABLE_PATH);
    private static final long TABLE_ID = 100;
    private static final int BUCKET_COUNT = 16;

    private File dataDir1;
    private File dataDir2;

    @Override
    protected FlussScheduler createTestScheduler() {
        return new FlussScheduler(2) {
            @Override
            public ScheduledFuture<?> schedule(
                    String name, Runnable task, long delayMs, long periodMs) {
                if ("disk-usage-monitor".equals(name)) {
                    // The test supplies disk usage samples explicitly.
                    return new ScheduledTask<>(Executors.callable(task), delayMs, periodMs);
                }
                return super.schedule(name, task, delayMs, periodMs);
            }
        };
    }

    @BeforeEach
    void registerTable() throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .distributedBy(BUCKET_COUNT)
                        .build();
        zkClient.registerTable(
                TABLE_PATH,
                TableRegistration.newTable(
                        TABLE_ID, conf.get(ConfigOptions.REMOTE_DATA_DIR), descriptor));
        zkClient.registerFirstSchema(TABLE_PATH, DATA1_SCHEMA_PK);
        dataDir1 = localDiskManager.dataDirs().get(0);
        dataDir2 = localDiskManager.dataDirs().get(1);
    }

    @Test
    void testBatchBalancesKvLeadersDespiteUnevenReplicaTotals() throws Exception {
        TableBucket follower1 = bucket(0);
        TableBucket removedFollower1 = bucket(1);
        TableBucket follower2 = bucket(2);
        TableBucket removedFollower2 = bucket(3);
        notifyReplicaRoles(
                followerNotification(follower1),
                followerNotification(removedFollower1),
                followerNotification(follower2),
                followerNotification(removedFollower2));
        stopReplica(removedFollower1, INITIAL_LEADER_EPOCH);
        stopReplica(removedFollower2, INITIAL_LEADER_EPOCH);

        TableBucket leader1 = bucket(4);
        TableBucket leader2 = bucket(5);
        notifyReplicaRoles(leaderNotification(leader1), leaderNotification(leader2));
        assertThat(replica(leader1).getLogTablet().getDataDir()).isEqualTo(dataDir2);
        assertThat(replica(leader2).getLogTablet().getDataDir()).isEqualTo(dataDir1);

        // The disk at dataDir2 has fewer replicas and now hosts the only leader.
        assertThat(notifyReplicaRoles(followerNotification(leader2, INITIAL_LEADER_EPOCH + 1)))
                .containsExactly(new NotifyLeaderAndIsrResultForBucket(leader2));

        TableBucket newFollower = bucket(6);
        TableBucket newStandby = bucket(7);
        notifyReplicaRoles(
                followerNotification(newFollower),
                notification(
                        newStandby,
                        NO_LEADER,
                        INITIAL_LEADER_EPOCH,
                        Collections.singletonList(TABLET_SERVER_ID)));
        assertThat(replica(newFollower).getLogTablet().getDataDir()).isEqualTo(dataDir2);
        assertThat(replica(newStandby).getLogTablet().getDataDir()).isEqualTo(dataDir2);
    }

    @Test
    void testWriteLockedKvLeaderBatchBalancesLogsBeforePromotion() throws Exception {
        TableBucket activeLeader = bucket(0);
        assertThat(notifyReplicaRoles(leaderNotification(activeLeader)))
                .containsExactly(new NotifyLeaderAndIsrResultForBucket(activeLeader));

        conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 0.85);
        conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO, 0.80);
        localDiskManager.reconfigure(conf);
        localDiskManager.getDiskUsageMonitor().update(0.99);

        TableBucket lockedBucket1 = bucket(1);
        TableBucket lockedBucket2 = bucket(2);
        assertThat(
                        notifyReplicaRoles(
                                leaderNotification(lockedBucket1),
                                leaderNotification(lockedBucket2)))
                .extracting(
                        NotifyLeaderAndIsrResultForBucket::getTableBucket,
                        result -> result.getError().error())
                .containsExactlyInAnyOrder(
                        tuple(lockedBucket1, Errors.DISK_WRITE_LOCKED),
                        tuple(lockedBucket2, Errors.DISK_WRITE_LOCKED));
        assertThat(replica(lockedBucket1).getLogTablet().getDataDir()).isEqualTo(dataDir2);
        assertThat(replica(lockedBucket2).getLogTablet().getDataDir()).isEqualTo(dataDir1);

        localDiskManager.getDiskUsageMonitor().update(0.50);
        assertThat(
                        notifyReplicaRoles(
                                leaderNotification(lockedBucket2),
                                leaderNotification(lockedBucket1)))
                .containsExactlyInAnyOrder(
                        new NotifyLeaderAndIsrResultForBucket(lockedBucket1),
                        new NotifyLeaderAndIsrResultForBucket(lockedBucket2));
        assertThat(replica(lockedBucket1).getLogTablet().getDataDir()).isEqualTo(dataDir2);
        assertThat(replica(lockedBucket2).getLogTablet().getDataDir()).isEqualTo(dataDir1);
        assertThat(Arrays.asList(replica(lockedBucket1), replica(lockedBucket2)))
                .allMatch(Replica::isLeader);
    }

    @Test
    void testRoleChangesAndRepeatedNotificationsUpdatePlacementOnce() throws Exception {
        TableBucket leader1 = bucket(0);
        TableBucket leader2 = bucket(1);
        TableBucket changingReplica = bucket(2);
        TableBucket otherFollower = bucket(3);
        notifyReplicaRoles(
                leaderNotification(leader1),
                leaderNotification(leader2),
                followerNotification(changingReplica),
                followerNotification(otherFollower));

        NotifyLeaderAndIsrData promotion = publishPromotion(changingReplica);
        assertThat(notifyReplicaRoles(promotion, promotion))
                .containsExactly(new NotifyLeaderAndIsrResultForBucket(changingReplica));
        assertThat(replica(changingReplica).getLogTablet().getDataDir()).isEqualTo(dataDir1);

        TableBucket leaderAfterPromotion = bucket(4);
        TableBucket nextLeader = bucket(5);
        notifyReplicaRoles(
                leaderNotification(leaderAfterPromotion), leaderNotification(nextLeader));
        assertThat(replica(leaderAfterPromotion).getLogTablet().getDataDir()).isEqualTo(dataDir2);
        assertThat(replica(nextLeader).getLogTablet().getDataDir()).isEqualTo(dataDir1);

        NotifyLeaderAndIsrData demotion =
                followerNotification(changingReplica, promotion.getLeaderEpoch() + 1);
        assertThat(notifyReplicaRoles(demotion))
                .containsExactly(new NotifyLeaderAndIsrResultForBucket(changingReplica));
        TableBucket leaderAfterDemotion = bucket(6);
        notifyReplicaRoles(leaderNotification(leaderAfterDemotion));
        assertThat(replica(leaderAfterDemotion).getLogTablet().getDataDir()).isEqualTo(dataDir1);

        stopReplica(leaderAfterDemotion, INITIAL_LEADER_EPOCH);
        TableBucket replacementLeader = bucket(7);
        notifyReplicaRoles(leaderNotification(replacementLeader));
        assertThat(replica(replacementLeader).getLogTablet().getDataDir()).isEqualTo(dataDir1);
    }

    @Test
    void testFailedPromotionReservationIsReleasedAndRebuiltOnRetry() throws Exception {
        TableBucket blockedPromotion = bucket(0);
        TableBucket otherFollower = bucket(1);
        notifyReplicaRoles(
                followerNotification(blockedPromotion), followerNotification(otherFollower));
        Path blockingFile =
                FlussPaths.kvTabletDir(dataDir1, PHYSICAL_PATH, blockedPromotion).toPath();
        Files.createFile(blockingFile);

        TableBucket newLeader = bucket(2);
        NotifyLeaderAndIsrData promotion = publishPromotion(blockedPromotion);
        assertThat(notifyReplicaRoles(leaderNotification(newLeader), promotion))
                .extracting(
                        NotifyLeaderAndIsrResultForBucket::getTableBucket,
                        result -> result.getError().error())
                .containsExactlyInAnyOrder(
                        tuple(blockedPromotion, Errors.KV_STORAGE_EXCEPTION),
                        tuple(newLeader, Errors.NONE));
        assertThat(replica(newLeader).getLogTablet().getDataDir()).isEqualTo(dataDir2);

        TableBucket extraFollower = bucket(3);
        notifyReplicaRoles(followerNotification(extraFollower));
        stopReplica(otherFollower, INITIAL_LEADER_EPOCH);
        TableBucket nextLeader = bucket(4);
        notifyReplicaRoles(leaderNotification(nextLeader));
        assertThat(replica(nextLeader).getLogTablet().getDataDir()).isEqualTo(dataDir1);

        stopReplica(nextLeader, INITIAL_LEADER_EPOCH);
        Files.delete(blockingFile);

        TableBucket leaderDuringRetry = bucket(5);
        assertThat(notifyReplicaRoles(leaderNotification(leaderDuringRetry), promotion))
                .containsExactlyInAnyOrder(
                        new NotifyLeaderAndIsrResultForBucket(blockedPromotion),
                        new NotifyLeaderAndIsrResultForBucket(leaderDuringRetry));
        assertThat(replica(blockedPromotion).isLeader()).isTrue();
        assertThat(replica(leaderDuringRetry).getLogTablet().getDataDir()).isEqualTo(dataDir2);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void testFailedDemotionAfterKvDetachAffectsPlacement() throws Exception {
        TableBucket blockedDemotion = bucket(0);
        TableBucket otherLeader = bucket(1);
        TableBucket follower = bucket(2);
        notifyReplicaRoles(
                leaderNotification(blockedDemotion),
                leaderNotification(otherLeader),
                followerNotification(follower));

        // Moving the open RocksDB directory lets us fail filesystem cleanup after detaching KV.
        Path tabletDir = replica(blockedDemotion).getKvTablet().getKvTabletDir().toPath();
        Files.move(tabletDir, tempDir.toPath().resolve("moved-kv-tablet"));
        Files.createFile(tabletDir);

        assertThat(
                        notifyReplicaRoles(
                                followerNotification(blockedDemotion, INITIAL_LEADER_EPOCH + 1)))
                .extracting(
                        NotifyLeaderAndIsrResultForBucket::getTableBucket,
                        result -> result.getError().error())
                .containsExactly(tuple(blockedDemotion, Errors.KV_STORAGE_EXCEPTION));
        assertThat(kvManager.getKv(blockedDemotion)).isEmpty();

        TableBucket newLeader = bucket(3);
        notifyReplicaRoles(leaderNotification(newLeader));
        assertThat(replica(newLeader).getLogTablet().getDataDir()).isEqualTo(dataDir1);
    }

    @Test
    void testBatchReservesPromotionForExistingLogBeforeNewPlacement() throws Exception {
        TableBucket loadedLeader = bucket(0);
        TableBucket loadedFollower = bucket(1);
        createUnhostedLog(dataDir1, loadedLeader);
        createUnhostedLog(dataDir2, loadedFollower);

        TableBucket newLeader1 = bucket(2);
        TableBucket newLeader2 = bucket(3);
        NotifyLeaderAndIsrData promoteLoadedLog = leaderNotification(loadedLeader);
        notifyReplicaRoles(
                leaderNotification(newLeader1),
                promoteLoadedLog,
                promoteLoadedLog,
                leaderNotification(newLeader2));
        assertThat(replica(loadedLeader).getLogTablet().getDataDir()).isEqualTo(dataDir1);
        assertThat(replica(newLeader1).getLogTablet().getDataDir()).isEqualTo(dataDir2);
        assertThat(replica(newLeader2).getLogTablet().getDataDir()).isEqualTo(dataDir1);
    }

    @Test
    void testSweepingOrphanFollowerAffectsPlacement() throws Exception {
        TableBucket orphan = bucket(0);
        createUnhostedLog(dataDir2, orphan);
        TableBucket existingFollower = bucket(1);
        notifyReplicaRoles(followerNotification(existingFollower));

        stopReplica(orphan, INITIAL_LEADER_EPOCH);

        TableBucket newFollower = bucket(2);
        notifyReplicaRoles(followerNotification(newFollower));
        assertThat(replica(newFollower).getLogTablet().getDataDir()).isEqualTo(dataDir2);
    }

    private NotifyLeaderAndIsrData leaderNotification(TableBucket bucket) {
        return notification(
                bucket, TABLET_SERVER_ID, INITIAL_LEADER_EPOCH, Collections.emptyList());
    }

    private NotifyLeaderAndIsrData publishPromotion(TableBucket bucket) throws Exception {
        NotifyLeaderAndIsrData promotion =
                notification(
                        bucket,
                        TABLET_SERVER_ID,
                        INITIAL_LEADER_EPOCH + 1,
                        Collections.emptyList());
        zkClient.registerLeaderAndIsr(
                bucket, promotion.getLeaderAndIsr(), ZkVersion.MATCH_ANY_VERSION.getVersion());
        return promotion;
    }

    private NotifyLeaderAndIsrData followerNotification(TableBucket bucket) {
        return followerNotification(bucket, INITIAL_LEADER_EPOCH);
    }

    private NotifyLeaderAndIsrData followerNotification(TableBucket bucket, int epoch) {
        return notification(bucket, NO_LEADER, epoch, Collections.emptyList());
    }

    private NotifyLeaderAndIsrData notification(
            TableBucket bucket, int leader, int epoch, List<Integer> standbys) {
        return new NotifyLeaderAndIsrData(
                PHYSICAL_PATH,
                bucket,
                Collections.singletonList(TABLET_SERVER_ID),
                new LeaderAndIsr(
                        leader,
                        epoch,
                        leader == NO_LEADER
                                ? Collections.emptyList()
                                : Collections.singletonList(leader),
                        standbys,
                        INITIAL_COORDINATOR_EPOCH,
                        epoch),
                BUCKET_COUNT,
                0L);
    }

    private List<NotifyLeaderAndIsrResultForBucket> notifyReplicaRoles(
            NotifyLeaderAndIsrData... data) throws Exception {
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> response =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH, Arrays.asList(data), response::complete);
        return response.get();
    }

    private void stopReplica(TableBucket bucket, int leaderEpoch) throws Exception {
        CompletableFuture<List<StopReplicaResultForBucket>> response = new CompletableFuture<>();
        replicaManager.stopReplicas(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new StopReplicaData(
                                bucket, true, false, INITIAL_COORDINATOR_EPOCH, leaderEpoch)),
                response::complete);
        assertThat(response.get()).containsExactly(new StopReplicaResultForBucket(bucket));
    }

    private void createUnhostedLog(File dataDir, TableBucket bucket) throws Exception {
        logManager.getOrCreateLog(dataDir, PHYSICAL_PATH, bucket, LogFormat.ARROW, 1, true);
    }

    private static TableBucket bucket(int id) {
        return new TableBucket(TABLE_ID, id);
    }

    private Replica replica(TableBucket bucket) {
        return replicaManager.getReplicaOrException(bucket);
    }
}
