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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the {@link CoordinatorRequestBatch}. */
class CoordinatorRequestBatchTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperClient zookeeperClient;
    private CoordinatorContext coordinatorContext;

    @BeforeEach
    void beforeEach() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        coordinatorContext = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
    }

    /**
     * When the NotifyLeaderAndIsr request to the actual leader (leader == serverId) fails, the
     * pending leader activation entry that this request added must be cleared via an
     * AccessContextEvent dispatched on the event manager.
     */
    @Test
    void testNotifyLeaderAndIsrSendFailureClearsLeaderPending() throws Exception {
        long tableId = 100L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t1");

        coordinatorContext.putTablePath(tableId, tablePath);
        zookeeperClient.registerTable(
                tablePath,
                TableRegistration.newTable(
                        tableId, DEFAULT_REMOTE_DATA_DIR, DATA1_TABLE_DESCRIPTOR));
        putTableInfo(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));
        coordinatorContext.updateBucketReplicaAssignment(tb, Collections.singletonList(0));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(tb, leaderAndIsr);

        TestCoordinatorChannelManager failingChannel = newAlwaysFailingChannelManager();
        EventManager eventManager = newSynchronousAccessContextEventManager();

        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        failingChannel, eventManager, coordinatorContext, zookeeperClient);
        // server 0 IS the leader, so this request will mark tb pending.
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(0),
                PhysicalTablePath.of(tablePath),
                tb,
                Collections.singletonList(0),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        // The failure callback must clear the pending entry that this request added.
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
    }

    @Test
    void testNotifyLeaderAndIsrSendFailurePreservesOverlappingSameBucketPending() throws Exception {
        long tableId = 150L;
        TableBucket tableBucket = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "overlapping_pending");

        coordinatorContext.putTablePath(tableId, tablePath);
        zookeeperClient.registerTable(
                tablePath,
                TableRegistration.newTable(
                        tableId, DEFAULT_REMOTE_DATA_DIR, DATA1_TABLE_DESCRIPTOR));
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));
        coordinatorContext.updateBucketReplicaAssignment(tableBucket, Collections.singletonList(0));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(tableBucket, leaderAndIsr);

        // An earlier in-flight role request already owns one pending activation for this bucket.
        coordinatorContext.addPendingLeaderActivation(tableBucket);

        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        newAlwaysFailingChannelManager(),
                        newSynchronousAccessContextEventManager(),
                        coordinatorContext,
                        zookeeperClient);
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(0),
                PhysicalTablePath.of(tablePath),
                tableBucket,
                Collections.singletonList(0),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        assertThat(coordinatorContext.getPendingLeaderActivationBuckets())
                .containsExactly(tableBucket);
        assertThat(coordinatorContext.isLeaderActive(tableBucket)).isFalse();
    }

    /**
     * When the NotifyLeaderAndIsr request to a follower (leader != serverId) fails, the failure
     * callback must NOT clear any pending leader activation entry. Specifically, it must not remove
     * entries added by another in-flight sender targeting the actual leader.
     */
    @Test
    void testNotifyLeaderAndIsrSendFailureToFollowerDoesNotClearOtherPending() throws Exception {
        long tableId = 200L;
        TableBucket followerTb = new TableBucket(tableId, 0);
        TableBucket otherLeaderTb = new TableBucket(tableId, 1);
        TablePath tablePath = TablePath.of("db1", "t2");

        coordinatorContext.putTablePath(tableId, tablePath);
        zookeeperClient.registerTable(
                tablePath,
                TableRegistration.newTable(
                        tableId, DEFAULT_REMOTE_DATA_DIR, DATA1_TABLE_DESCRIPTOR));
        putTableInfo(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Arrays.asList(0, 1)));
        coordinatorContext.updateBucketReplicaAssignment(followerTb, Arrays.asList(0, 1));
        coordinatorContext.updateBucketReplicaAssignment(otherLeaderTb, Arrays.asList(0, 1));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Arrays.asList(0, 1), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(followerTb, leaderAndIsr);

        // Simulate another in-flight sender having already marked otherLeaderTb pending - we
        // must not touch this entry on the unrelated follower-send failure.
        coordinatorContext.addPendingLeaderActivation(otherLeaderTb);

        TestCoordinatorChannelManager failingChannel = newAlwaysFailingChannelManager();
        EventManager eventManager = newSynchronousAccessContextEventManager();

        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        failingChannel, eventManager, coordinatorContext, zookeeperClient);
        // Send to server 1, which is a follower for followerTb (leader is 0). Because
        // leader != serverId, this request does NOT add followerTb to pending, so the failure
        // callback must short-circuit without dispatching an AccessContextEvent.
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(1),
                PhysicalTablePath.of(tablePath),
                followerTb,
                Arrays.asList(0, 1),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        // The unrelated leader's pending entry must remain intact.
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets())
                .containsExactly(otherLeaderTb);
    }

    @Test
    void testNotifyLeaderAndIsrAllowsMissingRoutingState() throws Exception {
        long tableId = 300L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t3");
        coordinatorContext.putTablePath(tableId, tablePath);
        zookeeperClient.registerTable(
                tablePath,
                TableRegistration.newTable(
                        tableId, DEFAULT_REMOTE_DATA_DIR, DATA1_TABLE_DESCRIPTOR));
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(tb, leaderAndIsr);

        AtomicReference<NotifyLeaderAndIsrRequest> sentRequest = new AtomicReference<>();
        TestCoordinatorChannelManager channelManager =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendBucketLeaderAndIsrRequest(
                            int receiveServerId,
                            NotifyLeaderAndIsrRequest request,
                            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable>
                                    responseConsumer) {
                        sentRequest.set(request);
                        responseConsumer.accept(
                                null, new NetworkException("simulated send failure for test"));
                    }
                };
        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        channelManager,
                        newSynchronousAccessContextEventManager(),
                        coordinatorContext,
                        zookeeperClient);

        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(0),
                PhysicalTablePath.of(tablePath),
                tb,
                Collections.singletonList(0),
                leaderAndIsr);

        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
        batch.sendRequestToTabletServers(0);

        assertThat(sentRequest.get()).isNotNull();
        assertThat(sentRequest.get().getNotifyBucketsLeaderReqsList()).hasSize(1);
        PbNotifyLeaderAndIsrReqForBucket bucketRequest =
                sentRequest.get().getNotifyBucketsLeaderReqsList().get(0);
        assertThat(bucketRequest.hasBucketCount()).isFalse();
        assertThat(bucketRequest.hasBucketCountEpoch()).isFalse();
        assertThat(bucketRequest.hasMetadataVersion()).isTrue();
        assertThat(bucketRequest.getMetadataVersion()).isZero();
        assertThat(bucketRequest.hasDataState()).isTrue();
        assertThat(bucketRequest.getDataState()).isEqualTo(BulkLoadDataState.ACTIVE.getCode());
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDoesNotNotifyDeletedOrRecreatedTarget(boolean recreated) throws Exception {
        long tableId = 400L;
        TablePath tablePath = TablePath.of("db1", "missing_target_" + recreated);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        zookeeperClient.registerTable(
                tablePath,
                TableRegistration.newTable(
                        tableId, DEFAULT_REMOTE_DATA_DIR, DATA1_TABLE_DESCRIPTOR));
        zookeeperClient.deleteTable(tablePath);
        if (recreated) {
            zookeeperClient.registerTable(
                    tablePath,
                    TableRegistration.newTable(
                            tableId + 1, DEFAULT_REMOTE_DATA_DIR, DATA1_TABLE_DESCRIPTOR));
        }
        coordinatorContext.putTablePath(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        AtomicInteger notifications = new AtomicInteger();
        TestCoordinatorChannelManager channelManager =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendBucketLeaderAndIsrRequest(
                            int receiveServerId,
                            NotifyLeaderAndIsrRequest request,
                            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable>
                                    responseConsumer) {
                        notifications.incrementAndGet();
                    }
                };
        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        channelManager,
                        newSynchronousAccessContextEventManager(),
                        coordinatorContext,
                        zookeeperClient);
        Runnable addNotification =
                () ->
                        batch.addNotifyLeaderRequestForTabletServers(
                                Collections.singleton(0),
                                PhysicalTablePath.of(tablePath),
                                tableBucket,
                                Collections.singletonList(0),
                                leaderAndIsr);
        if (recreated) {
            assertThatThrownBy(addNotification::run)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to load exact target metadata");
        } else {
            addNotification.run();
        }
        batch.sendRequestToTabletServers(0);

        assertThat(notifications).hasValue(0);
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
    }

    /** Registers table metadata so normal notifications carry the bucket layout epoch. */
    private void putTableInfo(long tableId, TablePath tablePath) {
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        tablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
    }

    @Test
    void testDiscardIncompleteBatchStartsWithNoPendingRequests() {
        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        new TestCoordinatorChannelManager(),
                        event -> {},
                        coordinatorContext,
                        zookeeperClient);
        TableBucket tableBucket = new TableBucket(3L, 0);
        coordinatorContext.updateBucketReplicaAssignment(tableBucket, Collections.singletonList(1));

        batch.addStopReplicaRequestForTabletServers(
                Collections.singleton(1), tableBucket, false, false, 4);
        batch.addUpdateMetadataRequestForTabletServers(
                Collections.singleton(1), 3L, null, Collections.emptySet());
        assertThatThrownBy(batch::newBatch).isInstanceOf(IllegalStateException.class);

        batch.discardIncompleteBatch();

        batch.newBatch();
        assertThat(coordinatorContext.getAssignment(tableBucket)).containsExactly(1);
        batch.addStopReplicaRequestForTabletServers(
                Collections.singleton(1), tableBucket, true, true, 4);
    }

    private static TestCoordinatorChannelManager newAlwaysFailingChannelManager() {
        return new TestCoordinatorChannelManager() {
            @Override
            public void sendBucketLeaderAndIsrRequest(
                    int receiveServerId,
                    NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest,
                    BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable> responseConsumer) {
                responseConsumer.accept(
                        null, new NetworkException("simulated send failure for test"));
            }
        };
    }

    /**
     * Returns an EventManager that synchronously executes {@link AccessContextEvent}s against the
     * test's CoordinatorContext, mimicking the serial event-thread semantics.
     */
    private EventManager newSynchronousAccessContextEventManager() {
        return event -> {
            if (event instanceof AccessContextEvent) {
                AccessContextEvent<?> accessContextEvent = (AccessContextEvent<?>) event;
                accessContextEvent.getAccessFunction().apply(coordinatorContext);
            }
        };
    }
}
