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
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.rpc.protocol.UpdateMetadataRequestCompat;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getUpdateMetadataRequestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTablePath;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link CoordinatorRequestBatch}. */
class CoordinatorRequestBatchTest {

    private CoordinatorContext coordinatorContext;

    @BeforeEach
    void beforeEach() {
        coordinatorContext = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
    }

    /**
     * When the NotifyLeaderAndIsr request to the actual leader (leader == serverId) fails, the
     * pending leader activation entry that this request added must be cleared via an
     * AccessContextEvent dispatched on the event manager.
     */
    @Test
    void testNotifyLeaderAndIsrSendFailureClearsLeaderPending() {
        long tableId = 100L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t1");

        coordinatorContext.putTablePath(tableId, tablePath);
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
                        failingChannel, eventManager, coordinatorContext, ignored -> null);
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

    /**
     * When the NotifyLeaderAndIsr request to a follower (leader != serverId) fails, the failure
     * callback must NOT clear any pending leader activation entry. Specifically, it must not remove
     * entries added by another in-flight sender targeting the actual leader.
     */
    @Test
    void testNotifyLeaderAndIsrSendFailureToFollowerDoesNotClearOtherPending() {
        long tableId = 200L;
        TableBucket followerTb = new TableBucket(tableId, 0);
        TableBucket otherLeaderTb = new TableBucket(tableId, 1);
        TablePath tablePath = TablePath.of("db1", "t2");

        coordinatorContext.putTablePath(tableId, tablePath);
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
                        failingChannel, eventManager, coordinatorContext, ignored -> null);
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
    void testNotifyLeaderAndIsrAllowsMissingRoutingState() {
        long tableId = 300L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t3");
        coordinatorContext.putTablePath(tableId, tablePath);
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
                        ignored -> null);

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
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableDeletionEncodingForMixedVersionTabletServers(boolean tableInfoPresent)
            throws Exception {
        long tableId = 100L;
        TablePath path = TablePath.of("db", "orders");
        if (tableInfoPresent) {
            putTableInfo(tableId, path);
        }
        coordinatorContext.queueTableDeletion(Collections.singleton(tableId));

        UpdateMetadataRequest request = sendMetadataUpdate(tableId, null);
        UpdateMetadataRequest legacy = roundTrip(UpdateMetadataRequestCompat.toVersion0(request));
        PbTableMetadata v0Table = legacy.getTableMetadataAt(0);
        TablePath expectedPath = tableInfoPresent ? path : DELETED_TABLE_PATH;
        long legacyId = tableInfoPresent ? DELETED_TABLE_ID : tableId;
        assertThat(v0Table.hasDeleted()).isFalse();
        assertThat(v0Table.getTableId()).isEqualTo(legacyId);
        assertThat(toTablePath(v0Table.getTablePath())).isEqualTo(expectedPath);
        TableInfo legacyDeletion =
                getUpdateMetadataRequestData(legacy).getTableMetadataList().get(0).getTableInfo();
        assertThat(legacyDeletion.getTableId()).isEqualTo(legacyId);
        assertThat(legacyDeletion.getTablePath()).isEqualTo(expectedPath);

        // The coordinator reuses the same request for the next tablet server.
        UpdateMetadataRequest current = roundTrip(request);
        PbTableMetadata v1Table = current.getTableMetadataAt(0);
        assertThat(v1Table.isDeleted()).isTrue();
        assertThat(v1Table.getTableId()).isEqualTo(tableId);
        assertThat(toTablePath(v1Table.getTablePath())).isEqualTo(expectedPath);
        TableInfo deletion =
                getUpdateMetadataRequestData(current).getTableMetadataList().get(0).getTableInfo();
        assertThat(deletion.getTableId()).isEqualTo(tableId);
        assertThat(deletion.getTablePath()).isEqualTo(expectedPath);
    }

    private static UpdateMetadataRequest roundTrip(UpdateMetadataRequest request) {
        UpdateMetadataRequest received = new UpdateMetadataRequest();
        received.parseFrom(request.toByteArray());
        return received;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionDeletionPreservesOriginalId(boolean nameInContext) throws Exception {
        long tableId = 100L;
        long partitionId = 200L;
        TablePath path = TablePath.of("db", "orders");
        putTableInfo(tableId, path);
        if (nameInContext) {
            ResolvedPartitionSpec spec =
                    new ResolvedPartitionSpec(
                            Collections.singletonList("region"), Collections.singletonList("west"));
            coordinatorContext.putPartition(
                    partitionId,
                    PhysicalTablePath.of(path, spec.getPartitionName()),
                    new PartitionInfo(partitionId, spec, DEFAULT_REMOTE_DATA_DIR, 1));
        }
        coordinatorContext.queuePartitionDeletion(
                Collections.singleton(new TablePartition(tableId, partitionId)));

        ClusterMetadata update =
                getUpdateMetadataRequestData(roundTrip(sendMetadataUpdate(tableId, partitionId)));
        assertThat(update.getPartitionMetadataList())
                .singleElement()
                .satisfies(
                        deleted -> {
                            assertThat(deleted.getTableId()).isEqualTo(tableId);
                            assertThat(deleted.getPartitionId()).isEqualTo(partitionId);
                            assertThat(deleted.getPartitionName())
                                    .isEqualTo(DELETED_PARTITION_NAME);
                        });
        // A delayed partition deletion must not re-publish a previously dropped parent table.
        assertThat(update.getTableMetadataList()).isEmpty();
    }

    private UpdateMetadataRequest sendMetadataUpdate(long tableId, @Nullable Long partitionId)
            throws Exception {
        AtomicReference<UpdateMetadataRequest> sent = new AtomicReference<>();
        TestCoordinatorChannelManager channel =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendUpdateMetadataRequest(
                            int serverId,
                            UpdateMetadataRequest request,
                            BiConsumer<UpdateMetadataResponse, ? super Throwable> callback) {
                        sent.set(request);
                        callback.accept(new UpdateMetadataResponse(), null);
                    }
                };
        try {
            CoordinatorRequestBatch batch =
                    new CoordinatorRequestBatch(
                            channel,
                            newSynchronousAccessContextEventManager(),
                            coordinatorContext,
                            ignored -> null);
            batch.addUpdateMetadataRequestForTabletServers(
                    Collections.singleton(0), tableId, partitionId, Collections.emptySet());
            batch.sendUpdateMetadataRequest();
            return sent.get();
        } finally {
            channel.close();
        }
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
