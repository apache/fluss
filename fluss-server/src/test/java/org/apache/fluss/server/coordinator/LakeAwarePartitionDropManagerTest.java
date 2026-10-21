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

import org.apache.fluss.exception.CoordinatorEpochFencedException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.FreezePartitionRequest;
import org.apache.fluss.rpc.messages.FreezePartitionResponse;
import org.apache.fluss.rpc.messages.ReportFrozenBucketsTieredRequest;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromTableBucket;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFreezePartitionData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests freezing, report fencing, aggregation, and recovery without coordinator offset polling. */
class LakeAwarePartitionDropManagerTest {
    private static final TablePath TABLE_PATH = TablePath.of("db", "table");
    private static final String PARTITION_NAME = "20260806";
    private static final long TABLE_ID = 11L;
    private static final long PARTITION_ID = 22L;
    private static final int SERVER_ID = 1;
    private static final int LEADER_EPOCH = 7;
    private static final int COORDINATOR_EPOCH = 3;
    private static final TableBucket BUCKET_0 = new TableBucket(TABLE_ID, PARTITION_ID, 0);
    private static final TableBucket BUCKET_1 = new TableBucket(TABLE_ID, PARTITION_ID, 1);

    @Test
    void testFreezeDoesNotDeleteUntilAllLeadersReportTiered() throws Exception {
        try (TestContext c = new TestContext()) {
            c.freeze();
            ArgumentCaptor<FreezePartitionRequest> request =
                    ArgumentCaptor.forClass(FreezePartitionRequest.class);
            verify(c.channelManager).sendFreezePartitionRequest(eq(SERVER_ID), request.capture());
            assertThat(getFreezePartitionData(request.getValue()))
                    .containsEntry(BUCKET_0, LEADER_EPOCH)
                    .containsEntry(BUCKET_1, LEADER_EPOCH);
            assertThat(c.registration.get().isFrozen()).isTrue();
            c.report(BUCKET_0, LEADER_EPOCH);
            c.report(BUCKET_0, LEADER_EPOCH);
            assertThat(c.registration.get()).isNotNull();
            c.report(BUCKET_1, LEADER_EPOCH);
            assertThat(c.registration.get()).isNull();
            // Duplicate delivery after deletion is harmless.
            c.report(BUCKET_1, LEADER_EPOCH);
            verify(c.metadataManager, times(1))
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
            verify(c.zooKeeperClient, never()).getLakeTableSnapshot(TABLE_ID, null);
        }
    }

    @Test
    void testLeaderChangeInvalidatesPreviouslyAcceptedReport() throws Exception {
        try (TestContext c = new TestContext()) {
            c.freeze();
            c.report(BUCKET_0, LEADER_EPOCH);
            c.leaders.put(BUCKET_0, leader(LEADER_EPOCH + 1));
            c.report(BUCKET_1, LEADER_EPOCH);
            c.report(BUCKET_0, LEADER_EPOCH);
            assertThat(c.registration.get()).isNotNull();
            c.report(BUCKET_0, LEADER_EPOCH + 1);
            assertThat(c.registration.get()).isNull();
        }
    }

    @Test
    void testStaleBucketInBatchDoesNotDiscardOtherReports() throws Exception {
        try (TestContext c = new TestContext()) {
            c.freeze();
            ReportFrozenBucketsTieredRequest request = report(BUCKET_0, LEADER_EPOCH - 1);
            request.addBucket()
                    .setTableBucket(fromTableBucket(BUCKET_1))
                    .setLeaderEpoch(LEADER_EPOCH);
            c.manager.reportTiered(request, c.context);
            assertThat(c.registration.get()).isNotNull();
            c.report(BUCKET_0, LEADER_EPOCH);
            assertThat(c.registration.get()).isNull();
        }
    }

    @Test
    void testRejectsWrongServerAndCoordinatorEpoch() throws Exception {
        try (TestContext c = new TestContext()) {
            c.freeze();
            ReportFrozenBucketsTieredRequest report = report(BUCKET_0, LEADER_EPOCH);
            report.setServerId(SERVER_ID + 1);
            c.manager.reportTiered(report, c.context);
            c.report(BUCKET_1, LEADER_EPOCH);
            assertThat(c.registration.get()).isNotNull();
            report.setServerId(SERVER_ID).setCoordinatorEpoch(COORDINATOR_EPOCH - 1);
            assertThatThrownBy(() -> c.manager.reportTiered(report, c.context))
                    .isInstanceOf(CoordinatorEpochFencedException.class);
            c.report(BUCKET_0, LEADER_EPOCH);
            assertThat(c.registration.get()).isNull();
        }
    }

    @Test
    void testIgnoresUnfrozenAndRecreatedPartitions() throws Exception {
        try (TestContext c = new TestContext()) {
            c.report(BUCKET_0, LEADER_EPOCH);
            c.report(BUCKET_1, LEADER_EPOCH);
            assertThat(c.registration.get().isFrozen()).isFalse();
            c.freeze();
            c.report(BUCKET_0, LEADER_EPOCH);
            c.registration.set(
                    new PartitionRegistration(TABLE_ID, PARTITION_ID + 1, "/remote", 2)
                            .withFrozen());
            c.report(BUCKET_1, LEADER_EPOCH);
            assertThat(c.registration.get().getPartitionId()).isEqualTo(PARTITION_ID + 1);
            verify(c.metadataManager, never())
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
        }
    }

    @Test
    void testRecoveryFromReportsDoesNotNeedRetentionToRunAgain() throws Exception {
        try (TestContext c = new TestContext()) {
            // Simulate a new coordinator discovering the durable frozen state through reports.
            c.registration.set(c.registration.get().withFrozen());
            c.report(BUCKET_1, LEADER_EPOCH);
            c.report(BUCKET_0, LEADER_EPOCH);
            assertThat(c.registration.get()).isNull();
            verify(c.channelManager, never()).sendFreezePartitionRequest(any(Integer.class), any());
        }
    }

    @Test
    void testRepeatedReportRetriesFailedDeletion() throws Exception {
        try (TestContext c = new TestContext()) {
            c.freeze();
            doReturn(false, true)
                    .when(c.metadataManager)
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
            c.report(BUCKET_0, LEADER_EPOCH);
            c.report(BUCKET_1, LEADER_EPOCH);
            c.report(BUCKET_1, LEADER_EPOCH);
            verify(c.metadataManager, times(2))
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
        }
    }

    @Test
    void testFreezeFailureIsRetriedWithoutClearingFence() throws Exception {
        try (TestContext c = new TestContext()) {
            CompletableFuture<FreezePartitionResponse> failure = new CompletableFuture<>();
            failure.completeExceptionally(new RuntimeException("unavailable"));
            when(c.channelManager.sendFreezePartitionRequest(eq(SERVER_ID), any()))
                    .thenReturn(
                            failure,
                            CompletableFuture.completedFuture(new FreezePartitionResponse()));
            c.freeze();
            c.freeze();
            assertThat(c.registration.get().isFrozen()).isTrue();
            verify(c.metadataManager, times(1))
                    .markPartitionFrozen(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
            verify(c.channelManager, times(2)).sendFreezePartitionRequest(eq(SERVER_ID), any());
        }
    }

    @Test
    void testCloseIgnoresLateReportsAndFreezeRequests() throws Exception {
        try (TestContext c = new TestContext()) {
            c.freeze();
            c.manager.close();
            c.report(BUCKET_0, LEADER_EPOCH);
            c.report(BUCKET_1, LEADER_EPOCH);
            c.freeze();
            assertThat(c.registration.get()).isNotNull();
            verify(c.channelManager, times(1)).sendFreezePartitionRequest(eq(SERVER_ID), any());
        }
    }

    private static ReportFrozenBucketsTieredRequest report(TableBucket bucket, int epoch) {
        ReportFrozenBucketsTieredRequest request = new ReportFrozenBucketsTieredRequest();
        request.setServerId(SERVER_ID).setCoordinatorEpoch(COORDINATOR_EPOCH);
        request.addBucket().setTableBucket(fromTableBucket(bucket)).setLeaderEpoch(epoch);
        return request;
    }

    private static LeaderAndIsr leader(int epoch) {
        return new LeaderAndIsr(
                SERVER_ID,
                epoch,
                Collections.singletonList(SERVER_ID),
                Collections.emptyList(),
                COORDINATOR_EPOCH,
                epoch);
    }

    private static final class TestContext implements AutoCloseable {
        private final MetadataManager metadataManager = mock(MetadataManager.class);
        private final ZooKeeperClient zooKeeperClient = mock(ZooKeeperClient.class);
        private final CoordinatorChannelManager channelManager =
                mock(CoordinatorChannelManager.class);
        private final CoordinatorContext context = mock(CoordinatorContext.class);
        private final Map<TableBucket, LeaderAndIsr> leaders = new HashMap<>();
        private final AtomicReference<PartitionRegistration> registration =
                new AtomicReference<>(
                        new PartitionRegistration(TABLE_ID, PARTITION_ID, "/remote", 2));
        private final LakeAwarePartitionDropManager manager =
                new LakeAwarePartitionDropManager(
                        metadataManager, zooKeeperClient, channelManager, Runnable::run, 100L);

        private TestContext() throws Exception {
            when(metadataManager.getOptionalPartitionRegistration(TABLE_PATH, PARTITION_NAME))
                    .thenAnswer(ignored -> Optional.ofNullable(registration.get()));
            when(metadataManager.markPartitionFrozen(
                            TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID))
                    .thenAnswer(
                            ignored -> {
                                registration.set(registration.get().withFrozen());
                                return Optional.of(registration.get());
                            });
            when(metadataManager.dropFrozenPartition(
                            TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID))
                    .thenAnswer(
                            ignored -> {
                                registration.set(null);
                                return true;
                            });
            Map<Integer, BucketAssignment> assignment = new HashMap<>();
            assignment.put(0, BucketAssignment.of(SERVER_ID));
            assignment.put(1, BucketAssignment.of(SERVER_ID));
            when(zooKeeperClient.getPartitionAssignment(PARTITION_ID))
                    .thenReturn(Optional.of(new PartitionAssignment(TABLE_ID, assignment)));
            leaders.put(BUCKET_0, leader(LEADER_EPOCH));
            leaders.put(BUCKET_1, leader(LEADER_EPOCH));
            when(zooKeeperClient.getLeaderAndIsrs(anyCollection())).thenReturn(leaders);
            when(channelManager.sendFreezePartitionRequest(eq(SERVER_ID), any()))
                    .thenReturn(CompletableFuture.completedFuture(new FreezePartitionResponse()));
            when(context.getCoordinatorEpoch()).thenReturn(COORDINATOR_EPOCH);
            when(context.containsPartitionId(PARTITION_ID)).thenReturn(true);
            when(context.getPhysicalTablePath(PARTITION_ID))
                    .thenReturn(Optional.of(PhysicalTablePath.of(TABLE_PATH, PARTITION_NAME)));
            when(context.getBucketLeaderAndIsr(any()))
                    .thenAnswer(
                            invocation ->
                                    Optional.ofNullable(leaders.get(invocation.getArgument(0))));
            Map<Integer, java.util.List<Integer>> contextAssignment = new HashMap<>();
            contextAssignment.put(0, Collections.singletonList(SERVER_ID));
            contextAssignment.put(1, Collections.singletonList(SERVER_ID));
            when(context.getPartitionAssignment(new TablePartition(TABLE_ID, PARTITION_ID)))
                    .thenReturn(contextAssignment);
        }

        private void freeze() {
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                            .distributedBy(2)
                            .build();
            manager.tryDrop(
                    TableInfo.of(TABLE_PATH, TABLE_ID, 1, descriptor, "/remote", 0L, 0L),
                    PARTITION_NAME);
        }

        private void report(TableBucket bucket, int epoch) {
            manager.reportTiered(LakeAwarePartitionDropManagerTest.report(bucket, epoch), context);
        }

        @Override
        public void close() {
            manager.close();
        }
    }
}
