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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.ReportFrozenBucketsTieredRequest;
import org.apache.fluss.rpc.messages.ReportFrozenBucketsTieredResponse;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.lake.LakeTableHelper;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PA_2024;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests completed lake tiering and retryable, repeatable tablet-server reports. */
class FrozenBucketReportingTest extends ReplicaTestBase {
    private CoordinatorGateway reportingGateway;

    @Override
    protected Configuration getServerConf() {
        Configuration configuration = super.getServerConf();
        configuration.set(ConfigOptions.CLIENT_REQUEST_TIMEOUT, Duration.ofSeconds(1));
        return configuration;
    }

    @Override
    protected ReplicaManager buildReplicaManager(CoordinatorGateway ignored) throws Exception {
        reportingGateway = mock(CoordinatorGateway.class);
        when(reportingGateway.reportFrozenBucketsTiered(any()))
                .thenAnswer(
                        invocation ->
                                CompletableFuture.completedFuture(
                                        new ReportFrozenBucketsTieredResponse()));
        ReplicaManager manager = spy(super.buildReplicaManager(reportingGateway));
        // Trigger reporting explicitly so retries and leader changes are deterministic.
        doNothing().when(manager).startup();
        return manager;
    }

    @Test
    void testOnlyFrozenFullyTieredLeadersReportAndRepeatAfterAcknowledgement() throws Exception {
        Replica replica = createLeader();
        long endOffset = replica.getLogTablet().localLogEndOffset();
        replica.getLogTablet().updateLakeLogEndOffset(endOffset);
        replicaManager.reportFrozenBucketsTiered();
        verify(reportingGateway, never()).reportFrozenBucketsTiered(any());

        // Progress already caught up before freezing; no further lake notification is required.
        replica.freezeWrites(replica.getLeaderEpoch());
        assertThatThrownBy(
                        () -> replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1))
                .isInstanceOf(InvalidPartitionException.class);
        replicaManager.reportFrozenBucketsTiered();
        replicaManager.reportFrozenBucketsTiered();
        ArgumentCaptor<ReportFrozenBucketsTieredRequest> reports =
                ArgumentCaptor.forClass(ReportFrozenBucketsTieredRequest.class);
        verify(reportingGateway, times(2)).reportFrozenBucketsTiered(reports.capture());
        ReportFrozenBucketsTieredRequest report = reports.getValue();
        assertThat(report.getServerId()).isEqualTo(TABLET_SERVER_ID);
        assertThat(report.getCoordinatorEpoch()).isEqualTo(replicaManager.getCoordinatorEpoch());
        assertThat(report.getBucketsList()).hasSize(1);
        assertThat(toTableBucket(report.getBucketsList().get(0).getTableBucket()))
                .isEqualTo(replica.getTableBucket());
        assertThat(report.getBucketsList().get(0).getLeaderEpoch())
                .isEqualTo(replica.getLeaderEpoch());

        replica.makeFollower(
                new NotifyLeaderAndIsrData(
                        DATA1_PHYSICAL_TABLE_PATH_PA_2024,
                        replica.getTableBucket(),
                        Arrays.asList(TABLET_SERVER_ID, 2),
                        new LeaderAndIsr(
                                2,
                                replica.getLeaderEpoch() + 1,
                                Arrays.asList(TABLET_SERVER_ID, 2),
                                Collections.emptyList(),
                                replicaManager.getCoordinatorEpoch(),
                                1),
                        3,
                        0L));
        assertThat(replica.getFrozenTieredLeaderEpoch()).isEmpty();
        replicaManager.reportFrozenBucketsTiered();
        verify(reportingGateway, times(2)).reportFrozenBucketsTiered(any());
    }

    @Test
    void testWaitsForLakeOffsetAndRetriesFailedReport() throws Exception {
        Replica replica = createLeader();
        replica.freezeWrites(replica.getLeaderEpoch());
        long endOffset = replica.getLogTablet().localLogEndOffset();
        replica.getLogTablet().updateLakeLogEndOffset(endOffset - 1);
        replicaManager.reportFrozenBucketsTiered();
        verify(reportingGateway, never()).reportFrozenBucketsTiered(any());
        CompletableFuture<ReportFrozenBucketsTieredResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("coordinator unavailable"));
        when(reportingGateway.reportFrozenBucketsTiered(any()))
                .thenReturn(
                        failed,
                        CompletableFuture.completedFuture(new ReportFrozenBucketsTieredResponse()));
        replica.getLogTablet().updateLakeLogEndOffset(endOffset);
        replicaManager.reportFrozenBucketsTiered();
        replicaManager.reportFrozenBucketsTiered();
        verify(reportingGateway, times(2)).reportFrozenBucketsTiered(any());
    }

    @Test
    void testFreezeRestoresLakeProgressWithoutNotification() throws Exception {
        Replica replica = createLeader();
        updateTableConfig(replica, ConfigOptions.TABLE_DATALAKE_ENABLED, "true");
        long endOffset = replica.getLogTablet().localLogEndOffset();
        new LakeTableHelper(zkClient, remoteDataDir)
                .registerLakeTableSnapshotV1(
                        replica.getTableBucket().getTableId(),
                        new LakeTableSnapshot(
                                1L, Collections.singletonMap(replica.getTableBucket(), endOffset)));
        assertThat(replica.getLakeLogEndOffset()).isNegative();
        replicaManager.freezePartitions(
                Collections.singletonMap(replica.getTableBucket(), replica.getLeaderEpoch()),
                results -> assertThat(results).hasSize(1).allMatch(result -> !result.failed()));
        assertThat(replica.getLakeLogEndOffset()).isEqualTo(endOffset);
        replicaManager.reportFrozenBucketsTiered();
        verify(reportingGateway).reportFrozenBucketsTiered(any());
    }

    @Test
    void testBoundsInflightReportAndRetriesAfterTimeout() throws Exception {
        Replica replica = createLeader();
        replica.freezeWrites(replica.getLeaderEpoch());
        replica.getLogTablet().updateLakeLogEndOffset(replica.getLogTablet().localLogEndOffset());
        CompletableFuture<ReportFrozenBucketsTieredResponse> pending = new CompletableFuture<>();
        CompletableFuture<Void> timeoutHandled = pending.handle((response, error) -> null);
        when(reportingGateway.reportFrozenBucketsTiered(any()))
                .thenReturn(
                        pending,
                        CompletableFuture.completedFuture(new ReportFrozenBucketsTieredResponse()));
        replicaManager.reportFrozenBucketsTiered();
        replicaManager.reportFrozenBucketsTiered();
        verify(reportingGateway, times(1)).reportFrozenBucketsTiered(any());
        assertThatThrownBy(pending::join).hasCauseInstanceOf(TimeoutException.class);
        timeoutHandled.join();
        replicaManager.reportFrozenBucketsTiered();
        verify(reportingGateway, times(2)).reportFrozenBucketsTiered(any());
    }

    private Replica createLeader() throws Exception {
        TableBucket bucket = makeTableBucket(true);
        makeLogTableAsLeader(bucket, true);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        return replica;
    }
}
