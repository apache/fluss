/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.exception.FencedLeaderEpochException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.AdjustIsrRequest;
import com.alibaba.fluss.rpc.messages.AdjustIsrResponse;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotRequest;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import com.alibaba.fluss.rpc.messages.CreateAclsRequest;
import com.alibaba.fluss.rpc.messages.CreateAclsResponse;
import com.alibaba.fluss.rpc.messages.CreateDatabaseRequest;
import com.alibaba.fluss.rpc.messages.CreateDatabaseResponse;
import com.alibaba.fluss.rpc.messages.CreatePartitionRequest;
import com.alibaba.fluss.rpc.messages.CreatePartitionResponse;
import com.alibaba.fluss.rpc.messages.CreateTableRequest;
import com.alibaba.fluss.rpc.messages.CreateTableResponse;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
import com.alibaba.fluss.rpc.messages.DropAclsRequest;
import com.alibaba.fluss.rpc.messages.DropAclsResponse;
import com.alibaba.fluss.rpc.messages.DropDatabaseRequest;
import com.alibaba.fluss.rpc.messages.DropDatabaseResponse;
import com.alibaba.fluss.rpc.messages.DropPartitionRequest;
import com.alibaba.fluss.rpc.messages.DropPartitionResponse;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.DropTableResponse;
import com.alibaba.fluss.rpc.messages.GetDatabaseInfoRequest;
import com.alibaba.fluss.rpc.messages.GetDatabaseInfoResponse;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotMetadataRequest;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import com.alibaba.fluss.rpc.messages.GetLatestKvSnapshotsRequest;
import com.alibaba.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import com.alibaba.fluss.rpc.messages.GetLatestLakeSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import com.alibaba.fluss.rpc.messages.GetTableInfoRequest;
import com.alibaba.fluss.rpc.messages.GetTableInfoResponse;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.GetTableSchemaResponse;
import com.alibaba.fluss.rpc.messages.ListAclsRequest;
import com.alibaba.fluss.rpc.messages.ListAclsResponse;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosRequest;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosResponse;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.messages.TableExistsResponse;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.messages.UpdateMetadataResponse;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.server.entity.AdjustIsrResultForBucket;
import com.alibaba.fluss.server.entity.CommitRemoteLogManifestData;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.RemoteLogManifestHandle;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getCommitRemoteLogManifestData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeAdjustIsrResponse;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** A {@link CoordinatorGateway} for test purpose. */
public class TestCoordinatorGateway implements CoordinatorGateway {

    private final @Nullable ZooKeeperClient zkClient;
    public final AtomicBoolean commitRemoteLogManifestFail = new AtomicBoolean(false);
    public final Map<TableBucket, Integer> currentLeaderEpoch = new HashMap<>();

    public TestCoordinatorGateway() {
        this(null);
    }

    public TestCoordinatorGateway(ZooKeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CreateDatabaseResponse> createDatabase(CreateDatabaseRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DropDatabaseResponse> dropDatabase(DropDatabaseRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CreateTableResponse> createTable(CreateTableRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DropTableResponse> dropTable(DropTableRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CreatePartitionResponse> createPartition(
            CreatePartitionRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DropPartitionResponse> dropPartition(DropPartitionRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetLatestLakeSnapshotResponse> getLatestLakeSnapshot(
            GetLatestLakeSnapshotRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListDatabasesResponse> listDatabases(ListDatabasesRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetDatabaseInfoResponse> getDatabaseInfo(
            GetDatabaseInfoRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DatabaseExistsResponse> databaseExists(DatabaseExistsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetTableSchemaResponse> getTableSchema(GetTableSchemaRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<TableExistsResponse> tableExists(TableExistsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<UpdateMetadataResponse> updateMetadata(UpdateMetadataRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetLatestKvSnapshotsResponse> getLatestKvSnapshots(
            GetLatestKvSnapshotsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetKvSnapshotMetadataResponse> getKvSnapshotMetadata(
            GetKvSnapshotMetadataRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetFileSystemSecurityTokenResponse> getFileSystemSecurityToken(
            GetFileSystemSecurityTokenRequest request) {
        return CompletableFuture.completedFuture(new GetFileSystemSecurityTokenResponse());
    }

    @Override
    public CompletableFuture<ListPartitionInfosResponse> listPartitionInfos(
            ListPartitionInfosRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<AdjustIsrResponse> adjustIsr(AdjustIsrRequest request) {
        Map<TableBucket, LeaderAndIsr> adjustIsrData = getAdjustIsrData(request);
        List<AdjustIsrResultForBucket> resultForBuckets = new ArrayList<>();

        adjustIsrData.forEach(
                (tb, leaderAndIsr) -> {
                    Integer currentLeaderEpoch = this.currentLeaderEpoch.getOrDefault(tb, 0);
                    int requestLeaderEpoch = leaderAndIsr.leaderEpoch();

                    AdjustIsrResultForBucket adjustIsrResultForBucket;
                    if (requestLeaderEpoch < currentLeaderEpoch) {
                        adjustIsrResultForBucket =
                                new AdjustIsrResultForBucket(
                                        tb,
                                        ApiError.fromThrowable(
                                                new FencedLeaderEpochException(
                                                        "request leader epoch is fenced.")));
                    } else {
                        adjustIsrResultForBucket =
                                new AdjustIsrResultForBucket(
                                        tb,
                                        new LeaderAndIsr(
                                                leaderAndIsr.leader(),
                                                currentLeaderEpoch,
                                                leaderAndIsr.isr(),
                                                leaderAndIsr.coordinatorEpoch(),
                                                leaderAndIsr.bucketEpoch() + 1));
                    }

                    resultForBuckets.add(adjustIsrResultForBucket);
                });
        return CompletableFuture.completedFuture(makeAdjustIsrResponse(resultForBuckets));
    }

    @Override
    public CompletableFuture<CommitKvSnapshotResponse> commitKvSnapshot(
            CommitKvSnapshotRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CommitRemoteLogManifestResponse> commitRemoteLogManifest(
            CommitRemoteLogManifestRequest request) {
        if (commitRemoteLogManifestFail.get()) {
            return CompletableFuture.completedFuture(
                    new CommitRemoteLogManifestResponse().setCommitSuccess(false));
        }
        checkNotNull(zkClient, "zkClient is null");
        CommitRemoteLogManifestData commitRemoteLogManifestData =
                getCommitRemoteLogManifestData(request);
        CommitRemoteLogManifestResponse response = new CommitRemoteLogManifestResponse();
        try {
            zkClient.upsertRemoteLogManifestHandle(
                    commitRemoteLogManifestData.getTableBucket(),
                    new RemoteLogManifestHandle(
                            commitRemoteLogManifestData.getRemoteLogManifestPath(),
                            commitRemoteLogManifestData.getRemoteLogEndOffset()));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(response.setCommitSuccess(false));
        }

        return CompletableFuture.completedFuture(response.setCommitSuccess(true));
    }

    @Override
    public CompletableFuture<CommitLakeTableSnapshotResponse> commitLakeTableSnapshot(
            CommitLakeTableSnapshotRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListAclsResponse> listAcls(ListAclsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CreateAclsResponse> createAcls(CreateAclsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DropAclsResponse> dropAcls(DropAclsRequest request) {
        throw new UnsupportedOperationException();
    }

    public void setCurrentLeaderEpoch(TableBucket tableBucket, int leaderEpoch) {
        currentLeaderEpoch.put(tableBucket, leaderEpoch);
    }
}
