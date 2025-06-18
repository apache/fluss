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

import org.apache.fluss.exception.FencedLeaderEpochException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AdjustIsrRequest;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.CommitKvSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitKvSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import org.apache.fluss.rpc.messages.ControlledShutdownRequest;
import org.apache.fluss.rpc.messages.ControlledShutdownResponse;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.CreateAclsResponse;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.CreateDatabaseResponse;
import org.apache.fluss.rpc.messages.CreatePartitionRequest;
import org.apache.fluss.rpc.messages.CreatePartitionResponse;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.CreateTableResponse;
import org.apache.fluss.rpc.messages.DatabaseExistsRequest;
import org.apache.fluss.rpc.messages.DatabaseExistsResponse;
import org.apache.fluss.rpc.messages.DropAclsRequest;
import org.apache.fluss.rpc.messages.DropAclsResponse;
import org.apache.fluss.rpc.messages.DropDatabaseRequest;
import org.apache.fluss.rpc.messages.DropDatabaseResponse;
import org.apache.fluss.rpc.messages.DropPartitionRequest;
import org.apache.fluss.rpc.messages.DropPartitionResponse;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.DropTableResponse;
import org.apache.fluss.rpc.messages.GetDatabaseInfoRequest;
import org.apache.fluss.rpc.messages.GetDatabaseInfoResponse;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataRequest;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotRequest;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.GetTableSchemaRequest;
import org.apache.fluss.rpc.messages.GetTableSchemaResponse;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.ListAclsRequest;
import org.apache.fluss.rpc.messages.ListAclsResponse;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.ListDatabasesResponse;
import org.apache.fluss.rpc.messages.ListPartitionInfosRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.TableExistsRequest;
import org.apache.fluss.rpc.messages.TableExistsResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.AdjustIsrResultForBucket;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getCommitRemoteLogManifestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeAdjustIsrResponse;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
    public CompletableFuture<LakeTieringHeartbeatResponse> lakeTieringHeartbeat(
            LakeTieringHeartbeatRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ControlledShutdownResponse> controlledShutdown(
            ControlledShutdownRequest request) {
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
