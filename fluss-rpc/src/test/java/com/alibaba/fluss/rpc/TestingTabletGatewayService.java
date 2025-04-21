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

package com.alibaba.fluss.rpc;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
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
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.LimitScanRequest;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosRequest;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosResponse;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.LookupResponse;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import com.alibaba.fluss.rpc.messages.PrefixLookupRequest;
import com.alibaba.fluss.rpc.messages.PrefixLookupResponse;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.rpc.messages.PutKvResponse;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.messages.StopReplicaResponse;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.messages.TableExistsResponse;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.messages.UpdateMetadataResponse;

import java.util.concurrent.CompletableFuture;

/** A testing implementation of the {@link TabletServerGateway} interface. */
public class TestingTabletGatewayService extends TestingGatewayService
        implements TabletServerGateway {
    @Override
    public ServerType providerType() {
        return null;
    }

    @Override
    public String name() {
        return "";
    }

    @Override
    public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest) {
        return null;
    }

    @Override
    public CompletableFuture<StopReplicaResponse> stopReplica(
            StopReplicaRequest stopBucketReplicaRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<PrefixLookupResponse> prefixLookup(PrefixLookupRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<InitWriterResponse> initWriter(InitWriterRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<NotifyRemoteLogOffsetsResponse> notifyRemoteLogOffsets(
            NotifyRemoteLogOffsetsRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<NotifyKvSnapshotOffsetResponse> notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<NotifyLakeTableOffsetResponse> notifyLakeTableOffset(
            NotifyLakeTableOffsetRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ListDatabasesResponse> listDatabases(ListDatabasesRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<GetDatabaseInfoResponse> getDatabaseInfo(
            GetDatabaseInfoRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<DatabaseExistsResponse> databaseExists(DatabaseExistsRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<GetTableSchemaResponse> getTableSchema(GetTableSchemaRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<TableExistsResponse> tableExists(TableExistsRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<UpdateMetadataResponse> updateMetadata(UpdateMetadataRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<GetLatestKvSnapshotsResponse> getLatestKvSnapshots(
            GetLatestKvSnapshotsRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<GetKvSnapshotMetadataResponse> getKvSnapshotMetadata(
            GetKvSnapshotMetadataRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<GetFileSystemSecurityTokenResponse> getFileSystemSecurityToken(
            GetFileSystemSecurityTokenRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ListPartitionInfosResponse> listPartitionInfos(
            ListPartitionInfosRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<GetLatestLakeSnapshotResponse> getLatestLakeSnapshot(
            GetLatestLakeSnapshotRequest request) {
        return null;
    }
}
