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

package org.apache.fluss.rpc;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.DatabaseExistsRequest;
import org.apache.fluss.rpc.messages.DatabaseExistsResponse;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
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
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.messages.InitWriterResponse;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.rpc.messages.ListAclsRequest;
import org.apache.fluss.rpc.messages.ListAclsResponse;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.ListDatabasesResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.ListPartitionInfosRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.messages.TableExistsRequest;
import org.apache.fluss.rpc.messages.TableExistsResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;

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

    @Override
    public CompletableFuture<ListAclsResponse> listAcls(ListAclsRequest request) {
        return null;
    }
}
