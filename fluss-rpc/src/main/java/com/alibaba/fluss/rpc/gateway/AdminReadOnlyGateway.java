/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.rpc.gateway;

import com.alibaba.fluss.rpc.RpcGateway;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
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
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.RPC;

import java.util.concurrent.CompletableFuture;

/** The gateway interface between the client and the server for the read-only metadata access. */
public interface AdminReadOnlyGateway extends RpcGateway {

    // ------ databases ------

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     */
    @RPC(api = ApiKeys.LIST_DATABASES)
    CompletableFuture<ListDatabasesResponse> listDatabases(ListDatabasesRequest request);

    /**
     * Return a {@link GetDatabaseInfoResponse} by the given {@link GetDatabaseInfoRequest}.
     *
     * @param request Name of the database
     * @return The response of requested database.
     */
    @RPC(api = ApiKeys.GET_DATABASE_INFO)
    CompletableFuture<GetDatabaseInfoResponse> getDatabaseInfo(GetDatabaseInfoRequest request);

    /**
     * Check if a database exists in this catalog.
     *
     * @param request Database exists request
     * @return a future with true if the given database exists in the catalog false otherwise
     */
    @RPC(api = ApiKeys.DATABASE_EXISTS)
    CompletableFuture<DatabaseExistsResponse> databaseExists(DatabaseExistsRequest request);

    // ------ tables ------

    /**
     * Get names of all tables and views under this database. An empty list is returned if none
     * exists.
     */
    @RPC(api = ApiKeys.LIST_TABLES)
    CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request);

    /**
     * Return a {@link GetTableInfoResponse} by the given {@link GetTableInfoRequest}.
     *
     * @param request Path of the table
     * @return The response of requested table
     */
    @RPC(api = ApiKeys.GET_TABLE_INFO)
    CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request);

    /**
     * Return a {@link GetTableSchemaResponse} identified by the given {@link
     * GetTableSchemaRequest}.
     *
     * @param request Request to get the schema
     * @return The response of getting schema
     */
    @RPC(api = ApiKeys.GET_TABLE_SCHEMA)
    CompletableFuture<GetTableSchemaResponse> getTableSchema(GetTableSchemaRequest request);

    /**
     * Check if a table exists.
     *
     * @param request table exists request
     * @return a future returns true if the given table exists in the catalog false otherwise
     */
    @RPC(api = ApiKeys.TABLE_EXISTS)
    CompletableFuture<TableExistsResponse> tableExists(TableExistsRequest request);

    /**
     * Get server and table metadata from server.
     *
     * @param request Get metadata request
     * @return a future returns metadata
     */
    @RPC(api = ApiKeys.GET_METADATA)
    CompletableFuture<MetadataResponse> metadata(MetadataRequest request);

    /**
     * request send to tablet server to update the metadata cache for every tablet server node,
     * asynchronously.
     *
     * @return the update metadata response
     */
    @RPC(api = ApiKeys.UPDATE_METADATA)
    CompletableFuture<UpdateMetadataResponse> updateMetadata(UpdateMetadataRequest request);

    /**
     * Get the latest kv snapshots of a primary key table. A kv snapshot is a snapshot of a kv
     * tablet, so a table can have multiple kv snapshots.
     *
     * @param request Get latest kv snapshots request
     * @return a future returns latest kv snapshots
     */
    @RPC(api = ApiKeys.GET_LATEST_KV_SNAPSHOTS)
    CompletableFuture<GetLatestKvSnapshotsResponse> getLatestKvSnapshots(
            GetLatestKvSnapshotsRequest request);

    /**
     * Get the kv snapshot metadata of a given kv snapshot.
     *
     * @param request request that specifies the kv bucket and snapshot id
     * @return a future returns kv snapshot metadata (including the snapshot files and log offset)
     */
    @RPC(api = ApiKeys.GET_KV_SNAPSHOT_METADATA)
    CompletableFuture<GetKvSnapshotMetadataResponse> getKvSnapshotMetadata(
            GetKvSnapshotMetadataRequest request);

    /**
     * Get the security token to access the files.
     *
     * @param request Get file access security token request
     * @return a future returns security token info
     */
    @RPC(api = ApiKeys.GET_FILESYSTEM_SECURITY_TOKEN)
    CompletableFuture<GetFileSystemSecurityTokenResponse> getFileSystemSecurityToken(
            GetFileSystemSecurityTokenRequest request);

    /**
     * List the partition infos of a table.
     *
     * @param request the list partition infos request
     * @return a future returns partition infos
     */
    @RPC(api = ApiKeys.LIST_PARTITION_INFOS)
    CompletableFuture<ListPartitionInfosResponse> listPartitionInfos(
            ListPartitionInfosRequest request);

    /**
     * Get the latest lake snapshot for the given table.
     *
     * @param request request that specifies that table path.
     * @return a future returns the lake snapshot of the table.
     */
    @RPC(api = ApiKeys.GET_LATEST_LAKE_SNAPSHOT)
    CompletableFuture<GetLatestLakeSnapshotResponse> getLatestLakeSnapshot(
            GetLatestLakeSnapshotRequest request);
}
