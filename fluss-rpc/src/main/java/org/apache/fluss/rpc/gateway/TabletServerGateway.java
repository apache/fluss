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

package org.apache.fluss.rpc.gateway;

import org.apache.fluss.rpc.RpcGateway;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.messages.InitWriterResponse;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
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
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.RPC;

import java.util.concurrent.CompletableFuture;

/** The entry point of RPC gateway interface for tablet server. */
public interface TabletServerGateway extends RpcGateway, AdminReadOnlyGateway {

    /**
     * Notify the bucket leader and isr.
     *
     * @return the response for bucket leader and isr notification
     */
    @RPC(api = ApiKeys.NOTIFY_LEADER_AND_ISR)
    CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest);

    /**
     * request send to tablet server to update the metadata cache for every tablet server node,
     * asynchronously.
     *
     * @return the update metadata response
     */
    @RPC(api = ApiKeys.UPDATE_METADATA)
    CompletableFuture<UpdateMetadataResponse> updateMetadata(UpdateMetadataRequest request);

    /**
     * Stop replica.
     *
     * @return the response for stop replica
     */
    @RPC(api = ApiKeys.STOP_REPLICA)
    CompletableFuture<StopReplicaResponse> stopReplica(StopReplicaRequest stopBucketReplicaRequest);

    /**
     * Produce log data to the specified table bucket.
     *
     * @return the produce response.
     */
    @RPC(api = ApiKeys.PRODUCE_LOG)
    CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request);

    /**
     * Fetch log data from the specified table bucket. The request can send by the client scanner or
     * other tablet server.
     *
     * @return the fetch response.
     */
    @RPC(api = ApiKeys.FETCH_LOG)
    CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request);

    /**
     * Put kv data to the specified table bucket.
     *
     * @return the produce response.
     */
    @RPC(api = ApiKeys.PUT_KV)
    CompletableFuture<PutKvResponse> putKv(PutKvRequest request);

    /**
     * Lookup value from the specified table bucket by key.
     *
     * @return the fetch response.
     */
    @RPC(api = ApiKeys.LOOKUP)
    CompletableFuture<LookupResponse> lookup(LookupRequest request);

    /**
     * Prefix lookup to get value by prefix key.
     *
     * @return Prefix lookup response.
     */
    @RPC(api = ApiKeys.PREFIX_LOOKUP)
    CompletableFuture<PrefixLookupResponse> prefixLookup(PrefixLookupRequest request);

    /**
     * Get limit number of values from the specified table bucket.
     *
     * @param request the limit scan request
     * @return the limit scan response
     */
    @RPC(api = ApiKeys.LIMIT_SCAN)
    CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request);

    /**
     * List offsets for the specified table bucket.
     *
     * @return the fetch response.
     */
    @RPC(api = ApiKeys.LIST_OFFSETS)
    CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request);

    /**
     * Init writer.
     *
     * @return the init writer response.
     */
    @RPC(api = ApiKeys.INIT_WRITER)
    CompletableFuture<InitWriterResponse> initWriter(InitWriterRequest request);

    /**
     * Notify remote log offsets.
     *
     * @return notify remote log offsets response.
     */
    @RPC(api = ApiKeys.NOTIFY_REMOTE_LOG_OFFSETS)
    CompletableFuture<NotifyRemoteLogOffsetsResponse> notifyRemoteLogOffsets(
            NotifyRemoteLogOffsetsRequest request);

    /**
     * Notify log offset of a kv snapshot.
     *
     * @return notify snapshot offset response.
     */
    @RPC(api = ApiKeys.NOTIFY_KV_SNAPSHOT_OFFSET)
    CompletableFuture<NotifyKvSnapshotOffsetResponse> notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetRequest request);

    /**
     * Notify log offset of a lakehouse table.
     *
     * @return notify lakehouse data response
     */
    @RPC(api = ApiKeys.NOTIFY_LAKE_TABLE_OFFSET)
    CompletableFuture<NotifyLakeTableOffsetResponse> notifyLakeTableOffset(
            NotifyLakeTableOffsetRequest request);
}
