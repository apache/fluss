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

package com.alibaba.fluss.rpc.gateway;

import com.alibaba.fluss.rpc.RpcGateway;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.LimitScanRequest;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.LookupResponse;
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
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.RPC;

import java.util.concurrent.CompletableFuture;

/** The entry point of RPC gateway interface for coordinator server. */
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
