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

package com.alibaba.fluss.client.admin.internal;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.exception.ApiException;
import com.alibaba.fluss.exception.LeaderNotAvailableException;
import com.alibaba.fluss.exception.NotLeaderOrFollowerException;
import com.alibaba.fluss.exception.RetriableException;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.PbListOffsetsRespForBucket;
import com.alibaba.fluss.rpc.protocol.ApiError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makeListOffsetsRequest;

/** ListOffsetsHandler is used to list offsets for a set of buckets. */
@Internal
public class ListOffsetsHandler implements AdminApiHandler<Integer, Long> {
    private static final Logger LOG = LoggerFactory.getLogger(ListOffsetsHandler.class);

    private final TabletServerLookupStrategy<Integer> lookupStrategy;
    private final MetadataUpdater metadataUpdater;
    private final long tableId;
    private final @Nullable Long partitionId;
    private final OffsetSpec offsetSpec;
    private final RpcClient client;

    public ListOffsetsHandler(
            MetadataUpdater metadataUpdater,
            long tableId,
            @Nullable Long partitionId,
            OffsetSpec offsetSpec,
            RpcClient client) {
        this.lookupStrategy =
                new BucketLeaderTabletServerLookupStrategy(metadataUpdater, tableId, partitionId);
        this.metadataUpdater = metadataUpdater;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.offsetSpec = offsetSpec;
        this.client = client;
    }

    @Override
    public TabletServerLookupStrategy<Integer> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public CompletableFuture<ApiResult<Integer, Long>> handle(int leader, Set<Integer> bucketIds) {
        CompletableFuture<ApiResult<Integer, Long>> future = new CompletableFuture<>();
        ListOffsetsRequest listOffsetsRequest =
                makeListOffsetsRequest(tableId, partitionId, bucketIds, offsetSpec);
        sendListOffsetsRequest(
                metadataUpdater, client, leader, listOffsetsRequest, future, bucketIds);
        return future;
    }

    private static void sendListOffsetsRequest(
            MetadataUpdater metadataUpdater,
            RpcClient client,
            int leader,
            ListOffsetsRequest request,
            CompletableFuture<ApiResult<Integer, Long>> future,
            Set<Integer> bucketIds) {

        TabletServerGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> metadataUpdater.getTabletServer(leader),
                        client,
                        TabletServerGateway.class);
        gateway.listOffsets(request)
                .thenAccept(
                        r -> {
                            Map<Integer, Long> completedKeys = new HashMap<>();
                            Map<Integer, Throwable> failedKeys = new HashMap<>();
                            Set<Integer> unmappedKeys = new HashSet<>();
                            Set<Integer> retryKeys = new HashSet<>();

                            for (PbListOffsetsRespForBucket resp : r.getBucketsRespsList()) {
                                if (resp.hasErrorCode()) {
                                    ApiException exception =
                                            ApiError.fromErrorMessage(resp).exception();

                                    if (exception instanceof LeaderNotAvailableException
                                            || exception instanceof NotLeaderOrFollowerException) {
                                        LOG.info(
                                                "Server {} is not leader or not available for bucket {}, will retry to lookup leader server later.",
                                                leader,
                                                resp.getBucketId());
                                        unmappedKeys.add(resp.getBucketId());
                                        // Even leader server can be lookup, server which hasn't
                                        // receive the NotifyLeaderAndIsrRequest still will throw
                                        // UNKNOWN_TABLE_OR_BUCKET_EXCEPTION.
                                    } else if (exception instanceof RetriableException) {
                                        LOG.warn(
                                                "Server {} return retry exception for table bucket {} , will retry to list offsets later.); The exception is: {} ",
                                                leader,
                                                resp.getBucketId(),
                                                exception);
                                        retryKeys.add(resp.getBucketId());
                                    } else {
                                        failedKeys.put(resp.getBucketId(), exception);
                                    }
                                } else {
                                    completedKeys.put(resp.getBucketId(), resp.getOffset());
                                }
                            }
                            // Sanity-check if the current leader for these partitions returned
                            // results for all of them
                            for (Integer bucketId : bucketIds) {
                                if (unmappedKeys.isEmpty()
                                        && !completedKeys.containsKey(bucketId)
                                        && !failedKeys.containsKey(bucketId)
                                        && !retryKeys.contains(bucketId)) {
                                    ApiException sanityCheckException =
                                            new ApiException(
                                                    "The response from server "
                                                            + leader
                                                            + " did not contain a result for table bucket "
                                                            + bucketId);
                                    LOG.error(
                                            "ListOffsets request for table bucket {} failed sanity check",
                                            bucketId,
                                            sanityCheckException);
                                    failedKeys.put(bucketId, sanityCheckException);
                                }
                            }

                            future.complete(
                                    new ApiResult<>(completedKeys, failedKeys, unmappedKeys));
                        });
    }

    public static AdminApiFuture.SimpleAdminApiFuture<Integer, Long> newFuture(
            Collection<Integer> bucketIds) {
        return AdminApiFuture.forKeys(new HashSet<>(bucketIds));
    }
}
