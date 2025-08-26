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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForTable;
import org.apache.fluss.rpc.messages.PbListOffsetsRespForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.log.ListOffsetsParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.getFetchLogResultForBucket;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeListOffsetsRequest;

/** Facilitates fetches from a remote replica leader in one tablet server. */
final class RemoteLeaderEndpoint implements LeaderEndpoint {
    private final int followerServerId;
    private final int remoteServerId;
    private final TabletServerGateway tabletServerGateway;
    /** The max size for the fetch response. */
    private final int maxFetchSize;
    /** The max fetch size for a bucket in bytes. */
    private final int maxFetchSizeForBucket;

    private final int minFetchBytes;
    private final int maxFetchWaitMs;

    RemoteLeaderEndpoint(
            Configuration conf,
            int followerServerId,
            int remoteServerId,
            TabletServerGateway tabletServerGateway) {
        this.followerServerId = followerServerId;
        this.remoteServerId = remoteServerId;
        this.maxFetchSize = (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES).getBytes();
        this.maxFetchSizeForBucket =
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES_FOR_BUCKET).getBytes();
        this.minFetchBytes = (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MIN_BYTES).getBytes();
        this.maxFetchWaitMs =
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_WAIT_MAX_TIME).toMillis();
        this.tabletServerGateway = tabletServerGateway;
    }

    @Override
    public int leaderServerId() {
        return remoteServerId;
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket) {
        return fetchLogOffset(tableBucket, ListOffsetsParam.LATEST_OFFSET_TYPE);
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket) {
        return fetchLogOffset(tableBucket, ListOffsetsParam.EARLIEST_OFFSET_TYPE);
    }

    @Override
    public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket) {
        return fetchLogOffset(tableBucket, ListOffsetsParam.LEADER_END_OFFSET_SNAPSHOT_TYPE);
    }

    @Override
    public CompletableFuture<FetchData> fetchLog(FetchLogContext fetchLogContext) {
        FetchLogRequest fetchLogRequest = fetchLogContext.getFetchLogRequest();
        return tabletServerGateway
                .fetchLog(fetchLogRequest)
                .thenApply(
                        fetchLogResponse -> {
                            Map<TableBucket, FetchLogResultForBucket> fetchLogResultMap =
                                    new HashMap<>();
                            List<PbFetchLogRespForTable> tablesRespList =
                                    fetchLogResponse.getTablesRespsList();
                            for (PbFetchLogRespForTable tableResp : tablesRespList) {
                                long tableId = tableResp.getTableId();
                                List<PbFetchLogRespForBucket> bucketsRespList =
                                        tableResp.getBucketsRespsList();
                                for (PbFetchLogRespForBucket bucketResp : bucketsRespList) {
                                    TableBucket tableBucket =
                                            new TableBucket(
                                                    tableId,
                                                    bucketResp.hasPartitionId()
                                                            ? bucketResp.getPartitionId()
                                                            : null,
                                                    bucketResp.getBucketId());
                                    TablePath tablePath = fetchLogContext.getTablePath(tableId);
                                    FetchLogResultForBucket fetchLogResultForBucket =
                                            getFetchLogResultForBucket(
                                                    tableBucket, tablePath, bucketResp);
                                    fetchLogResultMap.put(tableBucket, fetchLogResultForBucket);
                                }
                            }

                            return new FetchData(fetchLogResponse, fetchLogResultMap);
                        });
    }

    @Override
    public Optional<FetchLogContext> buildFetchLogContext(
            Map<TableBucket, BucketFetchStatus> replicas) {
        return buildFetchLogContext(
                replicas,
                followerServerId,
                maxFetchSize,
                maxFetchSizeForBucket,
                minFetchBytes,
                maxFetchWaitMs);
    }

    @Override
    public void close() {
        // nothing to do now.
    }

    static Optional<FetchLogContext> buildFetchLogContext(
            Map<TableBucket, BucketFetchStatus> replicas,
            int followerServerId,
            int maxFetchSize,
            int maxFetchSizeForBucket,
            int minFetchBytes,
            int maxFetchWaitMs) {
        Map<Long, TablePath> tableIdToTablePath = new HashMap<>();
        FetchLogRequest fetchRequest =
                new FetchLogRequest()
                        .setFollowerServerId(followerServerId)
                        .setMaxBytes(maxFetchSize)
                        .setMinBytes(minFetchBytes)
                        .setMaxWaitMs(maxFetchWaitMs);
        Map<Long, List<PbFetchLogReqForBucket>> fetchLogReqForBuckets = new HashMap<>();
        int readyForFetchCount = 0;
        for (Map.Entry<TableBucket, BucketFetchStatus> entry : replicas.entrySet()) {
            TableBucket tb = entry.getKey();
            BucketFetchStatus bucketFetchStatus = entry.getValue();
            if (bucketFetchStatus.isReadyForFetch()) {
                PbFetchLogReqForBucket fetchLogReqForBucket =
                        new PbFetchLogReqForBucket()
                                .setBucketId(tb.getBucket())
                                .setFetchOffset(bucketFetchStatus.fetchOffset())
                                .setMaxFetchBytes(maxFetchSizeForBucket);
                if (tb.getPartitionId() != null) {
                    fetchLogReqForBucket.setPartitionId(tb.getPartitionId());
                }
                fetchLogReqForBuckets
                        .computeIfAbsent(tb.getTableId(), key -> new ArrayList<>())
                        .add(fetchLogReqForBucket);

                tableIdToTablePath.put(tb.getTableId(), bucketFetchStatus.tablePath());
                readyForFetchCount++;
            }
        }

        if (readyForFetchCount == 0) {
            return Optional.empty();
        } else {
            fetchLogReqForBuckets.forEach(
                    (tableId, buckets) ->
                            fetchRequest
                                    .addTablesReq()
                                    .setProjectionPushdownEnabled(false)
                                    .setTableId(tableId)
                                    .addAllBucketsReqs(buckets));
            return Optional.of(new FetchLogContext(tableIdToTablePath, fetchRequest));
        }
    }

    /** Fetch log offset with given offset type. */
    private CompletableFuture<Long> fetchLogOffset(TableBucket tableBucket, int offsetType) {
        return tabletServerGateway
                .listOffsets(
                        makeListOffsetsRequest(
                                followerServerId,
                                offsetType,
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                tableBucket.getBucket()))
                .thenApply(
                        response -> {
                            PbListOffsetsRespForBucket respForBucket =
                                    response.getBucketsRespsList().get(0);
                            if (respForBucket.hasErrorCode()) {
                                throw Errors.forCode(respForBucket.getErrorCode())
                                        .exception(respForBucket.getErrorMessage());
                            } else {
                                return respForBucket.getOffset();
                            }
                        });
    }
}
