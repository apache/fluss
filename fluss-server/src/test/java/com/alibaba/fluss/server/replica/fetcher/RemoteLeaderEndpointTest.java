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

package com.alibaba.fluss.server.replica.fetcher;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.TestingTabletGatewayService;
import com.alibaba.fluss.rpc.entity.EpochAndLogEndOffsetForBucket;
import com.alibaba.fluss.rpc.entity.ListOffsetsResultForBucket;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.OffsetForLeaderEpochRequest;
import com.alibaba.fluss.rpc.messages.OffsetForLeaderEpochResponse;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.server.entity.OffsetForLeaderEpochData;
import com.alibaba.fluss.server.log.ListOffsetsParam;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getOffsetForLeaderEpochData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeListOffsetsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeOffsetForLeaderEpochResponse;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RemoteLeaderEndpoint}. */
public class RemoteLeaderEndpointTest {

    private TestingTabletServerGateway remoteServerGateway;
    private RemoteLeaderEndpoint remoteLeaderEndpoint;

    @BeforeEach
    public void setUp() {
        Configuration conf = new Configuration();
        int remoteServerId = 2;
        int followerServerId = 1;
        remoteServerGateway = new TestingTabletServerGateway();
        remoteLeaderEndpoint =
                new RemoteLeaderEndpoint(
                        conf, followerServerId, remoteServerId, remoteServerGateway);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFetchLogStartOffset(boolean isPartitionedTable) throws Exception {
        TableBucket tb = new TableBucket(1, isPartitionedTable ? 1000L : null, 0);
        // first add bucket leader status to mock gateway.
        remoteServerGateway.setBucketLeaderStatus(
                tb, new BucketLeaderStatus(tb, 0L, 100L, 0, Collections.singletonMap(0, 100L)));
        assertThat(remoteLeaderEndpoint.fetchLocalLogStartOffset(tb).get()).isEqualTo(0L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFetchLogEndOffset(boolean isPartitionedTable) throws Exception {
        TableBucket tb = new TableBucket(1, isPartitionedTable ? 1000L : null, 0);

        // first add bucket leader status to mock gateway.
        remoteServerGateway.setBucketLeaderStatus(
                tb, new BucketLeaderStatus(tb, 0L, 100L, 0, Collections.singletonMap(0, 100L)));

        assertThat(remoteLeaderEndpoint.fetchLocalLogEndOffset(tb).get()).isEqualTo(100L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fetchOffsetForLeaderEpoch(boolean isPartitionedTable) throws Exception {
        TableBucket tb0 = new TableBucket(1, isPartitionedTable ? 1000L : null, 0);
        TableBucket tb1 = new TableBucket(2, isPartitionedTable ? 1100L : null, 1);

        Map<Integer, Long> tb0LeaderEpochToEndOffset = new HashMap<>();
        tb0LeaderEpochToEndOffset.put(0, 20L);
        tb0LeaderEpochToEndOffset.put(1, 50L);
        tb0LeaderEpochToEndOffset.put(2, 100L);
        BucketLeaderStatus tb0Status =
                new BucketLeaderStatus(tb0, 0L, 100L, 2, tb0LeaderEpochToEndOffset);

        Map<Integer, Long> tb1LeaderEpochToEndOffset = new HashMap<>();
        tb1LeaderEpochToEndOffset.put(1, 10L);
        tb1LeaderEpochToEndOffset.put(2, 50L);
        tb1LeaderEpochToEndOffset.put(3, 100L);
        BucketLeaderStatus tb1Status =
                new BucketLeaderStatus(tb1, 10L, 100L, 3, tb1LeaderEpochToEndOffset);

        remoteServerGateway.setBucketLeaderStatus(tb0, tb0Status);
        remoteServerGateway.setBucketLeaderStatus(tb1, tb1Status);

        Map<TableBucket, OffsetForLeaderEpochData> leaderEpochDataMap = new HashMap<>();
        leaderEpochDataMap.put(tb0, new OffsetForLeaderEpochData(tb0, 1, 2));
        leaderEpochDataMap.put(tb1, new OffsetForLeaderEpochData(tb0, 1, 3));
        Map<TableBucket, EpochAndLogEndOffsetForBucket> result =
                remoteLeaderEndpoint.fetchOffsetForLeaderEpoch(leaderEpochDataMap).get();
        assertThat(result.get(tb0).getLogEndOffset()).isEqualTo(100L);
        assertThat(result.get(tb1).getLogEndOffset()).isEqualTo(100L);
    }

    private static class TestingTabletServerGateway extends TestingTabletGatewayService {
        Map<TableBucket, BucketLeaderStatus> bucketLeaderStatusMap;

        TestingTabletServerGateway() {
            this.bucketLeaderStatusMap = new HashMap<>();
        }

        @Override
        public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
            CompletableFuture<ListOffsetsResponse> response = new CompletableFuture<>();
            TableBucket tb =
                    new TableBucket(
                            request.getTableId(),
                            request.hasPartitionId() ? request.getPartitionId() : null,
                            request.getBucketIdAt(0));
            BucketLeaderStatus bucketLeaderStatus = bucketLeaderStatusMap.get(tb);
            ListOffsetsResultForBucket result;
            switch (request.getOffsetType()) {
                case ListOffsetsParam.LATEST_OFFSET_TYPE:
                    result = new ListOffsetsResultForBucket(tb, bucketLeaderStatus.logEndOffset);
                    break;
                case ListOffsetsParam.EARLIEST_OFFSET_TYPE:
                    result = new ListOffsetsResultForBucket(tb, bucketLeaderStatus.logStartOffset);
                    break;
                default:
                    result =
                            new ListOffsetsResultForBucket(
                                    tb,
                                    ApiError.fromThrowable(new UnsupportedOperationException()));
            }

            response.complete(makeListOffsetsResponse(Collections.singletonList(result)));
            return response;
        }

        @Override
        public CompletableFuture<OffsetForLeaderEpochResponse> offsetForLeaderEpoch(
                OffsetForLeaderEpochRequest request) {
            CompletableFuture<OffsetForLeaderEpochResponse> response = new CompletableFuture<>();

            List<OffsetForLeaderEpochData> requestData = getOffsetForLeaderEpochData(request);
            List<EpochAndLogEndOffsetForBucket> resultList = new ArrayList<>();
            requestData.forEach(
                    data -> {
                        TableBucket tb = data.getTableBucket();
                        BucketLeaderStatus bucketLeaderStatus = bucketLeaderStatusMap.get(tb);
                        Map<Integer, Long> cacheEpoch =
                                bucketLeaderStatus.leaderEpochToLogEndOffset;
                        int requestEpoch = data.getLeaderEpoch();
                        if (cacheEpoch.containsKey(requestEpoch)) {
                            resultList.add(
                                    new EpochAndLogEndOffsetForBucket(
                                            tb, requestEpoch, cacheEpoch.get(requestEpoch)));
                        } else {
                            resultList.add(
                                    new EpochAndLogEndOffsetForBucket(
                                            tb,
                                            ApiError.fromThrowable(
                                                    new UnsupportedOperationException())));
                        }
                    });

            response.complete(makeOffsetForLeaderEpochResponse(resultList));
            return response;
        }

        void setBucketLeaderStatus(TableBucket tableBucket, BucketLeaderStatus bucketLeaderStatus) {
            bucketLeaderStatusMap.put(tableBucket, bucketLeaderStatus);
        }
    }

    private static class BucketLeaderStatus {
        final TableBucket tableBucket;
        final long logStartOffset;
        final long logEndOffset;
        final int leaderEpoch;
        final Map<Integer, Long> leaderEpochToLogEndOffset;

        BucketLeaderStatus(
                TableBucket tableBucket,
                long logStartOffset,
                long logEndOffset,
                int leaderEpoch,
                Map<Integer, Long> leaderEpochToLogEndOffset) {
            this.tableBucket = tableBucket;
            this.logStartOffset = logStartOffset;
            this.logEndOffset = logEndOffset;
            this.leaderEpoch = leaderEpoch;
            this.leaderEpochToLogEndOffset = leaderEpochToLogEndOffset;
        }
    }
}
