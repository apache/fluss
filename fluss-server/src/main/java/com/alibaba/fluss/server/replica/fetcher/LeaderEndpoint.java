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

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Defines the interface to be used to access a tablet server that is a leader. */
interface LeaderEndpoint {

    /** The specific tablet server address we want to connect to. */
    ServerNode leaderNode();

    /** Fetches the local log end offset of the given table bucket. */
    CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket);

    /** Fetches the local log start offset of the given table bucket. */
    CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket);

    /**
     * Given a fetchLogRequest, carries out the expected request and returns the results from
     * fetching from the leader.
     *
     * @param fetchLogRequest The fetch request we want to carry out.
     * @return A map of table bucket -> fetch data.
     */
    CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> fetchLog(
            FetchLogRequest fetchLogRequest);

    /**
     * Builds a fetch request, given a bucket map.
     *
     * @param replicas A map of table replicas to their respective bucket fetch state.
     * @return A FetchRequest，
     */
    Optional<FetchLogRequest> buildFetchLogRequest(Map<TableBucket, BucketFetchStatus> replicas);

    /** Closes access to fetch from leader. */
    void close();
}
