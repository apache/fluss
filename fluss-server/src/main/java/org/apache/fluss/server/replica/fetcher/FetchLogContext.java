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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogReqForTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** FetchLogContext to fetch log from leader. */
public class FetchLogContext {
    private final Map<Long, TablePath> tableIdToTablePath;
    private final FetchLogRequest fetchLogRequest;
    private final Map<TableBucket, BucketFetchStatus> fetchStates;
    private final Map<TableBucket, Integer> leaderEpochs = new HashMap<>();
    private final Map<TableBucket, PbFetchLogReqForBucket> requests = new HashMap<>();

    public FetchLogContext(
            Map<Long, TablePath> tableIdToTablePath, FetchLogRequest fetchLogRequest) {
        this(tableIdToTablePath, fetchLogRequest, Collections.emptyMap());
    }

    public FetchLogContext(
            Map<Long, TablePath> tableIdToTablePath,
            FetchLogRequest fetchLogRequest,
            Map<TableBucket, BucketFetchStatus> fetchStates) {
        this.fetchStates = new HashMap<>(fetchStates);
        for (PbFetchLogReqForTable table : fetchLogRequest.getTablesReqsList()) {
            for (PbFetchLogReqForBucket bucket : table.getBucketsReqsList()) {
                requests.put(
                        new TableBucket(
                                table.getTableId(),
                                bucket.hasPartitionId() ? bucket.getPartitionId() : null,
                                bucket.getBucketId()),
                        bucket);
            }
        }
        this.tableIdToTablePath = tableIdToTablePath;
        this.fetchLogRequest = fetchLogRequest;
    }

    public FetchLogContext withFetchStates(Map<TableBucket, BucketFetchStatus> states) {
        return new FetchLogContext(tableIdToTablePath, fetchLogRequest, states);
    }

    public void setLeaderEpoch(TableBucket bucket, int epoch) {
        leaderEpochs.put(bucket, epoch);
    }

    public int leaderEpoch(TableBucket bucket) {
        return leaderEpochs.get(bucket);
    }

    public boolean matches(TableBucket bucket, BucketFetchStatus state) {
        return fetchStates.get(bucket) == state;
    }

    public PbFetchLogReqForBucket getRequest(TableBucket bucket) {
        return requests.get(bucket);
    }

    public FetchLogRequest getFetchLogRequest() {
        return fetchLogRequest;
    }

    public TablePath getTablePath(long tableId) {
        return tableIdToTablePath.get(tableId);
    }
}
