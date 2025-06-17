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

package com.alibaba.fluss.server.replica.delay;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.NotLeaderOrFollowerException;
import com.alibaba.fluss.exception.UnknownTableOrBucketException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.server.entity.FetchData;
import com.alibaba.fluss.server.log.FetchIsolation;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.log.LogOffsetMetadata;
import com.alibaba.fluss.server.log.LogOffsetSnapshot;
import com.alibaba.fluss.server.metrics.group.TabletServerMetricGroup;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.replica.ReplicaManager.LogReadResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A delayed fetch log operation that can be created by the {@link ReplicaManager} and watched in
 * the delayed fetch log operation manager. delayed fetch log operation can be sent by {@link
 * FetchLogRequest}.
 */
public class DelayedFetchLog extends DelayedOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedFetchLog.class);

    private final FetchParams params;
    private final ReplicaManager replicaManager;
    private final Map<TableBucket, FetchBucketStatus> fetchBucketStatusMap;
    private final Consumer<Map<TableBucket, FetchLogResultForBucket>> responseCallback;
    private final TabletServerMetricGroup serverMetricGroup;

    public DelayedFetchLog(
            FetchParams params,
            ReplicaManager replicaManager,
            Map<TableBucket, FetchBucketStatus> fetchBucketStatusMap,
            Consumer<Map<TableBucket, FetchLogResultForBucket>> responseCallback,
            TabletServerMetricGroup serverMetricGroup) {
        super(params.maxWaitMs());
        this.params = params;
        this.replicaManager = replicaManager;
        this.fetchBucketStatusMap = fetchBucketStatusMap;
        this.responseCallback = responseCallback;
        this.serverMetricGroup = serverMetricGroup;
    }

    /** Upon completion, read whatever data is available and pass to the complete callback. */
    @Override
    public void onComplete() {
        Map<TableBucket, FetchLogResultForBucket> result = new HashMap<>();

        Map<TableBucket, FetchData> reFetchBuckets = new HashMap<>();
        for (Map.Entry<TableBucket, FetchBucketStatus> fetchBucketStatusEntry :
                fetchBucketStatusMap.entrySet()) {
            FetchBucketStatus fetchBucketStatus = fetchBucketStatusEntry.getValue();
            TableBucket tb = fetchBucketStatusEntry.getKey();
            if (fetchBucketStatus.previousFetchLogResultForBucket.fetchFromRemote()) {
                result.put(tb, fetchBucketStatus.previousFetchLogResultForBucket);
            } else {
                reFetchBuckets.put(tb, fetchBucketStatus.fetchData);
            }
        }

        // re-fetch data.
        Map<TableBucket, LogReadResult> reReadResult =
                replicaManager.readFromLog(params, reFetchBuckets);
        reReadResult.forEach((key, value) -> result.put(key, value.getFetchLogResultForBucket()));
        responseCallback.accept(result);
    }

    /**
     * The delayed fetch operation can be completed if:
     *
     * <ul>
     *   <li>Case A: The server is no longer the leader for some buckets it tries to fetch
     *   <li>Case B: The replica is no longer available on this server
     *   <li>Case C: This server doesn't know of some buckets it tries to fetch
     *   <li>Case D: The fetch offset locates not on the last segment of the log
     *   <li>Case E: The accumulated bytes from all the fetching buckets exceeds the minimum bytes
     * </ul>
     *
     * <p>Upon completion, should return whatever data is available for each valid bucket.
     */
    @Override
    public boolean tryComplete() {
        int accumulatedSize = 0;

        for (Map.Entry<TableBucket, FetchBucketStatus> entry : fetchBucketStatusMap.entrySet()) {
            TableBucket tb = entry.getKey();
            FetchBucketStatus fetchBucketStatus = entry.getValue();
            LogOffsetMetadata fetchOffset = fetchBucketStatus.startOffsetMetadata;
            try {
                if (!fetchBucketStatus.previousFetchLogResultForBucket.fetchFromRemote()
                        && fetchOffset != LogOffsetMetadata.UNKNOWN_OFFSET_METADATA) {
                    Replica replica = replicaManager.getReplicaOrException(tb);
                    LogOffsetSnapshot logOffsetSnapshot =
                            replica.fetchOffsetSnapshot(params.fetchOnlyLeader());
                    LogOffsetMetadata endOffset;
                    if (params.isolation() == FetchIsolation.LOG_END) {
                        endOffset = logOffsetSnapshot.logEndOffset;
                    } else if (params.isolation() == FetchIsolation.HIGH_WATERMARK) {
                        endOffset = logOffsetSnapshot.highWatermark;
                    } else {
                        throw new FlussRuntimeException("Unknown fetch isolation.");
                    }

                    // Go directly to the check for Case E if the log offsets are the same. If
                    // the log segment has just rolled, then the high watermark offset will remain
                    // the same but be on the old segment, which would incorrectly be seen as an
                    // instance of Case D.
                    if (endOffset.getMessageOffset() != fetchOffset.getMessageOffset()) {
                        if (endOffset.onOlderSegment(fetchOffset)) {
                            // Case D, this can happen when the new fetch log operation is on a
                            // truncated leader.
                            LOG.debug(
                                    "Satisfying delayed fetch log since it is fetching later segments of bucket {}.",
                                    tb);
                            return forceComplete();
                        } else if (fetchOffset.onOlderSegment(endOffset)) {
                            // Case D, this can happen when the fetch operation is falling behind
                            // the current segment or the bucket has just rolled a new segment.
                            LOG.debug(
                                    "Satisfying delayed fetch log since it is fetching older segments of bucket {}.",
                                    tb);
                            return forceComplete();
                        } else if (fetchOffset.getMessageOffset() < endOffset.getMessageOffset()) {
                            // We take the bucket fetch size as upper bound when accumulating the
                            // bytes.
                            int bytesAvailable =
                                    Math.min(
                                            endOffset.positionDiff(fetchOffset),
                                            fetchBucketStatus.fetchData.getMaxBytes());
                            accumulatedSize += bytesAvailable;
                        }
                    }
                }
            } catch (NotLeaderOrFollowerException e) {
                // case A and B.
                LOG.debug(
                        "TabletServer is no longer the leader or follower of table-bucket {}, satisfy delayFetchLog immediately.",
                        tb);
                return forceComplete();
            } catch (UnknownTableOrBucketException e) {
                // case C
                LOG.debug(
                        "TabletServer os mp longer knows of table-bucket {}, satisfy delayFetchLog immediately.",
                        tb);
                return forceComplete();
            } catch (IOException e) {
                LOG.debug(
                        "There is an storage exception append for table-bucket {}, satisfy delayFetchLog immediately.",
                        tb,
                        e);
                return forceComplete();
            }
        }

        // Case F.
        if (accumulatedSize >= params.minFetchBytes()) {
            return forceComplete();
        } else {
            return false;
        }
    }

    @Override
    public void onExpiration() {
        if (params.isFromFollower()) {
            serverMetricGroup.delayedFetchFromFollowerExpireCount().inc();
        } else {
            serverMetricGroup.delayedFetchFromClientExpireCount().inc();
        }
    }

    @Override
    public String toString() {
        return "DelayedFetchLog{"
                + "params="
                + params
                + ", numBuckets="
                + fetchBucketStatusMap.size()
                + '}';
    }

    /** The status of a bucket in a delayed log fetch operation. */
    public static final class FetchBucketStatus {
        private final FetchData fetchData;
        private final LogOffsetMetadata startOffsetMetadata;
        private final FetchLogResultForBucket previousFetchLogResultForBucket;

        public FetchBucketStatus(
                FetchData fetchData,
                LogOffsetMetadata startOffsetMetadata,
                FetchLogResultForBucket previousFetchLogResultForBucket) {
            this.fetchData = fetchData;
            this.startOffsetMetadata = startOffsetMetadata;
            this.previousFetchLogResultForBucket = previousFetchLogResultForBucket;
        }

        @Override
        public String toString() {
            return "FetchBucketStatus{"
                    + "fetchData="
                    + fetchData
                    + ", startOffsetMetadata="
                    + startOffsetMetadata
                    + ", previousFetchLogResultForBucket="
                    + previousFetchLogResultForBucket
                    + '}';
        }
    }
}
