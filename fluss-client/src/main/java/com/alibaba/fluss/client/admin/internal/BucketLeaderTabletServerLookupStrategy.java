/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.admin.internal;

import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.exception.LeaderNotAvailableException;
import com.alibaba.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Strategy to lookup leader tablet servers ID for bucket. This strategy is responsible for finding
 * the leader tablet servers for a given set of buckets.
 */
public class BucketLeaderTabletServerLookupStrategy implements TabletServerLookupStrategy<Integer> {
    private static final Logger LOG =
            LoggerFactory.getLogger(BucketLeaderTabletServerLookupStrategy.class);

    private final MetadataUpdater metadataUpdater;
    private final long tableId;
    private final @Nullable Long partitionId;

    public BucketLeaderTabletServerLookupStrategy(
            MetadataUpdater metadataUpdater, long tableId, @Nullable Long partitionId) {
        this.metadataUpdater = metadataUpdater;
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    @Override
    public CompletableFuture<LookupServerResult<Integer>> lookup(Set<Integer> buckets) {
        return CompletableFuture.supplyAsync(
                () -> {
                    Map<Integer, Integer> mappedKeys = new HashMap<>();
                    Map<Integer, Throwable> failedKeys = new HashMap<>();
                    for (Integer bucketId : buckets) {
                        try {
                            int leader =
                                    metadataUpdater.leaderFor(
                                            new TableBucket(tableId, partitionId, bucketId));
                            mappedKeys.put(bucketId, leader);
                        } catch (LeaderNotAvailableException e) {
                            LOG.warn("Leader not available and will retry lookup later", e);
                        } catch (Throwable t) {
                            LOG.warn("Failed to lookup bucket leader", t);
                            failedKeys.put(bucketId, t);
                        }
                    }
                    return new LookupServerResult<>(mappedKeys, failedKeys);
                });
    }
}
