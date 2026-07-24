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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.SubscribedBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExecutorUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Periodically refreshes latest log end offsets for subscribed source reader buckets. */
class LogEndOffsetRefresher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LogEndOffsetRefresher.class);

    private final Admin admin;
    private final TablePath tablePath;
    private final FlinkSourceReaderMetrics metrics;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean refreshInFlight = new AtomicBoolean(false);
    private final long refreshIntervalMillis;

    private volatile boolean closed;

    LogEndOffsetRefresher(
            Admin admin,
            TablePath tablePath,
            FlinkSourceReaderMetrics metrics,
            Duration refreshInterval) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.metrics = metrics;
        this.refreshIntervalMillis = refreshInterval.toMillis();
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("fluss-log-end-offset-refresher"));
    }

    void start() {
        if (refreshIntervalMillis <= 0) {
            LOG.info("Disable log end offset refreshing because refresh interval is non-positive.");
            return;
        }
        executor.scheduleWithFixedDelay(
                this::refresh, 0L, refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    void refresh() {
        if (closed || !refreshInFlight.compareAndSet(false, true)) {
            return;
        }

        List<SubscribedBucket> subscribedBuckets =
                metrics.subscribedBucketsForLogEndOffsetRefresh();
        if (subscribedBuckets.isEmpty()) {
            refreshInFlight.set(false);
            return;
        }

        Map<String, List<SubscribedBucket>> bucketsByPartitionName =
                groupByPartitionName(subscribedBuckets);
        List<CompletableFuture<Void>> futures = new ArrayList<>(bucketsByPartitionName.size());
        for (Map.Entry<String, List<SubscribedBucket>> entry : bucketsByPartitionName.entrySet()) {
            futures.add(refreshPartition(entry.getKey(), entry.getValue()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete(
                        (ignored, throwable) -> {
                            if (throwable != null) {
                                LOG.warn(
                                        "Failed to refresh log end offsets for subscribed buckets.",
                                        throwable);
                            }
                            refreshInFlight.set(false);
                        });
    }

    private Map<String, List<SubscribedBucket>> groupByPartitionName(
            List<SubscribedBucket> subscribedBuckets) {
        Map<String, List<SubscribedBucket>> bucketsByPartitionName = new HashMap<>();
        for (SubscribedBucket subscribedBucket : subscribedBuckets) {
            String partitionName = subscribedBucket.partitionName();
            List<SubscribedBucket> buckets = bucketsByPartitionName.get(partitionName);
            if (buckets == null) {
                buckets = new ArrayList<>();
                bucketsByPartitionName.put(partitionName, buckets);
            }
            buckets.add(subscribedBucket);
        }
        return bucketsByPartitionName;
    }

    private CompletableFuture<Void> refreshPartition(
            String partitionName, List<SubscribedBucket> subscribedBuckets) {
        try {
            Collection<Integer> buckets = new ArrayList<>(subscribedBuckets.size());
            for (SubscribedBucket subscribedBucket : subscribedBuckets) {
                buckets.add(subscribedBucket.tableBucket().getBucket());
            }

            ListOffsetsResult result =
                    partitionName == null
                            ? admin.listOffsets(tablePath, buckets, new OffsetSpec.LatestSpec())
                            : admin.listOffsets(
                                    tablePath, partitionName, buckets, new OffsetSpec.LatestSpec());
            return result.all()
                    .thenAccept(
                            offsets -> {
                                for (SubscribedBucket subscribedBucket : subscribedBuckets) {
                                    TableBucket tableBucket = subscribedBucket.tableBucket();
                                    Long logEndOffset = offsets.get(tableBucket.getBucket());
                                    if (logEndOffset != null) {
                                        metrics.recordLogEndOffset(tableBucket, logEndOffset);
                                    }
                                }
                            })
                    .exceptionally(
                            throwable -> {
                                LOG.debug(
                                        "Failed to refresh log end offsets for buckets {}.",
                                        buckets,
                                        throwable);
                                return null;
                            });
        } catch (Throwable t) {
            LOG.debug("Failed to send list offsets request for buckets {}.", subscribedBuckets, t);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public void close() {
        try {
            closed = true;
            ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
            admin.close();
        } catch (Exception e) {
            LOG.warn("Failed to close admin for log end offset refresher.", e);
        }
    }
}
