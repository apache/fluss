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

package org.apache.fluss.flink.source.metrics;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.flink.source.reader.FlinkSourceReader;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A collection class for handling metrics in {@link FlinkSourceReader} of Fluss.
 *
 * <p>All metrics of the source reader are registered under group "fluss.reader", which is a child
 * group of {@link org.apache.flink.metrics.groups.OperatorMetricGroup}. Metrics related to a
 * specific table bucket will be registered in the group:
 *
 * <p>"fluss.reader.bucket.{bucket_id}" for non-partitioned bucket or
 * "fluss.reader.partition.{partition_id}.bucket.{bucket_id}" for partitioned bucket.
 *
 * <p>For example, current consuming offset of table "my-table" and bucket 1 will be reported in
 * metric: "{some_parent_groups}.operator.fluss.reader.table.my-table.bucket.1.currentOffset"
 */
public class FlinkSourceReaderMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceReaderMetrics.class);

    // Constants
    public static final String FLUSS_METRIC_GROUP = "fluss";
    public static final String READER_METRIC_GROUP = "reader";
    public static final String PARTITION_GROUP = "partition";
    public static final String BUCKET_GROUP = "bucket";
    public static final String CURRENT_OFFSET_METRIC_GAUGE = "currentOffset";
    public static final String LOG_END_OFFSET_METRIC_GAUGE = "logEndOffset";
    public static final String RECORDS_LAG_METRIC_GAUGE = "recordsLag";
    public static final String RECORDS_LAG_MAX_METRIC_GAUGE = "recordsLagMax";
    public static final String RECORDS_LAG_SUM_METRIC_GAUGE = "recordsLagSum";
    public static final String LAGGING_BUCKETS_METRIC_GAUGE = "laggingBuckets";

    public static final long INITIAL_OFFSET = -1;
    public static final long UNINITIALIZED = -1;

    // Source reader metric group
    private final SourceReaderMetricGroup sourceReaderMetricGroup;

    // Metric group for registering Fluss specific reader metrics
    private final MetricGroup flussSourceReaderMetricGroup;

    // Map for tracking current consuming offsets and lag state.
    private final Map<TableBucket, BucketMetrics> bucketMetrics = new ConcurrentHashMap<>();

    // For currentFetchEventTimeLag metric
    private volatile long currentFetchEventTimeLag = UNINITIALIZED;

    public FlinkSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
        this.flussSourceReaderMetricGroup =
                sourceReaderMetricGroup.addGroup(FLUSS_METRIC_GROUP).addGroup(READER_METRIC_GROUP);
        registerRecordsLagMetrics();
    }

    public void reportRecordEventTime(long lag) {
        if (currentFetchEventTimeLag == UNINITIALIZED) {
            // Lazily register the currentFetchEventTimeLag
            // Set the lag before registering the metric to avoid metric reporter getting
            // the uninitialized value
            currentFetchEventTimeLag = lag;
            sourceReaderMetricGroup.gauge(
                    MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, () -> currentFetchEventTimeLag);
            return;
        }
        currentFetchEventTimeLag = lag;
    }

    @VisibleForTesting
    protected void registerTableBucket(TableBucket tableBucket) {
        registerTableBucket(tableBucket, null, INITIAL_OFFSET, null, true);
    }

    public void registerTableBucket(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            @Nullable Long stoppingOffset,
            boolean refreshLogEndOffset) {
        bucketMetrics.computeIfAbsent(
                tableBucket,
                key -> {
                    BucketMetrics newMetrics =
                            new BucketMetrics(
                                    partitionName,
                                    startingOffset,
                                    stoppingOffset,
                                    refreshLogEndOffset);
                    registerOffsetMetricsForTableBucket(key, newMetrics);
                    return newMetrics;
                });
    }

    /**
     * Update current consuming offset of the given {@link TableBucket}.
     *
     * @param tb Updating table bucket
     * @param offset Current consuming offset
     */
    public void recordCurrentOffset(TableBucket tb, long offset) {
        BucketMetrics metrics = bucketMetrics.get(tb);
        if (metrics != null) {
            metrics.recordCurrentOffset(offset);
        }
    }

    public void recordLogEndOffset(TableBucket tb, long logEndOffset) {
        BucketMetrics metrics = bucketMetrics.get(tb);
        if (metrics != null) {
            metrics.updateLogEndOffsetIfAdvanced(logEndOffset);
        }
    }

    public void unregisterTableBucket(TableBucket tb) {
        BucketMetrics metrics = bucketMetrics.get(tb);
        if (metrics != null) {
            metrics.deactivate();
        }
    }

    public List<SubscribedBucket> subscribedBucketsForLogEndOffsetRefresh() {
        List<SubscribedBucket> subscribedBuckets = new ArrayList<>();
        for (Map.Entry<TableBucket, BucketMetrics> entry : bucketMetrics.entrySet()) {
            BucketMetrics metrics = entry.getValue();
            if (metrics.needsLogEndOffsetRefresh()) {
                subscribedBuckets.add(metrics.toSubscribedBucket(entry.getKey()));
            }
        }
        return subscribedBuckets;
    }

    // -------- Helper functions --------
    private void registerRecordsLagMetrics() {
        flussSourceReaderMetricGroup.gauge(RECORDS_LAG_MAX_METRIC_GAUGE, this::recordsLagMax);
        flussSourceReaderMetricGroup.gauge(RECORDS_LAG_SUM_METRIC_GAUGE, this::recordsLagSum);
        flussSourceReaderMetricGroup.gauge(LAGGING_BUCKETS_METRIC_GAUGE, this::laggingBuckets);
    }

    private void registerOffsetMetricsForTableBucket(
            TableBucket tableBucket, BucketMetrics bucketMetrics) {
        final MetricGroup metricGroup =
                tableBucket.getPartitionId() == null
                        ? this.flussSourceReaderMetricGroup
                        : this.flussSourceReaderMetricGroup.addGroup(
                                PARTITION_GROUP, String.valueOf(tableBucket.getPartitionId()));
        final MetricGroup bucketGroup =
                metricGroup.addGroup(BUCKET_GROUP, String.valueOf(tableBucket.getBucket()));
        bucketGroup.gauge(CURRENT_OFFSET_METRIC_GAUGE, () -> bucketMetrics.currentOffset());
        bucketGroup.gauge(LOG_END_OFFSET_METRIC_GAUGE, () -> bucketMetrics.logEndOffset());
        bucketGroup.gauge(RECORDS_LAG_METRIC_GAUGE, () -> bucketMetrics.recordsLag());
    }

    private long recordsLagMax() {
        long maxLag = 0L;
        for (BucketMetrics metrics : bucketMetrics.values()) {
            maxLag = Math.max(maxLag, metrics.recordsLag());
        }
        return maxLag;
    }

    private long recordsLagSum() {
        long lagSum = 0L;
        for (BucketMetrics metrics : bucketMetrics.values()) {
            lagSum += metrics.recordsLag();
        }
        return lagSum;
    }

    private int laggingBuckets() {
        int laggingBuckets = 0;
        for (BucketMetrics metrics : bucketMetrics.values()) {
            if (metrics.recordsLag() > 0) {
                laggingBuckets++;
            }
        }
        return laggingBuckets;
    }

    public SourceReaderMetricGroup getSourceReaderMetricGroup() {
        return sourceReaderMetricGroup;
    }

    private static class BucketMetrics {
        @Nullable private volatile String partitionName;
        private volatile boolean active;
        private volatile long currentOffset;
        private volatile long nextConsumedOffset;
        private volatile long logEndOffset;
        private volatile boolean refreshLogEndOffset;

        private BucketMetrics(
                @Nullable String partitionName,
                long startingOffset,
                @Nullable Long stoppingOffset,
                boolean refreshLogEndOffset) {
            this.partitionName = partitionName;
            this.currentOffset = INITIAL_OFFSET;
            this.nextConsumedOffset = startingOffset;
            this.logEndOffset = stoppingOffset == null ? UNINITIALIZED : stoppingOffset;
            this.refreshLogEndOffset = refreshLogEndOffset && stoppingOffset == null;
            this.active = true;
        }

        private void recordCurrentOffset(long offset) {
            currentOffset = offset;
            nextConsumedOffset = offset + 1;
        }

        private void updateLogEndOffsetIfAdvanced(long logEndOffset) {
            if (active && refreshLogEndOffset && logEndOffset > this.logEndOffset) {
                this.logEndOffset = logEndOffset;
            }
        }

        private void deactivate() {
            active = false;
        }

        private boolean needsLogEndOffsetRefresh() {
            return active && refreshLogEndOffset;
        }

        private SubscribedBucket toSubscribedBucket(TableBucket tableBucket) {
            return new SubscribedBucket(tableBucket, partitionName);
        }

        private long currentOffset() {
            return currentOffset;
        }

        private long logEndOffset() {
            return logEndOffset;
        }

        private long recordsLag() {
            if (!active || logEndOffset < 0 || nextConsumedOffset < 0) {
                return 0L;
            }
            return Math.max(0L, logEndOffset - nextConsumedOffset);
        }
    }

    /** The subscribed bucket whose latest log end offset needs to be refreshed. */
    public static class SubscribedBucket {
        private final TableBucket tableBucket;
        @Nullable private final String partitionName;

        private SubscribedBucket(TableBucket tableBucket, @Nullable String partitionName) {
            this.tableBucket = tableBucket;
            this.partitionName = partitionName;
        }

        public TableBucket tableBucket() {
            return tableBucket;
        }

        @Nullable
        public String partitionName() {
            return partitionName;
        }
    }
}
