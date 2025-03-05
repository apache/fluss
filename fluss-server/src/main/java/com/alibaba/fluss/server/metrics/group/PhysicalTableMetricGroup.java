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

package com.alibaba.fluss.server.metrics.group;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.NoOpCounter;
import com.alibaba.fluss.metrics.ThreadSafeSimpleCounter;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.registry.MetricRegistry;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.metrics.utils.MetricGroupUtils.makeScope;

/**
 * Metrics for the physical tables(tables or partitions) in server with {@link
 * TabletServerMetricGroup} as parent group.
 */
public class PhysicalTableMetricGroup extends AbstractMetricGroup {

    private final Map<Integer, BucketMetricGroup> buckets = new HashMap<>();

    private final PhysicalTablePath physicalTablePath;

    // ---- metrics for log, when the table is for kv, it's for cdc log
    private final LogMetricGroup logMetrics;

    // ---- metrics for kv, will be null if the table isn't a kv table ----
    private final @Nullable KvMetricGroup kvMetrics;

    public PhysicalTableMetricGroup(
            MetricRegistry registry,
            PhysicalTablePath physicalTablePath,
            boolean isKvTable,
            TabletServerMetricGroup serverMetricGroup) {
        super(
                registry,
                makeScope(
                        serverMetricGroup,
                        physicalTablePath.getDatabaseName(),
                        physicalTablePath.getTableName()),
                serverMetricGroup);
        this.physicalTablePath = physicalTablePath;

        // if is kv table, create kv metrics
        if (isKvTable) {
            kvMetrics = new KvMetricGroup(this);
            logMetrics = new LogMetricGroup(this, TabletType.CDC_LOG);
        } else {
            // otherwise, create log produce metrics
            kvMetrics = null;
            logMetrics = new LogMetricGroup(this, TabletType.LOG);
        }
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put("database", physicalTablePath.getDatabaseName());
        variables.put("table", physicalTablePath.getTableName());

        if (physicalTablePath.getPartitionName() != null) {
            variables.put("partition", physicalTablePath.getPartitionName());
        } else {
            // value of empty string indicates non-partitioned tables
            variables.put("partition", "");
        }
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        // partition and table share same logic group name
        return "table";
    }

    public Counter logMessageIn() {
        return logMetrics.messagesIn;
    }

    public Counter logBytesIn() {
        return logMetrics.bytesIn;
    }

    public Counter logBytesOut() {
        return logMetrics.bytesOut;
    }

    public Counter totalFetchLogRequests() {
        return logMetrics.totalFetchLogRequests;
    }

    public Counter failedFetchLogRequests() {
        return logMetrics.failedFetchLogRequests;
    }

    public Counter totalProduceLogRequests() {
        return logMetrics.totalProduceLogRequests;
    }

    public Counter failedProduceLogRequests() {
        return logMetrics.failedProduceLogRequests;
    }

    public Counter remoteLogCopyBytes() {
        return logMetrics.remoteLogCopyBytes;
    }

    public Counter remoteLogCopyRequests() {
        return logMetrics.remoteLogCopyRequests;
    }

    public Counter remoteLogCopyErrors() {
        return logMetrics.remoteLogCopyErrors;
    }

    public Counter remoteLogDeleteRequests() {
        return logMetrics.remoteLogDeleteRequests;
    }

    public Counter remoteLogDeleteErrors() {
        return logMetrics.remoteLogDeleteErrors;
    }

    public Counter kvMessageIn() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.messagesIn;
        }
    }

    public Counter kvBytesIn() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.bytesIn;
        }
    }

    public Counter totalLookupRequests() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.totalLookupRequests;
        }
    }

    public Counter failedLookupRequests() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.failedLookupRequests;
        }
    }

    public Counter totalPutKvRequests() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.totalPutKvRequests;
        }
    }

    public Counter failedPutKvRequests() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.failedPutKvRequests;
        }
    }

    public Counter totalLimitScanRequests() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.totalLimitScanRequests;
        }
    }

    public Counter failedLimitScanRequests() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.failedLimitScanRequests;
        }
    }

    public Counter totalPrefixLookupRequests() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.totalPrefixLookupRequests;
        }
    }

    public Counter failedPrefixLookupRequests() {
        if (kvMetrics == null) {
            return NoOpCounter.INSTANCE;
        } else {
            return kvMetrics.failedPrefixLookupRequests;
        }
    }

    // ------------------------------------------------------------------------
    //  bucket groups
    // ------------------------------------------------------------------------
    public BucketMetricGroup addBucketMetricGroup(int bucketId) {
        return buckets.computeIfAbsent(
                bucketId, (bucket) -> new BucketMetricGroup(registry, bucketId, this));
    }

    public void removeBucketMetricGroup(int bucketId) {
        BucketMetricGroup metricGroup = buckets.remove(bucketId);
        metricGroup.close();
    }

    public int bucketGroupsCount() {
        return buckets.size();
    }

    /** Metric group for specific kind of tablet of a table. */
    private static class TabletMetricGroup extends AbstractMetricGroup {
        private final TabletType tabletType;

        // general metrics for all kinds of tablets
        protected final Counter messagesIn;
        protected final Counter bytesIn;
        protected final Counter bytesOut;

        private TabletMetricGroup(
                PhysicalTableMetricGroup physicalTableMetricGroup, TabletType tabletType) {
            super(
                    physicalTableMetricGroup.registry,
                    makeScope(physicalTableMetricGroup, tabletType.name),
                    physicalTableMetricGroup);
            this.tabletType = tabletType;

            messagesIn = new ThreadSafeSimpleCounter();
            meter(MetricNames.MESSAGES_IN_RATE, new MeterView(messagesIn));
            bytesIn = new ThreadSafeSimpleCounter();
            meter(MetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
            bytesOut = new ThreadSafeSimpleCounter();
            meter(MetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));
        }

        @Override
        protected void putVariables(Map<String, String> variables) {
            variables.put("tablet_type", tabletType.name);
        }

        @Override
        protected String getGroupName(CharacterFilter filter) {
            // make the group name be "" to make the different kinds of tablet
            // has same logic scope
            return "";
        }
    }

    private static class LogMetricGroup extends TabletMetricGroup {

        private final Counter totalFetchLogRequests;
        private final Counter failedFetchLogRequests;

        // will be NOP when it's for cdc log
        private final Counter totalProduceLogRequests;
        private final Counter failedProduceLogRequests;

        // remote log metrics
        private final Counter remoteLogCopyBytes;
        private final Counter remoteLogCopyRequests;
        private final Counter remoteLogCopyErrors;
        private final Counter remoteLogDeleteRequests;
        private final Counter remoteLogDeleteErrors;

        private LogMetricGroup(
                PhysicalTableMetricGroup physicalTableMetricGroup, TabletType groupType) {
            super(physicalTableMetricGroup, groupType);
            // for fetch log requests
            totalFetchLogRequests = new ThreadSafeSimpleCounter();
            meter(MetricNames.TOTAL_FETCH_LOG_REQUESTS_RATE, new MeterView(totalFetchLogRequests));
            failedFetchLogRequests = new ThreadSafeSimpleCounter();
            meter(
                    MetricNames.FAILED_FETCH_LOG_REQUESTS_RATE,
                    new MeterView(failedFetchLogRequests));
            if (groupType == TabletType.LOG) {
                // for produce log request
                totalProduceLogRequests = new ThreadSafeSimpleCounter();
                meter(
                        MetricNames.TOTAL_PRODUCE_FETCH_LOG_REQUESTS_RATE,
                        new MeterView(totalProduceLogRequests));
                failedProduceLogRequests = new ThreadSafeSimpleCounter();
                meter(
                        MetricNames.FAILED_PRODUCE_FETCH_LOG_REQUESTS_RATE,
                        new MeterView(failedProduceLogRequests));
            } else {
                totalProduceLogRequests = NoOpCounter.INSTANCE;
                failedProduceLogRequests = NoOpCounter.INSTANCE;
            }

            // remote log copy metrics.
            remoteLogCopyBytes = new ThreadSafeSimpleCounter();
            meter(MetricNames.REMOTE_LOG_COPY_BYTES_RATE, new MeterView(remoteLogCopyBytes));
            remoteLogCopyRequests = new ThreadSafeSimpleCounter();
            meter(MetricNames.REMOTE_LOG_COPY_REQUESTS_RATE, new MeterView(remoteLogCopyRequests));
            remoteLogCopyErrors = new ThreadSafeSimpleCounter();
            meter(MetricNames.REMOTE_LOG_COPY_ERROR_RATE, new MeterView(remoteLogCopyErrors));
            remoteLogDeleteRequests = new ThreadSafeSimpleCounter();
            meter(
                    MetricNames.REMOTE_LOG_DELETE_REQUESTS_RATE,
                    new MeterView(remoteLogDeleteRequests));
            remoteLogDeleteErrors = new ThreadSafeSimpleCounter();
            meter(MetricNames.REMOTE_LOG_DELETE_ERROR_RATE, new MeterView(remoteLogDeleteErrors));
        }

        @Override
        protected String getGroupName(CharacterFilter filter) {
            return super.getGroupName(filter);
        }
    }

    private static class KvMetricGroup extends TabletMetricGroup {

        private final Counter totalLookupRequests;
        private final Counter failedLookupRequests;
        private final Counter totalPutKvRequests;
        private final Counter failedPutKvRequests;
        private final Counter totalLimitScanRequests;
        private final Counter failedLimitScanRequests;
        private final Counter totalPrefixLookupRequests;
        private final Counter failedPrefixLookupRequests;

        public KvMetricGroup(PhysicalTableMetricGroup physicalTableMetricGroup) {
            super(physicalTableMetricGroup, TabletType.KV);

            // for lookup request
            totalLookupRequests = new ThreadSafeSimpleCounter();
            meter(MetricNames.TOTAL_LOOKUP_REQUESTS_RATE, new MeterView(totalLookupRequests));
            failedLookupRequests = new ThreadSafeSimpleCounter();
            meter(MetricNames.FAILED_LOOKUP_REQUESTS_RATE, new MeterView(failedLookupRequests));
            // for put kv request
            totalPutKvRequests = new ThreadSafeSimpleCounter();
            meter(MetricNames.TOTAL_PUT_KV_REQUESTS_RATE, new MeterView(totalPutKvRequests));
            failedPutKvRequests = new ThreadSafeSimpleCounter();
            meter(MetricNames.FAILED_PUT_KV_REQUESTS_RATE, new MeterView(failedPutKvRequests));
            // for limit scan request
            totalLimitScanRequests = new ThreadSafeSimpleCounter();
            meter(
                    MetricNames.TOTAL_LIMIT_SCAN_REQUESTS_RATE,
                    new MeterView(totalLimitScanRequests));
            failedLimitScanRequests = new ThreadSafeSimpleCounter();
            meter(
                    MetricNames.FAILED_LIMIT_SCAN_REQUESTS_RATE,
                    new MeterView(failedLimitScanRequests));

            // for prefix lookup request
            totalPrefixLookupRequests = new ThreadSafeSimpleCounter();
            meter(
                    MetricNames.TOTAL_PREFIX_LOOKUP_REQUESTS_RATE,
                    new MeterView(totalPrefixLookupRequests));
            failedPrefixLookupRequests = new ThreadSafeSimpleCounter();
            meter(
                    MetricNames.FAILED_PREFIX_LOOKUP_REQUESTS_RATE,
                    new MeterView(failedPrefixLookupRequests));
        }

        @Override
        protected String getGroupName(CharacterFilter filter) {
            return super.getGroupName(filter);
        }
    }

    private enum TabletType {
        LOG("log"),
        KV("kv"),
        CDC_LOG("cdc_log");

        private final String name;

        TabletType(String name) {
            this.name = name;
        }
    }
}
