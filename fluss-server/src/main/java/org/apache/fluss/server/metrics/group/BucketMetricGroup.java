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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/**
 * Metrics for the table buckets with table as parent group.
 *
 * <p>For KV tables, this class also manages the RocksDB statistics lifecycle. The statistics are
 * registered when KvTablet is initialized and automatically cleaned up when this metric group is
 * closed.
 */
public class BucketMetricGroup extends AbstractMetricGroup {

    private static final Logger LOG = LoggerFactory.getLogger(BucketMetricGroup.class);

    // will be null if the bucket doesn't belong to a partition
    private final @Nullable String partitionName;
    private final int bucket;

    // RocksDB statistics for this bucket (null for non-KV tables)
    private volatile @Nullable RocksDBStatistics rocksDBStatistics;

    public BucketMetricGroup(
            MetricRegistry registry,
            @Nullable String partitionName,
            int bucket,
            TableMetricGroup parent) {
        super(registry, makeScope(parent, String.valueOf(bucket)), parent);
        this.partitionName = partitionName;
        this.bucket = bucket;
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        if (partitionName != null) {
            variables.put("partition", partitionName);
        } else {
            // value of empty string indicates non-partitioned tables
            variables.put("partition", "");
        }
        variables.put("bucket", String.valueOf(bucket));
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "bucket";
    }

    public TableMetricGroup getTableMetricGroup() {
        return (TableMetricGroup) parent;
    }

    /**
     * Register RocksDB statistics for this bucket. This should be called when KvTablet is
     * initialized.
     *
     * <p>This method must be paired with {@link #unregisterRocksDBStatistics()} to ensure proper
     * resource cleanup.
     *
     * @param statistics the RocksDB statistics collector
     */
    public void registerRocksDBStatistics(RocksDBStatistics statistics) {
        if (this.rocksDBStatistics != null) {
            LOG.warn(
                    "RocksDB statistics already registered for bucket {}, this may indicate a resource leak",
                    bucket);
        }
        this.rocksDBStatistics = statistics;
        LOG.debug("Registered RocksDB statistics for bucket {}", bucket);
    }

    /**
     * Unregister and close RocksDB statistics for this bucket. This should be called when KvTablet
     * is destroyed.
     *
     * <p>This method must be paired with {@link #registerRocksDBStatistics(RocksDBStatistics)} to
     * ensure proper resource cleanup.
     */
    public void unregisterRocksDBStatistics() {
        if (rocksDBStatistics != null) {
            LOG.debug("Unregistering RocksDB statistics for bucket {}", bucket);
            IOUtils.closeQuietly(rocksDBStatistics);
            rocksDBStatistics = null;
        }
    }

    /**
     * Get the RocksDB statistics for this bucket.
     *
     * @return the RocksDB statistics, or null if not a KV table or not yet initialized
     */
    @Nullable
    public RocksDBStatistics getRocksDBStatistics() {
        return rocksDBStatistics;
    }

    @Override
    public void close() {
        // Clean up RocksDB statistics before closing the metric group
        // This handles the case when the bucket is removed entirely
        unregisterRocksDBStatistics();
        super.close();
    }
}
