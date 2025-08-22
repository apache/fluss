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

package com.alibaba.fluss.server.metrics.group;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.utils.MapUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** The metric group for coordinator server. */
public class CoordinatorMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "coordinator";

    private final Map<TablePath, SimpleTableMetricGroup> metricGroupByTable =
            MapUtils.newConcurrentHashMap();

    protected final String clusterId;
    protected final String hostname;
    protected final String serverId;

    public CoordinatorMetricGroup(
            MetricRegistry registry, String clusterId, String hostname, String serverId) {
        super(registry, new String[] {clusterId, hostname, NAME}, null);
        this.clusterId = clusterId;
        this.hostname = hostname;
        this.serverId = serverId;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("cluster_id", clusterId);
        variables.put("host", hostname);
        variables.put("server_id", serverId);
    }

    // ------------------------------------------------------------------------
    //  table buckets groups
    // ------------------------------------------------------------------------

    public @Nullable BucketMetricGroup getTableBucketMetricGroup(
            TablePath tablePath, TableBucket tableBucket) {
        SimpleTableMetricGroup tableMetricGroup = metricGroupByTable.get(tablePath);
        if (tableMetricGroup == null) {
            return null;
        }
        return tableMetricGroup.buckets.get(tableBucket);
    }

    public void addTableBucketMetricGroup(
            PhysicalTablePath physicalTablePath,
            long tableId,
            @Nullable Long partitionId,
            Set<Integer> assignments) {
        TablePath tablePath = physicalTablePath.getTablePath();
        SimpleTableMetricGroup tableMetricGroup =
                metricGroupByTable.computeIfAbsent(
                        tablePath, table -> new SimpleTableMetricGroup(registry, tablePath, this));
        assignments.forEach(
                bucket ->
                        tableMetricGroup.addBucketMetricGroup(
                                physicalTablePath.getPartitionName(),
                                new TableBucket(tableId, partitionId, bucket)));
    }

    public void removeTableMetricGroup(TablePath tablePath, long tableId) {
        SimpleTableMetricGroup tableMetricGroup = metricGroupByTable.remove(tablePath);
        if (tableMetricGroup != null) {
            tableMetricGroup.removeBucketMetricsGroupForTable(tableId);
            tableMetricGroup.close();
        }
    }

    public void removeTablePartitionMetricsGroup(
            TablePath tablePath, long tableId, long partitionId) {
        SimpleTableMetricGroup tableMetricGroup = metricGroupByTable.get(tablePath);
        if (tableMetricGroup != null) {
            tableMetricGroup.removeBucketMetricsGroupForPartition(tableId, partitionId);
        }
    }

    /** The metric group for table. */
    public static class SimpleTableMetricGroup extends AbstractMetricGroup {

        private final Map<TableBucket, BucketMetricGroup> buckets = new HashMap<>();

        private final TablePath tablePath;

        private final MetricRegistry registry;

        public SimpleTableMetricGroup(
                MetricRegistry registry,
                TablePath tablePath,
                AbstractMetricGroup serverMetricGroup) {
            super(
                    registry,
                    makeScope(
                            serverMetricGroup,
                            tablePath.getDatabaseName(),
                            tablePath.getTableName()),
                    serverMetricGroup);

            this.tablePath = tablePath;
            this.registry = registry;
        }

        @Override
        protected void putVariables(Map<String, String> variables) {
            variables.put("database", tablePath.getDatabaseName());
            variables.put("table", tablePath.getTableName());
        }

        @Override
        protected String getGroupName(CharacterFilter filter) {
            // partition and table share same logic group name
            return "table";
        }

        // ------------------------------------------------------------------------
        //  bucket groups
        // ------------------------------------------------------------------------
        public void addBucketMetricGroup(@Nullable String partitionName, TableBucket tableBucket) {
            buckets.computeIfAbsent(
                    tableBucket,
                    (bucket) ->
                            new BucketMetricGroup(
                                    registry, partitionName, tableBucket.getBucket(), this));
        }

        public void removeBucketMetricsGroupForTable(long tableId) {
            List<TableBucket> tableBuckets = new ArrayList<>();
            buckets.forEach(
                    (tableBucket, bucketMetricGroup) -> {
                        if (tableBucket.getTableId() == tableId) {
                            tableBuckets.add(tableBucket);
                        }
                    });
            tableBuckets.forEach(this::removeBucketMetricGroup);
        }

        public void removeBucketMetricsGroupForPartition(long tableId, long partitionId) {
            List<TableBucket> tableBuckets = new ArrayList<>();
            buckets.forEach(
                    (tableBucket, bucketMetricGroup) -> {
                        Long bucketPartitionId = tableBucket.getPartitionId();
                        if (tableBucket.getTableId() == tableId
                                && bucketPartitionId != null
                                && bucketPartitionId == partitionId) {
                            tableBuckets.add(tableBucket);
                        }
                    });
            tableBuckets.forEach(this::removeBucketMetricGroup);
        }

        public void removeBucketMetricGroup(TableBucket tb) {
            BucketMetricGroup metricGroup = buckets.remove(tb);
            metricGroup.close();
        }
    }
}
