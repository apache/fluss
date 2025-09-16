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

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.utils.MapUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** The metric group for coordinator server. */
public class CoordinatorMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "coordinator";

    private final Map<TablePath, SimpleTableMetricGroup> metricGroupByTable =
            MapUtils.newConcurrentHashMap();

    protected final String clusterId;
    protected final String hostname;
    protected final String serverId;

    private final Map<Class<? extends CoordinatorEvent>, CoordinatorEventMetricGroup>
            eventMetricGroups = MapUtils.newConcurrentHashMap();

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

    public CoordinatorEventMetricGroup getOrAddEventTypeMetricGroup(
            Class<? extends CoordinatorEvent> eventClass) {
        return eventMetricGroups.computeIfAbsent(
                eventClass, e -> new CoordinatorEventMetricGroup(registry, eventClass, this));
    }

    // ------------------------------------------------------------------------
    //  table buckets groups
    // ------------------------------------------------------------------------

    public @Nullable MetricGroup getTableBucketMetricGroup(
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
            Set<Integer> buckets) {
        TablePath tablePath = physicalTablePath.getTablePath();
        SimpleTableMetricGroup tableMetricGroup =
                metricGroupByTable.computeIfAbsent(
                        tablePath, table -> new SimpleTableMetricGroup(registry, tablePath, this));
        buckets.forEach(
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
    private static class SimpleTableMetricGroup extends AbstractMetricGroup {

        private final Map<TableBucket, SimpleBucketMetricGroup> buckets = new HashMap<>();

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
                            new SimpleBucketMetricGroup(
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
            SimpleBucketMetricGroup metricGroup = buckets.remove(tb);
            metricGroup.close();
        }
    }

    /** The metric group for bucket. */
    private static class SimpleBucketMetricGroup extends AbstractMetricGroup {
        // will be null if the bucket doesn't belong to a partition
        private final @Nullable String partitionName;
        private final int bucket;

        public SimpleBucketMetricGroup(
                MetricRegistry registry,
                @Nullable String partitionName,
                int bucket,
                SimpleTableMetricGroup parent) {
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
    }
}
