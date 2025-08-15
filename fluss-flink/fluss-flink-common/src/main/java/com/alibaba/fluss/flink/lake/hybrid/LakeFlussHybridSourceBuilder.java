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

package com.alibaba.fluss.flink.lake.hybrid;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.source.FlinkSource;
import com.alibaba.fluss.flink.utils.DataLakeUtils;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePlugin;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The builder for hybrid Lake source and fluss source. */
public class LakeFlussHybridSourceBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final Configuration flussConfig;

    private int[][] projectedFields;
    private List<ResolvedExpression> filters;

    private FlinkSource.Builder<RowData> flussFlinkSourceBuilder;

    private LakeFlussHybridSourceBuilder(TablePath tablePath, Configuration flussConfig) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
    }

    public static LakeFlussHybridSourceBuilder builder(
            TablePath tablePath, Configuration flussConfig) {
        return new LakeFlussHybridSourceBuilder(tablePath, flussConfig);
    }

    public LakeFlussHybridSourceBuilder setProjectionFields(int[][] projectedFields) {
        this.projectedFields = projectedFields;
        return this;
    }

    public LakeFlussHybridSourceBuilder setFilters(List<ResolvedExpression> filters) {
        this.filters = filters;
        return this;
    }

    public LakeFlussHybridSourceBuilder setFlussFlinkSourceBuilder(
            FlinkSource.Builder<RowData> flussFlinkSourceBuilder) {
        this.flussFlinkSourceBuilder = flussFlinkSourceBuilder;
        return this;
    }

    public Source<RowData, ?, ?> build() {
        try {
            LakeStoragePlugin lakeStoragePlugin =
                    LakeStoragePluginSetUp.fromDataLakeFormat(
                            DataLakeFormat.PAIMON.toString(), null);

            Map<String, String> lakeProperties;
            long snapshotId;
            Map<TableBucket, Long> tableBucketsOffset;
            Map<Integer, Long> bucketOffset = new HashMap<>();
            Map<String, Long> partitionIdByName = new HashMap<>();
            try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                    Admin admin = connection.getAdmin()) {
                TableInfo tableInfo = admin.getTableInfo(tablePath).get();
                lakeProperties =
                        DataLakeUtils.extractLakeCatalogProperties(tableInfo.getProperties());

                LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
                snapshotId = lakeSnapshot.getSnapshotId();
                tableBucketsOffset = lakeSnapshot.getTableBucketsOffset();
                for (Map.Entry<TableBucket, Long> entry : tableBucketsOffset.entrySet()) {
                    bucketOffset.put(entry.getKey().getBucket(), entry.getValue());
                }
                if (tableInfo.isPartitioned()) {
                    List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
                    for (PartitionInfo partitionInfo : partitionInfos) {
                        partitionIdByName.put(
                                partitionInfo.getPartitionName(), partitionInfo.getPartitionId());
                    }
                }
            }

            LakeStorage lakeStorage =
                    lakeStoragePlugin.createLakeStorage(Configuration.fromMap(lakeProperties));

            LakeFlinkSource lakeFlinkSourceFactory =
                    (LakeFlinkSource) lakeStorage.createLakeFlinkSourceFactory();

            LakeSplitAssigner lakeSplitAssigner = new LakeSplitAssigner(partitionIdByName);

            FlinkSourceContext flinkSourceContext =
                    new FlinkSourceContext(
                            snapshotId,
                            tablePath,
                            projectedFields,
                            filters,
                            partitionIdByName.keySet(),
                            lakeSplitAssigner);

            Source<RowData, ?, ?> paimonSource =
                    lakeFlinkSourceFactory.createFlinkSource(flinkSourceContext);
            return HybridSource.builder(paimonSource)
                    .addSource(
                            switchContext ->
                                    flussFlinkSourceBuilder
                                            .setOffsetsInitializer(
                                                    new HybridOffsetsInitializer(bucketOffset))
                                            .build(),
                            Boundedness.CONTINUOUS_UNBOUNDED)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
