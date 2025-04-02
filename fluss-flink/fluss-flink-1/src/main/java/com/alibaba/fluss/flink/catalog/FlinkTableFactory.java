/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.flink.catalog;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.sink.FlinkTableSink;
import com.alibaba.fluss.flink.source.FlinkTableSource;
import com.alibaba.fluss.flink.utils.FlinkConnectorOptionsUtils;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.MergeEngineType;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** Factory to create table source and table sink for Fluss. */
public class FlinkTableFactory extends AbstractTableFactory {

    @Override
    public DynamicTableSource dynamicTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableOutputType,
            int[] primaryKeyIndexes,
            int[] bucketKeyIndexes,
            int[] partitionKeyIndexes,
            boolean streaming,
            FlinkConnectorOptionsUtils.StartupOptions startupOptions,
            int lookupMaxRetryTimes,
            boolean lookupAsync,
            @Nullable LookupCache cache,
            long scanPartitionDiscoveryIntervalMs,
            boolean isDataLakeEnabled,
            @Nullable MergeEngineType mergeEngineType) {
        return new FlinkTableSource(
                tablePath,
                flussConfig,
                tableOutputType,
                primaryKeyIndexes,
                bucketKeyIndexes,
                partitionKeyIndexes,
                streaming,
                startupOptions,
                lookupMaxRetryTimes,
                lookupAsync,
                cache,
                scanPartitionDiscoveryIntervalMs,
                isDataLakeEnabled,
                mergeEngineType);
    }

    @Override
    public DynamicTableSink dynamicTableSink(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            int[] primaryKeyIndexes,
            List<String> partitionKeys,
            boolean streaming,
            @Nullable MergeEngineType mergeEngineType,
            @Nullable DataLakeFormat lakeFormat,
            boolean ignoreDelete,
            int numBucket,
            List<String> bucketKeys,
            boolean shuffleByBucketId) {
        return new FlinkTableSink(
                tablePath,
                flussConfig,
                tableRowType,
                primaryKeyIndexes,
                partitionKeys,
                streaming,
                mergeEngineType,
                lakeFormat,
                ignoreDelete,
                numBucket,
                bucketKeys,
                shuffleByBucketId);
    }
}
