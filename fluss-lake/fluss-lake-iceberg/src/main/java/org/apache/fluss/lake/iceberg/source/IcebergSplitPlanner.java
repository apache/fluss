/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.utils.IcebergCatalogUtils;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;

/** Iceberg split planner. */
public class IcebergSplitPlanner implements Planner<IcebergSplit> {

    private final Configuration icebergConfig;
    private final TablePath tablePath;
    private final long snapshotId;
    private final @Nullable Expression filter;

    public IcebergSplitPlanner(
            Configuration icebergConfig, TablePath tablePath, long snapshotId, Expression filter) {
        this.icebergConfig = icebergConfig;
        this.tablePath = tablePath;
        this.snapshotId = snapshotId;
        this.filter = filter;
    }

    @Override
    public List<IcebergSplit> plan() throws IOException {
        List<IcebergSplit> splits = new ArrayList<>();
        Catalog catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        Table table = catalog.loadTable(toIceberg(tablePath));
        Function<FileScanTask, List<String>> partitionExtract = createPartitionExtractor(table);
        Function<FileScanTask, Integer> bucketExtractor = createBucketExtractor(table);
        TableScan tableScan = table.newScan().useSnapshot(snapshotId).includeColumnStats();
        if (filter != null) {
            tableScan = tableScan.filter(filter);
        }
        try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
            tasks.forEach(
                    task ->
                            splits.add(
                                    new IcebergSplit(
                                            task,
                                            bucketExtractor.apply(task),
                                            partitionExtract.apply(task))));
        }
        return splits;
    }

    private Function<FileScanTask, Integer> createBucketExtractor(Table table) {
        PartitionSpec partitionSpec = table.spec();
        List<PartitionField> partitionFields = partitionSpec.fields();

        // the last one must be partition by fluss bucket
        PartitionField bucketField = partitionFields.get(partitionFields.size() - 1);

        if (table.schema()
                .asStruct()
                .field(bucketField.sourceId())
                .name()
                .equals(BUCKET_COLUMN_NAME)) {
            // partition by __bucket column, should be fluss log table without bucket key,
            // we don't care about the bucket since it's bucket un-aware
            return task -> -1;
        } else {
            int bucketFieldIndex = partitionFields.size() - 1;
            return task -> task.file().partition().get(bucketFieldIndex, Integer.class);
        }
    }

    private Function<FileScanTask, List<String>> createPartitionExtractor(Table table) {
        PartitionSpec partitionSpec = table.spec();
        List<PartitionField> partitionFields = partitionSpec.fields();

        // if only one partition, it must not be partitioned table since we will always use
        // partition by fluss bucket
        if (partitionSpec.fields().size() <= 1) {
            return task -> Collections.emptyList();
        } else {
            List<Integer> partitionFieldIndices =
                    // since will always first partition by fluss partition columns, then fluss
                    // bucket,
                    // just ignore the last partition column of iceberg
                    IntStream.range(0, partitionFields.size() - 1)
                            .boxed()
                            .collect(Collectors.toList());
            return task ->
                    partitionFieldIndices.stream()
                            // since currently, only string partition is supported
                            .map(index -> task.partition().get(index, String.class))
                            .collect(Collectors.toList());
        }
    }
}
