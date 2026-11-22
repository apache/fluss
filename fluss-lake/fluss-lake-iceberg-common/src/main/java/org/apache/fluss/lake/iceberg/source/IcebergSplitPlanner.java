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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.utils.IcebergCatalogUtils;
import org.apache.fluss.lake.iceberg.utils.IcebergPartitionSpecUtils;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.io.CloseableIterable;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

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
        TableScan tableScan = table.newScan().useSnapshot(snapshotId);
        if (filter != null) {
            Set<String> filterColumns = referencedColumns(filter);
            if (!filterColumns.isEmpty()) {
                tableScan = tableScan.includeColumnStats(filterColumns);
            }
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

    @VisibleForTesting
    Set<String> referencedColumns(Expression expression) {
        return ExpressionVisitors.visit(
                expression,
                new ExpressionVisitors.ExpressionVisitor<Set<String>>() {
                    @Override
                    public Set<String> alwaysTrue() {
                        return Collections.emptySet();
                    }

                    @Override
                    public Set<String> alwaysFalse() {
                        return Collections.emptySet();
                    }

                    @Override
                    public Set<String> not(Set<String> result) {
                        return result;
                    }

                    @Override
                    public Set<String> and(Set<String> leftResult, Set<String> rightResult) {
                        return union(leftResult, rightResult);
                    }

                    @Override
                    public Set<String> or(Set<String> leftResult, Set<String> rightResult) {
                        return union(leftResult, rightResult);
                    }

                    @Override
                    public <T> Set<String> predicate(BoundPredicate<T> pred) {
                        return Collections.singleton(pred.ref().name());
                    }

                    @Override
                    public <T> Set<String> predicate(UnboundPredicate<T> pred) {
                        return Collections.singleton(pred.ref().name());
                    }
                });
    }

    private Set<String> union(Set<String> left, Set<String> right) {
        Set<String> result = new LinkedHashSet<>(left);
        result.addAll(right);
        return result;
    }

    private Function<FileScanTask, Integer> createBucketExtractor(Table table) {
        List<PartitionField> partitionFields = table.spec().fields();
        if (partitionFields.isEmpty()) {
            return task -> -1;
        }

        // Only a bucket(bucketKey) transform carries a physical bucket value in the partition
        // tuple.
        // A legacy identity(__bucket) partition or a bucket-unaware user partition has none.
        int bucketFieldIndex = partitionFields.size() - 1;
        if (!IcebergPartitionSpecUtils.isBucketTransform(partitionFields.get(bucketFieldIndex))) {
            return task -> -1;
        }
        return task -> task.file().partition().get(bucketFieldIndex, Integer.class);
    }

    private Function<FileScanTask, List<String>> createPartitionExtractor(Table table) {
        List<PartitionField> partitionFields = table.spec().fields();
        if (partitionFields.isEmpty()) {
            return task -> Collections.emptyList();
        }

        // The trailing field is the Fluss bucket (legacy identity(__bucket) or a bucket(bucketKey)
        // transform); everything before it is the Fluss partition columns.
        PartitionField lastField = partitionFields.get(partitionFields.size() - 1);
        int partitionColCount =
                IcebergPartitionSpecUtils.isFlussBucketField(table.schema(), lastField)
                        ? partitionFields.size() - 1
                        : partitionFields.size();

        if (partitionColCount == 0) {
            return task -> Collections.emptyList();
        }
        List<Integer> partitionFieldIndices =
                IntStream.range(0, partitionColCount).boxed().collect(Collectors.toList());
        return task ->
                partitionFieldIndices.stream()
                        // since currently, only string partition is supported
                        .map(index -> task.partition().get(index, String.class))
                        .collect(Collectors.toList());
    }
}
