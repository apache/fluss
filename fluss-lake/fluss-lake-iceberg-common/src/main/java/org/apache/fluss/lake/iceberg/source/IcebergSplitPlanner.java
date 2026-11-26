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
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;

/** Iceberg split planner. */
public class IcebergSplitPlanner implements Planner<IcebergSplit> {

    /**
     * Orders splits by the data sequence number of their file, which follows the commit order of
     * the tiering rounds. Files without a data sequence number are ordered first.
     *
     * <p>Files of one commit share a sequence number, and Iceberg may list them in any order: a
     * commit's files can span several manifests, which {@code planFiles()} reads in parallel. A
     * tiering round writes each bucket with a single rolling writer, whose file names differ only
     * in a zero-padded counter that grows as it rolls files, so the file name puts those files
     * back in write order. The name, not the full location, is compared, since a location provider
     * may prefix it with a hash.
     */
    private static final Comparator<IcebergSplit> SEQUENCE_NUMBER_ORDER =
            Comparator.<IcebergSplit, Long>comparing(
                            split -> split.fileScanTask().file().dataSequenceNumber(),
                            Comparator.nullsFirst(Comparator.<Long>naturalOrder()))
                    .thenComparing(split -> fileName(split.fileScanTask().file().location()));

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
        // Tables tiered before FIP-27 carry the __offset column, which orders a bucket's rows
        // exactly, also after compaction.
        Types.NestedField offsetField = table.schema().findField(OFFSET_COLUMN_NAME);
        TableScan tableScan = table.newScan().useSnapshot(snapshotId);
        Set<String> statsColumns = new HashSet<>();
        if (filter != null) {
            statsColumns.addAll(referencedColumns(filter));
        }
        if (offsetField != null) {
            statsColumns.add(OFFSET_COLUMN_NAME);
        }
        if (!statsColumns.isEmpty()) {
            tableScan = tableScan.includeColumnStats(statsColumns);
        }
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
        // Iceberg plans files in no particular order: manifests are kept in a hash set and,
        // with more than one manifest, planned concurrently on a worker pool. The Flink source
        // reads a bucket's lake splits one after another, so put them back in write order.
        sortInWriteOrder(splits, offsetField == null ? null : offsetField.fieldId());
        return splits;
    }

    /**
     * Sorts splits so that the files of each bucket are in write order.
     *
     * <p>If the table has an {@code __offset} column and every file records its lower bound, the
     * files are ordered by it. The offsets of a bucket's files do not overlap, also after
     * compaction, which rewrites runs of files sorted by {@code __offset}, so this order is exact.
     * Otherwise, the files are ordered by {@link #SEQUENCE_NUMBER_ORDER}.
     */
    @VisibleForTesting
    static void sortInWriteOrder(List<IcebergSplit> splits, @Nullable Integer offsetFieldId) {
        if (offsetFieldId != null
                && splits.stream()
                        .allMatch(split -> offsetLowerBound(split, offsetFieldId) != null)) {
            splits.sort(
                    Comparator.<IcebergSplit, Long>comparing(
                                    split -> offsetLowerBound(split, offsetFieldId))
                            .thenComparing(SEQUENCE_NUMBER_ORDER));
        } else {
            splits.sort(SEQUENCE_NUMBER_ORDER);
        }
    }

    @Nullable
    private static Long offsetLowerBound(IcebergSplit split, int offsetFieldId) {
        Map<Integer, ByteBuffer> lowerBounds = split.fileScanTask().file().lowerBounds();
        ByteBuffer lowerBound = lowerBounds == null ? null : lowerBounds.get(offsetFieldId);
        return lowerBound == null
                ? null
                : Conversions.<Long>fromByteBuffer(Types.LongType.get(), lowerBound);
    }

    private static String fileName(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
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
