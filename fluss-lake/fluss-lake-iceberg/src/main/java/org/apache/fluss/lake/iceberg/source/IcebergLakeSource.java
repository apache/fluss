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
import org.apache.fluss.lake.iceberg.utils.FlussToIcebergPredicateConverter;
import org.apache.fluss.lake.iceberg.utils.IcebergCatalogUtils;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.SnapshotUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

/** Iceberg lake source. */
public class IcebergLakeSource implements LakeSource<IcebergSplit> {
    private static final long serialVersionUID = 1L;
    private final Configuration icebergConfig;
    private final TablePath tablePath;
    private @Nullable int[][] project;
    private @Nullable Expression filter;

    public IcebergLakeSource(Configuration icebergConfig, TablePath tablePath) {
        this.icebergConfig = icebergConfig;
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        List<Predicate> unConsumedPredicates = new ArrayList<>();
        List<Predicate> consumedPredicates = new ArrayList<>();
        List<Expression> converted = new ArrayList<>();
        Schema schema = getSchema(tablePath);
        for (Predicate predicate : predicates) {
            Optional<Expression> optPredicate =
                    FlussToIcebergPredicateConverter.convert(schema, predicate);
            if (optPredicate.isPresent()) {
                consumedPredicates.add(predicate);
                converted.add(optPredicate.get());
            } else {
                unConsumedPredicates.add(predicate);
            }
        }
        if (!converted.isEmpty()) {
            filter = converted.stream().reduce(Expressions::and).orElse(null);
        }
        return FilterPushDownResult.of(consumedPredicates, unConsumedPredicates);
    }

    @Override
    public Planner<IcebergSplit> createPlanner(PlannerContext context) throws IOException {
        return new IcebergSplitPlanner(icebergConfig, tablePath, context.snapshotId(), filter);
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<IcebergSplit> context) throws IOException {
        // The reader takes ownership of the catalog and closes it (releasing its S3 connection
        // pool) when its row iterator closes; otherwise every split leaks an S3FileIO.
        Catalog catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        Table table = catalog.loadTable(toIceberg(tablePath));
        return new IcebergRecordReader(context.lakeSplit().fileScanTask(), table, project, catalog);
    }

    @Override
    @Nullable
    public DataDeltaPlan<IcebergSplit> planDelta(long fromSnapshotId) {
        // planDelta runs every tiering round; close the catalog before returning, else the tiering
        // job leaks one S3FileIO (and its connection pool) per round.
        Catalog catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        try {
            Table table = catalog.loadTable(toIceberg(tablePath));

            // Mirror the Paimon model (FIP-31 §7): process a pending compaction BEFORE new appends,
            // in its own round with its own compactSnapshotId. Paimon indexes appends in its write
            // path; Iceberg indexes them here (below), so planDelta does compaction-then-append.
            DataDeltaPlan<IcebergSplit> compactionPlan = planCompaction(table, fromSnapshotId);
            if (compactionPlan != null) {
                return compactionPlan;
            }

            // Index the latest APPEND snapshot's data files; skip DV/rewrite snapshots (Puffin
            // materialization) which carry no new data and break IncrementalAppendScan.
            Snapshot toAppend = table.currentSnapshot();
            while (toAppend != null
                    && !org.apache.iceberg.DataOperations.APPEND.equals(toAppend.operation())) {
                Long parentId = toAppend.parentId();
                toAppend = parentId == null ? null : table.snapshot(parentId);
            }
            if (toAppend == null) {
                return null;
            }
            long toSnapshotId = toAppend.snapshotId();
            boolean hasValidFrom = fromSnapshotId > 0 && table.snapshot(fromSnapshotId) != null;
            // from past the last append (an index-only round committed a compaction id): its plan
            // already indexed everything up to it
            if (hasValidFrom
                    && fromSnapshotId != toSnapshotId
                    && SnapshotUtil.isAncestorOf(table, fromSnapshotId, toSnapshotId)) {
                return null;
            }

            // The RowPos scan runs BEFORE this round's data is committed, so `toAppend` is the last
            // committed append. Append-only Iceberg tiering thus sees from == to on the steady
            // state: index the files added in that snapshot itself (parent -> to). Only when a
            // strictly newer append exists do we scan the delta from -> to.
            Long fromExclusive;
            if (hasValidFrom && fromSnapshotId != toSnapshotId) {
                fromExclusive = fromSnapshotId;
            } else {
                fromExclusive = toAppend.parentId(); // null if toAppend is the first snapshot
            }

            Function<FileScanTask, Integer> bucketExtractor =
                    IcebergSplitPlanner.createBucketExtractor(table);
            Function<FileScanTask, List<String>> partitionExtractor =
                    IcebergSplitPlanner.createPartitionExtractor(table);

            // Newly appended data files (each carries __rowid) to index into the RowId->FilePos
            // SST.
            List<IcebergSplit> newSplits = new ArrayList<>();
            IncrementalAppendScan scan = table.newIncrementalAppendScan().toSnapshot(toSnapshotId);
            if (fromExclusive != null) {
                scan = scan.fromSnapshotExclusive(fromExclusive);
            }
            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (FileScanTask task : tasks) {
                    newSplits.add(
                            new IcebergSplit(
                                    task,
                                    bucketExtractor.apply(task),
                                    partitionExtractor.apply(task)));
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to plan Iceberg delta files.", e);
            }

            if (newSplits.isEmpty()) {
                return null;
            }
            // Appends carry no removed files, so no oldFiles cleanup here (compaction, handled
            // above, is the only source of removed files).
            return new DataDeltaPlan<>(toSnapshotId, newSplits, Collections.emptyMap());
        } finally {
            closeCatalogQuietly(catalog);
        }
    }

    /**
     * Detects a pending compaction and turns it into a {@link DataDeltaPlan}, mirroring {@code
     * PaimonLakeSource.planDelta} (FIP-31 §7): find the latest compaction snapshot in {@code
     * (fromSnapshotId, current]}, diff {@code fromSnapshotId -> compactSnapshot}, and emit the
     * files new-in-compact as {@code splits} (re-indexed into RowPosIndex at their compacted
     * positions) and the files removed-by-compact as {@code deletedFiles} (basenames grouped by
     * partition/bucket) so {@code DvManager.cleanupOldFiles} prunes their stale LakeDv/RowPosIndex
     * entries. When {@code fromSnapshotId} is invalid (first run or expired), all files at the
     * compaction snapshot are treated as new (full scan). The returned {@code compactSnapshotId} is
     * the compaction snapshot's id. Returns null when no compaction is pending.
     *
     * <p>Iceberg has no Paimon-style {@code commitKind}; a compaction is a {@code REPLACE} snapshot
     * that rewrites DATA files (non-empty {@code removedDataFiles}), which distinguishes it from
     * the DV-Puffin {@code RewriteFiles} that touch only delete files.
     */
    @Nullable
    private DataDeltaPlan<IcebergSplit> planCompaction(Table table, long fromSnapshotId) {
        Snapshot current = table.currentSnapshot();
        if (current == null) {
            return null;
        }
        boolean hasValidFrom = fromSnapshotId > 0 && table.snapshot(fromSnapshotId) != null;
        FileIO io = table.io();

        // Find the latest compaction snapshot in (fromSnapshotId, current].
        Snapshot compactSnapshot = null;
        for (Snapshot s = current;
                s != null && !(hasValidFrom && s.snapshotId() == fromSnapshotId); ) {
            if (isDataCompaction(s, io)) {
                compactSnapshot = s;
                break;
            }
            Long parent = s.parentId();
            s = parent == null ? null : table.snapshot(parent);
        }
        if (compactSnapshot == null) {
            return null;
        }
        long compactSnapshotId = compactSnapshot.snapshotId();

        // Diff fromSnapshotId -> compactSnapshot: net-added files (new) and net-removed files.
        Map<String, DataFile> netAdded = new LinkedHashMap<>();
        Map<String, DataFile> netRemoved = new LinkedHashMap<>();
        if (hasValidFrom) {
            // `from` is the last committed append; its files are not indexed yet (append path
            // indexes them when from == to), so this compaction round must index them too
            Snapshot fromSnap = table.snapshot(fromSnapshotId);
            if (DataOperations.APPEND.equals(fromSnap.operation())) {
                fromSnap.addedDataFiles(io).forEach(df -> netAdded.put(df.location(), df));
            }
            List<Snapshot> lineage = new ArrayList<>();
            for (Snapshot s = compactSnapshot; s != null && s.snapshotId() != fromSnapshotId; ) {
                lineage.add(s);
                Long parent = s.parentId();
                s = parent == null ? null : table.snapshot(parent);
            }
            for (int i = lineage.size() - 1; i >= 0; i--) {
                Snapshot snap = lineage.get(i);
                for (DataFile df : snap.removedDataFiles(io)) {
                    if (netAdded.remove(df.location()) == null) {
                        netRemoved.put(df.location(), df);
                    }
                }
                for (DataFile df : snap.addedDataFiles(io)) {
                    netAdded.put(df.location(), df);
                    netRemoved.remove(df.location());
                }
            }
        } else {
            // Expired / first-time base: treat all files at the compaction snapshot as new.
            try (CloseableIterable<FileScanTask> tasks =
                    table.newScan().useSnapshot(compactSnapshotId).planFiles()) {
                for (FileScanTask task : tasks) {
                    netAdded.put(task.file().location(), task.file());
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to scan Iceberg compaction snapshot.", e);
            }
        }

        return toCompactionPlan(table, compactSnapshotId, netAdded, netRemoved);
    }

    /** A REPLACE that removes data files (compaction), not a DV-only Puffin rewrite. */
    private static boolean isDataCompaction(Snapshot s, FileIO io) {
        return DataOperations.REPLACE.equals(s.operation())
                && s.removedDataFiles(io).iterator().hasNext();
    }

    /**
     * Re-index {@code netAdded} at the compaction snapshot; group {@code netRemoved} for cleanup.
     */
    @Nullable
    private DataDeltaPlan<IcebergSplit> toCompactionPlan(
            Table table,
            long compactSnapshotId,
            Map<String, DataFile> netAdded,
            Map<String, DataFile> netRemoved) {
        // Re-index the new files: fetch their FileScanTasks at the compaction snapshot.
        Function<FileScanTask, Integer> bucketExtractor =
                IcebergSplitPlanner.createBucketExtractor(table);
        Function<FileScanTask, List<String>> partitionExtractor =
                IcebergSplitPlanner.createPartitionExtractor(table);
        List<IcebergSplit> newSplits = new ArrayList<>();
        if (!netAdded.isEmpty()) {
            try (CloseableIterable<FileScanTask> tasks =
                    table.newScan().useSnapshot(compactSnapshotId).planFiles()) {
                for (FileScanTask task : tasks) {
                    if (netAdded.containsKey(task.file().location())) {
                        newSplits.add(
                                new IcebergSplit(
                                        task,
                                        bucketExtractor.apply(task),
                                        partitionExtractor.apply(task)));
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to plan Iceberg compacted files.", e);
            }
        }

        // Removed files (basenames) grouped by partition -> bucket, for oldFiles cleanup.
        Function<StructLike, Integer> bucketFromPartition =
                IcebergSplitPlanner.createBucketExtractorFromPartition(table);
        Function<StructLike, List<String>> partitionFromPartition =
                IcebergSplitPlanner.createPartitionExtractorFromPartition(table);
        Map<String, Map<Integer, List<String>>> deletedFiles = new HashMap<>();
        for (DataFile df : netRemoved.values()) {
            int bucket = bucketFromPartition.apply(df.partition());
            List<String> partitionValues = partitionFromPartition.apply(df.partition());
            String partitionName =
                    partitionValues.isEmpty()
                            ? null
                            : String.join(
                                    ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR,
                                    partitionValues);
            String fullPath = df.location();
            String basename = fullPath.substring(fullPath.lastIndexOf('/') + 1);
            deletedFiles
                    .computeIfAbsent(partitionName, k -> new HashMap<>())
                    .computeIfAbsent(bucket, k -> new ArrayList<>())
                    .add(basename);
        }

        if (newSplits.isEmpty() && deletedFiles.isEmpty()) {
            return null;
        }
        return new DataDeltaPlan<>(compactSnapshotId, newSplits, deletedFiles);
    }

    @Override
    public SimpleVersionedSerializer<IcebergSplit> getSplitSerializer() {
        return new IcebergSplitSerializer();
    }

    private Schema getSchema(TablePath tablePath) {
        Catalog catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        try {
            return catalog.loadTable(toIceberg(tablePath)).schema();
        } finally {
            closeCatalogQuietly(catalog);
        }
    }

    /** Close the catalog (releasing its FileIO/S3 connection pool); never throw on close. */
    private static void closeCatalogQuietly(Catalog catalog) {
        if (catalog instanceof AutoCloseable) {
            try {
                ((AutoCloseable) catalog).close();
            } catch (Exception ignored) {
                // best-effort; a failed catalog close must not fail planning
            }
        }
    }
}
