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

package org.apache.fluss.flink.source.enumerator;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.LakeSplitGenerator;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.enumerator.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.flink.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer.BucketOffsetsRetriever;
import org.apache.fluss.flink.source.enumerator.initializer.SnapshotOffsetsInitializer;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.state.SourceEnumeratorState;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * An implementation of {@link SplitEnumerator} for the data of Fluss.
 *
 * <p>The enumerator is responsible for:
 *
 * <ul>
 *   <li>Get the all splits(lake split + kv snapshot split + log split) for a table of Fluss to be
 *       read.
 *   <li>Assign the splits to readers with the guarantee that the splits belong to the same bucket
 *       will be assigned to same reader.
 * </ul>
 */
@Internal
public class FlinkSourceEnumerator
        implements SplitEnumerator<SourceSplitBase, SourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceEnumerator.class);

    private final WorkerExecutor workerExecutor;
    private final TablePath tablePath;
    private final boolean hasPrimaryKey;
    private final boolean isPartitioned;
    private final Configuration flussConf;

    private final SplitEnumeratorContext<SourceSplitBase> context;

    private final Map<Integer, List<SourceSplitBase>> pendingSplitAssignment;

    /**
     * Partitions that have been assigned to readers, will be empty when the table is not
     * partitioned. Mapping from partition id to partition name.
     *
     * <p>It's mainly used to help enumerator to broadcast the partition removed event to the
     * readers when partitions is dropped.
     *
     * <p>If an assigned partition exists only in the lake and has already expired in Fluss, it will
     * remain here indefinitely and will not be removed. However, considering that only a small
     * number of such lake-only partitions might exist during the initial startup, and they consume
     * minimal memory, this issue is being ignored for now.
     */
    private final Map<Long, String> assignedPartitions;

    /** Buckets that have been assigned to readers. */
    private final Set<TableBucket> assignedTableBuckets;

    @Nullable private List<SourceSplitBase> pendingHybridLakeFlussSplits;

    private final long scanPartitionDiscoveryIntervalMs;

    private final boolean streaming;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;

    // Lazily instantiated or mutable fields.
    private Connection connection;
    private Admin flussAdmin;
    private BucketOffsetsRetriever bucketOffsetsRetriever;
    private TableInfo tableInfo;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // split initializing has finished.
    private boolean noMoreNewSplits = false;

    private boolean lakeEnabled = false;

    private volatile boolean closed = false;

    @Nullable private final Predicate partitionFilters;

    @Nullable private final LakeSource<LakeSplit> lakeSource;

    public FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource) {
        this(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                context,
                Collections.emptySet(),
                Collections.emptyMap(),
                null,
                startingOffsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming,
                partitionFilters,
                lakeSource);
    }

    public FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            Set<TableBucket> assignedTableBuckets,
            Map<Long, String> assignedPartitions,
            List<SourceSplitBase> pendingHybridLakeFlussSplits,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource) {
        this(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                context,
                assignedTableBuckets,
                assignedPartitions,
                pendingHybridLakeFlussSplits,
                startingOffsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming,
                partitionFilters,
                lakeSource,
                new WorkerExecutor(context));
    }

    FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            Set<TableBucket> assignedTableBuckets,
            Map<Long, String> assignedPartitions,
            List<SourceSplitBase> pendingHybridLakeFlussSplits,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            WorkerExecutor workerExecutor) {
        this.tablePath = checkNotNull(tablePath);
        this.flussConf = checkNotNull(flussConf);
        this.hasPrimaryKey = hasPrimaryKey;
        this.isPartitioned = isPartitioned;
        this.context = checkNotNull(context);
        this.pendingSplitAssignment = new HashMap<>();
        this.assignedTableBuckets = new HashSet<>(assignedTableBuckets);
        this.assignedPartitions = new HashMap<>(assignedPartitions);
        this.pendingHybridLakeFlussSplits =
                pendingHybridLakeFlussSplits == null
                        ? null
                        : new LinkedList<>(pendingHybridLakeFlussSplits);
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.streaming = streaming;
        this.partitionFilters = partitionFilters;
        this.stoppingOffsetsInitializer =
                streaming ? new NoStoppingOffsetsInitializer() : OffsetsInitializer.latest();
        this.lakeSource = lakeSource;
        this.workerExecutor = workerExecutor;
    }

    @Override
    public void start() {
        // init admin client
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(flussAdmin, tablePath);
        try {
            tableInfo = flussAdmin.getTableInfo(tablePath).get();
            lakeEnabled = tableInfo.getTableConfig().isDataLakeEnabled();
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to get table info for %s", tablePath),
                    ExceptionUtils.stripCompletionException(e));
        }

        if (isPartitioned) {
            if (streaming) {
                if (lakeSource != null) {
                    // we'll need to consider lake splits
                    List<SourceSplitBase> hybridLakeFlussSplits = generateHybridLakeFlussSplits();
                    if (hybridLakeFlussSplits != null) {
                        LOG.info(
                                "Generated {} hybrid lake splits for table {}.",
                                hybridLakeFlussSplits.size(),
                                tablePath);
                        // handle hybrid lake fluss splits firstly
                        handleSplitsAdd(hybridLakeFlussSplits, null);
                    }
                }

                if (scanPartitionDiscoveryIntervalMs > 0) {
                    // should do partition discovery
                    LOG.info(
                            "Starting the FlussSourceEnumerator for table {} "
                                    + "with new partition discovery interval of {} ms.",
                            tablePath,
                            scanPartitionDiscoveryIntervalMs);
                    // discover new partitions and handle new partitions at fixed delay.
                    workerExecutor.callAsyncAtFixedDelay(
                            this::listPartitions,
                            this::checkPartitionChanges,
                            0,
                            scanPartitionDiscoveryIntervalMs);
                } else {
                    // just call once
                    LOG.info(
                            "Starting the FlussSourceEnumerator for table {} without partition discovery.",
                            tablePath);
                    workerExecutor.callAsync(this::listPartitions, this::checkPartitionChanges);
                }
            } else {
                startInBatchMode();
            }
        } else {
            if (streaming) {
                startInStreamModeForNonPartitionedTable();
            } else {
                startInBatchMode();
            }
        }
    }

    private void startInBatchMode() {
        if (lakeEnabled) {
            context.callAsync(
                    () -> {
                        List<SourceSplitBase> splits = generateHybridLakeFlussSplits();
                        if (splits == null) {
                            throw new UnsupportedOperationException(
                                    "Currently, Batch mode can only be supported if one lake snapshot exists for the table.");
                        }
                        return splits;
                    },
                    this::handleSplitsAdd);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Batch only supports when table option '%s' is set to true.",
                            ConfigOptions.TABLE_DATALAKE_ENABLED));
        }
    }

    private void startInStreamModeForNonPartitionedTable() {
        if (lakeSource != null) {
            context.callAsync(
                    () -> {
                        // firstly, try to generate hybrid lake splits,
                        List<SourceSplitBase> splits = generateHybridLakeFlussSplits();
                        // splits is null,
                        // we'll fall back to normal fluss splits generation logic
                        if (splits == null) {
                            splits = this.initNonPartitionedSplits();
                        }
                        return splits;
                    },
                    this::handleSplitsAdd);
        } else {
            // init bucket splits and assign
            context.callAsync(this::initNonPartitionedSplits, this::handleSplitsAdd);
        }
    }

    private List<SourceSplitBase> initNonPartitionedSplits() {
        if (hasPrimaryKey && startingOffsetsInitializer instanceof SnapshotOffsetsInitializer) {
            // get the table snapshot info
            final KvSnapshots kvSnapshots;
            try {
                kvSnapshots = flussAdmin.getLatestKvSnapshots(tablePath).get();
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format("Failed to get table snapshot for %s", tablePath),
                        ExceptionUtils.stripCompletionException(e));
            }
            return getSnapshotAndLogSplits(kvSnapshots, null);
        } else {
            return getLogSplit(null, null);
        }
    }

    private Set<PartitionInfo> listPartitions() {
        if (closed) {
            return Collections.emptySet();
        }
        try {
            List<PartitionInfo> partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
            partitionInfos = applyPartitionFilter(partitionInfos);
            return new LinkedHashSet<>(partitionInfos);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to list partitions for %s", tablePath),
                    ExceptionUtils.stripCompletionException(e));
        }
    }

    /** Apply partition filter. */
    private List<PartitionInfo> applyPartitionFilter(List<PartitionInfo> partitionInfos) {
        if (partitionFilters == null) {
            return partitionInfos;
        } else {
            int originalSize = partitionInfos.size();
            List<PartitionInfo> filteredPartitionInfos =
                    partitionInfos.stream()
                            .filter(partition -> partitionFilters.test(toInternalRow(partition)))
                            .collect(Collectors.toList());

            int filteredSize = filteredPartitionInfos.size();
            if (originalSize != filteredSize) {
                LOG.debug(
                        "Applied partition filter for table {}: {} partitions filtered down to {} "
                                + "matching partitions with predicate: {}. Matching partitions after filtering: {}",
                        tablePath,
                        originalSize,
                        filteredSize,
                        partitionFilters,
                        filteredPartitionInfos);
            } else {
                LOG.debug(
                        "Partition filter applied for table {}, but all {} partitions matched the predicate",
                        tablePath,
                        originalSize);
            }
            return filteredPartitionInfos;
        }
    }

    private static InternalRow toInternalRow(PartitionInfo partitionInfo) {
        List<String> partitionValues =
                partitionInfo.getResolvedPartitionSpec().getPartitionValues();
        GenericRow genericRow = new GenericRow(partitionValues.size());
        for (int i = 0; i < partitionValues.size(); i++) {
            genericRow.setField(i, BinaryString.fromString(partitionValues.get(i)));
        }
        return genericRow;
    }

    /** Init the splits for Fluss. */
    private void checkPartitionChanges(Set<PartitionInfo> partitionInfos, Throwable t) {
        if (closed) {
            // skip if the enumerator is closed to avoid unnecessary error logs
            return;
        }
        if (t != null) {
            LOG.error("Failed to list partitions for {}", tablePath, t);
            return;
        }

        LOG.debug(
                "Checking partition changes for table {}, found {} partitions",
                tablePath,
                partitionInfos.size());

        final PartitionChange partitionChange = getPartitionChange(partitionInfos);
        if (partitionChange.isEmpty()) {
            LOG.debug("No partition changes detected for table {}", tablePath);
            return;
        }

        // handle removed partitions
        if (!partitionChange.removedPartitions.isEmpty()) {
            LOG.info(
                    "Handling {} removed partitions for table {}: {}",
                    partitionChange.removedPartitions.size(),
                    tablePath,
                    partitionChange.removedPartitions);
            handlePartitionsRemoved(partitionChange.removedPartitions);
        }

        // handle new partitions
        if (!partitionChange.newPartitions.isEmpty()) {
            LOG.info(
                    "Handling {} new partitions for table {}: {}",
                    partitionChange.newPartitions.size(),
                    tablePath,
                    partitionChange.newPartitions);
            workerExecutor.callAsync(
                    () -> initPartitionedSplits(partitionChange.newPartitions),
                    this::handleSplitsAdd);
        }
    }

    private PartitionChange getPartitionChange(Set<PartitionInfo> fetchedPartitionInfos) {
        final Set<Partition> newPartitions =
                fetchedPartitionInfos.stream()
                        .map(p -> new Partition(p.getPartitionId(), p.getPartitionName()))
                        .collect(Collectors.toSet());
        final Set<Partition> removedPartitions = new HashSet<>();

        Set<Partition> assignedOrPendingPartitions = new HashSet<>();
        assignedPartitions.forEach(
                (partitionId, partitionName) ->
                        assignedOrPendingPartitions.add(new Partition(partitionId, partitionName)));

        pendingSplitAssignment.values().stream()
                .flatMap(Collection::stream)
                .forEach(
                        split -> {
                            long partitionId =
                                    checkNotNull(
                                            split.getTableBucket().getPartitionId(),
                                            "partition id shouldn't be null for the splits of partitioned table.");
                            String partitionName =
                                    checkNotNull(
                                            split.getPartitionName(),
                                            "partition name shouldn't be null for the splits of partitioned table.");
                            assignedOrPendingPartitions.add(
                                    new Partition(partitionId, partitionName));
                        });

        assignedOrPendingPartitions.forEach(
                p -> {
                    if (!newPartitions.remove(p)) {
                        removedPartitions.add(p);
                    }
                });

        if (!removedPartitions.isEmpty()) {
            LOG.info("Discovered removed partitions: {}", removedPartitions);
        }
        if (!newPartitions.isEmpty()) {
            LOG.info("Discovered new partitions: {}", newPartitions);
        }

        return new PartitionChange(newPartitions, removedPartitions);
    }

    private List<SourceSplitBase> initPartitionedSplits(Collection<Partition> newPartitions) {
        if (hasPrimaryKey && startingOffsetsInitializer instanceof SnapshotOffsetsInitializer) {
            return initPrimaryKeyTablePartitionSplits(newPartitions);
        } else {
            return initLogTablePartitionSplits(newPartitions);
        }
    }

    private List<SourceSplitBase> initLogTablePartitionSplits(Collection<Partition> newPartitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (Partition partition : newPartitions) {
            splits.addAll(getLogSplit(partition.getPartitionId(), partition.getPartitionName()));
        }
        return splits;
    }

    private List<SourceSplitBase> initPrimaryKeyTablePartitionSplits(
            Collection<Partition> newPartitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (Partition partition : newPartitions) {
            String partitionName = partition.getPartitionName();
            // get the table snapshot info
            final KvSnapshots kvSnapshots;
            try {
                kvSnapshots = flussAdmin.getLatestKvSnapshots(tablePath, partitionName).get();
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Failed to get table snapshot for table %s and partition %s",
                                tablePath, partitionName),
                        ExceptionUtils.stripCompletionException(e));
            }
            splits.addAll(getSnapshotAndLogSplits(kvSnapshots, partitionName));
        }
        return splits;
    }

    private List<SourceSplitBase> getSnapshotAndLogSplits(
            KvSnapshots snapshots, @Nullable String partitionName) {
        long tableId = snapshots.getTableId();
        Long partitionId = snapshots.getPartitionId();
        List<SourceSplitBase> splits = new ArrayList<>();
        List<Integer> bucketsNeedInitOffset = new ArrayList<>();
        for (Integer bucketId : snapshots.getBucketIds()) {
            TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
            // the ignore logic rely on the enumerator will always send splits for same bucket
            // in one batch; if we can ignore the bucket, we can skip all the splits(snapshot +
            // log) for the bucket
            if (ignoreTableBucket(tb)) {
                continue;
            }
            OptionalLong snapshotId = snapshots.getSnapshotId(bucketId);
            if (snapshotId.isPresent()) {
                // hybrid snapshot log split;
                OptionalLong logOffset = snapshots.getLogOffset(bucketId);
                checkState(
                        logOffset.isPresent(),
                        "Log offset should be present if snapshot id is present.");
                splits.add(
                        new HybridSnapshotLogSplit(
                                tb, partitionName, snapshotId.getAsLong(), logOffset.getAsLong()));
            } else {
                bucketsNeedInitOffset.add(bucketId);
            }
        }

        if (!bucketsNeedInitOffset.isEmpty()) {
            startingOffsetsInitializer
                    .getBucketOffsets(partitionName, bucketsNeedInitOffset, bucketOffsetsRetriever)
                    .forEach(
                            (bucketId, startingOffset) ->
                                    splits.add(
                                            new LogSplit(
                                                    new TableBucket(tableId, partitionId, bucketId),
                                                    partitionName,
                                                    startingOffset)));
        }

        return splits;
    }

    private List<SourceSplitBase> getLogSplit(
            @Nullable Long partitionId, @Nullable String partitionName) {
        // always assume the bucket is from 0 to bucket num
        List<SourceSplitBase> splits = new ArrayList<>();
        List<Integer> bucketsNeedInitOffset = new ArrayList<>();
        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
            if (ignoreTableBucket(tableBucket)) {
                continue;
            }
            bucketsNeedInitOffset.add(bucketId);
        }

        if (!bucketsNeedInitOffset.isEmpty()) {
            startingOffsetsInitializer
                    .getBucketOffsets(partitionName, bucketsNeedInitOffset, bucketOffsetsRetriever)
                    .forEach(
                            (bucketId, startingOffset) ->
                                    splits.add(
                                            new LogSplit(
                                                    new TableBucket(
                                                            tableInfo.getTableId(),
                                                            partitionId,
                                                            bucketId),
                                                    partitionName,
                                                    startingOffset)));
        }
        return splits;
    }

    /** Return the hybrid lake and fluss splits. Return null if no lake snapshot. */
    @Nullable
    private List<SourceSplitBase> generateHybridLakeFlussSplits() {
        // still have pending lake fluss splits,
        // should be restored from checkpoint, shouldn't
        // list splits again
        if (pendingHybridLakeFlussSplits != null) {
            LOG.info("Still have pending lake fluss splits, shouldn't list splits again.");
            return pendingHybridLakeFlussSplits;
        }
        try {
            LakeSplitGenerator lakeSplitGenerator =
                    new LakeSplitGenerator(
                            tableInfo,
                            flussAdmin,
                            lakeSource,
                            bucketOffsetsRetriever,
                            stoppingOffsetsInitializer,
                            tableInfo.getNumBuckets(),
                            this::listPartitions);
            List<SourceSplitBase> generatedSplits =
                    lakeSplitGenerator.generateHybridLakeFlussSplits();
            if (generatedSplits == null) {
                // no hybrid lake splits, set the pending splits to empty list
                pendingHybridLakeFlussSplits = Collections.emptyList();
                return null;
            } else {
                pendingHybridLakeFlussSplits = generatedSplits;
                return generatedSplits;
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to generate hybrid lake fluss splits", e);
        }
    }

    private boolean ignoreTableBucket(TableBucket tableBucket) {
        // if the bucket has been assigned, we can ignore it
        // the bucket has been assigned, skip
        return assignedTableBuckets.contains(tableBucket);
    }

    private void handlePartitionsRemoved(Collection<Partition> removedPartitionInfo) {
        if (removedPartitionInfo.isEmpty()) {
            return;
        }

        Map<Long, String> removedPartitionsMap =
                removedPartitionInfo.stream()
                        .collect(
                                Collectors.toMap(
                                        Partition::getPartitionId, Partition::getPartitionName));

        // remove from the pending split assignment
        pendingSplitAssignment.forEach(
                (reader, splits) ->
                        splits.removeIf(
                                split -> {
                                    // Never remove LakeSnapshotSplit, because during union reads,
                                    // data from the lake must still be read even if the partition
                                    // has already expired in Fluss.
                                    if (split instanceof LakeSnapshotSplit) {
                                        return false;
                                    }

                                    // Similar to LakeSnapshotSplit, if it contains any lake split,
                                    // never remove it; otherwise, it can be removed when the Fluss
                                    // partition expires.
                                    if (split instanceof LakeSnapshotAndFlussLogSplit) {
                                        LakeSnapshotAndFlussLogSplit hybridSplit =
                                                (LakeSnapshotAndFlussLogSplit) split;
                                        if (!hybridSplit.isLakeSplitFinished()) {
                                            return false;
                                        }
                                    }

                                    return removedPartitionsMap.containsKey(
                                            split.getTableBucket().getPartitionId());
                                }));

        // send partition removed event to all readers
        PartitionsRemovedEvent event = new PartitionsRemovedEvent(removedPartitionsMap);
        for (int readerId : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(readerId, event);
        }
    }

    private void handleSplitsAdd(List<SourceSplitBase> splits, Throwable t) {
        if (t != null) {
            if (isPartitioned && streaming && scanPartitionDiscoveryIntervalMs > 0) {
                // it means continuously read new partition splits, not throw exception, temporally
                // warn it to avoid job fail. TODO: fix me in #288
                LOG.warn("Failed to list splits for {}.", tablePath, t);
                return;
            } else {
                throw new FlinkRuntimeException(
                        String.format("Failed to list splits for %s to read due to ", tablePath),
                        t);
            }
        }
        if (isPartitioned) {
            if (!streaming || scanPartitionDiscoveryIntervalMs <= 0) {
                // if not streaming or partition discovery is disabled
                // should only add splits only once, no more new splits
                noMoreNewSplits = true;
            }
        } else {
            // if not partitioned, only will add splits only once,
            // so, noMoreNewPartitionSplits should be set to true
            noMoreNewSplits = true;
        }
        doHandleSplitsAdd(splits);
    }

    private void doHandleSplitsAdd(List<SourceSplitBase> splits) {
        addSplitToPendingAssignments(splits);
        assignPendingSplits(context.registeredReaders().keySet());
    }

    private void addSplitToPendingAssignments(Collection<SourceSplitBase> newSplits) {
        for (SourceSplitBase sourceSplit : newSplits) {
            int task = getSplitOwner(sourceSplit);
            pendingSplitAssignment.computeIfAbsent(task, k -> new LinkedList<>()).add(sourceSplit);
        }
    }

    private void assignPendingSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<SourceSplitBase>> incrementalAssignment = new HashMap<>();

        // Check if there's any pending splits for given readers
        for (int pendingReader : pendingReaders) {
            checkReaderRegistered(pendingReader);

            // Remove pending assignment for the reader
            final List<SourceSplitBase> pendingAssignmentForReader =
                    pendingSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Put pending assignment into incremental assignment
                incrementalAssignment
                        .computeIfAbsent(pendingReader, (ignored) -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);

                // Mark pending bucket assignment as already assigned
                pendingAssignmentForReader.forEach(
                        split -> {
                            TableBucket tableBucket = split.getTableBucket();
                            assignedTableBuckets.add(tableBucket);

                            if (pendingHybridLakeFlussSplits != null) {
                                // removed from the pendingHybridLakeFlussSplits
                                // since this split already be assigned
                                pendingHybridLakeFlussSplits.removeIf(
                                        hybridLakeFlussSplit ->
                                                hybridLakeFlussSplit
                                                        .splitId()
                                                        .equals(split.splitId()));
                            }

                            if (isPartitioned) {
                                long partitionId =
                                        checkNotNull(
                                                tableBucket.getPartitionId(),
                                                "partition id shouldn't be null for the splits of partitioned table.");
                                String partitionName =
                                        checkNotNull(
                                                split.getPartitionName(),
                                                "partition name shouldn't be null for the splits of partitioned table.");
                                assignedPartitions.put(partitionId, partitionName);
                            }
                        });
            }
        }

        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("Assigning splits to readers {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }

        if (noMoreNewSplits) {
            LOG.info(
                    "No more FlussSplits to assign. Sending NoMoreSplitsEvent to reader {}",
                    pendingReaders);
            pendingReaders.forEach(context::signalNoMoreSplits);
        }
    }

    /**
     * Returns the index of the target subtask that a specific split should be assigned to.
     *
     * <p>The resulting distribution of splits of a single table has the following contract:
     *
     * <ul>
     *   <li>1. Splits in same bucket are assigned to same subtask
     *   <li>2. Uniformly distributed across subtasks
     *   <li>3. For partitioned table, the buckets in same partition are round-robin distributed
     *       (strictly clockwise w.r.t. ascending subtask indices) by using the partition id as the
     *       offset from a starting index. The starting index is the index of the subtask which
     *       bucket 0 of the partition will be assigned to, determined using the partition id to
     *       make sure the partitions' buckets of a table are distributed uniformly
     * </ul>
     *
     * @param split the split to assign.
     * @return the id of the subtask that owns the split.
     */
    @VisibleForTesting
    protected int getSplitOwner(SourceSplitBase split) {
        TableBucket tableBucket = split.getTableBucket();
        int startIndex =
                tableBucket.getPartitionId() == null
                        ? 0
                        : ((tableBucket.getPartitionId().hashCode() * 31) & 0x7FFFFFFF)
                                % context.currentParallelism();

        // super hack logic, if the bucket is -1, it means the split is
        // for bucket unaware, like paimon unaware bucket log table,
        // we use hash split id to get the split owner
        // todo: refactor the split assign logic
        if (split.isLakeSplit() && tableBucket.getBucket() == -1) {
            return (split.splitId().hashCode() & 0x7FFFFFFF) % context.currentParallelism();
        }

        return (startIndex + tableBucket.getBucket()) % context.currentParallelism();
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the fluss source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionBucketsUnsubscribedEvent) {
            PartitionBucketsUnsubscribedEvent removedEvent =
                    (PartitionBucketsUnsubscribedEvent) sourceEvent;

            Set<Long> partitionsPendingRemove = new HashSet<>();
            // remove from the assigned table buckets
            for (TableBucket tableBucket : removedEvent.getRemovedTableBuckets()) {
                assignedTableBuckets.remove(tableBucket);
                partitionsPendingRemove.add(tableBucket.getPartitionId());
            }

            for (TableBucket tableBucket : assignedTableBuckets) {
                Long partitionId = tableBucket.getPartitionId();
                if (partitionId != null) {
                    // we shouldn't remove the partition if still there is buckets assigned.
                    boolean removed = partitionsPendingRemove.remove(partitionId);
                    if (removed && partitionsPendingRemove.isEmpty()) {
                        // no need to check the rest of the buckets
                        break;
                    }
                }
            }

            // remove partitions if no assigned buckets belong to the partition
            for (Long partitionToRemove : partitionsPendingRemove) {
                assignedPartitions.remove(partitionToRemove);
            }
        }
    }

    @VisibleForTesting
    Map<Long, String> getAssignedPartitions() {
        return assignedPartitions;
    }

    @VisibleForTesting
    Map<Integer, List<SourceSplitBase>> getPendingSplitAssignment() {
        return pendingSplitAssignment;
    }

    @Override
    public void addSplitsBack(List<SourceSplitBase> splits, int subtaskId) {
        LOG.debug("Flink Source Enumerator adds splits back: {}", splits);
        addSplitToPendingAssignments(splits);

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader: {} to Flink Source enumerator.", subtaskId);
        assignPendingSplits(Collections.singleton(subtaskId));
    }

    @Override
    public SourceEnumeratorState snapshotState(long checkpointId) {
        final SourceEnumeratorState enumeratorState =
                new SourceEnumeratorState(
                        assignedTableBuckets, assignedPartitions, pendingHybridLakeFlussSplits);
        LOG.debug("Source Checkpoint is {}", enumeratorState);
        return enumeratorState;
    }

    @Override
    public void close() throws IOException {
        try {
            closed = true;
            if (workerExecutor != null) {
                workerExecutor.close();
            }
            if (flussAdmin != null) {
                flussAdmin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close Flink Source enumerator.", e);
        }
    }

    // --------------- private class ---------------
    /** A container class to hold the newly added partitions and removed partitions. */
    private static class PartitionChange {
        private final Collection<Partition> newPartitions;
        private final Collection<Partition> removedPartitions;

        PartitionChange(
                Collection<Partition> newPartitions, Collection<Partition> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public boolean isEmpty() {
            return newPartitions.isEmpty() && removedPartitions.isEmpty();
        }
    }

    /** A container class to hold the partition id and partition name. */
    private static class Partition {
        final long partitionId;
        final String partitionName;

        Partition(long partitionId, String partitionName) {
            this.partitionId = partitionId;
            this.partitionName = partitionName;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public String getPartitionName() {
            return partitionName;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Partition partition = (Partition) o;
            return partitionId == partition.partitionId
                    && Objects.equals(partitionName, partition.partitionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId, partitionName);
        }

        @Override
        public String toString() {
            return "Partition{" + "id=" + partitionId + ", name='" + partitionName + '\'' + '}';
        }
    }
}
