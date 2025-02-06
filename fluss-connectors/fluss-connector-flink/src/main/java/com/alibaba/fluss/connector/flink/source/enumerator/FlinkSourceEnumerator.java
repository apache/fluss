/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source.enumerator;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.snapshot.BucketSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.BucketsSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.KvSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.PartitionSnapshotInfo;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.lakehouse.LakeSplitGenerator;
import com.alibaba.fluss.connector.flink.source.enumerator.initializer.BucketOffsetsRetrieverImpl;
import com.alibaba.fluss.connector.flink.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import com.alibaba.fluss.connector.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.connector.flink.source.enumerator.initializer.OffsetsInitializer.BucketOffsetsRetriever;
import com.alibaba.fluss.connector.flink.source.enumerator.initializer.SnapshotOffsetsInitializer;
import com.alibaba.fluss.connector.flink.source.event.PartitionBucketsUnsubscribedEvent;
import com.alibaba.fluss.connector.flink.source.event.PartitionsRemovedEvent;
import com.alibaba.fluss.connector.flink.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.connector.flink.source.split.LogSplit;
import com.alibaba.fluss.connector.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.connector.flink.source.state.SourceEnumeratorState;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.ExceptionUtils;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * An implementation of {@link SplitEnumerator} for the data of Fluss.
 *
 * <p>The enumerator is responsible for:
 *
 * <ul>
 *   <li>Get the all splits(snapshot split + log split) for a table of Fluss to be read.
 *   <li>Assign the splits to readers with the guarantee that the splits belong to the same bucket
 *       will be assigned to same reader.
 * </ul>
 */
public class FlinkSourceEnumerator
        implements SplitEnumerator<SourceSplitBase, SourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceEnumerator.class);

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
     */
    private final Map<Long, String> assignedPartitions;

    /** buckets that have been assigned to readers. */
    private final Set<TableBucket> assignedTableBuckets;

    private final long scanPartitionDiscoveryIntervalMs;

    private final boolean streaming;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;

    // Lazily instantiated or mutable fields.
    private Connection connection;
    private Admin flussAdmin;
    private BucketOffsetsRetriever bucketOffsetsRetriever;
    private long tableId;
    private int bucketCount;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // split initializing has finished.
    private boolean noMoreNewSplits = false;

    private boolean lakeEnabled = false;

    public FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            boolean streaming) {
        this(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                context,
                Collections.emptySet(),
                Collections.emptyMap(),
                startingOffsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming);
    }

    public FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            Set<TableBucket> assignedTableBuckets,
            Map<Long, String> assignedPartitions,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            boolean streaming) {
        this.tablePath = checkNotNull(tablePath);
        this.flussConf = checkNotNull(flussConf);
        this.hasPrimaryKey = hasPrimaryKey;
        this.isPartitioned = isPartitioned;
        this.context = checkNotNull(context);
        this.pendingSplitAssignment = new HashMap<>();
        this.assignedTableBuckets = new HashSet<>(assignedTableBuckets);
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.assignedPartitions = new HashMap<>(assignedPartitions);
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.streaming = streaming;
        this.stoppingOffsetsInitializer =
                streaming ? new NoStoppingOffsetsInitializer() : OffsetsInitializer.latest();
    }

    @Override
    public void start() {
        // init admin client
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(flussAdmin, tablePath);
        try {
            TableInfo tableInfo = flussAdmin.getTable(tablePath).get();
            tableId = tableInfo.getTableId();
            lakeEnabled = tableInfo.getTableDescriptor().isDataLakeEnabled();
            bucketCount =
                    tableInfo
                            .getTableDescriptor()
                            .getTableDistribution()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Table distribution is not set."))
                            .getBucketCount()
                            .orElseThrow(
                                    () -> new IllegalStateException("Bucket count is not set."));
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to get table info for %s", tablePath),
                    ExceptionUtils.stripCompletionException(e));
        }

        if (isPartitioned) {
            if (streaming && scanPartitionDiscoveryIntervalMs > 0) {
                // should do partition discovery
                LOG.info(
                        "Starting the FlussSourceEnumerator for table {} "
                                + "with new partition discovery interval of {} ms.",
                        tablePath,
                        scanPartitionDiscoveryIntervalMs);
                // discover new partitions and handle new partitions
                context.callAsync(
                        this::listPartitions,
                        this::checkPartitionChanges,
                        0,
                        scanPartitionDiscoveryIntervalMs);
            } else {
                if (!streaming) {
                    startInBatchMode();
                } else {
                    // just call once
                    LOG.info(
                            "Starting the FlussSourceEnumerator for table {} without partition discovery.",
                            tablePath);
                    context.callAsync(this::listPartitions, this::checkPartitionChanges);
                }
            }

        } else {
            if (!streaming) {
                startInBatchMode();
            } else {
                // init bucket splits and assign
                context.callAsync(this::initNonPartitionedSplits, this::handleSplitsAdd);
            }
        }
    }

    private void startInBatchMode() {
        if (lakeEnabled) {
            context.callAsync(this::getLakeSplit, this::handleSplitsAdd);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Batch only supports when table option '%s' is set to true.",
                            ConfigOptions.TABLE_DATALAKE_ENABLED));
        }
    }

    private List<SourceSplitBase> initNonPartitionedSplits() {
        if (hasPrimaryKey && startingOffsetsInitializer instanceof SnapshotOffsetsInitializer) {
            // get the table snapshot info
            KvSnapshotInfo kvSnapshotInfo;
            try {
                kvSnapshotInfo = flussAdmin.getKvSnapshot(tablePath).get();
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format("Failed to get table snapshot for %s", tablePath),
                        ExceptionUtils.stripCompletionException(e));
            }
            return getSnapshotAndLogSplits(
                    kvSnapshotInfo.getTableId(), null, null, kvSnapshotInfo.getBucketsSnapshots());
        } else {
            return getLogSplit(null, null);
        }
    }

    private Set<PartitionInfo> listPartitions() {
        try {
            List<PartitionInfo> partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
            return new HashSet<>(partitionInfos);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to list partitions for %s", tablePath),
                    ExceptionUtils.stripCompletionException(e));
        }
    }

    /** Init the splits for Fluss. */
    private void checkPartitionChanges(Set<PartitionInfo> partitionInfos, Throwable t) {
        if (t != null) {
            LOG.error("Failed to list partitions for {}", tablePath, t);
            return;
        }
        final PartitionChange partitionChange = getPartitionChange(partitionInfos);
        if (partitionChange.isEmpty()) {
            return;
        }

        // handle removed partitions
        handlePartitionsRemoved(partitionChange.removedPartitions);

        // handle new partitions
        context.callAsync(
                () -> initPartitionedSplits(partitionChange.newPartitions), this::handleSplitsAdd);
    }

    private PartitionChange getPartitionChange(Set<PartitionInfo> fetchedPartitionInfos) {
        final Set<PartitionInfo> removedPartitionIds = new HashSet<>();

        Consumer<PartitionInfo> dedupOrMarkAsRemoved =
                (tp) -> {
                    if (!fetchedPartitionInfos.remove(tp)) {
                        removedPartitionIds.add(tp);
                    }
                };

        Set<PartitionInfo> assignedOrPendingPartitions = new HashSet<>();
        assignedPartitions.forEach(
                (partitionId, partitionName) ->
                        assignedOrPendingPartitions.add(
                                new PartitionInfo(partitionId, partitionName)));

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
                                    new PartitionInfo(partitionId, partitionName));
                        });

        assignedOrPendingPartitions.forEach(dedupOrMarkAsRemoved);

        if (!removedPartitionIds.isEmpty()) {
            LOG.info("Discovered removed partitions: {}", removedPartitionIds);
        }
        if (!fetchedPartitionInfos.isEmpty()) {
            LOG.info("Discovered new partitions: {}", fetchedPartitionInfos);
        }

        return new PartitionChange(fetchedPartitionInfos, removedPartitionIds);
    }

    private List<SourceSplitBase> initPartitionedSplits(Collection<PartitionInfo> newPartitions) {
        if (hasPrimaryKey && startingOffsetsInitializer instanceof SnapshotOffsetsInitializer) {
            return initPrimaryKeyTablePartitionSplits(newPartitions);
        } else {
            return initLogTablePartitionSplits(newPartitions);
        }
    }

    private List<SourceSplitBase> initLogTablePartitionSplits(
            Collection<PartitionInfo> newPartitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (PartitionInfo partition : newPartitions) {
            splits.addAll(getLogSplit(partition.getPartitionId(), partition.getPartitionName()));
        }
        return splits;
    }

    private List<SourceSplitBase> initPrimaryKeyTablePartitionSplits(
            Collection<PartitionInfo> newPartitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (PartitionInfo partitionInfo : newPartitions) {
            PartitionSnapshotInfo partitionSnapshotInfo;
            String partitionName = partitionInfo.getPartitionName();
            try {
                partitionSnapshotInfo =
                        flussAdmin.getPartitionSnapshot(tablePath, partitionName).get();
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Failed to get snapshot for partition '%s' of table '%s'.",
                                partitionName, tablePath),
                        ExceptionUtils.stripCompletionException(e));
            }
            splits.addAll(
                    getSnapshotAndLogSplits(
                            partitionSnapshotInfo.getTableId(),
                            partitionInfo.getPartitionId(),
                            partitionInfo.getPartitionName(),
                            partitionSnapshotInfo.getBucketsSnapshotInfo()));
        }
        return splits;
    }

    private List<SourceSplitBase> getSnapshotAndLogSplits(
            long tableId,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            BucketsSnapshotInfo bucketsSnapshotInfo) {
        List<SourceSplitBase> splits = new ArrayList<>();
        List<Integer> bucketsNeedInitOffset = new ArrayList<>();
        for (Integer bucketId : bucketsSnapshotInfo.getBucketIds()) {
            TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
            // the ignore logic rely on the enumerator will always send splits for same bucket
            // in one batch; if we can ignore the bucket, we can skip all the splits(snapshot +
            // log) for the bucket
            if (ignoreTableBucket(tb)) {
                continue;
            }
            // if has any snapshot, then we need read snapshot split + log split;
            Optional<BucketSnapshotInfo> optionalBucketSnapshotInfo =
                    bucketsSnapshotInfo.getBucketSnapshotInfo(bucketId);
            if (optionalBucketSnapshotInfo.isPresent()) {
                // hybrid snapshot log split;
                BucketSnapshotInfo snapshot = optionalBucketSnapshotInfo.get();
                splits.add(
                        new HybridSnapshotLogSplit(
                                tb,
                                partitionName,
                                snapshot.getSnapshotFiles(),
                                snapshot.getLogOffset()));
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
        for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
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
                                                    new TableBucket(tableId, partitionId, bucketId),
                                                    partitionName,
                                                    startingOffset)));
        }
        return splits;
    }

    private List<SourceSplitBase> getLakeSplit() throws Exception {
        LakeSplitGenerator lakeSplitGenerator =
                new LakeSplitGenerator(
                        tableId,
                        tablePath,
                        flussAdmin,
                        bucketOffsetsRetriever,
                        stoppingOffsetsInitializer,
                        bucketCount);
        return lakeSplitGenerator.generateLakeSplits();
    }

    private boolean ignoreTableBucket(TableBucket tableBucket) {
        // if the bucket has been assigned, we can ignore it
        // the bucket has been assigned, skip
        return assignedTableBuckets.contains(tableBucket);
    }

    private void handlePartitionsRemoved(Collection<PartitionInfo> removedPartitionInfo) {
        if (removedPartitionInfo.isEmpty()) {
            return;
        }

        Map<Long, String> removedPartitionsMap =
                removedPartitionInfo.stream()
                        .collect(
                                Collectors.toMap(
                                        PartitionInfo::getPartitionId,
                                        PartitionInfo::getPartitionName));

        // remove from the pending split assignment
        pendingSplitAssignment.forEach(
                (reader, splits) ->
                        splits.removeIf(
                                split ->
                                        removedPartitionsMap.containsKey(
                                                split.getTableBucket().getPartitionId())));

        // send partition removed event to all readers
        PartitionsRemovedEvent event = new PartitionsRemovedEvent(removedPartitionsMap);
        for (int readerId : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(readerId, event);
        }
    }

    private void handleSplitsAdd(List<SourceSplitBase> splits, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException(
                    String.format("Failed to list splits for %s to read due to ", tablePath), t);
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
                new SourceEnumeratorState(assignedTableBuckets, assignedPartitions);
        LOG.debug("Source Checkpoint is {}", enumeratorState);
        return enumeratorState;
    }

    @Override
    public void close() throws IOException {
        try {
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
        private final Collection<PartitionInfo> newPartitions;
        private final Collection<PartitionInfo> removedPartitions;

        PartitionChange(
                Collection<PartitionInfo> newPartitions,
                Collection<PartitionInfo> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public boolean isEmpty() {
            return newPartitions.isEmpty() && removedPartitions.isEmpty();
        }
    }
}
