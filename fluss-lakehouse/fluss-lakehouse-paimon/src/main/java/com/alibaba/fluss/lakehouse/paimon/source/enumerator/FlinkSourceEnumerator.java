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

package com.alibaba.fluss.lakehouse.paimon.source.enumerator;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.KvSnapshots;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.source.Filter;
import com.alibaba.fluss.lakehouse.paimon.source.NewTablesAddedListener;
import com.alibaba.fluss.lakehouse.paimon.source.event.PartitionsRemovedEvent;
import com.alibaba.fluss.lakehouse.paimon.source.event.TableBucketsUnsubscribedEvent;
import com.alibaba.fluss.lakehouse.paimon.source.event.TablesRemovedEvent;
import com.alibaba.fluss.lakehouse.paimon.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.LogSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.SourceSplitBase;
import com.alibaba.fluss.lakehouse.paimon.source.state.SourceEnumeratorState;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.ExceptionUtils;

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
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * An implementation of {@link SplitEnumerator} for the data of Fluss.
 *
 * <p>The enumerator is responsible for:
 *
 * <ul>
 *   <li>Get the all splits({@link HybridSnapshotLogSplit} or {@link LogSplit}) for a table of Fluss
 *       to be read.
 *   <li>Assign the splits to readers with the guarantee that the splits belong to the same bucket
 *       will be assigned to same reader.
 * </ul>
 */
public class FlinkSourceEnumerator
        implements SplitEnumerator<SourceSplitBase, SourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceEnumerator.class);

    private static final long discoveryNewTableOrPartitionInterval = 3000L;

    private final SplitEnumeratorContext<SourceSplitBase> context;
    private final Configuration flussConf;

    private final Map<Long, TablePath> assignedTables;

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

    private final Map<Integer, List<SourceSplitBase>> pendingSplitAssignment;

    // Lazily instantiated or mutable fields.
    private Connection connection;
    private Admin flussAdmin;

    private final Filter<String> databaseFilter;
    private final Filter<TableInfo> tableFilter;

    private final NewTablesAddedListener newTablesAddedListener;

    public FlinkSourceEnumerator(
            Configuration flussConf,
            Filter<String> databaseFilter,
            Filter<TableInfo> tableFilter,
            NewTablesAddedListener newTablesAddedListener,
            SplitEnumeratorContext<SourceSplitBase> context) {
        this(
                flussConf,
                databaseFilter,
                tableFilter,
                newTablesAddedListener,
                context,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet());
    }

    public FlinkSourceEnumerator(
            Configuration flussConf,
            Filter<String> databaseFilter,
            Filter<TableInfo> tableFilter,
            NewTablesAddedListener newTablesAddedListener,
            SplitEnumeratorContext<SourceSplitBase> context,
            Map<Long, TablePath> assignedTables,
            Map<Long, String> assignedPartitions,
            Set<TableBucket> assignedTableBuckets) {
        this.flussConf = checkNotNull(flussConf);
        this.databaseFilter = checkNotNull(databaseFilter);
        this.tableFilter = checkNotNull(tableFilter);
        this.newTablesAddedListener = newTablesAddedListener;
        this.context = checkNotNull(context);
        this.assignedTables = new HashMap<>(assignedTables);
        this.assignedPartitions = new HashMap<>(assignedPartitions);
        this.pendingSplitAssignment = new HashMap<>();
        this.assignedTableBuckets = new HashSet<>(assignedTableBuckets);
    }

    @Override
    public void start() {
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        context.callAsync(
                this::listTableAndPartitions,
                this::checkTableAndPartitionChanges,
                0,
                discoveryNewTableOrPartitionInterval);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the fluss source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof TableBucketsUnsubscribedEvent) {
            TableBucketsUnsubscribedEvent removedEvent =
                    (TableBucketsUnsubscribedEvent) sourceEvent;

            Set<Long> tablesPendingRemove = new HashSet<>();
            Set<Long> partitionsPendingRemove = new HashSet<>();

            // remove from the assigned table buckets
            for (TableBucket tableBucket : removedEvent.getRemovedTableBuckets()) {
                assignedTableBuckets.remove(tableBucket);
                tablesPendingRemove.add(tableBucket.getTableId());
                if (tableBucket.getPartitionId() != null) {
                    partitionsPendingRemove.add(tableBucket.getPartitionId());
                }
            }

            for (TableBucket tableBucket : assignedTableBuckets) {
                // we shouldn't remove the table if still there is buckets assigned.
                boolean tableRemoved = tablesPendingRemove.remove(tableBucket.getTableId());
                if (tableRemoved && tablesPendingRemove.isEmpty()) {
                    // no need to check the rest of the buckets
                    break;
                }

                Long partitionId = tableBucket.getPartitionId();
                if (partitionId != null) {
                    // we shouldn't remove the partition if still there is buckets assigned.
                    boolean partitionRemoved = partitionsPendingRemove.remove(partitionId);
                    if (partitionRemoved && partitionsPendingRemove.isEmpty()) {
                        // no need to check the rest of the buckets
                        break;
                    }
                }
            }

            // remove table if no assigned buckets belong to the table
            for (Long tableToRemove : tablesPendingRemove) {
                assignedTables.remove(tableToRemove);
            }

            //  // remove partition if no assigned buckets belong to the partition
            for (Long partitionToRemove : partitionsPendingRemove) {
                assignedPartitions.remove(partitionToRemove);
            }
        }
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
                new SourceEnumeratorState(assignedTables, assignedTableBuckets, assignedPartitions);
        LOG.debug("Source Checkpoint is {}", enumeratorState);
        return enumeratorState;
    }

    private TableAndPartitions listTableAndPartitions() throws Exception {
        // first, list all tables
        List<String> databases = flussAdmin.listDatabases().get();
        Map<Long, TableInfo> tableInfos = new HashMap<>();
        Map<Long, TablePath> partitionedTables = new HashMap<>();
        for (String database : databases) {
            if (!databaseFilter.test(database)) {
                continue;
            }
            // todo: consider use only one rpc call
            List<String> tables = flussAdmin.listTables(database).get();
            for (String tableName : tables) {
                TableInfo tableInfo =
                        flussAdmin.getTableInfo(new TablePath(database, tableName)).get();
                if (tableFilter.test(tableInfo)) {
                    tableInfos.put(tableInfo.getTableId(), tableInfo);
                    if (tableInfo.getTableDescriptor().isPartitioned()) {
                        partitionedTables.put(tableInfo.getTableId(), tableInfo.getTablePath());
                    }
                }
            }
        }

        // then, list partitions of partitioned table
        Map<Long, Set<PartitionInfo>> partitionInfosByTableId = listPartitions(partitionedTables);
        return new TableAndPartitions(tableInfos, partitionInfosByTableId);
    }

    private Map<Long, Set<PartitionInfo>> listPartitions(Map<Long, TablePath> partitionedTables) {
        Map<Long, Set<PartitionInfo>> partitionInfosByTableId = new HashMap<>();
        for (Map.Entry<Long, TablePath> entry : partitionedTables.entrySet()) {
            Set<PartitionInfo> partitionInfos = listPartitions(entry.getValue());
            partitionInfosByTableId.put(entry.getKey(), partitionInfos);
        }
        return partitionInfosByTableId;
    }

    private Set<PartitionInfo> listPartitions(TablePath tablePath) {
        try {
            List<PartitionInfo> partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
            return new HashSet<>(partitionInfos);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to list partitions for %s", tablePath),
                    ExceptionUtils.stripCompletionException(e));
        }
    }

    private void checkTableAndPartitionChanges(TableAndPartitions tableAndPartitions, Throwable t) {
        if (t != null) {
            LOG.error(
                    "Failed to list tables and partitions from Fluss.",
                    ExceptionUtils.stripCompletionException(t));
            return;
        }

        // first, check table changes
        checkTableChanges(tableAndPartitions.tableInfos);

        // then, check partition changes
        checkPartitionChanges(tableAndPartitions.tableInfos, tableAndPartitions.partitionInfos);
    }

    private void checkTableChanges(Map<Long, TableInfo> fetchedTableInfos) {
        Set<TableIdAndPath> fetchedTableIdAndPaths = new HashSet<>();
        fetchedTableInfos
                .values()
                .forEach(
                        tableInfo ->
                                fetchedTableIdAndPaths.add(
                                        new TableIdAndPath(
                                                tableInfo.getTableId(), tableInfo.getTablePath())));

        final TableChange tableChange = getTableChange(fetchedTableIdAndPaths);
        if (tableChange.isEmpty()) {
            return;
        }

        Set<TableInfo> newTables = new HashSet<>();
        tableChange.newTables.forEach(
                tableIdAndPath -> newTables.add(fetchedTableInfos.get(tableIdAndPath.tableId)));
        // handle new tables
        handleTablesAdd(newTables);

        // handle removed tables
        handleTablesRemoved(tableChange.removedTables);
        // handle new tables handle new partitions
        context.callAsync(
                () -> initTableSplits(fetchedTableInfos, tableChange.newTables),
                this::handleSplitsAdd);
    }

    private void checkPartitionChanges(
            Map<Long, TableInfo> tableInfos, Map<Long, Set<PartitionInfo>> fetchedPartitionInfos) {
        Map<Long, Collection<PartitionInfo>> newPartitions = new HashMap<>();
        Map<Long, Collection<PartitionInfo>> removedPartitions = new HashMap<>();
        for (Map.Entry<Long, Set<PartitionInfo>> entry : fetchedPartitionInfos.entrySet()) {
            long tableId = entry.getKey();
            Set<PartitionInfo> partitionInfos = entry.getValue();
            final PartitionChange partitionChange = getPartitionChange(partitionInfos);
            if (partitionChange.isEmpty()) {
                continue;
            }
            newPartitions.put(tableId, partitionChange.newPartitions);
            removedPartitions.put(tableId, partitionChange.removedPartitions);
        }

        // handle removed partitions
        handlePartitionsRemoved(removedPartitions);
        // handle new partitions
        context.callAsync(
                () -> initPartitionedSplits(tableInfos, newPartitions), this::handleSplitsAdd);
    }

    private List<SourceSplitBase> initTableSplits(
            Map<Long, TableInfo> tableInfos, Collection<TableIdAndPath> tables) throws Exception {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (TableIdAndPath tableIdAndPath : tables) {
            TableInfo tableInfo = tableInfos.get(tableIdAndPath.tableId);

            // if is partitioned table, we don't generate splits,
            // we will generate splits during partition discovering
            if (tableInfo.getTableDescriptor().isPartitioned()) {
                continue;
            }
            TableDescriptor tableDescriptor = tableInfo.getTableDescriptor();
            if (tableDescriptor.hasPrimaryKey()) {
                final KvSnapshots kvSnapshots;
                try {
                    kvSnapshots = flussAdmin.getLatestKvSnapshots(tableIdAndPath.tablePath).get();
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Failed to get table snapshot for %s",
                                    tableIdAndPath.tablePath),
                            ExceptionUtils.stripCompletionException(e));
                }
                return getHybridSnapshotAndLogSplits(kvSnapshots, tableInfo.getTablePath(), null);
            } else {
                splits.addAll(
                        getLogSplits(tableIdAndPath, getBucketCount(tableDescriptor), null, null));
            }
        }
        return splits;
    }

    private List<SourceSplitBase> initPartitionedSplits(
            Map<Long, TableInfo> tableInfos, Map<Long, Collection<PartitionInfo>> newPartitions) {
        List<SourceSplitBase> sourceSplitBases = new ArrayList<>();
        for (Map.Entry<Long, Collection<PartitionInfo>> entry : newPartitions.entrySet()) {
            long tableId = entry.getKey();
            Collection<PartitionInfo> partitionInfos = entry.getValue();
            TableInfo tableInfo = tableInfos.get(tableId);
            if (tableInfo.getTableDescriptor().hasPrimaryKey()) {
                sourceSplitBases.addAll(
                        initPrimaryKeyTablePartitionSplits(
                                tableId, tableInfo.getTablePath(), partitionInfos));
            } else {
                sourceSplitBases.addAll(initLogTablePartitionSplits(tableInfo, partitionInfos));
            }
        }
        return sourceSplitBases;
    }

    private List<SourceSplitBase> initLogTablePartitionSplits(
            TableInfo tableInfo, Collection<PartitionInfo> newPartitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (PartitionInfo partition : newPartitions) {
            splits.addAll(
                    getLogSplits(
                            new TableIdAndPath(tableInfo.getTableId(), tableInfo.getTablePath()),
                            getBucketCount(tableInfo.getTableDescriptor()),
                            partition.getPartitionId(),
                            partition.getPartitionName()));
        }
        return splits;
    }

    private List<SourceSplitBase> initPrimaryKeyTablePartitionSplits(
            long tableId, TablePath tablePath, Collection<PartitionInfo> newPartitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (PartitionInfo partitionInfo : newPartitions) {
            String partitionName = partitionInfo.getPartitionName();
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
            splits.addAll(
                    getHybridSnapshotAndLogSplits(
                            kvSnapshots, tablePath, partitionInfo.getPartitionName()));
        }
        return splits;
    }

    private List<SourceSplitBase> getHybridSnapshotAndLogSplits(
            KvSnapshots kvSnapshots, TablePath tablePath, @Nullable String partitionName) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (Integer bucketId : kvSnapshots.getBucketIds()) {
            TableBucket tb =
                    new TableBucket(
                            kvSnapshots.getTableId(), kvSnapshots.getPartitionId(), bucketId);
            // the ignore logic rely on the enumerator will always send splits for same bucket
            // in one batch; if we can ignore the bucket, we can skip all the splits(snapshot +
            // log) for the bucket
            if (ignoreTableBucket(tb)) {
                continue;
            }
            // if has any snapshot, then we need read snapshot split + log split;
            OptionalLong snapshotId = kvSnapshots.getSnapshotId(bucketId);
            if (snapshotId.isPresent()) {
                // hybrid snapshot and log split;
                OptionalLong logOffset = kvSnapshots.getLogOffset(bucketId);
                checkState(
                        logOffset.isPresent(), "Log offset should be present if snapshot id is.");
                splits.add(
                        new HybridSnapshotLogSplit(
                                tablePath,
                                tb,
                                partitionName,
                                snapshotId.getAsLong(),
                                logOffset.getAsLong()));
            } else {
                splits.add(new LogSplit(tablePath, tb, partitionName, EARLIEST_OFFSET));
            }
        }

        return splits;
    }

    private int getBucketCount(TableDescriptor tableDescriptor) {
        return tableDescriptor
                .getTableDistribution()
                .orElseThrow(() -> new IllegalStateException("Table distribution is not set."))
                .getBucketCount()
                .orElseThrow(() -> new IllegalStateException("Bucket count is not set."));
    }

    private List<SourceSplitBase> getLogSplits(
            TableIdAndPath tableIdAndPath,
            int bucketCount,
            @Nullable Long partitionId,
            @Nullable String partitionName) {
        // always assume the bucket is from 0 to bucket num
        List<SourceSplitBase> splits = new ArrayList<>();
        for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
            TableBucket tableBucket =
                    new TableBucket(tableIdAndPath.tableId, partitionId, bucketId);
            if (!ignoreTableBucket(tableBucket)) {
                splits.add(
                        new LogSplit(
                                tableIdAndPath.tablePath,
                                tableBucket,
                                partitionName,
                                EARLIEST_OFFSET));
            }
        }
        return splits;
    }

    private boolean ignoreTableBucket(TableBucket tableBucket) {
        // if the bucket has been assigned, we can ignore it
        // the bucket has been assigned, skip
        return assignedTableBuckets.contains(tableBucket);
    }

    private void handleSplitsAdd(List<SourceSplitBase> splits, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException(
                    "Failed to init splits for tables", ExceptionUtils.stripCompletionException(t));
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

    protected int getSplitOwner(SourceSplitBase split) {
        TableBucket tableBucket = split.getTableBucket();
        int startIndex =
                tableBucket.getPartitionId() == null
                        ? 0
                        : ((tableBucket.getPartitionId().hashCode() * 31) & 0x7FFFFFFF)
                                % context.currentParallelism();

        return (startIndex + tableBucket.getBucket()) % context.currentParallelism();
    }

    private TableChange getTableChange(Set<TableIdAndPath> fetchedTables) {
        final Set<TableIdAndPath> removedTables = new HashSet<>();

        Consumer<TableIdAndPath> dedupOrMarkAsRemoved =
                (tp) -> {
                    if (!fetchedTables.remove(tp)) {
                        removedTables.add(tp);
                    }
                };

        assignedTables.forEach(
                (tableId, tableName) ->
                        dedupOrMarkAsRemoved.accept(new TableIdAndPath(tableId, tableName)));

        pendingSplitAssignment.forEach(
                (reader, splits) ->
                        splits.forEach(
                                split -> {
                                    TableIdAndPath tableIdAndPath =
                                            new TableIdAndPath(
                                                    split.getTableBucket().getTableId(),
                                                    split.getTablePath());
                                    dedupOrMarkAsRemoved.accept(tableIdAndPath);
                                }));

        if (!removedTables.isEmpty()) {
            LOG.info("Discovered removed tables: {}", removedTables);
        }
        if (!fetchedTables.isEmpty()) {
            LOG.info("Discovered new tables: {}", fetchedTables);
        }

        return new TableChange(fetchedTables, removedTables);
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

    private void handleTablesAdd(Collection<TableInfo> addedTables) {
        newTablesAddedListener.onNewTablesAdded(addedTables);
    }

    private void handleTablesRemoved(Collection<TableIdAndPath> removedTables) {
        if (removedTables.isEmpty()) {
            return;
        }

        Map<Long, TablePath> removedTablesMap =
                removedTables.stream()
                        .collect(
                                Collectors.toMap(
                                        TableIdAndPath::tableId, TableIdAndPath::tablePath));

        // remove from the pending split assignment
        pendingSplitAssignment.forEach(
                (reader, splits) ->
                        splits.removeIf(
                                split ->
                                        removedTablesMap.containsKey(
                                                split.getTableBucket().getTableId())));

        // send table removed event to all readers
        TablesRemovedEvent event = new TablesRemovedEvent(removedTablesMap);
        for (int readerId : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(readerId, event);
        }
    }

    private void handlePartitionsRemoved(
            Map<Long, Collection<PartitionInfo>> removedPartitionInfoByTableId) {
        if (removedPartitionInfoByTableId.isEmpty()) {
            return;
        }

        Map<Long, Map<Long, String>> removedPartitionsByTableId = new HashMap<>();

        for (Map.Entry<Long, Collection<PartitionInfo>> removedPartitionInfoEntry :
                removedPartitionInfoByTableId.entrySet()) {
            long tableId = removedPartitionInfoEntry.getKey();
            Map<Long, String> removedPartitionsMap =
                    removedPartitionInfoEntry.getValue().stream()
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
            removedPartitionsByTableId.put(tableId, removedPartitionsMap);
        }

        // send partition removed event to all readers
        PartitionsRemovedEvent event = new PartitionsRemovedEvent(removedPartitionsByTableId);
        for (int readerId : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(readerId, event);
        }
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
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
                            assignedTables.put(tableBucket.getTableId(), split.getTablePath());
                            if (tableBucket.getPartitionId() != null) {
                                String partitionName =
                                        checkNotNull(
                                                split.getPartitionName(),
                                                "partition name shouldn't be null for the splits of partitioned table.");
                                assignedPartitions.put(tableBucket.getPartitionId(), partitionName);
                            }
                        });
            }
        }

        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("Assigning splits to readers {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }
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
    /** A container class to hold the newly added tables and removed tables. */
    private static class TableChange {
        private final Collection<TableIdAndPath> newTables;
        private final Collection<TableIdAndPath> removedTables;

        TableChange(
                Collection<TableIdAndPath> newTables,
                Collection<TableIdAndPath> removedPartitions) {
            this.newTables = newTables;
            this.removedTables = removedPartitions;
        }

        public boolean isEmpty() {
            return newTables.isEmpty() && removedTables.isEmpty();
        }
    }

    private static class TableAndPartitions {
        private final Map<Long, TableInfo> tableInfos;
        private final Map<Long, Set<PartitionInfo>> partitionInfos;

        public TableAndPartitions(
                Map<Long, TableInfo> tableInfos, Map<Long, Set<PartitionInfo>> partitionInfos) {
            this.tableInfos = tableInfos;
            this.partitionInfos = partitionInfos;
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

    private static class TableIdAndPath {
        private final long tableId;
        private final TablePath tablePath;

        public TableIdAndPath(long tableId, TablePath tablePath) {
            this.tableId = tableId;
            this.tablePath = tablePath;
        }

        public long tableId() {
            return tableId;
        }

        public TablePath tablePath() {
            return tablePath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableIdAndPath)) {
                return false;
            }
            TableIdAndPath that = (TableIdAndPath) o;
            return tableId == that.tableId && Objects.equals(tablePath, that.tablePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, tablePath);
        }

        @Override
        public String toString() {
            return "TableIdAndName{"
                    + "tableId="
                    + tableId
                    + ", tablePath='"
                    + tablePath
                    + '\''
                    + '}';
        }
    }
}
