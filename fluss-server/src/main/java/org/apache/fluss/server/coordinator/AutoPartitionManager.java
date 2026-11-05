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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InsufficientKvLeaderReplicaCapacityException;
import org.apache.fluss.exception.OperationNotAttemptedException;
import org.apache.fluss.exception.PartitionAlreadyExistsException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.exception.TooManyPartitionsException;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.PartitionUtils.generateAutoPartition;
import static org.apache.fluss.utils.PartitionUtils.generateAutoPartitionTime;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * An auto partition manager which will trigger auto partition for the tables in cluster
 * periodically. It'll use a {@link ScheduledExecutorService} to schedule the auto partition which
 * will trigger auto partition for them.
 *
 * <p>TODO: migrate the jittered partition-creation throttling logic into {@link
 * TableLifecycleThrottler} so that partition creation and deletion share the same lifecycle gate.
 * Tracked by https://github.com/apache/fluss/issues/3457.
 */
public class AutoPartitionManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AutoPartitionManager.class);

    /** scheduled executor, periodically trigger auto partition. */
    private final ScheduledExecutorService periodicExecutor;

    private final ServerMetadataCache metadataCache;
    private final MetadataManager metadataManager;
    private final RemoteDirDynamicLoader remoteDirDynamicLoader;
    private final ReplicaCapacityController replicaCapacityController;
    private final Clock clock;

    private final long periodicInterval;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    @GuardedBy("lock")
    private final Map<Long, AutoPartitionTable> autoPartitionTables = new HashMap<>();

    @GuardedBy("lock")
    private final Map<Long, Boolean> ddlRetries = new HashMap<>();

    @GuardedBy("lock")
    private boolean ddlRetryScheduled;

    private final Lock lock = new ReentrantLock();

    public AutoPartitionManager(
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            RemoteDirDynamicLoader remoteDirDynamicLoader,
            Configuration conf,
            ReplicaCapacityController replicaCapacityController) {
        this(
                metadataCache,
                metadataManager,
                remoteDirDynamicLoader,
                conf,
                replicaCapacityController,
                SystemClock.getInstance(),
                // TODO: Reuse the CoordinatorServer shared scheduler for this lightweight
                // coordinator periodic task instead of creating a component-owned scheduler.
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("periodic-auto-partition-manager")));
    }

    @VisibleForTesting
    AutoPartitionManager(
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            RemoteDirDynamicLoader remoteDirDynamicLoader,
            Configuration conf,
            ReplicaCapacityController replicaCapacityController,
            Clock clock,
            ScheduledExecutorService periodicExecutor) {
        this.metadataCache = metadataCache;
        this.metadataManager = metadataManager;
        this.remoteDirDynamicLoader = remoteDirDynamicLoader;
        this.replicaCapacityController = replicaCapacityController;
        this.clock = clock;
        this.periodicExecutor = periodicExecutor;
        this.periodicInterval = conf.get(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL).toMillis();
    }

    public void initAutoPartitionTables(List<TableInfo> tableInfos) {
        // Startup runs before the coordinator event loop starts. Preserve historical repair before
        // it begins processing requests, without holding the cache lock across metadata I/O.
        tableInfos.forEach(
                tableInfo -> {
                    inLock(
                            lock,
                            () ->
                                    autoPartitionTables.put(
                                            tableInfo.getTableId(),
                                            new AutoPartitionTable(tableInfo)));
                    if (tableInfo.getTableConfig().isHistoricalPartitionEnabled()) {
                        createHistoricalPartition(tableInfo);
                    } else {
                        dropHistoricalPartition(tableInfo);
                    }
                    inLock(lock, () -> retryAutoPartition(tableInfo.getTableId(), false, 0));
                });
    }

    public void updateAutoPartitionTables(TableInfo tableInfo) {
        addAutoPartitionTable(tableInfo, true);
    }

    public void addAutoPartitionTable(TableInfo tableInfo, boolean forceDoAutoPartition) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    autoPartitionTables.put(
                            tableInfo.getTableId(), new AutoPartitionTable(tableInfo));
                    retryAutoPartition(tableInfo.getTableId(), forceDoAutoPartition, 0);
                });
    }

    public void removeAutoPartitionTable(long tableId) {
        checkNotClosed();
        AutoPartitionTable removed = inLock(lock, () -> removeAutoPartitionTableLocked(tableId));
        TableInfo tableInfo = removed == null ? null : removed.tableInfo;
        if (tableInfo != null) {
            LOG.info(
                    "Removed auto partition table [{}] (id={}) from scheduler",
                    tableInfo.getTablePath(),
                    tableInfo.getTableId());
        }
    }

    /**
     * Handles a table's auto-partition strategy change after table properties are updated.
     *
     * @param newTableInfo the updated table information
     * @param oldStrategy the old auto partition strategy
     * @param newStrategy the updated auto partition strategy
     */
    public void handleAutoPartitionStrategyChange(
            TableInfo newTableInfo,
            AutoPartitionStrategy oldStrategy,
            AutoPartitionStrategy newStrategy) {
        checkNotClosed();
        long tableId = newTableInfo.getTableId();
        boolean oldAutoPartitionEnabled = oldStrategy.isAutoPartitionEnabled();
        boolean newAutoPartitionEnabled = newStrategy.isAutoPartitionEnabled();

        if (!oldAutoPartitionEnabled && newAutoPartitionEnabled) {
            LOG.info("Table {} auto partition enabled from false to true.", tableId);
            addAutoPartitionTable(newTableInfo, true);
        } else if (oldAutoPartitionEnabled && !newAutoPartitionEnabled) {
            LOG.info("Table {} auto partition enabled from true to false.", tableId);
            removeAutoPartitionTable(tableId);
        } else if (newAutoPartitionEnabled) {
            LOG.info("Table {} auto partition strategy changed.", tableId);
            updateAutoPartitionTables(newTableInfo);
        }
    }

    /** Creates the historical system partition if it does not already exist. */
    void createHistoricalPartition(TableInfo tableInfo) {
        updateHistoricalPartition(tableInfo, true);
    }

    /** Best-effort deletes the historical system partition if it exists. */
    void dropHistoricalPartition(TableInfo tableInfo) {
        checkNotClosed();
        try {
            updateHistoricalPartition(tableInfo, false);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to delete historical partition for table [{}].",
                    tableInfo.getTablePath(),
                    e);
        }
    }

    private void updateHistoricalPartition(TableInfo tableInfo, boolean create) {
        checkNotClosed();
        TablePath tablePath = tableInfo.getTablePath();
        metadataManager.tryRunTableDdl(
                tablePath,
                scoped -> {
                    TableInfo current = scoped.getTable(tablePath);
                    if (current.getTableId() != tableInfo.getTableId()) {
                        return null;
                    }
                    PartitionSnapshot snapshot = snapshot(tableInfo.getTableId());
                    if (snapshot == null) {
                        return null;
                    }
                    loadPartitions(scoped, current, snapshot);
                    if (create) {
                        if (!snapshot.partitions.containsKey(HISTORICAL_PARTITION_VALUE)) {
                            createPartition(
                                    scoped,
                                    current,
                                    new ResolvedPartitionSpec(
                                            current.getPartitionKeys(),
                                            Collections.singletonList(HISTORICAL_PARTITION_VALUE)),
                                    snapshot.partitions,
                                    current.getNumBuckets());
                        }
                    } else if (snapshot.partitions.containsKey(HISTORICAL_PARTITION_VALUE)) {
                        scoped.dropPartition(
                                tablePath,
                                current.getTableId(),
                                new ResolvedPartitionSpec(
                                        current.getPartitionKeys(),
                                        Collections.singletonList(HISTORICAL_PARTITION_VALUE)),
                                true);
                        snapshot.partitions.remove(HISTORICAL_PARTITION_VALUE);
                    }
                    publishSnapshot(tableInfo.getTableId(), snapshot);
                    return null;
                });
    }

    /** Must be called while holding {@link #lock}. */
    @Nullable
    private AutoPartitionTable removeAutoPartitionTableLocked(long tableId) {
        ddlRetries.remove(tableId);
        return autoPartitionTables.remove(tableId);
    }

    /**
     * Try to add a partition to cache if this table is autoPartitionedTable and partition not
     * exists in cache.
     */
    public void addPartition(long tableId, String partitionName) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    AutoPartitionTable state = autoPartitionTables.get(tableId);
                    if (state != null) {
                        addPartitionToPartitionsByTable(
                                state.tableInfo, state.partitions, partitionName);
                        state.version++;
                    }
                });
    }

    /** Removes a partition from the auto-partition cache. */
    public void removePartition(long tableId, String partitionName) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    AutoPartitionTable state = autoPartitionTables.get(tableId);
                    if (state != null) {
                        removePartitionFromPartitionsByTable(
                                state.tableInfo, state.partitions, partitionName);
                        state.version++;
                    }
                });
    }

    public void start() {
        checkNotClosed();
        periodicExecutor.scheduleWithFixedDelay(
                this::doAutoPartition, periodicInterval, periodicInterval, TimeUnit.MILLISECONDS);
        LOG.info("Auto partitioning task is scheduled at fixed interval {}ms.", periodicInterval);
    }

    private void checkNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("AutoPartitionManager is already closed.");
        }
    }

    private String extractAutoPartitionValue(TableInfo tableInfo, String partitionName) {
        // for single partition key table, the full partition name is the auto partition value
        if (tableInfo.getPartitionKeys().size() == 1) {
            return partitionName;
        }

        String autoPartitionKey = tableInfo.getTableConfig().getAutoPartitionStrategy().key();
        int autoPartitionKeyIndex = tableInfo.getPartitionKeys().indexOf(autoPartitionKey);
        return partitionName.split("\\$")[autoPartitionKeyIndex];
    }

    private void addPartitionToPartitionsByTable(
            TableInfo tableInfo,
            NavigableMap<String, Set<String>> partitionMap,
            String partitionName) {
        if (tableInfo.getPartitionKeys().size() > 1) {
            Set<String> partitionSet =
                    partitionMap.computeIfAbsent(
                            extractAutoPartitionValue(tableInfo, partitionName),
                            k -> new HashSet<>());
            checkNotNull(partitionSet, "Partition set is null.");
            partitionSet.add(partitionName);
        } else {
            partitionMap.put(partitionName, null);
        }
    }

    private void removePartitionFromPartitionsByTable(
            TableInfo tableInfo,
            NavigableMap<String, Set<String>> partitionMap,
            String partitionName) {
        if (tableInfo.getPartitionKeys().size() > 1) {
            String autoPartitionValue = extractAutoPartitionValue(tableInfo, partitionName);
            Set<String> partitionSet = partitionMap.get(autoPartitionValue);
            if (partitionSet != null) {
                partitionSet.remove(partitionName);
                if (partitionSet.isEmpty()) {
                    partitionMap.remove(autoPartitionValue);
                }
            }
        } else {
            partitionMap.remove(partitionName);
        }
    }

    private void doAutoPartition() {
        Set<Long> tables = inLock(lock, () -> new HashSet<>(autoPartitionTables.keySet()));
        doAutoPartition(clock.instant(), tables, false);
    }

    private void doAutoPartition(Instant now, Set<Long> tableIds, boolean forceDoAutoPartition) {
        for (Long tableId : tableIds) {
            if (isClosed.get()) {
                return;
            }
            PartitionSnapshot snapshot = snapshot(tableId);
            if (snapshot == null) {
                continue;
            }
            TablePath tablePath = snapshot.state.tableInfo.getTablePath();
            Instant plannedCreateInstant =
                    forceDoAutoPartition
                            ? now
                            : now.minus(Duration.ofMinutes(snapshot.state.delayMinutes));
            try {
                metadataManager.tryRunTableDdl(
                        tablePath,
                        scoped -> {
                            TableInfo current = scoped.getTable(tablePath);
                            if (current.getTableId() != tableId) {
                                return null;
                            }
                            loadPartitions(scoped, current, snapshot);
                            if (!current.getTableConfig()
                                    .getAutoPartitionStrategy()
                                    .isAutoPartitionEnabled()) {
                                return null;
                            }
                            dropPartitions(
                                    scoped,
                                    tablePath,
                                    current.getPartitionKeys(),
                                    now,
                                    current.getTableConfig().getAutoPartitionStrategy(),
                                    snapshot.partitions);
                            createPartitions(
                                    scoped, current, plannedCreateInstant, snapshot.partitions);
                            publishSnapshot(tableId, snapshot);
                            return null;
                        });
            } catch (OperationNotAttemptedException e) {
                inLock(lock, () -> retryAutoPartition(tableId, forceDoAutoPartition, 1));
            } catch (Exception e) {
                LOG.warn(
                        "Skipping auto partitioning for table [{}] (id={}) as its metadata could not be updated.",
                        tablePath,
                        tableId,
                        e);
            }
        }
    }

    @Nullable
    private PartitionSnapshot snapshot(long tableId) {
        return inLock(
                lock,
                () -> {
                    AutoPartitionTable state = autoPartitionTables.get(tableId);
                    return state == null ? null : new PartitionSnapshot(state);
                });
    }

    private void loadPartitions(
            MetadataManager scoped, TableInfo table, PartitionSnapshot snapshot) {
        if (!snapshot.initialized) {
            snapshot.partitions.clear();
            scoped.getPartitions(table.getTablePath())
                    .forEach(
                            name ->
                                    addPartitionToPartitionsByTable(
                                            table, snapshot.partitions, name));
        }
    }

    private void publishSnapshot(long tableId, PartitionSnapshot snapshot) {
        inLock(
                lock,
                () -> {
                    AutoPartitionTable current = autoPartitionTables.get(tableId);
                    if (current != snapshot.state
                            || current.version != snapshot.version
                            || isClosed.get()) {
                        // Watcher events already updated the cache. Replaying completed DDL here
                        // could recreate a partition removed after this pass; reload on the next
                        // periodic pass if initialization was interrupted.
                        return;
                    }
                    current.partitions = snapshot.partitions;
                    current.initialized = true;
                    current.version++;
                });
    }

    /** Must be called while holding {@link #lock}. */
    private void retryAutoPartition(long tableId, boolean forceDoAutoPartition, long delaySeconds) {
        if (isClosed.get() || !autoPartitionTables.containsKey(tableId)) {
            return;
        }
        ddlRetries.merge(tableId, forceDoAutoPartition, Boolean::logicalOr);
        if (!ddlRetryScheduled) {
            ddlRetryScheduled = true;
            try {
                periodicExecutor.schedule(
                        this::retryAutoPartitions, delaySeconds, TimeUnit.SECONDS);
            } catch (RejectedExecutionException e) {
                ddlRetryScheduled = false;
                ddlRetries.clear();
                if (!isClosed.get()) {
                    throw e;
                }
            }
        }
    }

    private void retryAutoPartitions() {
        Map<Long, Boolean> retries =
                inLock(
                        lock,
                        () -> {
                            ddlRetryScheduled = false;
                            Map<Long, Boolean> pending = new HashMap<>(ddlRetries);
                            ddlRetries.clear();
                            return pending;
                        });
        retries.forEach(
                (tableId, force) ->
                        doAutoPartition(clock.instant(), Collections.singleton(tableId), force));
    }

    private void createPartitions(
            MetadataManager scopedMetadata,
            TableInfo tableInfo,
            Instant currentInstant,
            TreeMap<String, Set<String>> currentPartitions) {
        // get the partitions needed to create
        List<ResolvedPartitionSpec> partitionsToPreCreate =
                partitionNamesToPreCreate(
                        tableInfo.getPartitionKeys(),
                        currentInstant,
                        tableInfo.getTableConfig().getAutoPartitionStrategy(),
                        currentPartitions);
        if (partitionsToPreCreate.isEmpty()) {
            return;
        }

        for (ResolvedPartitionSpec partition : partitionsToPreCreate) {
            createPartition(
                    scopedMetadata,
                    tableInfo,
                    partition,
                    currentPartitions,
                    tableInfo.getNumBuckets());
        }
    }

    private void createPartition(
            MetadataManager scopedMetadata,
            TableInfo tableInfo,
            ResolvedPartitionSpec partition,
            TreeMap<String, Set<String>> currentPartitions,
            int bucketCount) {
        TablePath tablePath = tableInfo.getTablePath();
        long tableId = tableInfo.getTableId();
        int replicaFactor = tableInfo.getTableConfig().getReplicationFactor();
        TabletServerInfo[] servers = metadataCache.getLiveServers();
        long newKvLeaderReplicaCount = tableInfo.hasPrimaryKey() ? bucketCount : 0;
        try {
            replicaCapacityController.checkCanCreateKvLeaderReplicas(newKvLeaderReplicaCount);

            Map<Integer, BucketAssignment> bucketAssignments =
                    generateAssignment(bucketCount, replicaFactor, servers).getBucketAssignments();
            PartitionAssignment partitionAssignment =
                    new PartitionAssignment(tableInfo.getTableId(), bucketAssignments);

            String remoteDataDir = remoteDirDynamicLoader.getRemoteDirSelector().nextDataDir();
            scopedMetadata.createPartition(
                    tablePath,
                    tableId,
                    remoteDataDir,
                    partitionAssignment,
                    partition,
                    false,
                    bucketCount);
            currentPartitions.put(partition.getPartitionName(), null);
            LOG.info(
                    "Auto partitioning created partition {} for table [{}].", partition, tablePath);
        } catch (PartitionAlreadyExistsException e) {
            currentPartitions.put(partition.getPartitionName(), null);
            LOG.info(
                    "Auto partitioning skip to create partition {} for table [{}] as the partition is exist.",
                    partition,
                    tablePath);
        } catch (TooManyPartitionsException t) {
            LOG.warn(
                    "Auto partitioning skip to create partition {} for table [{}], "
                            + "because exceed the maximum number of partitions.",
                    partition,
                    tablePath);
        } catch (TooManyBucketsException t) {
            LOG.warn(
                    "Auto partitioning skip to create partition {} for table [{}], "
                            + "because exceed the maximum number of buckets per partition.",
                    partition,
                    tablePath);
        } catch (InsufficientKvLeaderReplicaCapacityException t) {
            LOG.warn(
                    "Auto partitioning skip to create partition {} for table [{}], because {}",
                    partition,
                    tablePath,
                    t.getMessage());
        } catch (Exception e) {
            LOG.error(
                    "Auto partitioning failed to create partition {} for table [{}].",
                    partition,
                    tablePath,
                    e);
        }
    }

    private List<ResolvedPartitionSpec> partitionNamesToPreCreate(
            List<String> partitionKeys,
            Instant currentInstant,
            AutoPartitionStrategy autoPartitionStrategy,
            TreeMap<String, Set<String>> currentPartitions) {
        AutoPartitionTimeUnit autoPartitionTimeUnit = autoPartitionStrategy.timeUnit();
        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(
                        currentInstant, autoPartitionStrategy.timeZone().toZoneId());

        int partitionToPreCreate = autoPartitionStrategy.numPreCreate();
        List<ResolvedPartitionSpec> partitionsToCreate = new ArrayList<>();
        for (int idx = 0; idx < partitionToPreCreate; idx++) {
            ResolvedPartitionSpec partition =
                    generateAutoPartition(
                            partitionKeys,
                            currentZonedDateTime,
                            idx,
                            autoPartitionTimeUnit,
                            autoPartitionStrategy);
            // if the partition already exists, we don't need to create it, otherwise, create it
            if (!currentPartitions.containsKey(partition.getPartitionName())) {
                partitionsToCreate.add(partition);
            }
        }
        return partitionsToCreate;
    }

    private void dropPartitions(
            MetadataManager scopedMetadata,
            TablePath tablePath,
            List<String> partitionKeys,
            Instant currentInstant,
            AutoPartitionStrategy autoPartitionStrategy,
            NavigableMap<String, Set<String>> currentPartitions) {
        int numToRetain = autoPartitionStrategy.numToRetain();
        // negative value means not to drop partitions
        if (numToRetain < 0) {
            return;
        }

        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(
                        currentInstant, autoPartitionStrategy.timeZone().toZoneId());

        // Get the earliest one partition time that need to retain.
        String lastRetainPartitionTime =
                generateAutoPartitionTime(
                        currentZonedDateTime,
                        -numToRetain,
                        autoPartitionStrategy.timeUnit(),
                        autoPartitionStrategy);

        // For partition table with a single partition key, for example dt(yyyyMMdd)
        // assuming now is 20250508, and table.auto-partition.num-retention=2 then partition
        // 20250506 and 20250507 will be retained.
        //
        // For partition table with multiple partition keys, for example a,dt(yyyyMMdd),b
        // which means dt is a partition time key and a,b are normal partition key,
        // assuming now is 20250508, and table.auto-partition.num-retention=2.
        // assuming we have the following partitions:
        // (a=1,dt=20250505,b=1) (a=1,dt=20250506,b=1) (a=1,dt=20250507,b=1)
        // (a=2,dt=20250505,b=1) (a=2,dt=20250506,b=1) (a=2,dt=20250507,b=1)
        // then partition of pattern:
        // (a=?,dt=20250506,b=?) (a=?,dt=20250507,b=?) will be retained.
        Iterator<Map.Entry<String, Set<String>>> iterator =
                currentPartitions.headMap(lastRetainPartitionTime).entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> entry = iterator.next();
            // Historical system partitions are managed explicitly by table configuration
            // changes and Coordinator recovery, never by normal retention cleanup.
            if (HISTORICAL_PARTITION_VALUE.equals(entry.getKey())) {
                continue;
            }
            dropPartitions(scopedMetadata, tablePath, partitionKeys, iterator, entry);
        }
    }

    private void dropPartitions(
            MetadataManager scopedMetadata,
            TablePath tablePath,
            List<String> partitionKeys,
            Iterator<Map.Entry<String, Set<String>>> iterator,
            Map.Entry<String, Set<String>> entry) {
        Iterator<String> dropIterator;
        if (entry.getValue() == null) {
            dropIterator = new HashSet<>(Collections.singleton(entry.getKey())).iterator();
        } else {
            dropIterator = entry.getValue().iterator();
        }

        boolean deletionFailed = false;
        while (dropIterator.hasNext()) {
            String partitionName = dropIterator.next();
            try {
                scopedMetadata.dropPartition(
                        tablePath,
                        ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName),
                        false);
            } catch (PartitionNotExistException e) {
                LOG.info(
                        "Auto partitioning skip to delete partition {} for table [{}] as the partition is not exist.",
                        partitionName,
                        tablePath);
            } catch (Exception e) {
                LOG.warn(
                        "Auto partitioning failed to delete partition {} for table [{}].",
                        partitionName,
                        tablePath,
                        e);
                deletionFailed = true;
                continue;
            }

            dropIterator.remove();
            LOG.info(
                    "Auto partitioning deleted partition {} for table [{}].",
                    partitionName,
                    tablePath);
        }
        if (!deletionFailed) {
            iterator.remove();
        }
    }

    @VisibleForTesting
    @Nullable
    protected Integer getAutoCreateDayDelayMinutes(long tableId) {
        return inLock(
                lock,
                () -> {
                    AutoPartitionTable state = autoPartitionTables.get(tableId);
                    return state == null
                                    || state.tableInfo
                                                    .getTableConfig()
                                                    .getAutoPartitionStrategy()
                                                    .timeUnit()
                                            != AutoPartitionTimeUnit.DAY
                            ? null
                            : state.delayMinutes;
                });
    }

    /** Returns a snapshot of the cached partitions for the specified table. */
    @VisibleForTesting
    @Nullable
    protected Map<String, Set<String>> getPartitionsByTable(long tableId) {
        PartitionSnapshot snapshot = snapshot(tableId);
        return snapshot == null ? null : snapshot.partitions;
    }

    private static final class AutoPartitionTable {
        private final TableInfo tableInfo;
        private final int delayMinutes;
        private TreeMap<String, Set<String>> partitions = new TreeMap<>();
        private long version;
        private boolean initialized;

        private AutoPartitionTable(TableInfo tableInfo) {
            this.tableInfo = tableInfo;
            this.delayMinutes =
                    tableInfo.getTableConfig().getAutoPartitionStrategy().timeUnit()
                                    == AutoPartitionTimeUnit.DAY
                            ? ThreadLocalRandom.current().nextInt(60 * 23)
                            : 0;
        }
    }

    private static final class PartitionSnapshot {
        private final AutoPartitionTable state;
        private final long version;
        private final boolean initialized;
        private final TreeMap<String, Set<String>> partitions = new TreeMap<>();

        private PartitionSnapshot(AutoPartitionTable state) {
            this.state = state;
            this.version = state.version;
            this.initialized = state.initialized;
            state.partitions.forEach(
                    (key, values) ->
                            partitions.put(key, values == null ? null : new HashSet<>(values)));
        }
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            periodicExecutor.shutdownNow();
            inLock(
                    lock,
                    () -> {
                        autoPartitionTables.clear();
                        ddlRetries.clear();
                        ddlRetryScheduled = false;
                    });
        }
    }
}
