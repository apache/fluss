/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.MetadataCache;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.PartitionAlreadyExistsException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.TooManyPartitionsException;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.utils.TableAssignmentUtils;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.utils.AutoPartitionStrategy;
import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.PartitionUtils.generateAutoPartition;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * An auto partition manager which will trigger auto partition for the tables in cluster
 * periodically. It'll use a {@link ScheduledExecutorService} to schedule the auto partition which
 * will trigger auto partition for them.
 */
public class AutoPartitionManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AutoPartitionManager.class);

    /** scheduled executor, periodically trigger auto partition. */
    private final ScheduledExecutorService periodicExecutor;

    private final MetadataCache metadataCache;
    private final MetadataManager metadataManager;
    private final Clock clock;

    private final long periodicInterval;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    // TODO these two local cache can be removed if we introduce server cache.
    @GuardedBy("lock")
    private final Map<Long, TableInfo> autoPartitionTables = new HashMap<>();

    // table id -> the minutes in day when auto partition will be triggered
    // now, only consider day partition, todo: need to consider all partition unit
    private final Map<Long, Integer> autoCreateDayPartitionDelayMinutes = new HashMap<>();

    @GuardedBy("lock")
    private final Map<Long, TreeSet<String>> partitionsByTable = new HashMap<>();

    private final Lock lock = new ReentrantLock();

    public AutoPartitionManager(
            MetadataCache metadataCache, MetadataManager metadataManager, Configuration conf) {
        this(
                metadataCache,
                metadataManager,
                conf,
                SystemClock.getInstance(),
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("periodic-auto-partition-manager")));
    }

    @VisibleForTesting
    AutoPartitionManager(
            MetadataCache metadataCache,
            MetadataManager metadataManager,
            Configuration conf,
            Clock clock,
            ScheduledExecutorService periodicExecutor) {
        this.metadataCache = metadataCache;
        this.metadataManager = metadataManager;
        this.clock = clock;
        this.periodicExecutor = periodicExecutor;
        this.periodicInterval = conf.get(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL).toMillis();
    }

    public void initAutoPartitionTables(List<TableInfo> tableInfos) {
        tableInfos.forEach(tableInfo -> addAutoPartitionTable(tableInfo, false));
    }

    public void addAutoPartitionTable(TableInfo tableInfo, boolean forceDoAutoPartition) {
        checkNotClosed();
        long tableId = tableInfo.getTableId();
        Set<String> partitions = metadataManager.getPartitions(tableInfo.getTablePath());
        inLock(
                lock,
                () -> {
                    autoPartitionTables.put(tableId, tableInfo);
                    Set<String> partitionSet =
                            partitionsByTable.computeIfAbsent(
                                    tableInfo.getTableId(), k -> new TreeSet<>());
                    checkNotNull(partitionSet, "Partition set is null.");
                    partitionSet.addAll(partitions);
                    if (tableInfo.getTableConfig().getAutoPartitionStrategy().timeUnit()
                            == AutoPartitionTimeUnit.DAY) {
                        // get the delay minutes to create partition
                        int delayMinutes = ThreadLocalRandom.current().nextInt(60 * 23);

                        autoCreateDayPartitionDelayMinutes.put(tableId, delayMinutes);
                    }
                });

        // schedule auto partition for this table immediately
        periodicExecutor.schedule(
                () -> doAutoPartition(tableId, forceDoAutoPartition), 0, TimeUnit.MILLISECONDS);
    }

    public void removeAutoPartitionTable(long tableId) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    autoPartitionTables.remove(tableId);
                    partitionsByTable.remove(tableId);
                    autoCreateDayPartitionDelayMinutes.remove(tableId);
                });
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
                    if (autoPartitionTables.containsKey(tableId)) {
                        partitionsByTable.get(tableId).add(partitionName);
                    }
                });
    }

    /**
     * Remove a partition from cache if this table is autoPartitionedTable and partition exists in
     * cache.
     */
    public void removePartition(long tableId, String partitionName) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    if (autoPartitionTables.containsKey(tableId)) {
                        partitionsByTable.get(tableId).remove(partitionName);
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

    private void doAutoPartition() {
        Instant now = clock.instant();
        LOG.info("Start auto partitioning for all tables at {}.", now);
        inLock(lock, () -> doAutoPartition(now, autoPartitionTables.keySet(), false));
    }

    private void doAutoPartition(long tableId, boolean forceDoAutoPartition) {
        Instant now = clock.instant();
        LOG.info("Start auto partitioning for table {} at {}.", tableId, now);
        inLock(
                lock,
                () -> doAutoPartition(now, Collections.singleton(tableId), forceDoAutoPartition));
    }

    private void doAutoPartition(Instant now, Set<Long> tableIds, boolean forceDoAutoPartition) {
        for (Long tableId : tableIds) {
            Instant createPartitionInstant = now;
            TableInfo tableInfo = autoPartitionTables.get(tableId);
            if (!forceDoAutoPartition) {
                // not to force do auto partition and delay exist,
                // we use now - delayMinutes as current instant to mock the delay
                Integer delayMinutes = autoCreateDayPartitionDelayMinutes.get(tableId);
                if (delayMinutes != null) {
                    createPartitionInstant = now.minus(Duration.ofMinutes(delayMinutes));
                }
            }
            TreeSet<String> currentPartitions =
                    partitionsByTable.computeIfAbsent(tableId, k -> new TreeSet<>());
            dropPartitions(
                    tableInfo.getTablePath(),
                    tableInfo.getPartitionKeys(),
                    createPartitionInstant,
                    tableInfo.getTableConfig().getAutoPartitionStrategy(),
                    currentPartitions);
            createPartitions(tableInfo, createPartitionInstant, currentPartitions);
        }
    }

    private void createPartitions(
            TableInfo tableInfo, Instant currentInstant, TreeSet<String> currentPartitions) {
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

        TablePath tablePath = tableInfo.getTablePath();
        for (ResolvedPartitionSpec partition : partitionsToPreCreate) {
            long tableId = tableInfo.getTableId();
            int replicaFactor = tableInfo.getTableConfig().getReplicationFactor();
            int[] servers = metadataCache.getLiveServerIds();
            Map<Integer, BucketAssignment> bucketAssignments =
                    TableAssignmentUtils.generateAssignment(
                                    tableInfo.getNumBuckets(), replicaFactor, servers)
                            .getBucketAssignments();
            PartitionAssignment partitionAssignment =
                    new PartitionAssignment(tableInfo.getTableId(), bucketAssignments);

            try {
                metadataManager.createPartition(
                        tablePath, tableId, partitionAssignment, partition, false);
                currentPartitions.add(partition.getPartitionName());
                LOG.info(
                        "Auto partitioning created partition {} for table [{}].",
                        partition,
                        tablePath);
            } catch (PartitionAlreadyExistsException e) {
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
            } catch (Exception e) {
                LOG.error(
                        "Auto partitioning failed to create partition {} for table [{}].",
                        partition,
                        tablePath,
                        e);
            }
        }
    }

    private List<ResolvedPartitionSpec> partitionNamesToPreCreate(
            List<String> partitionKeys,
            Instant currentInstant,
            AutoPartitionStrategy autoPartitionStrategy,
            TreeSet<String> currentPartitions) {
        AutoPartitionTimeUnit autoPartitionTimeUnit = autoPartitionStrategy.timeUnit();
        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(
                        currentInstant, autoPartitionStrategy.timeZone().toZoneId());

        int partitionToPreCreate = autoPartitionStrategy.numPreCreate();
        List<ResolvedPartitionSpec> partitionsToCreate = new ArrayList<>();
        for (int idx = 0; idx < partitionToPreCreate; idx++) {
            ResolvedPartitionSpec partition =
                    generateAutoPartition(
                            partitionKeys, currentZonedDateTime, idx, autoPartitionTimeUnit);
            // if the partition already exists, we don't need to create it, otherwise, create it
            if (!currentPartitions.contains(partition.getPartitionName())) {
                partitionsToCreate.add(partition);
            }
        }
        return partitionsToCreate;
    }

    private void dropPartitions(
            TablePath tablePath,
            List<String> partitionKeys,
            Instant currentInstant,
            AutoPartitionStrategy autoPartitionStrategy,
            NavigableSet<String> currentPartitions) {
        int numToRetain = autoPartitionStrategy.numToRetain();
        // negative value means not to drop partitions
        if (numToRetain < 0) {
            return;
        }

        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(
                        currentInstant, autoPartitionStrategy.timeZone().toZoneId());

        // get the earliest one partition that need to retain
        ResolvedPartitionSpec lastRetainPartition =
                generateAutoPartition(
                        partitionKeys,
                        currentZonedDateTime,
                        -numToRetain,
                        autoPartitionStrategy.timeUnit());

        Iterator<String> partitionsToExpire =
                currentPartitions.headSet(lastRetainPartition.getPartitionName(), false).iterator();

        while (partitionsToExpire.hasNext()) {
            String partitionName = partitionsToExpire.next();
            // drop the partition
            try {
                metadataManager.dropPartition(
                        tablePath,
                        ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName),
                        false);
            } catch (PartitionNotExistException e) {
                LOG.info(
                        "Auto partitioning skip to delete partition {} for table [{}] as the partition is not exist.",
                        partitionName,
                        tablePath);
            }

            // only remove when zk success, this reflects to the partitionsByTable
            partitionsToExpire.remove();
            LOG.info(
                    "Auto partitioning deleted partition {} for table [{}].",
                    partitionName,
                    tablePath);
        }
    }

    @VisibleForTesting
    @Nullable
    protected Integer getAutoCreateDayDelayMinutes(long tableId) {
        return autoCreateDayPartitionDelayMinutes.get(tableId);
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            periodicExecutor.shutdownNow();
        }
    }
}
