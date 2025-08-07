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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.PartitionAlreadyExistsException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.TooManyBucketsException;
import com.alibaba.fluss.exception.TooManyPartitionsException;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableRegistration;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static com.alibaba.fluss.utils.PartitionUtils.generateAutoPartition;
import static com.alibaba.fluss.utils.PartitionUtils.generateAutoPartitionTime;
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

    private final ServerMetadataCache metadataCache;
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

    // table id -> (value of auto partition time key -> partition name set)
    // for single partition key, the partition name set will be null to reduce memory usage
    @GuardedBy("lock")
    private final Map<Long, TreeMap<String, Set<String>>> partitionsByTable = new HashMap<>();

    private final Lock lock = new ReentrantLock();

    public AutoPartitionManager(
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            Configuration conf) {
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
            ServerMetadataCache metadataCache,
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
                    TreeMap<String, Set<String>> partitionMap =
                            partitionsByTable.computeIfAbsent(
                                    tableInfo.getTableId(), k -> new TreeMap<>());
                    checkNotNull(partitionMap, "Partition map is null.");
                    partitions.forEach(
                            partitionName ->
                                    addPartitionToPartitionsByTable(
                                            tableInfo, partitionMap, partitionName));
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
        LOG.info(
                "Added auto partition table [{}] (id={}) into scheduler",
                tableInfo.getTablePath(),
                tableInfo.getTableId());
    }

    public void removeAutoPartitionTable(long tableId) {
        checkNotClosed();
        TableInfo tableInfo =
                inLock(
                        lock,
                        () -> {
                            partitionsByTable.remove(tableId);
                            autoCreateDayPartitionDelayMinutes.remove(tableId);
                            return autoPartitionTables.remove(tableId);
                        });
        if (tableInfo != null) {
            LOG.info(
                    "Removed auto partition table [{}] (id={}) from scheduler",
                    tableInfo.getTablePath(),
                    tableInfo.getTableId());
        }
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
                        addPartitionToPartitionsByTable(
                                autoPartitionTables.get(tableId),
                                partitionsByTable.get(tableId),
                                partitionName);
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

    private void doAutoPartition() {
        Instant now = clock.instant();
        inLock(lock, () -> doAutoPartition(now, autoPartitionTables.keySet(), false));
    }

    private void doAutoPartition(long tableId, boolean forceDoAutoPartition) {
        Instant now = clock.instant();
        inLock(
                lock,
                () -> doAutoPartition(now, Collections.singleton(tableId), forceDoAutoPartition));
    }

    private void doAutoPartition(Instant now, Set<Long> tableIds, boolean forceDoAutoPartition) {
        LOG.info("Start auto partitioning for {} tables at {}.", tableIds.size(), now);
        for (Long tableId : tableIds) {
            Instant createPartitionInstant = now;
            if (!forceDoAutoPartition) {
                // not to force do auto partition and delay exist,
                // we use now - delayMinutes as current instant to mock the delay
                Integer delayMinutes = autoCreateDayPartitionDelayMinutes.get(tableId);
                if (delayMinutes != null) {
                    createPartitionInstant = now.minus(Duration.ofMinutes(delayMinutes));
                }
            }

            TableInfo tableInfo = autoPartitionTables.get(tableId);
            TablePath tablePath = tableInfo.getTablePath();
            TreeMap<String, Set<String>> currentPartitions =
                    partitionsByTable.computeIfAbsent(
                            tableId,
                            tableInfo.getPartitionKeys().size() > 1
                                    ? k -> new TreeMap<>()
                                    : k -> null);
            TableRegistration table;
            try {
                table = metadataManager.getTableRegistration(tablePath);
            } catch (Exception e) {
                LOG.warn(
                        "Skipping auto partitioning for table [{}] (id={}) as failed to get table information.",
                        tablePath,
                        tableId,
                        e);
                continue;
            }
            if (table.tableId != tableId) {
                LOG.warn(
                        "Skipping auto partitioning for table [{}] (id={}) as the table has been dropped.",
                        tablePath,
                        tableId);
                continue;
            }
            dropPartitions(
                    tablePath,
                    tableInfo.getPartitionKeys(),
                    createPartitionInstant,
                    tableInfo.getTableConfig().getAutoPartitionStrategy(),
                    currentPartitions);
            createPartitions(tableInfo, createPartitionInstant, currentPartitions);
        }
    }

    private void createPartitions(
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

        TablePath tablePath = tableInfo.getTablePath();
        for (ResolvedPartitionSpec partition : partitionsToPreCreate) {
            long tableId = tableInfo.getTableId();
            int replicaFactor = tableInfo.getTableConfig().getReplicationFactor();
            TabletServerInfo[] servers = metadataCache.getLiveServers();
            try {
                Map<Integer, BucketAssignment> bucketAssignments =
                        generateAssignment(tableInfo.getNumBuckets(), replicaFactor, servers)
                                .getBucketAssignments();
                PartitionAssignment partitionAssignment =
                        new PartitionAssignment(tableInfo.getTableId(), bucketAssignments);

                metadataManager.createPartition(
                        tablePath, tableId, partitionAssignment, partition, false);
                // only single partition key table supports automatic creation of partitions
                currentPartitions.put(partition.getPartitionName(), null);
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
            } catch (TooManyBucketsException t) {
                LOG.warn(
                        "Auto partitioning skip to create partition {} for table [{}], "
                                + "because exceed the maximum number of buckets.",
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
                            partitionKeys, currentZonedDateTime, idx, autoPartitionTimeUnit);
            // if the partition already exists, we don't need to create it, otherwise, create it
            if (!currentPartitions.containsKey(partition.getPartitionName())) {
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
                        currentZonedDateTime, -numToRetain, autoPartitionStrategy.timeUnit());

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

            Iterator<String> dropIterator;
            if (entry.getValue() == null) {
                dropIterator = new HashSet<>(Collections.singleton(entry.getKey())).iterator();
            } else {
                dropIterator = entry.getValue().iterator();
            }

            while (dropIterator.hasNext()) {
                String partitionName = dropIterator.next();
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
                dropIterator.remove();
                LOG.info(
                        "Auto partitioning deleted partition {} for table [{}].",
                        partitionName,
                        tablePath);
            }
            iterator.remove();
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
