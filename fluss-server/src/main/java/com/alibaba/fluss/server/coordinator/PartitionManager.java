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
import com.alibaba.fluss.exception.PartitionSpecInvalidException;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.utils.TableAssignmentUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.utils.AutoPartitionStrategy;
import com.alibaba.fluss.utils.PartitionUtils;
import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.PartitionUtils.generateAutoPartitionName;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A partition manager which will manage static partition tables and auto partition tables.
 *
 * <p>For static partition tables, it's responsible for adding partitions to or drop partitions from
 * static partition table.
 *
 * <p>For auto partition tables, it's responsible not only for adding partitions to or drop
 * partitions from auto partition table, but also triggering auto partition for these auto partition
 * tables in cluster periodically. It'll use a {@link ScheduledExecutorService} to schedule the auto
 * partition which trigger auto partition for them.
 */
public class PartitionManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    private final ZooKeeperClient zooKeeperClient;
    private final MetadataCache metadataCache;
    private final Lock lock = new ReentrantLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final Clock clock;
    private final long periodicInterval;

    /** scheduled executor, periodically trigger auto partition. */
    private final ScheduledExecutorService periodicExecutor;

    @GuardedBy("lock")
    private final Map<Long, TableInfo> autoPartitionTables = new HashMap<>();

    /** A map from tableId to the set of partitions for auto partition tables. */
    @GuardedBy("lock")
    private final Map<Long, TreeSet<String>> partitionsForAutoPartitionTable = new HashMap<>();

    /** A map from tableId to the set of partitions for static partition tables. */
    @GuardedBy("lock")
    private final Map<Long, Set<String>> partitionsForStaticPartitionTable = new HashMap<>();

    public PartitionManager(
            MetadataCache metadataCache, ZooKeeperClient zooKeeperClient, Configuration conf) {
        this(
                zooKeeperClient,
                metadataCache,
                conf,
                SystemClock.getInstance(),
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("periodic-auto-partition-manager")));
    }

    @VisibleForTesting
    PartitionManager(
            ZooKeeperClient zooKeeperClient,
            MetadataCache metadataCache,
            Configuration conf,
            Clock clock,
            ScheduledExecutorService periodicExecutor) {
        this.metadataCache = metadataCache;
        this.zooKeeperClient = zooKeeperClient;
        this.clock = clock;
        this.periodicExecutor = periodicExecutor;
        this.periodicInterval = conf.get(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL).toMillis();
    }

    public void start() {
        checkNotClosed();
        periodicExecutor.scheduleWithFixedDelay(
                this::doAutoPartition, periodicInterval, periodicInterval, TimeUnit.MILLISECONDS);
        LOG.info("Auto partitioning task is scheduled at fixed interval {}ms.", periodicInterval);
    }

    public void initPartitionTables(List<TableInfo> tableInfos) {
        tableInfos.forEach(this::addPartitionTable);
    }

    public void addPartitionTable(TableInfo tableInfo) {
        checkNotClosed();
        long tableId = tableInfo.getTableId();
        inLock(
                lock,
                () -> {
                    if (isAutoPartitionTable(tableInfo)) {
                        autoPartitionTables.put(tableId, tableInfo);
                    }

                    Set<String> partitionSet = getOrCreatePartitionSet(tableInfo);
                    checkNotNull(partitionSet, "Partition set is null.");
                    try {
                        partitionSet.addAll(
                                zooKeeperClient.getPartitions(tableInfo.getTablePath()));
                    } catch (Exception e) {
                        LOG.error(
                                "Fail to get partitions from zookeeper for table {}.",
                                tableInfo.getTablePath(),
                                e);
                    }
                });

        if (isAutoPartitionTable(tableInfo)) {
            // schedule auto partition for this table immediately
            periodicExecutor.schedule(() -> doAutoPartition(tableId), 0, TimeUnit.MILLISECONDS);
        }
    }

    public void removePartitionTable(long tableId, boolean isAutoPartitionTable) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    if (isAutoPartitionTable) {
                        autoPartitionTables.remove(tableId);
                        partitionsForAutoPartitionTable.remove(tableId);
                    } else {
                        partitionsForStaticPartitionTable.remove(tableId);
                    }
                });
    }

    public void addPartition(
            TableInfo tableInfo, PartitionSpec partitionSpec, boolean ignoreIfExists) {
        checkNotClosed();

        long tableId = tableInfo.getTableId();
        String partitionName = getPartitionName(tableInfo, partitionSpec);
        inLock(
                lock,
                () -> {
                    Set<String> partitionSet = getOrCreatePartitionSet(tableInfo);
                    if (partitionSet.contains(partitionName)) {
                        if (ignoreIfExists) {
                            return;
                        }
                        throw new PartitionAlreadyExistsException(
                                "Partition '"
                                        + partitionName
                                        + "' already exists for table "
                                        + tableInfo.getTablePath());
                    }

                    registerPartitionToZk(
                            tableInfo.getTablePath(), tableId, tableInfo, partitionName);
                    partitionSet.add(partitionName);
                });
    }

    public void dropPartition(
            TableInfo tableInfo, PartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        checkNotClosed();

        String partitionName = getPartitionName(tableInfo, partitionSpec);
        inLock(
                lock,
                () -> {
                    Set<String> partitionSet = getOrCreatePartitionSet(tableInfo);
                    if (!partitionSet.contains(partitionName)) {
                        if (ignoreIfNotExists) {
                            return;
                        }
                        throw new PartitionNotExistException(
                                "Partition '"
                                        + partitionName
                                        + "' does not exist for table "
                                        + tableInfo.getTablePath());
                    }
                    try {
                        zooKeeperClient.deletePartition(tableInfo.getTablePath(), partitionName);
                    } catch (Exception e) {
                        LOG.error(
                                "Fail to delete partition '{}' from zookeeper for table {}.",
                                partitionName,
                                tableInfo.getTablePath(),
                                e);
                    }
                    partitionSet.remove(partitionName);
                });
    }

    private void doAutoPartition() {
        Instant now = clock.instant();
        LOG.info("Start auto partitioning for all tables at {}.", now);
        inLock(lock, () -> doAutoPartition(now, autoPartitionTables.keySet()));
    }

    private void doAutoPartition(long tableId) {
        Instant now = clock.instant();
        LOG.info("Start auto partitioning for table {} at {}.", tableId, now);
        inLock(lock, () -> doAutoPartition(now, Collections.singleton(tableId)));
    }

    private boolean isAutoPartitionTable(TableInfo tableInfo) {
        return tableInfo.isAutoPartitioned();
    }

    private Set<String> getOrCreatePartitionSet(TableInfo tableInfo) {
        if (isAutoPartitionTable(tableInfo)) {
            return partitionsForAutoPartitionTable.computeIfAbsent(
                    tableInfo.getTableId(), k -> new TreeSet<>());
        } else {
            return partitionsForStaticPartitionTable.computeIfAbsent(
                    tableInfo.getTableId(), k -> new HashSet<>());
        }
    }

    private String getPartitionName(TableInfo tableInfo, PartitionSpec partitionSpec) {
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        Map<String, String> partitionSpecMap = partitionSpec.getPartitionSpec();
        if (partitionKeys.size() != partitionSpecMap.size()) {
            throw new PartitionSpecInvalidException(
                    String.format(
                            "Partition spec size is not equal to partition keys size for partitioned table %s.",
                            tableInfo.getTablePath()));
        }
        List<String> reOrderedPartitionValue = new ArrayList<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            if (!partitionSpecMap.containsKey(partitionKey)) {
                throw new PartitionSpecInvalidException(
                        String.format(
                                "Partition spec does not contain partition key '"
                                        + partitionKey
                                        + "' for partitioned table %s.",
                                tableInfo.getTablePath()));
            } else {
                reOrderedPartitionValue.add(partitionSpecMap.get(partitionKey));
            }
        }
        return PartitionUtils.getPartitionName(partitionKeys, reOrderedPartitionValue);
    }

    private void registerPartitionToZk(
            TablePath tablePath, long tableId, TableInfo tableInfo, String partitionName) {
        try {
            long partitionId = zooKeeperClient.getPartitionIdAndIncrement();
            // register partition assignments to zk first
            registerPartitionAssignment(tableId, partitionId, tableInfo);
            // then register the partition metadata to zk
            zooKeeperClient.registerPartition(tablePath, tableId, partitionName, partitionId);
            LOG.info(
                    "Register partition {} to zookeeper for table [{}].", partitionName, tablePath);
        } catch (Exception e) {
            LOG.error(
                    "Register partition to zookeeper failed to create partition {} for table [{}]",
                    partitionName,
                    tablePath,
                    e);
        }
    }

    private void registerPartitionAssignment(long tableId, long partitionId, TableInfo tableInfo)
            throws Exception {
        int replicaFactor = tableInfo.getTableConfig().getReplicationFactor();
        int[] servers = metadataCache.getLiveServerIds();
        // bucket count must exist for table has been created
        int bucketCount = tableInfo.getNumBuckets();
        Map<Integer, BucketAssignment> bucketAssignments =
                TableAssignmentUtils.generateAssignment(bucketCount, replicaFactor, servers)
                        .getBucketAssignments();
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, bucketAssignments);
        // register table assignment
        zooKeeperClient.registerPartitionAssignment(partitionId, partitionAssignment);
    }

    private void doAutoPartition(Instant now, Set<Long> tableIds) {
        for (Long tableId : tableIds) {
            TreeSet<String> currentPartitions =
                    partitionsForAutoPartitionTable.computeIfAbsent(tableId, k -> new TreeSet<>());
            TableInfo tableInfo = autoPartitionTables.get(tableId);
            autoDropPartitions(
                    tableInfo.getPartitionKeys(),
                    tableInfo.getTablePath(),
                    now,
                    tableInfo.getTableConfig().getAutoPartitionStrategy(),
                    currentPartitions);
            autoCreatePartitions(tableInfo, now, currentPartitions);
        }
    }

    private void autoCreatePartitions(
            TableInfo tableInfo, Instant currentInstant, TreeSet<String> currentPartitions) {
        // get the partitions needed to create
        List<String> partitionsToPreCreate =
                autoPartitionNamesToPreCreate(
                        tableInfo.getPartitionKeys(),
                        currentInstant,
                        tableInfo.getTableConfig().getAutoPartitionStrategy(),
                        currentPartitions);
        if (partitionsToPreCreate.isEmpty()) {
            return;
        }

        TablePath tablePath = tableInfo.getTablePath();
        for (String partitionName : partitionsToPreCreate) {
            long tableId = tableInfo.getTableId();
            try {
                registerPartitionToZk(tablePath, tableId, tableInfo, partitionName);
                currentPartitions.add(partitionName);
                LOG.info(
                        "Auto partitioning created partition {} for table [{}].",
                        partitionName,
                        tablePath);
            } catch (Exception e) {
                LOG.error(
                        "Auto partitioning failed to create partition {} for table [{}]",
                        partitionName,
                        tablePath,
                        e);
            }
        }
    }

    private List<String> autoPartitionNamesToPreCreate(
            List<String> partitionKeys,
            Instant currentInstant,
            AutoPartitionStrategy autoPartitionStrategy,
            TreeSet<String> currentPartitions) {
        AutoPartitionTimeUnit autoPartitionTimeUnit = autoPartitionStrategy.timeUnit();
        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(
                        currentInstant, autoPartitionStrategy.timeZone().toZoneId());

        int partitionToPreCreate = autoPartitionStrategy.numPreCreate();
        List<String> partitionsToCreate = new ArrayList<>();
        for (int idx = 0; idx < partitionToPreCreate; idx++) {
            String partition =
                    generateAutoPartitionName(
                            partitionKeys, currentZonedDateTime, idx, autoPartitionTimeUnit);
            // if the partition already exists, we don't need to create it,
            // otherwise, create it
            if (!currentPartitions.contains(partition)) {
                partitionsToCreate.add(partition);
            }
        }
        return partitionsToCreate;
    }

    private void autoDropPartitions(
            List<String> partitionKeys,
            TablePath tablePath,
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
        String lastRetainPartitionName =
                generateAutoPartitionName(
                        partitionKeys,
                        currentZonedDateTime,
                        -numToRetain,
                        autoPartitionStrategy.timeUnit());

        Iterator<String> partitionsToExpire =
                currentPartitions.headSet(lastRetainPartitionName, false).iterator();

        while (partitionsToExpire.hasNext()) {
            String partitionName = partitionsToExpire.next();
            // drop the partition
            try {
                zooKeeperClient.deletePartition(tablePath, partitionName);
                // only remove when zk success, this reflects to the partitionsByTable
                partitionsToExpire.remove();
                LOG.info(
                        "Auto partitioning deleted partition {} for table [{}].",
                        partitionName,
                        tablePath);
            } catch (Exception e) {
                LOG.error(
                        "Auto partitioning failed to delete partition {} for table [{}]",
                        partitionName,
                        tablePath,
                        e);
            }
        }
    }

    private void checkNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("PartitionManager is already closed.");
        }
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            periodicExecutor.shutdownNow();
        }
    }
}
