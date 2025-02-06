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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.MetadataCache;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.utils.TableAssignmentUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.utils.AutoPartitionStrategy;
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

import static com.alibaba.fluss.utils.AutoPartitionUtils.getPartitionString;
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

    private final ZooKeeperClient zooKeeperClient;
    private final MetadataCache metadataCache;
    private final Clock clock;

    private final long periodicInterval;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    @GuardedBy("lock")
    private final Map<Long, TableInfo> autoPartitionTables = new HashMap<>();

    @GuardedBy("lock")
    private final Map<Long, TreeSet<String>> partitionsByTable = new HashMap<>();

    private final Lock lock = new ReentrantLock();

    public AutoPartitionManager(
            MetadataCache metadataCache, ZooKeeperClient zooKeeperClient, Configuration conf) {
        this(
                metadataCache,
                zooKeeperClient,
                conf,
                SystemClock.getInstance(),
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("periodic-auto-partition-manager")));
    }

    @VisibleForTesting
    AutoPartitionManager(
            MetadataCache metadataCache,
            ZooKeeperClient zooKeeperClient,
            Configuration conf,
            Clock clock,
            ScheduledExecutorService periodicExecutor) {
        this.metadataCache = metadataCache;
        this.zooKeeperClient = zooKeeperClient;
        this.clock = clock;
        this.periodicExecutor = periodicExecutor;
        this.periodicInterval = conf.get(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL).toMillis();
    }

    public void initAutoPartitionTables(Map<Long, TableInfo> tableInfos) {
        inLock(lock, () -> autoPartitionTables.putAll(tableInfos));
    }

    public void addAutoPartitionTable(TableInfo tableInfo) {
        checkNotClosed();
        long tableId = tableInfo.getTableId();
        inLock(lock, () -> autoPartitionTables.put(tableId, tableInfo));
        // schedule auto partition for this table immediately
        periodicExecutor.schedule(() -> doAutoPartition(tableId), 0, TimeUnit.MILLISECONDS);
    }

    public void removeAutoPartitionTable(long tableId) {
        inLock(lock, () -> autoPartitionTables.remove(tableId));
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
        inLock(lock, () -> autoPartition(now, autoPartitionTables.keySet()));
    }

    private void doAutoPartition(long tableId) {
        Instant now = clock.instant();
        LOG.info("Start auto partitioning for table {} at {}.", tableId, now);
        inLock(lock, () -> autoPartition(now, Collections.singleton(tableId)));
    }

    @VisibleForTesting
    protected void autoPartition(Instant now, Set<Long> tableIds) {
        for (Long tableId : tableIds) {
            TreeSet<String> currentPartitions =
                    partitionsByTable.computeIfAbsent(tableId, k -> new TreeSet<>());
            TableInfo tableInfo = autoPartitionTables.get(tableId);
            dropPartitions(
                    tableInfo.getTablePath(),
                    now,
                    tableInfo.getTableConfig().getAutoPartitionStrategy(),
                    currentPartitions);
            createPartitions(tableInfo, now, currentPartitions);
        }
    }

    protected void createPartitions(
            TableInfo tableInfo, Instant currentInstant, TreeSet<String> currentPartitions) {
        // get the partitions needed to create
        List<String> partitionsToPreCreate =
                partitionNamesToPreCreate(
                        currentInstant,
                        tableInfo.getTableConfig().getAutoPartitionStrategy(),
                        currentPartitions);
        if (partitionsToPreCreate.isEmpty()) {
            return;
        }

        TablePath tablePath = tableInfo.getTablePath();
        for (String partitionName : partitionsToPreCreate) {
            try {
                long tableId = tableInfo.getTableId();
                long partitionId = zooKeeperClient.getPartitionIdAndIncrement();
                // register partition assignments to zk first
                registerPartitionAssignment(tableId, partitionId, tableInfo);
                // then register the partition metadata to zk
                zooKeeperClient.registerPartition(tablePath, tableId, partitionName, partitionId);
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

    private static List<String> partitionNamesToPreCreate(
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
            String partition = getPartitionString(currentZonedDateTime, idx, autoPartitionTimeUnit);
            // if the partition already exists, we don't need to create it,
            // otherwise, create it
            if (!currentPartitions.contains(partition)) {
                partitionsToCreate.add(partition);
            }
        }
        return partitionsToCreate;
    }

    private void dropPartitions(
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
                getPartitionString(
                        currentZonedDateTime, -numToRetain, autoPartitionStrategy.timeUnit());

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

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            periodicExecutor.shutdownNow();
        }
    }
}
