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
import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;
import com.alibaba.fluss.utils.types.Tuple2;

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

import static com.alibaba.fluss.utils.PartitionUtils.extractAutoSpecFromPartitionName;
import static com.alibaba.fluss.utils.PartitionUtils.generateAutoPartitionName;
import static com.alibaba.fluss.utils.PartitionUtils.generateAutoSpec;
import static com.alibaba.fluss.utils.PartitionUtils.getPartitionName;
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

    /**
     * A map from tableId to the set of auto spec for auto partition tables. autoSpec is the field
     * for auto partitioning. If the partition keys only contain one field. the autoSpec is this
     * field. If the partition keys contain multiple fields, the autoSpec is the last field.
     *
     * <p>For example, if the partition keys are ["ds"], the autoSpec is "ds". If the partition keys
     * are ["country", "city", "ds"], the autoSpec is "ds".
     */
    @GuardedBy("lock")
    private final Map<Long, TreeSet<String>> autoSpecForAutoPartitionTable = new HashMap<>();

    /**
     * A map from 'tableId-autoSpec' to the set of partitions for auto partition table. This is used
     * to trace the created partitions for auto partition table.
     */
    @GuardedBy("lock")
    private final Map<String, Set<String>> partitionsForAutoPartitionTable = new HashMap<>();

    /**
     * A map from tableId to the set of autoPartitionNamePrefix for auto partition table.
     * autoPartitionNamePrefix is the remaining parts exclude autoSpec.
     *
     * <p>For example, if one auto partition table with tableId '1' and partition keys are
     * ["country", "city", "ds"], the autoPartitionNamePrefix is in format "country$city". If
     * partitionSpecs are [country="China", city="Beijing", ds="20230101"] and [country="China",
     * city="Shanghai", ds="20230101"], the value is 0 -> ["China$beijing", "China$Shanghai"].
     *
     * <p>This is used to create auto partitions when this manager generates one new autoSpec. The
     * generated partition name is in format "autoPartitionNamePrefix$autoSpec".
     */
    @GuardedBy("lock")
    private final Map<Long, Set<String>> autoPartitionNamePrefixMap = new HashMap<>();

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
        boolean isAuto = isAuto(tableInfo);
        inLock(
                lock,
                () -> {
                    Set<String> existsPartitions = getPartitionsFromZk(tableInfo.getTablePath());
                    addPartitionsToLocal(tableInfo, existsPartitions);

                    if (isAuto) {
                        // add auto partition name prefix to autoPartitionNamePrefixSet.
                        Set<String> namePrefixes =
                                autoPartitionNamePrefixMap.computeIfAbsent(
                                        tableId, k -> new HashSet<>());
                        namePrefixes.addAll(
                                tableInfo
                                        .getTableConfig()
                                        .getAutoPartitionStrategy()
                                        .partitionNamePrefixSet());
                    }
                });

        if (isAuto) {
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

                        Set<String> autoSpecSet = autoSpecForAutoPartitionTable.get(tableId);
                        for (String autoSpec : autoSpecSet) {
                            partitionsForAutoPartitionTable.remove(tableId + "-" + autoSpec);
                        }
                        autoSpecForAutoPartitionTable.remove(tableId);

                        autoPartitionNamePrefixMap.remove(tableId);
                    } else {
                        partitionsForStaticPartitionTable.remove(tableId);
                    }
                });
    }

    public void addPartition(
            TableInfo tableInfo, PartitionSpec partitionSpec, boolean ignoreIfExists) {
        checkNotClosed();
        Tuple2<Boolean, String> tuple2 =
                checkPartitionSpec(tableInfo, partitionSpec, tableInfo.isAutoPartitioned());
        boolean isPartitionNamePrefix = tuple2.f0;
        if (isPartitionNamePrefix) {
            addAutoPartitionNamePrefix(tableInfo, tuple2.f1);
        } else {
            addPartition(tableInfo, tuple2.f1, ignoreIfExists);
        }
    }

    public void dropPartition(
            TableInfo tableInfo, PartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        checkNotClosed();
        Tuple2<Boolean, String> tuple2 =
                checkPartitionSpec(tableInfo, partitionSpec, tableInfo.isAutoPartitioned());
        boolean isPartitionNamePrefix = tuple2.f0;
        if (isPartitionNamePrefix) {
            dropAutoPartitionNamePrefix(tableInfo, tuple2.f1);
        } else {
            dropPartition(tableInfo, tuple2.f1, ignoreIfNotExists);
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

    private void addPartition(TableInfo tableInfo, String partitionName, boolean ignoreIfExists) {
        long tableId = tableInfo.getTableId();
        inLock(
                lock,
                () -> {
                    if (containsPartition(
                            tableInfo.getTableId(), isAuto(tableInfo), partitionName)) {
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

                    addPartitionsToLocal(tableInfo, Collections.singleton(partitionName));
                });
    }

    private void dropPartition(
            TableInfo tableInfo, String partitionName, boolean ignoreIfNotExists) {
        inLock(
                lock,
                () -> {
                    if (!containsPartition(
                            tableInfo.getTableId(), isAuto(tableInfo), partitionName)) {
                        if (ignoreIfNotExists) {
                            return;
                        }
                        throw new PartitionNotExistException(
                                "Partition '"
                                        + partitionName
                                        + "' does not exist for table "
                                        + tableInfo.getTablePath());
                    }
                    deletePartitionsFromZk(
                            tableInfo.getTablePath(), Collections.singletonList(partitionName));
                    removePartitionFromLocal(tableInfo, partitionName);
                });
    }

    private void addAutoPartitionNamePrefix(TableInfo tableInfo, String autoPartitionNamePrefix) {
        checkNotClosed();
        long tableId = tableInfo.getTableId();
        inLock(
                lock,
                () -> {
                    Set<String> autoPartitionNamePrefixSet =
                            autoPartitionNamePrefixMap.computeIfAbsent(
                                    tableId, k -> new HashSet<>());
                    if (!autoPartitionNamePrefixSet.contains(autoPartitionNamePrefix)) {
                        autoPartitionNamePrefixSet.add(autoPartitionNamePrefix);

                        // TODO, here we need to alter table config
                        // 'TABLE_AUTO_PARTITION_PARTITION_NAME_PREFIX' while we add new
                        // autoPartitionNamePrefix. See: https://github.com/alibaba/fluss/issues/380

                        // For these existing auto spec, we need to create partitions.
                        for (String autoSpec : autoSpecForAutoPartitionTable.get(tableId)) {
                            addPartition(
                                    tableInfo,
                                    generateAutoPartitionName(autoPartitionNamePrefix, autoSpec),
                                    true);
                        }
                    }
                });
    }

    private void dropAutoPartitionNamePrefix(TableInfo tableInfo, String autoPartitionNamePrefix) {
        checkNotClosed();
        long tableId = tableInfo.getTableId();
        inLock(
                lock,
                () -> {
                    Set<String> autoPartitionNamePrefixSet =
                            autoPartitionNamePrefixMap.computeIfAbsent(
                                    tableId, k -> new HashSet<>());
                    if (autoPartitionNamePrefixSet.contains(autoPartitionNamePrefix)) {
                        autoPartitionNamePrefixSet.remove(autoPartitionNamePrefix);

                        // TODO, here we need to alter table config
                        // 'TABLE_AUTO_PARTITION_PARTITION_NAME_PREFIX' while we drop an exists
                        // autoPartitionNamePrefix. See: https://github.com/alibaba/fluss/issues/380

                        // For these existing auto spec, we need to drop these partitions.
                        for (String autoSpec : autoSpecForAutoPartitionTable.get(tableId)) {
                            dropPartition(
                                    tableInfo,
                                    generateAutoPartitionName(autoPartitionNamePrefix, autoSpec),
                                    true);
                        }
                    }
                });
    }

    /**
     * Check if the table is auto partitioned.
     *
     * @param tableInfo the table info
     * @return true if the table is auto partitioned, false otherwise
     */
    private boolean isAuto(TableInfo tableInfo) {
        return tableInfo.isAutoPartitioned();
    }

    private boolean containsPartition(long tableId, boolean isAuto, String partitionName) {
        if (isAuto) {
            String specKey = tableId + "-" + extractAutoSpecFromPartitionName(partitionName);
            if (partitionsForAutoPartitionTable.containsKey(specKey)) {
                return partitionsForAutoPartitionTable.get(specKey).contains(partitionName);
            } else {
                return false;
            }
        } else {
            Set<String> partitionSets =
                    partitionsForStaticPartitionTable.computeIfAbsent(
                            tableId, k -> new HashSet<>());
            return partitionSets.contains(partitionName);
        }
    }

    /**
     * Add the partition names to local cache for static partition tables or auto partition tables.
     */
    private void addPartitionsToLocal(TableInfo tableInfo, Set<String> partitionNames) {
        long tableId = tableInfo.getTableId();
        if (isAuto(tableInfo)) {
            autoPartitionTables.put(tableId, tableInfo);
            Set<String> autoSpecSet =
                    autoSpecForAutoPartitionTable.computeIfAbsent(
                            tableInfo.getTableId(), k -> new TreeSet<>());
            for (String partitionName : partitionNames) {
                // 1. First add the auto spec to autoSpecForAutoPartitionTable.
                String autoSpec = extractAutoSpecFromPartitionName(partitionName);
                autoSpecSet.add(autoSpec);

                // 2. Second add the partition to partitionsForAutoSpec.
                Set<String> partitionSet =
                        partitionsForAutoPartitionTable.computeIfAbsent(
                                tableId + "-" + autoSpec, k -> new HashSet<>());
                partitionSet.add(partitionName);
            }
        } else {
            Set<String> partitionSet =
                    partitionsForStaticPartitionTable.computeIfAbsent(
                            tableInfo.getTableId(), k -> new HashSet<>());
            partitionSet.addAll(partitionNames);
        }
    }

    private void removePartitionFromLocal(TableInfo tableInfo, String partitionName) {
        long tableId = tableInfo.getTableId();
        if (isAuto(tableInfo)) {
            String specKey = tableId + "-" + extractAutoSpecFromPartitionName(partitionName);
            partitionsForAutoPartitionTable.get(specKey).remove(partitionName);
        } else {
            Set<String> partitionSet =
                    partitionsForStaticPartitionTable.computeIfAbsent(
                            tableInfo.getTableId(), k -> new HashSet<>());
            partitionSet.remove(partitionName);
        }
    }

    private Tuple2<Boolean, String> checkPartitionSpec(
            TableInfo tableInfo, PartitionSpec partitionSpec, boolean isAuto) {
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        Map<String, String> partitionSpecMap = partitionSpec.getPartitionSpec();

        // check spec size.
        boolean isPartitionNamePrefix = false;
        if (partitionKeys.size() != partitionSpecMap.size()) {
            if (!isAuto) {
                throw new PartitionSpecInvalidException(
                        String.format(
                                "Partition spec size is not equal to partition keys size for static partitioned table %s.",
                                tableInfo.getTablePath()));
            } else {
                if (partitionKeys.size() - 1 == partitionSpecMap.size()) {
                    isPartitionNamePrefix = true;
                } else {
                    throw new PartitionSpecInvalidException(
                            String.format(
                                    "Partition spec is illegal for auto partitioned table %s. We only support partition "
                                            + "spec matching as a prefix of the partition key or matching the "
                                            + "partition key itself.",
                                    tableInfo.getTablePath()));
                }
            }
        }

        List<String> reOrderedPartitionSpec = new ArrayList<>(partitionSpecMap.size());
        List<String> keyList;
        if (isPartitionNamePrefix) {
            keyList = partitionKeys.subList(0, partitionKeys.size() - 1);
        } else {
            keyList = partitionKeys;
        }
        for (String partitionKey : keyList) {
            if (!partitionSpecMap.containsKey(partitionKey)) {
                throw new PartitionSpecInvalidException(
                        String.format(
                                "Partition spec does not contain partition key '"
                                        + partitionKey
                                        + "' for partitioned table %s.",
                                tableInfo.getTablePath()));
            } else {
                reOrderedPartitionSpec.add(partitionSpecMap.get(partitionKey));
            }
        }
        return Tuple2.of(isPartitionNamePrefix, getPartitionName(reOrderedPartitionSpec));
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

    private void doAutoPartition(Instant now, Set<Long> tableIds) {
        for (Long tableId : tableIds) {
            TableInfo tableInfo = autoPartitionTables.get(tableId);
            TreeSet<String> autoSpecs =
                    autoSpecForAutoPartitionTable.computeIfAbsent(tableId, k -> new TreeSet<>());
            autoDropPartitions(
                    tableInfo.getPartitionKeys(),
                    tableInfo.getTableId(),
                    tableInfo.getTablePath(),
                    now,
                    tableInfo.getTableConfig().getAutoPartitionStrategy(),
                    autoSpecs);
            autoCreatePartitions(tableInfo, now, autoSpecs);
        }
    }

    private void autoCreatePartitions(
            TableInfo tableInfo, Instant currentInstant, TreeSet<String> currentAutoSpecs) {
        // get the auto specs needed to create
        List<String> autoSpecToPreCreate =
                autoSpecsToPreCreate(
                        currentInstant,
                        tableInfo.getTableConfig().getAutoPartitionStrategy(),
                        currentAutoSpecs);
        if (autoSpecToPreCreate.isEmpty()) {
            return;
        }

        long tableId = tableInfo.getTableId();
        List<String> partitionKeys = tableInfo.getPartitionKeys();

        TablePath tablePath = tableInfo.getTablePath();
        List<String> partitionNames = new ArrayList<>();
        Set<String> partitionNamePrefixSet = autoPartitionNamePrefixMap.get(tableId);
        for (String autoSpec : autoSpecToPreCreate) {
            Set<String> partitionSets =
                    partitionsForAutoPartitionTable.computeIfAbsent(
                            tableId + "-" + autoSpec, k -> new HashSet<>());
            if (partitionKeys.size() > 1) {
                for (String partitionNamePrefix : partitionNamePrefixSet) {
                    String partitionName = generateAutoPartitionName(partitionNamePrefix, autoSpec);
                    registerPartitionToZk(tablePath, tableId, tableInfo, partitionName);
                    partitionSets.add(partitionName);
                    partitionNames.add(partitionName);
                }
            } else {
                registerPartitionToZk(tablePath, tableId, tableInfo, autoSpec);
                partitionSets.add(autoSpec);
                partitionNames.add(autoSpec);
            }
            currentAutoSpecs.add(autoSpec);
        }
        LOG.info(
                "Auto partitioning created partition {} for table [{}].",
                partitionNames,
                tablePath);
    }

    private List<String> autoSpecsToPreCreate(
            Instant currentInstant,
            AutoPartitionStrategy autoPartitionStrategy,
            TreeSet<String> currentAutoSpecs) {
        AutoPartitionTimeUnit autoPartitionTimeUnit = autoPartitionStrategy.timeUnit();
        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(
                        currentInstant, autoPartitionStrategy.timeZone().toZoneId());

        int autoSpecToPreCreate = autoPartitionStrategy.numPreCreate();
        List<String> autoSpecsToCreate = new ArrayList<>();
        for (int idx = 0; idx < autoSpecToPreCreate; idx++) {
            String autoSpec = generateAutoSpec(currentZonedDateTime, idx, autoPartitionTimeUnit);
            // if the autoSpec already exists, we don't need to create it,
            // otherwise, create it
            if (!currentAutoSpecs.contains(autoSpec)) {
                autoSpecsToCreate.add(autoSpec);
            }
        }
        return autoSpecsToCreate;
    }

    private void autoDropPartitions(
            List<String> partitionKeys,
            long tableId,
            TablePath tablePath,
            Instant currentInstant,
            AutoPartitionStrategy autoPartitionStrategy,
            NavigableSet<String> currentAutoSpecs) {
        int numToRetain = autoPartitionStrategy.numToRetain();
        // negative value means not to drop partitions
        if (numToRetain < 0) {
            return;
        }

        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(
                        currentInstant, autoPartitionStrategy.timeZone().toZoneId());

        // get the earliest one auto spec that need to retain
        String lastRetainAutoSpec =
                generateAutoSpec(
                        currentZonedDateTime, -numToRetain, autoPartitionStrategy.timeUnit());

        Iterator<String> autoSpecsToExpire =
                currentAutoSpecs.headSet(lastRetainAutoSpec, false).iterator();

        while (autoSpecsToExpire.hasNext()) {
            String autoSpec = autoSpecsToExpire.next();
            List<String> partitionNames = new ArrayList<>();
            if (partitionKeys.size() > 1) {
                partitionNames.addAll(
                        partitionsForAutoPartitionTable.get(tableId + "-" + autoSpec));
            } else {
                partitionNames.add(autoSpec);
            }

            deletePartitionsFromZk(tablePath, partitionNames);
            autoSpecsToExpire.remove();
            partitionsForAutoPartitionTable.remove(tableId + "-" + autoSpec);
            LOG.info(
                    "Auto partitioning deleted partition {} for table [{}].",
                    partitionNames,
                    tablePath);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Zk related
    // ------------------------------------------------------------------------------------------

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

    private Set<String> getPartitionsFromZk(TablePath tablePath) {
        Set<String> existsPartitions = new HashSet<>();
        try {
            existsPartitions.addAll(zooKeeperClient.getPartitions(tablePath));
        } catch (Exception e) {
            LOG.error("Fail to get partitions from zookeeper for table {}.", tablePath, e);
        }
        return existsPartitions;
    }

    private void deletePartitionsFromZk(TablePath tablePath, List<String> partitionNames) {
        for (String partitionName : partitionNames) {
            try {
                zooKeeperClient.deletePartition(tablePath, partitionName);
            } catch (Exception e) {
                LOG.error(
                        "Fail to delete partition '{}' from zookeeper for table {}.",
                        partitionName,
                        tablePath,
                        e);
            }
        }
    }
}
