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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.KvSnapshotLeaseForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLease;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadataManager;
import org.apache.fluss.server.zk.data.lease.KvSnapshotTableLease;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** A manager to manage kv snapshot lease acquire, renew, release and drop. */
@ThreadSafe
public class KvSnapshotLeaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(KvSnapshotLeaseManager.class);

    private final KvSnapshotLeaseMetadataManager metadataManager;
    private final CoordinatorContext coordinatorContext;
    private final Clock clock;
    private final ScheduledExecutorService scheduledExecutor;
    private final Configuration conf;

    private final Map<String, ReadWriteLock> leaseLocks = MapUtils.newConcurrentHashMap();
    /** lease id to kv snapshot lease. */
    @GuardedBy("leaseLocks")
    private final Map<String, KvSnapshotLease> kvSnapshotLeaseMap;

    private final ReadWriteLock refCountLock = new ReentrantReadWriteLock();

    /**
     * KvSnapshotLeaseForBucket to the ref count, which means this table bucket + snapshotId has
     * been leased by how many lease id.
     */
    private final Map<KvSnapshotLeaseForBucket, AtomicInteger> refCount =
            MapUtils.newConcurrentHashMap();

    /** For metrics. */
    private final AtomicInteger leasedBucketCount = new AtomicInteger(0);

    public KvSnapshotLeaseManager(
            Configuration conf,
            KvSnapshotLeaseMetadataManager metadataManager,
            CoordinatorContext coordinatorContext,
            Clock clock,
            CoordinatorMetricGroup coordinatorMetricGroup) {
        this(
                conf,
                metadataManager,
                coordinatorContext,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("kv-snapshot-lease-cleaner")),
                clock,
                coordinatorMetricGroup);
    }

    @VisibleForTesting
    public KvSnapshotLeaseManager(
            Configuration conf,
            KvSnapshotLeaseMetadataManager metadataManager,
            CoordinatorContext coordinatorContext,
            ScheduledExecutorService scheduledExecutor,
            Clock clock,
            CoordinatorMetricGroup coordinatorMetricGroup) {
        this.metadataManager = metadataManager;
        this.conf = conf;
        this.scheduledExecutor = scheduledExecutor;
        this.coordinatorContext = coordinatorContext;
        this.clock = clock;
        this.kvSnapshotLeaseMap = MapUtils.newConcurrentHashMap();

        registerMetrics(coordinatorMetricGroup);
    }

    public void start() {
        LOG.info("kv snapshot lease manager has been started.");
        scheduledExecutor.scheduleWithFixedDelay(
                this::expireLeases,
                0L,
                conf.get(ConfigOptions.KV_SNAPSHOT_LEASE_EXPIRATION_CHECK_INTERVAL).toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void initialize() throws Exception {
        for (String leaseId : metadataManager.getLeasesList()) {
            Optional<KvSnapshotLease> kvSnapshotLeaseOpt = metadataManager.getLease(leaseId);
            if (kvSnapshotLeaseOpt.isPresent()) {
                KvSnapshotLease kvSnapshotLease = kvSnapshotLeaseOpt.get();
                this.leaseLocks.put(leaseId, new ReentrantReadWriteLock());
                this.kvSnapshotLeaseMap.put(leaseId, kvSnapshotLease);

                initializeRefCount(kvSnapshotLease);

                leasedBucketCount.addAndGet(kvSnapshotLease.getLeasedSnapshotCount());
            }
        }
    }

    public boolean snapshotLeaseNotExist(KvSnapshotLeaseForBucket kvSnapshotLeaseForBucket) {
        return inReadLock(
                refCountLock,
                () -> {
                    AtomicInteger count = refCount.get(kvSnapshotLeaseForBucket);
                    return count == null || count.get() <= 0;
                });
    }

    /**
     * Acquire kv snapshot lease.
     *
     * @param leaseId the lease id
     * @param leaseDuration the lease duration
     * @param tableIdToLeaseBucket the table id to lease bucket
     * @return the map of unavailable snapshots that failed to be leased
     */
    public Map<TableBucket, Long> acquireLease(
            String leaseId,
            long leaseDuration,
            Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToLeaseBucket)
            throws Exception {
        ReadWriteLock lock = leaseLocks.computeIfAbsent(leaseId, k -> new ReentrantReadWriteLock());
        return inWriteLock(
                lock,
                () -> {
                    // To record the unavailable snapshots such as the kv snapshotId to lease not
                    // exists.
                    Map<TableBucket, Long> unavailableSnapshots = new HashMap<>();

                    boolean update = kvSnapshotLeaseMap.containsKey(leaseId);
                    // set the expiration time as: current time + leaseDuration
                    long newExpirationTime = clock.milliseconds() + leaseDuration;
                    KvSnapshotLease kvSnapshotLease =
                            kvSnapshotLeaseMap.compute(
                                    leaseId,
                                    (key, existingLease) -> {
                                        if (existingLease == null) {
                                            LOG.info(
                                                    "kv snapshot lease '{}' has been acquired. The lease expiration "
                                                            + "time is {}",
                                                    leaseId,
                                                    newExpirationTime);
                                            return new KvSnapshotLease(newExpirationTime);
                                        } else {
                                            existingLease.setExpirationTime(newExpirationTime);
                                            return existingLease;
                                        }
                                    });

                    for (Map.Entry<Long, List<KvSnapshotLeaseForBucket>> entry :
                            tableIdToLeaseBucket.entrySet()) {
                        Long tableId = entry.getKey();
                        TableInfo tableInfo = coordinatorContext.getTableInfoById(tableId);
                        int numBuckets = tableInfo.getNumBuckets();
                        List<KvSnapshotLeaseForBucket> buckets = entry.getValue();
                        for (KvSnapshotLeaseForBucket bucket : buckets) {

                            TableBucket tableBucket = bucket.getTableBucket();
                            long kvSnapshotId = bucket.getKvSnapshotId();
                            try {
                                boolean snapshotExists =
                                        metadataManager.isSnapshotExists(tableBucket, kvSnapshotId);
                                if (!snapshotExists) {
                                    unavailableSnapshots.put(tableBucket, kvSnapshotId);
                                    continue;
                                }
                            } catch (Exception e) {
                                LOG.error(
                                        "Failed to check snapshotExists for tableBucket when acquire kv "
                                                + "snapshot kvSnapshotLease {}.",
                                        tableBucket,
                                        e);
                                unavailableSnapshots.put(tableBucket, kvSnapshotId);
                                continue;
                            }

                            long originalSnapshotId =
                                    kvSnapshotLease.acquireBucket(
                                            tableBucket, kvSnapshotId, numBuckets);
                            if (originalSnapshotId == -1L) {
                                leasedBucketCount.incrementAndGet();
                            } else {
                                // clear the original ref.
                                decrementRefCount(
                                        new KvSnapshotLeaseForBucket(
                                                tableBucket, originalSnapshotId));
                            }
                            incrementRefCount(bucket);
                        }
                    }

                    if (update) {
                        metadataManager.updateLease(leaseId, kvSnapshotLease);
                    } else {
                        metadataManager.registerLease(leaseId, kvSnapshotLease);
                    }

                    return unavailableSnapshots;
                });
    }

    public void release(String leaseId, Map<Long, List<TableBucket>> tableIdToUnregisterBucket)
            throws Exception {
        ReadWriteLock lock = leaseLocks.get(leaseId);
        if (lock == null) {
            return;
        }

        inWriteLock(
                lock,
                () -> {
                    KvSnapshotLease lease = kvSnapshotLeaseMap.get(leaseId);
                    if (lease == null) {
                        return;
                    }

                    for (Map.Entry<Long, List<TableBucket>> entry :
                            tableIdToUnregisterBucket.entrySet()) {
                        List<TableBucket> buckets = entry.getValue();
                        for (TableBucket bucket : buckets) {
                            long snapshotId = lease.releaseBucket(bucket);
                            if (snapshotId != -1L) {
                                leasedBucketCount.decrementAndGet();
                                decrementRefCount(new KvSnapshotLeaseForBucket(bucket, snapshotId));
                            }
                        }
                    }

                    if (lease.isEmpty()) {
                        releaseAll(leaseId);
                    } else {
                        metadataManager.updateLease(leaseId, lease);
                    }
                });
    }

    /**
     * Release kv snapshot lease.
     *
     * @param leaseId the lease id
     * @return true if clear success, false if lease not exist
     */
    public boolean releaseAll(String leaseId) throws Exception {
        ReadWriteLock lock = leaseLocks.get(leaseId);
        if (lock == null) {
            return false;
        }

        boolean exist =
                inWriteLock(
                        lock,
                        () -> {
                            KvSnapshotLease kvSnapshotLease = kvSnapshotLeaseMap.remove(leaseId);
                            if (kvSnapshotLease == null) {
                                return false;
                            }

                            clearRefCount(kvSnapshotLease);
                            metadataManager.deleteLease(leaseId);

                            LOG.info(
                                    "kv snapshots of lease '"
                                            + leaseId
                                            + "' has been all released.");
                            return true;
                        });

        leaseLocks.remove(leaseId);
        return exist;
    }

    private void initializeRefCount(KvSnapshotLease lease) {
        for (Map.Entry<Long, KvSnapshotTableLease> tableEntry :
                lease.getTableIdToTableLease().entrySet()) {
            long tableId = tableEntry.getKey();
            KvSnapshotTableLease tableLease = tableEntry.getValue();
            if (tableLease.getBucketSnapshots() != null) {
                Long[] snapshots = tableLease.getBucketSnapshots();
                for (int i = 0; i < snapshots.length; i++) {
                    if (snapshots[i] == -1L) {
                        continue;
                    }

                    incrementRefCount(
                            new KvSnapshotLeaseForBucket(
                                    new TableBucket(tableId, i), snapshots[i]));
                }
            } else {
                Map<Long, Long[]> partitionSnapshots = tableLease.getPartitionSnapshots();
                for (Map.Entry<Long, Long[]> entry : partitionSnapshots.entrySet()) {
                    Long partitionId = entry.getKey();
                    Long[] snapshots = entry.getValue();
                    for (int i = 0; i < snapshots.length; i++) {
                        if (snapshots[i] == -1L) {
                            continue;
                        }

                        incrementRefCount(
                                new KvSnapshotLeaseForBucket(
                                        new TableBucket(tableId, partitionId, i), snapshots[i]));
                    }
                }
            }
        }
    }

    private void clearRefCount(KvSnapshotLease lease) {
        for (Map.Entry<Long, KvSnapshotTableLease> tableEntry :
                lease.getTableIdToTableLease().entrySet()) {
            long tableId = tableEntry.getKey();
            KvSnapshotTableLease tableLease = tableEntry.getValue();
            if (tableLease.getBucketSnapshots() != null) {
                Long[] snapshots = tableLease.getBucketSnapshots();
                for (int i = 0; i < snapshots.length; i++) {
                    if (snapshots[i] == -1L) {
                        continue;
                    }
                    decrementRefCount(
                            new KvSnapshotLeaseForBucket(
                                    new TableBucket(tableId, i), snapshots[i]));
                    leasedBucketCount.decrementAndGet();
                }
            } else {
                Map<Long, Long[]> partitionSnapshots = tableLease.getPartitionSnapshots();
                for (Map.Entry<Long, Long[]> entry : partitionSnapshots.entrySet()) {
                    Long partitionId = entry.getKey();
                    Long[] snapshots = entry.getValue();
                    for (int i = 0; i < snapshots.length; i++) {
                        if (snapshots[i] == -1L) {
                            continue;
                        }

                        decrementRefCount(
                                new KvSnapshotLeaseForBucket(
                                        new TableBucket(tableId, partitionId, i), snapshots[i]));
                        leasedBucketCount.decrementAndGet();
                    }
                }
            }
        }
    }

    private void incrementRefCount(KvSnapshotLeaseForBucket kvSnapshotLeaseForBucket) {
        inWriteLock(
                refCountLock,
                () ->
                        refCount.computeIfAbsent(
                                        kvSnapshotLeaseForBucket, k -> new AtomicInteger(0))
                                .incrementAndGet());
    }

    private void decrementRefCount(KvSnapshotLeaseForBucket kvSnapshotLeaseForBucket) {
        inWriteLock(
                refCountLock,
                () -> {
                    AtomicInteger atomicInteger = refCount.get(kvSnapshotLeaseForBucket);
                    if (atomicInteger != null) {
                        int decrementAndGet = atomicInteger.decrementAndGet();
                        if (decrementAndGet <= 0) {
                            refCount.remove(kvSnapshotLeaseForBucket);
                        }
                    }
                });
    }

    private void expireLeases() {
        long currentTime = clock.milliseconds();
        // 1. First collect all expired lease IDs
        List<String> expiredLeaseIds =
                kvSnapshotLeaseMap.entrySet().stream()
                        .filter(entry -> entry.getValue().getExpirationTime() < currentTime)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

        // 2. Then process each collected ID
        expiredLeaseIds.forEach(
                leaseId -> {
                    try {
                        releaseAll(leaseId);
                    } catch (Exception e) {
                        LOG.error("Failed to clear kv snapshot lease {}", leaseId, e);
                    }
                });
    }

    private void registerMetrics(CoordinatorMetricGroup coordinatorMetricGroup) {
        coordinatorMetricGroup.gauge(MetricNames.KV_SNAPSHOT_LEASE_COUNT, this::getLeaseCount);
        // TODO register as table or bucket level.
        coordinatorMetricGroup.gauge(
                MetricNames.LEASED_KV_SNAPSHOT_COUNT, this::getLeasedBucketCount);
    }

    @VisibleForTesting
    int getLeaseCount() {
        return kvSnapshotLeaseMap.size();
    }

    @VisibleForTesting
    int getLeasedBucketCount() {
        return leasedBucketCount.get();
    }

    @VisibleForTesting
    int getRefCount(KvSnapshotLeaseForBucket kvSnapshotLeaseForBucket) {
        return inReadLock(
                refCountLock,
                () -> {
                    AtomicInteger count = refCount.get(kvSnapshotLeaseForBucket);
                    return count == null ? 0 : count.get();
                });
    }

    @VisibleForTesting
    KvSnapshotLease getKvSnapshotLease(String leaseId) {
        return kvSnapshotLeaseMap.get(leaseId);
    }
}
