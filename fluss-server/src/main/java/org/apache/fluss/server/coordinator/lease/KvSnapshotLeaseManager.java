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

package org.apache.fluss.server.coordinator.lease;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketSnapshot;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.lease.KvSnapshotTableLease;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Clock clock;
    private final ScheduledExecutorService scheduledExecutor;
    private final long leaseExpirationCheckInterval;

    private final ReadWriteLock managerLock = new ReentrantReadWriteLock();

    /** lease id to kv snapshot lease. */
    @GuardedBy("managerLock")
    private final ConcurrentHashMap<String, KvSnapshotLeaseHandler> kvSnapshotLeaseMap =
            MapUtils.newConcurrentHashMap();

    /**
     * KvSnapshotLeaseForBucket to the ref count, which means this table bucket + snapshotId has
     * been leased by how many lease id.
     */
    @GuardedBy("managerLock")
    private final Map<TableBucketSnapshot, AtomicInteger> refCount =
            MapUtils.newConcurrentHashMap();

    /** For metrics. */
    private final AtomicInteger leasedBucketCount = new AtomicInteger(0);

    public KvSnapshotLeaseManager(
            long leaseExpirationCheckInterval,
            ZooKeeperClient zkClient,
            String remoteDataDir,
            Clock clock,
            CoordinatorMetricGroup coordinatorMetricGroup) {
        this(
                leaseExpirationCheckInterval,
                zkClient,
                remoteDataDir,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("kv-snapshot-lease-cleaner")),
                clock,
                coordinatorMetricGroup);
    }

    @VisibleForTesting
    public KvSnapshotLeaseManager(
            long leaseExpirationCheckInterval,
            ZooKeeperClient zkClient,
            String remoteDataDir,
            ScheduledExecutorService scheduledExecutor,
            Clock clock,
            CoordinatorMetricGroup coordinatorMetricGroup) {
        this.metadataManager = new KvSnapshotLeaseMetadataManager(zkClient, remoteDataDir);
        this.leaseExpirationCheckInterval = leaseExpirationCheckInterval;
        this.scheduledExecutor = scheduledExecutor;
        this.clock = clock;

        registerMetrics(coordinatorMetricGroup);
    }

    public void start() {
        LOG.info("kv snapshot lease manager has been started.");

        List<String> leasesList = new ArrayList<>();
        try {
            leasesList = metadataManager.getLeasesList();
        } catch (Exception e) {
            LOG.error("Failed to get leases list from zookeeper.", e);
        }

        for (String leaseId : leasesList) {
            Optional<KvSnapshotLeaseHandler> kvSnapshotLeaseOpt = Optional.empty();
            try {
                kvSnapshotLeaseOpt = metadataManager.getLease(leaseId);
            } catch (Exception e) {
                LOG.error("Failed to get kv snapshot lease from zookeeper.", e);
            }

            if (kvSnapshotLeaseOpt.isPresent()) {
                KvSnapshotLeaseHandler kvSnapshotLeasehandle = kvSnapshotLeaseOpt.get();
                this.kvSnapshotLeaseMap.put(leaseId, kvSnapshotLeasehandle);

                initializeRefCount(kvSnapshotLeasehandle);

                leasedBucketCount.addAndGet(kvSnapshotLeasehandle.getLeasedSnapshotCount());
            }
        }

        scheduledExecutor.scheduleWithFixedDelay(
                this::expireLeases, 0L, leaseExpirationCheckInterval, TimeUnit.MILLISECONDS);
    }

    public boolean snapshotLeaseNotExist(TableBucketSnapshot tableBucketSnapshot) {
        return inReadLock(
                managerLock,
                () -> {
                    AtomicInteger count = refCount.get(tableBucketSnapshot);
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
            Map<Long, List<TableBucketSnapshot>> tableIdToLeaseBucket)
            throws Exception {
        return inWriteLock(
                managerLock,
                () -> {
                    // To record the unavailable snapshots such as the kv snapshotId to lease not
                    // exists.
                    Map<TableBucket, Long> unavailableSnapshots = new HashMap<>();

                    boolean update = kvSnapshotLeaseMap.containsKey(leaseId);
                    // set the expiration time as: current time + leaseDuration
                    long newExpirationTime = clock.milliseconds() + leaseDuration;
                    KvSnapshotLeaseHandler kvSnapshotLeaseHandle =
                            kvSnapshotLeaseMap.compute(
                                    leaseId,
                                    (key, existingLease) -> {
                                        if (existingLease == null) {
                                            LOG.info(
                                                    "kv snapshot lease '{}' has been acquired. The lease expiration "
                                                            + "time is {}",
                                                    leaseId,
                                                    newExpirationTime);
                                            return new KvSnapshotLeaseHandler(newExpirationTime);
                                        } else {
                                            existingLease.setExpirationTime(newExpirationTime);
                                            return existingLease;
                                        }
                                    });

                    for (Map.Entry<Long, List<TableBucketSnapshot>> entry :
                            tableIdToLeaseBucket.entrySet()) {
                        List<TableBucketSnapshot> buckets = entry.getValue();
                        for (TableBucketSnapshot bucket : buckets) {
                            TableBucket tableBucket = bucket.getTableBucket();
                            long kvSnapshotId = bucket.getSnapshotId();
                            // TODO Check whether this snapshot exists, if not add it to the
                            // unavailable snapshots. Trace by:
                            // https://github.com/apache/fluss/issues/2600

                            long originalSnapshotId =
                                    kvSnapshotLeaseHandle.acquireBucket(tableBucket, kvSnapshotId);
                            if (originalSnapshotId == -1L) {
                                leasedBucketCount.incrementAndGet();
                            } else {
                                // clear the original ref.
                                decrementRefCount(
                                        new TableBucketSnapshot(tableBucket, originalSnapshotId));
                            }
                            incrementRefCount(bucket);
                        }
                    }

                    if (update) {
                        metadataManager.updateLease(leaseId, kvSnapshotLeaseHandle);
                    } else {
                        metadataManager.registerLease(leaseId, kvSnapshotLeaseHandle);
                    }

                    return unavailableSnapshots;
                });
    }

    /**
     * Release kv snapshot lease.
     *
     * @param leaseId the lease id
     * @param tableBucketsToRelease the table buckets to release
     */
    public void release(String leaseId, List<TableBucket> tableBucketsToRelease) throws Exception {
        inWriteLock(
                managerLock,
                () -> {
                    KvSnapshotLeaseHandler lease = kvSnapshotLeaseMap.get(leaseId);
                    if (lease == null) {
                        return;
                    }

                    for (TableBucket bucket : tableBucketsToRelease) {
                        long snapshotId = lease.releaseBucket(bucket);
                        if (snapshotId != -1L) {
                            leasedBucketCount.decrementAndGet();
                            decrementRefCount(new TableBucketSnapshot(bucket, snapshotId));
                        }
                    }

                    if (lease.isEmpty()) {
                        dropLease(leaseId);
                    } else {
                        metadataManager.updateLease(leaseId, lease);
                    }
                });
    }

    /**
     * Drop kv snapshot lease.
     *
     * @param leaseId the lease id
     * @return true if clear success, false if lease not exist
     */
    public boolean dropLease(String leaseId) throws Exception {
        return inWriteLock(
                managerLock,
                () -> {
                    KvSnapshotLeaseHandler kvSnapshotLeasehandle =
                            kvSnapshotLeaseMap.remove(leaseId);
                    if (kvSnapshotLeasehandle == null) {
                        return false;
                    }

                    clearRefCount(kvSnapshotLeasehandle);
                    metadataManager.deleteLease(leaseId);

                    LOG.info("kv snapshots of lease '{}' has been all released.", leaseId);
                    return true;
                });
    }

    private void initializeRefCount(KvSnapshotLeaseHandler lease) {
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
                            new TableBucketSnapshot(new TableBucket(tableId, i), snapshots[i]));
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
                                new TableBucketSnapshot(
                                        new TableBucket(tableId, partitionId, i), snapshots[i]));
                    }
                }
            }
        }
    }

    private void clearRefCount(KvSnapshotLeaseHandler lease) {
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
                            new TableBucketSnapshot(new TableBucket(tableId, i), snapshots[i]));
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
                                new TableBucketSnapshot(
                                        new TableBucket(tableId, partitionId, i), snapshots[i]));
                        leasedBucketCount.decrementAndGet();
                    }
                }
            }
        }
    }

    private void incrementRefCount(TableBucketSnapshot tableBucketSnapshot) {
        refCount.computeIfAbsent(tableBucketSnapshot, k -> new AtomicInteger(0)).incrementAndGet();
    }

    private void decrementRefCount(TableBucketSnapshot tableBucketSnapshot) {
        refCount.computeIfPresent(
                tableBucketSnapshot,
                (k, v) -> {
                    int newCount = v.decrementAndGet();
                    return newCount <= 0 ? null : v;
                });
    }

    private void expireLeases() {
        long currentTime = clock.milliseconds();
        // 1. First collect all expired lease IDs under read lock
        List<String> expiredLeaseIds =
                inReadLock(
                        managerLock,
                        () ->
                                kvSnapshotLeaseMap.entrySet().stream()
                                        .filter(
                                                entry ->
                                                        entry.getValue().getExpirationTime()
                                                                < currentTime)
                                        .map(Map.Entry::getKey)
                                        .collect(Collectors.toList()));

        // 2. Then process each collected ID (dropLease acquires write lock)
        expiredLeaseIds.forEach(
                leaseId -> {
                    try {
                        dropLease(leaseId);
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
        return inReadLock(managerLock, kvSnapshotLeaseMap::size);
    }

    @VisibleForTesting
    int getLeasedBucketCount() {
        return leasedBucketCount.get();
    }

    @VisibleForTesting
    int getRefCount(TableBucketSnapshot tableBucketSnapshot) {
        return inReadLock(
                managerLock,
                () -> {
                    AtomicInteger count = refCount.get(tableBucketSnapshot);
                    return count == null ? 0 : count.get();
                });
    }

    @VisibleForTesting
    KvSnapshotLeaseHandler getKvSnapshotLeaseData(String leaseId) {
        return inReadLock(managerLock, () -> kvSnapshotLeaseMap.get(leaseId));
    }

    @VisibleForTesting
    KvSnapshotLeaseMetadataManager getMetadataManager() {
        return metadataManager;
    }
}
