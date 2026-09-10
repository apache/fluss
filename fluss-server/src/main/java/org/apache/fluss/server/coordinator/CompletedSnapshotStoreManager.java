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
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandleStore;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore.SnapshotInUseChecker;
import org.apache.fluss.server.kv.snapshot.SharedKvFileRegistry;
import org.apache.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A manager to manage the {@link CompletedSnapshotStore} for each {@link TableBucket}. When the
 * {@link CompletedSnapshotStore} not exist for a {@link TableBucket}, it will create a new {@link
 * CompletedSnapshotStore} for it.
 */
@ThreadSafe
public class CompletedSnapshotStoreManager {

    private static final Logger LOG = LoggerFactory.getLogger(CompletedSnapshotStoreManager.class);
    private final int maxNumberOfSnapshotsToRetain;
    private final ZooKeeperClient zooKeeperClient;
    private final ConcurrentHashMap<TableBucket, CompletedSnapshotStore>
            bucketCompletedSnapshotStores;
    private final Executor ioExecutor;
    private final Function<ZooKeeperClient, CompletedSnapshotHandleStore>
            makeZookeeperCompletedSnapshotHandleStore;
    private final SnapshotInUseChecker snapshotInUseChecker;
    private final CoordinatorMetricGroup coordinatorMetricGroup;

    public CompletedSnapshotStoreManager(
            int maxNumberOfSnapshotsToRetain,
            Executor ioExecutor,
            ZooKeeperClient zooKeeperClient,
            CoordinatorMetricGroup coordinatorMetricGroup,
            SnapshotInUseChecker snapshotInUseChecker) {
        this(
                maxNumberOfSnapshotsToRetain,
                ioExecutor,
                zooKeeperClient,
                ZooKeeperCompletedSnapshotHandleStore::new,
                coordinatorMetricGroup,
                snapshotInUseChecker);
    }

    @VisibleForTesting
    CompletedSnapshotStoreManager(
            int maxNumberOfSnapshotsToRetain,
            Executor ioExecutor,
            ZooKeeperClient zooKeeperClient,
            Function<ZooKeeperClient, CompletedSnapshotHandleStore>
                    makeZookeeperCompletedSnapshotHandleStore,
            CoordinatorMetricGroup coordinatorMetricGroup,
            SnapshotInUseChecker snapshotInUseChecker) {
        checkArgument(
                maxNumberOfSnapshotsToRetain > 0, "maxNumberOfSnapshotsToRetain must be positive");
        this.maxNumberOfSnapshotsToRetain = maxNumberOfSnapshotsToRetain;
        this.zooKeeperClient = zooKeeperClient;
        this.bucketCompletedSnapshotStores = new ConcurrentHashMap<>();
        this.ioExecutor = ioExecutor;
        this.snapshotInUseChecker = snapshotInUseChecker;
        this.makeZookeeperCompletedSnapshotHandleStore = makeZookeeperCompletedSnapshotHandleStore;
        this.coordinatorMetricGroup = coordinatorMetricGroup;

        registerMetrics();
    }

    private void registerMetrics() {
        MetricGroup physicalStorage = coordinatorMetricGroup.addGroup("physicalStorage");
        physicalStorage.gauge(
                MetricNames.SERVER_PHYSICAL_STORAGE_REMOTE_KV_SIZE,
                this::physicalStorageRemoteKvSize);
    }

    private long physicalStorageRemoteKvSize() {
        return bucketCompletedSnapshotStores.values().stream()
                .map(CompletedSnapshotStore::getPhysicalStorageRemoteKvSize)
                .reduce(0L, Long::sum);
    }

    public CompletedSnapshotStore getOrCreateCompletedSnapshotStore(
            TablePath tablePath, TableBucket tableBucket) {
        return bucketCompletedSnapshotStores.computeIfAbsent(
                tableBucket,
                (bucket) -> {
                    try {
                        LOG.info("Creating snapshot store for table bucket {}.", bucket);
                        long start = System.currentTimeMillis();
                        CompletedSnapshotStore snapshotStore =
                                createCompletedSnapshotStore(tableBucket, ioExecutor);
                        long end = System.currentTimeMillis();
                        LOG.info(
                                "Created snapshot store for table bucket {} in {} ms.",
                                bucket,
                                end - start);

                        MetricGroup bucketMetricGroup =
                                coordinatorMetricGroup.getTableBucketMetricGroup(
                                        tablePath, tableBucket);
                        if (bucketMetricGroup != null) {
                            LOG.info("Add bucketMetricGroup for tableBucket {}.", bucket);
                            bucketMetricGroup.gauge(
                                    MetricNames.KV_NUM_SNAPSHOTS, snapshotStore::getNumSnapshots);
                            bucketMetricGroup.gauge(
                                    MetricNames.KV_ALL_SNAPSHOT_SIZE,
                                    snapshotStore::getPhysicalStorageRemoteKvSize);
                        } else {
                            LOG.warn(
                                    "Failed to add bucketMetricGroup for tableBucket {} when creating completed snapshot.",
                                    bucket);
                        }
                        return snapshotStore;
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to create completed snapshot store for table bucket "
                                        + bucket,
                                e);
                    }
                });
    }

    /**
     * Registers an immutable, externally produced snapshot and adopts its files into retention.
     *
     * <p>The caller must keep the target replicas inactive until registration completes, use a
     * snapshot ID reserved from the target bucket's snapshot counter, and produce files using the
     * target table's schema IDs and KV encoding. Files must belong to the target and remain
     * immutable; ownership transfers to snapshot retention after registration. This method does not
     * copy files, migrate schemas or coordinate concurrent table writes.
     *
     * <p>Run this operation on an IO executor. A failed or uncertain registration can be retried
     * with the same handle; callers must not delete its files on failure.
     */
    public void registerExternalSnapshot(
            TablePath tablePath,
            TableBucket tableBucket,
            CompletedSnapshotHandle handle,
            int coordinatorZkVersion)
            throws Exception {
        CompletedSnapshot snapshot = handle.retrieveCompleteSnapshot();
        checkArgument(
                tableBucket.equals(snapshot.getTableBucket()),
                "Snapshot bucket does not match target.");
        checkArgument(
                handle.getSnapshotId() == snapshot.getSnapshotID()
                        && handle.getLogOffset() == snapshot.getLogOffset()
                        && handle.getMetadataFilePath().equals(snapshot.getMetadataFilePath()),
                "Snapshot metadata does not match its handle.");
        checkArgument(
                snapshot.getSnapshotID() >= 0 && snapshot.getLogOffset() >= 0,
                "Snapshot ID and log offset must be non-negative.");
        long nextSnapshotId =
                new ZkSequenceIDCounter(
                                zooKeeperClient.getCuratorClient(),
                                ZkData.BucketSnapshotSequenceIdZNode.path(tableBucket))
                        .getCurrent();
        checkArgument(
                snapshot.getSnapshotID() < nextSnapshotId,
                "External snapshot ID must be below the target bucket counter.");
        getOrCreateCompletedSnapshotStore(tablePath, tableBucket)
                .registerExternalSnapshot(snapshot, coordinatorZkVersion);
    }

    public void removeCompletedSnapshotStoreByTableBuckets(Set<TableBucket> tableBuckets) {
        for (TableBucket tableBucket : tableBuckets) {
            bucketCompletedSnapshotStores.remove(tableBucket);
        }
    }

    private CompletedSnapshotStore createCompletedSnapshotStore(
            TableBucket tableBucket, Executor ioExecutor) throws Exception {
        final CompletedSnapshotHandleStore completedSnapshotHandleStore =
                this.makeZookeeperCompletedSnapshotHandleStore.apply(zooKeeperClient);

        // Get all there is first.
        List<CompletedSnapshotHandle> initialSnapshots =
                completedSnapshotHandleStore.getAllCompletedSnapshotHandles(tableBucket);

        final int numberOfInitialSnapshots = initialSnapshots.size();

        LOG.info(
                "Found {} snapshots in {}.",
                numberOfInitialSnapshots,
                completedSnapshotHandleStore.getClass().getSimpleName());

        final List<CompletedSnapshot> retrievedSnapshots =
                new ArrayList<>(numberOfInitialSnapshots);

        LOG.info("Trying to fetch {} snapshots from storage.", numberOfInitialSnapshots);

        for (CompletedSnapshotHandle snapshotStateHandle : initialSnapshots) {
            try {
                retrievedSnapshots.add(
                        checkNotNull(snapshotStateHandle.retrieveCompleteSnapshot()));
            } catch (Exception e) {
                if (CompletedSnapshot.isSnapshotDataNotExists(
                        e, snapshotStateHandle.getMetadataFilePath())) {
                    LOG.error(
                            "Metadata not found for snapshot {} of table bucket {}, maybe snapshot already removed or broken.",
                            snapshotStateHandle.getSnapshotId(),
                            tableBucket,
                            e);
                    try {
                        completedSnapshotHandleStore.remove(
                                tableBucket, snapshotStateHandle.getSnapshotId());
                    } catch (Exception t) {
                        LOG.error(
                                "Failed to remove snapshotStateHandle {}.", snapshotStateHandle, t);
                        throw t;
                    }
                } else {
                    LOG.error(
                            "Failed to retrieveCompleteSnapshot for snapshotStateHandle {}.",
                            snapshotStateHandle,
                            e);
                    throw e;
                }
            }
        }

        // register all the files to shared kv file registry
        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry(ioExecutor);
        for (CompletedSnapshot completedSnapshot : retrievedSnapshots) {
            try {
                sharedKvFileRegistry.registerAllAfterRestored(completedSnapshot);
            } catch (Exception e) {
                LOG.error(
                        "Failed to registerAllAfterRestored for completedSnapshot {}.",
                        completedSnapshot,
                        e);
                throw e;
            }
        }

        return new CompletedSnapshotStore(
                maxNumberOfSnapshotsToRetain,
                sharedKvFileRegistry,
                retrievedSnapshots,
                completedSnapshotHandleStore,
                ioExecutor,
                snapshotInUseChecker);
    }

    /**
     * Returns active snapshot IDs per bucket for the given (tableId, partitionId) scope. The result
     * includes both cached snapshots and every persistent snapshot handle. A registered external
     * snapshot must remain protected even if its registration response is lost before the in-memory
     * store adopts it. Failure to read persistent handles fails the query so callers cannot mistake
     * an uncertain result for an empty active set.
     */
    public Map<Integer, Set<Long>> getActiveSnapshotIdsByBucket(
            long tableId, @Nullable Long partitionId, int numBuckets) {
        Map<Integer, Set<Long>> result = new HashMap<>();
        for (int i = 0; i < numBuckets; i++) {
            TableBucket tb = new TableBucket(tableId, partitionId, i);
            CompletedSnapshotStore store = bucketCompletedSnapshotStores.get(tb);
            Set<Long> ids = new HashSet<>(readActiveSnapshotIdsFromZk(tb));
            if (store != null) {
                ids.addAll(store.getActiveSnapshotIds());
            }
            if (!ids.isEmpty()) {
                result.put(i, ids);
            }
        }
        return result;
    }

    private Set<Long> readActiveSnapshotIdsFromZk(TableBucket tableBucket) {
        List<Long> snapshotIds;
        try {
            snapshotIds = zooKeeperClient.listBucketSnapshotIds(tableBucket);
        } catch (Exception e) {
            throw new ApiException(
                    "Failed to read snapshot IDs from ZK for "
                            + tableBucket
                            + ": "
                            + e.getMessage(),
                    e);
        }
        if (snapshotIds.isEmpty()) {
            return Collections.emptySet();
        }
        // Treat every snapshot still referenced in ZK as active. As long as the
        // coordinator has not pruned a snapshot's ZK handle, it is potentially in use
        // (lease, in-flight RPC, recovery, etc.), so the orphan cleaner must keep its
        // files. Orphan cleanup is conservative by design — a slightly broader active
        // set is harmless, while excluding a still-referenced snapshot is not.
        return new HashSet<>(snapshotIds);
    }

    @VisibleForTesting
    Map<TableBucket, CompletedSnapshotStore> getBucketCompletedSnapshotStores() {
        return bucketCompletedSnapshotStores;
    }
}
