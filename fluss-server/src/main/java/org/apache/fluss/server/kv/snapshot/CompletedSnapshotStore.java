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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A store containing a bounded LIFO-queue of {@link CompletedSnapshot} instances. It's designed for
 * managing the completed snapshots including store/subsume/get completed snapshots for single one
 * table bucket.
 */
@ThreadSafe
public class CompletedSnapshotStore {

    private static final Logger LOG = LoggerFactory.getLogger(CompletedSnapshotStore.class);

    /** The maximum number of snapshots to retain (at least 1). */
    private final int maxNumberOfSnapshotsToRetain;

    /** Completed snapshots state kv store. */
    private final CompletedSnapshotHandleStore completedSnapshotHandleStore;

    private final SharedKvFileRegistry sharedKvFileRegistry;

    private final Executor ioExecutor;
    private final SnapshotsCleaner snapshotsCleaner;
    private final SubsumptionChecker subsumptionChecker;

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Local copy of the completed snapshots in snapshot store. This is restored from snapshot
     * handel store when recovering.
     */
    private final ArrayDeque<CompletedSnapshot> completedSnapshots;

    /**
     * Snapshots that are still in use by a lease but have been moved out of the standard retention
     * window. These snapshots are protected from deletion (both metadata and SST files) until their
     * lease expires.
     *
     * <p>When a snapshot should be subsumed (beyond the retention window) but cannot be (because
     * it's leased), it is moved here instead of staying in {@link #completedSnapshots}. This
     * ensures the effective {@code lowestSnapshotID} is computed from retained snapshots only,
     * allowing SST files from non-leased subsumed snapshots to be cleaned up properly.
     */
    @VisibleForTesting final Map<Long, CompletedSnapshot> stillInUseSnapshots = new HashMap<>();

    public CompletedSnapshotStore(
            int maxNumberOfSnapshotsToRetain,
            SharedKvFileRegistry sharedKvFileRegistry,
            Collection<CompletedSnapshot> completedSnapshots,
            CompletedSnapshotHandleStore completedSnapshotHandleStore,
            Executor executor,
            SubsumptionChecker subsumptionChecker) {
        this.maxNumberOfSnapshotsToRetain = maxNumberOfSnapshotsToRetain;
        this.sharedKvFileRegistry = sharedKvFileRegistry;
        this.completedSnapshots = new ArrayDeque<>();
        this.completedSnapshots.addAll(completedSnapshots);
        this.completedSnapshotHandleStore = completedSnapshotHandleStore;
        this.subsumptionChecker = subsumptionChecker;
        this.ioExecutor = executor;
        this.snapshotsCleaner = new SnapshotsCleaner();
    }

    public void add(final CompletedSnapshot completedSnapshot) throws Exception {
        inLock(
                lock,
                () ->
                        addSnapshotAndSubsumeOldestOne(
                                completedSnapshot, snapshotsCleaner, () -> {}));
    }

    public long getPhysicalStorageRemoteKvSize() {
        return sharedKvFileRegistry.getFileSize();
    }

    public long getNumSnapshots() {
        return inLock(lock, () -> completedSnapshots.size());
    }

    /**
     * Synchronously writes the new snapshots to snapshot handle store and asynchronously removes
     * older ones.
     *
     * @param snapshot Completed snapshot to add.
     */
    @VisibleForTesting
    CompletedSnapshot addSnapshotAndSubsumeOldestOne(
            final CompletedSnapshot snapshot,
            SnapshotsCleaner snapshotsCleaner,
            Runnable postCleanup)
            throws Exception {
        checkNotNull(snapshot, "Snapshot");

        // register the completed snapshot to the shared registry
        snapshot.registerSharedKvFilesAfterRestored(sharedKvFileRegistry);

        CompletedSnapshotHandle completedSnapshotHandle = store(snapshot);
        completedSnapshotHandleStore.add(
                snapshot.getTableBucket(), snapshot.getSnapshotID(), completedSnapshotHandle);

        // Now add the new one. If it fails, we don't want to lose existing data.
        return inLock(
                lock,
                () -> {
                    completedSnapshots.addLast(snapshot);

                    // Remove completed snapshot from queue and snapshotStateHandleStore, not
                    // discard.
                    subsume(
                            completedSnapshots,
                            maxNumberOfSnapshotsToRetain,
                            completedSnapshot -> {
                                remove(
                                        completedSnapshot.getTableBucket(),
                                        completedSnapshot.getSnapshotID());
                                snapshotsCleaner.addSubsumedSnapshot(completedSnapshot);
                            },
                            subsumptionChecker);

                    // Move leased snapshots that should have been subsumed but couldn't
                    // (protected by a lease) from completedSnapshots to stillInUseSnapshots.
                    // This ensures the effective lowestSnapshotID is computed from retained
                    // (non-leased) snapshots only, allowing SST files from non-leased
                    // subsumed snapshots to be cleaned up properly.
                    CompletedSnapshot latest = completedSnapshots.peekLast();
                    Iterator<CompletedSnapshot> leaseIt = completedSnapshots.iterator();
                    while (leaseIt.hasNext()) {
                        CompletedSnapshot next = leaseIt.next();
                        if (next != latest
                                && !subsumptionChecker.canSubsume(
                                        new TableBucketSnapshot(
                                                next.getTableBucket(), next.getSnapshotID()))) {
                            leaseIt.remove();
                            stillInUseSnapshots.put(next.getSnapshotID(), next);
                            LOG.debug(
                                    "Moved leased snapshot {} to stillInUseSnapshots",
                                    next.getSnapshotID());
                        }
                    }

                    // Check if any previously still-in-use snapshots can now be released
                    // (lease expired).
                    Iterator<Map.Entry<Long, CompletedSnapshot>> stillInUseIter =
                            stillInUseSnapshots.entrySet().iterator();
                    while (stillInUseIter.hasNext()) {
                        Map.Entry<Long, CompletedSnapshot> entry = stillInUseIter.next();
                        CompletedSnapshot s = entry.getValue();
                        if (subsumptionChecker.canSubsume(
                                new TableBucketSnapshot(s.getTableBucket(), s.getSnapshotID()))) {
                            stillInUseIter.remove();
                            try {
                                remove(s.getTableBucket(), s.getSnapshotID());
                            } catch (Exception e) {
                                LOG.warn(
                                        "Failed to remove released snapshot {} from store",
                                        s.getSnapshotID(),
                                        e);
                            }
                            snapshotsCleaner.addSubsumedSnapshot(s);
                            LOG.debug(
                                    "Released snapshot {} from stillInUseSnapshots (lease expired)",
                                    s.getSnapshotID());
                        }
                    }

                    // SST file cleanup: compute effective lowest from retained (non-leased)
                    // snapshots only, and protect files referenced by still-in-use snapshots.
                    Set<Long> stillInUseIds = new HashSet<>(stillInUseSnapshots.keySet());
                    findLowest(completedSnapshots)
                            .ifPresent(
                                    id ->
                                            sharedKvFileRegistry.unregisterUnusedKvFile(
                                                    id, stillInUseIds));

                    // Snapshot metadata/private files cleanup: use the latest snapshot
                    // ID + 1 so subsumed snapshots can be cleaned even when a lower
                    // snapshot has a lease. This is safe because
                    // KvSnapshotHandle.discard() only deletes private files and
                    // metadata, not shared SST files registered in SharedKvFileRegistry.
                    snapshotsCleaner.cleanSubsumedSnapshots(
                            snapshot.getSnapshotID() + 1, stillInUseIds, postCleanup, ioExecutor);
                    return null;
                });
    }

    public List<CompletedSnapshot> getAllSnapshots() {
        return inLock(lock, () -> new ArrayList<>(completedSnapshots));
    }

    private static void subsume(
            Deque<CompletedSnapshot> snapshots,
            int numRetain,
            SubsumeAction subsumeAction,
            SubsumptionChecker subsumptionChecker) {
        if (snapshots.isEmpty()) {
            return;
        }

        CompletedSnapshot latest = snapshots.peekLast();
        Iterator<CompletedSnapshot> iterator = snapshots.iterator();
        while (snapshots.size() > numRetain && iterator.hasNext()) {
            CompletedSnapshot next = iterator.next();
            if (canSubsume(next, latest, subsumptionChecker)) {
                iterator.remove();
                try {
                    subsumeAction.subsume(next);
                } catch (Exception e) {
                    LOG.warn("Fail to subsume the old snapshot.", e);
                }
            }
        }
    }

    @FunctionalInterface
    interface SubsumeAction {
        void subsume(CompletedSnapshot snapshot) throws Exception;
    }

    /** A function to check whether a snapshot can be subsumed. */
    @FunctionalInterface
    public interface SubsumptionChecker {
        boolean canSubsume(TableBucketSnapshot bucket);
    }

    private static boolean canSubsume(
            CompletedSnapshot next,
            CompletedSnapshot latest,
            SubsumptionChecker subsumptionChecker) {
        // if the snapshot is equal to the latest snapshot, it means it can't be subsumed
        if (next == latest) {
            return false;
        }

        return subsumptionChecker.canSubsume(
                new TableBucketSnapshot(next.getTableBucket(), next.getSnapshotID()));
    }

    /**
     * Tries to remove the snapshot identified by the given snapshot id.
     *
     * @param snapshotId identifying the snapshot to remove
     */
    private void remove(TableBucket tableBucket, long snapshotId) throws Exception {
        completedSnapshotHandleStore.remove(tableBucket, snapshotId);
    }

    protected static Optional<Long> findLowest(Deque<CompletedSnapshot> unSubsumedSnapshots) {
        for (CompletedSnapshot p : unSubsumedSnapshots) {
            return Optional.of(p.getSnapshotID());
        }
        return Optional.empty();
    }

    /**
     * Returns the latest {@link CompletedSnapshot} instance or <code>empty</code> if none was
     * added.
     */
    public Optional<CompletedSnapshot> getLatestSnapshot() {
        return inLock(lock, () -> Optional.ofNullable(completedSnapshots.peekLast()));
    }

    /**
     * Serialize the completed snapshot to a metadata file, and return the handle wrapping the
     * metadata file path.
     */
    private CompletedSnapshotHandle store(CompletedSnapshot snapshot) throws Exception {
        // Flink use another path 'high-availability.storageDir' to store the snapshot meta info,
        // and save the path to zk to keep the zk store less data;
        // we just reuse the snapshot dir to store the snapshot info to avoid another path
        // config
        Exception latestException = null;
        FsPath filePath = snapshot.getMetadataFilePath();
        FileSystem fs = filePath.getFileSystem();
        byte[] jsonBytes = CompletedSnapshotJsonSerde.toJson(snapshot);
        for (int attempt = 0; attempt < 10; attempt++) {
            try (FSDataOutputStream outStream =
                    fs.create(filePath, FileSystem.WriteMode.OVERWRITE)) {
                outStream.write(jsonBytes);
                return new CompletedSnapshotHandle(
                        snapshot.getSnapshotID(), filePath, snapshot.getLogOffset());
            } catch (Exception e) {
                latestException = e;
            }
        }
        throw new Exception(
                "Could not open output stream for storing kv to a retrievable kv handle.",
                latestException);
    }
}
