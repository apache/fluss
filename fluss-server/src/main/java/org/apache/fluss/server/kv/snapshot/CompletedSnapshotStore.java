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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
import static org.apache.fluss.utils.Preconditions.checkState;
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
    private final SnapshotInUseChecker snapshotInUseChecker;

    private final ReentrantLock lock = new ReentrantLock();

    @Nullable private Long recoverySnapshotId;

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
            SnapshotInUseChecker snapshotInUseChecker) {
        this.maxNumberOfSnapshotsToRetain = maxNumberOfSnapshotsToRetain;
        this.sharedKvFileRegistry = sharedKvFileRegistry;
        this.completedSnapshots = new ArrayDeque<>();
        this.completedSnapshots.addAll(completedSnapshots);
        this.completedSnapshotHandleStore = completedSnapshotHandleStore;
        this.snapshotInUseChecker = snapshotInUseChecker;
        this.ioExecutor = executor;
        this.snapshotsCleaner = new SnapshotsCleaner();
    }

    /** Commits using the leader epoch captured when the original KV target was created. */
    public void add(
            CompletedSnapshot snapshot,
            @Nullable Long expectedRecoverySnapshotId,
            @Nullable Integer expectedLeaderEpoch)
            throws Exception {
        inLock(
                lock,
                () ->
                        registerSnapshot(
                                snapshot,
                                expectedRecoverySnapshotId,
                                expectedLeaderEpoch,
                                snapshotsCleaner,
                                () -> {}));
    }

    /** Publishes a snapshot reference and selects it for recovery atomically. */
    public void selectRecoverySnapshot(CompletedSnapshot snapshot) throws Exception {
        inLock(
                lock,
                () -> {
                    CompletedSnapshotHandle handle = prepareSnapshotHandle(snapshot);
                    completedSnapshotHandleStore.selectRecoverySnapshot(
                            snapshot.getTableBucket(), handle);
                    if (containsSnapshot(snapshot.getSnapshotID())) {
                        recoverySnapshotId = snapshot.getSnapshotID();
                        subsumeOldSnapshots(snapshot, snapshotsCleaner, () -> {});
                    } else {
                        adoptConfirmedSnapshot(snapshot, snapshotsCleaner, () -> {});
                    }
                });
    }

    public long getPhysicalStorageRemoteKvSize() {
        return sharedKvFileRegistry.getFileSize();
    }

    public long getNumSnapshots() {
        return inLock(lock, completedSnapshots::size);
    }

    /**
     * Returns the union of every snapshot still held by this store and the snapshots recorded as
     * still-in-use. Any snapshot whose ZK handle has not yet been pruned is potentially active
     * (lease, in-flight RPC, recovery, etc.), so the orphan cleaner must keep its files. Orphan
     * cleanup is conservative by design — a slightly broader active set is harmless, while
     * excluding a still-referenced snapshot is not.
     */
    public Set<Long> getActiveSnapshotIds() {
        return inLock(
                lock,
                () -> {
                    Set<Long> ids =
                            new HashSet<>(completedSnapshots.size() + stillInUseSnapshots.size());
                    for (CompletedSnapshot snapshot : completedSnapshots) {
                        ids.add(snapshot.getSnapshotID());
                    }
                    ids.addAll(stillInUseSnapshots.keySet());
                    return ids;
                });
    }

    /**
     * Synchronously writes the new snapshots to snapshot handle store and asynchronously removes
     * older ones.
     *
     * @param snapshot Completed snapshot to add.
     */
    @VisibleForTesting
    void addSnapshotAndSubsumeOldestOne(
            final CompletedSnapshot snapshot,
            SnapshotsCleaner snapshotsCleaner,
            Runnable postCleanup)
            throws Exception {
        registerSnapshot(snapshot, null, null, snapshotsCleaner, postCleanup);
    }

    private void registerSnapshot(
            CompletedSnapshot snapshot,
            @Nullable Long expectedRecoverySnapshotId,
            @Nullable Integer expectedLeaderEpoch,
            SnapshotsCleaner snapshotsCleaner,
            Runnable postCleanup)
            throws Exception {
        CompletedSnapshotHandle completedSnapshotHandle = prepareSnapshotHandle(snapshot);
        completedSnapshotHandleStore.add(
                snapshot.getTableBucket(),
                snapshot.getSnapshotID(),
                completedSnapshotHandle,
                expectedRecoverySnapshotId,
                expectedLeaderEpoch);

        adoptConfirmedSnapshot(snapshot, snapshotsCleaner, postCleanup);
    }

    private CompletedSnapshotHandle prepareSnapshotHandle(CompletedSnapshot snapshot)
            throws Exception {
        checkNotNull(snapshot, "Snapshot");
        Optional<CompletedSnapshotHandle> previous =
                completedSnapshotHandleStore.get(
                        snapshot.getTableBucket(), snapshot.getSnapshotID());
        CompletedSnapshotHandle completedSnapshotHandle;
        if (previous.isPresent()) {
            completedSnapshotHandle = previous.get();
            checkState(
                    completedSnapshotHandle.getLogOffset() == snapshot.getLogOffset()
                            && completedSnapshotHandle
                                    .getMetadataFilePath()
                                    .equals(snapshot.getMetadataFilePath()),
                    "Conflicting snapshot reference.");
        } else {
            completedSnapshotHandle = store(snapshot);
        }
        return completedSnapshotHandle;
    }

    private void adoptConfirmedSnapshot(
            CompletedSnapshot snapshot, SnapshotsCleaner snapshotsCleaner, Runnable postCleanup)
            throws Exception {
        snapshot.registerSharedKvFilesAfterRestored(sharedKvFileRegistry);
        completedSnapshots.removeIf(
                existing -> existing.getSnapshotID() == snapshot.getSnapshotID());
        stillInUseSnapshots.remove(snapshot.getSnapshotID());
        completedSnapshots.addLast(snapshot);
        Optional<CompletedSnapshotHandle> selected =
                completedSnapshotHandleStore.getRecoverySnapshot(snapshot.getTableBucket());
        checkState(
                recoverySnapshotId == null || selected.isPresent(),
                "Recovery snapshot selection disappeared.");
        if (selected.isPresent() && !containsSnapshot(selected.get().getSnapshotId())) {
            CompletedSnapshot source = selected.get().retrieveCompleteSnapshot();
            source.registerSharedKvFilesAfterRestored(sharedKvFileRegistry);
            stillInUseSnapshots.put(source.getSnapshotID(), source);
        }
        recoverySnapshotId = selected.map(CompletedSnapshotHandle::getSnapshotId).orElse(null);
        subsumeOldSnapshots(snapshot, snapshotsCleaner, postCleanup);
    }

    private boolean containsSnapshot(long snapshotId) {
        return stillInUseSnapshots.containsKey(snapshotId)
                || completedSnapshots.stream()
                        .anyMatch(snapshot -> snapshot.getSnapshotID() == snapshotId);
    }

    private void subsumeOldSnapshots(
            CompletedSnapshot snapshot, SnapshotsCleaner snapshotsCleaner, Runnable postCleanup)
            throws Exception {

        // Remove completed snapshot from queue and snapshotStateHandleStore, not
        // discard.
        subsume(
                completedSnapshots,
                maxNumberOfSnapshotsToRetain,
                completedSnapshot -> {
                    if (isSnapshotInUse(completedSnapshot)) {
                        LOG.debug(
                                "Snapshot {} is still in use, move it to stillInUseSnapshots",
                                completedSnapshot.getSnapshotID());
                        stillInUseSnapshots.put(
                                completedSnapshot.getSnapshotID(), completedSnapshot);
                    } else {
                        remove(
                                completedSnapshot.getTableBucket(),
                                completedSnapshot.getSnapshotID());
                        snapshotsCleaner.addSubsumedSnapshot(completedSnapshot);
                    }
                });

        // Check if any previously still-in-use snapshots can now be released
        // (lease expired).
        removeUnusedSnapshots(snapshotsCleaner);

        // SST file cleanup: compute effective lowest from retained (non-leased)
        // snapshots only, and protect files referenced by still-in-use snapshots.
        Set<Long> stillInUseIds = new HashSet<>(stillInUseSnapshots.keySet());
        findLowest(completedSnapshots)
                .ifPresent(id -> sharedKvFileRegistry.unregisterUnusedKvFile(id, stillInUseIds));

        // Snapshot metadata/private files cleanup: use the latest snapshot
        // ID + 1 so subsumed snapshots can be cleaned even when a lower
        // snapshot has a lease. This is safe because
        // KvSnapshotHandle.discard() only deletes private files and
        // metadata, not shared SST files registered in SharedKvFileRegistry.
        snapshotsCleaner.cleanSubsumedSnapshots(
                snapshot.getSnapshotID() + 1, stillInUseIds, postCleanup, ioExecutor);
    }

    private void removeUnusedSnapshots(SnapshotsCleaner snapshotsCleaner) throws Exception {
        Iterator<Map.Entry<Long, CompletedSnapshot>> stillInUseIter =
                stillInUseSnapshots.entrySet().iterator();
        while (stillInUseIter.hasNext()) {
            Map.Entry<Long, CompletedSnapshot> entry = stillInUseIter.next();
            CompletedSnapshot snapshot = entry.getValue();
            if (!isSnapshotInUse(snapshot)) {
                remove(snapshot.getTableBucket(), snapshot.getSnapshotID());
                stillInUseIter.remove();
                snapshotsCleaner.addSubsumedSnapshot(snapshot);
                LOG.debug(
                        "Released snapshot {} from stillInUseSnapshots (lease expired)",
                        snapshot.getSnapshotID());
            }
        }
    }

    private boolean isSnapshotInUse(CompletedSnapshot snapshot) {
        return (recoverySnapshotId != null && recoverySnapshotId == snapshot.getSnapshotID())
                || snapshotInUseChecker.isInUse(snapshot);
    }

    /**
     * Returns a list of all the completed snapshots in this store. Note this doesn't include the
     * snapshots that are still in use but have been moved out of the standard retention window,
     * which can be obtained from {@link #stillInUseSnapshots}.
     */
    @VisibleForTesting
    public List<CompletedSnapshot> getAllSnapshots() {
        return inLock(lock, () -> new ArrayList<>(completedSnapshots));
    }

    private static void subsume(
            Deque<CompletedSnapshot> snapshots, int numRetain, SubsumeAction subsumeAction)
            throws Exception {
        if (snapshots.isEmpty()) {
            return;
        }

        CompletedSnapshot latest = snapshots.peekLast();
        Iterator<CompletedSnapshot> iterator = snapshots.iterator();
        while (snapshots.size() > numRetain && iterator.hasNext()) {
            CompletedSnapshot next = iterator.next();
            if (canSubsume(next, latest)) {
                subsumeAction.subsume(next);
                iterator.remove();
            }
        }
    }

    @FunctionalInterface
    interface SubsumeAction {
        void subsume(CompletedSnapshot snapshot) throws Exception;
    }

    /** A function to check whether a snapshot still in use. */
    @FunctionalInterface
    public interface SnapshotInUseChecker {
        boolean isInUse(CompletedSnapshot snapshot);
    }

    private static boolean canSubsume(CompletedSnapshot next, CompletedSnapshot latest) {
        // if the snapshot is equal to the latest snapshot, it means it can't be subsumed
        if (next == latest) {
            return false;
        }
        // else, we always subsume it as we will only keep single one snapshot currently
        // todo: consider some client are pining this snapshot in FLUSS-54730210
        return true;
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
        return unSubsumedSnapshots.stream()
                .map(CompletedSnapshot::getSnapshotID)
                .min(Long::compare);
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
