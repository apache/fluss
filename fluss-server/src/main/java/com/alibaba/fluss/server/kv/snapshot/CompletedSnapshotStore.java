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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A store containing a bounded LIFO-queue of {@link CompletedSnapshot} instances. It's designed for
 * managing the completed snapshots including store/subsume/get completed snapshots for single one
 * table bucket.
 */
public class CompletedSnapshotStore {

    private static final Logger LOG = LoggerFactory.getLogger(CompletedSnapshotStore.class);

    /** The maximum number of snapshots to retain (at least 1). */
    private final int maxNumberOfSnapshotsToRetain;

    /** Completed snapshots state kv store. */
    private final CompletedSnapshotHandleStore completedSnapshotHandleStore;

    private final SharedKvFileRegistry sharedKvFileRegistry;

    private final Executor ioExecutor;
    private final SnapshotsCleaner snapshotsCleaner;

    /**
     * Local copy of the completed snapshots in snapshot store. This is restored from snapshot
     * handel store when recovering.
     */
    private final ArrayDeque<CompletedSnapshot> completedSnapshots;

    public CompletedSnapshotStore(
            int maxNumberOfSnapshotsToRetain,
            SharedKvFileRegistry sharedKvFileRegistry,
            Collection<CompletedSnapshot> completedSnapshots,
            CompletedSnapshotHandleStore completedSnapshotHandleStore,
            Executor executor) {
        this.maxNumberOfSnapshotsToRetain = maxNumberOfSnapshotsToRetain;
        this.sharedKvFileRegistry = sharedKvFileRegistry;
        this.completedSnapshots = new ArrayDeque<>();
        this.completedSnapshots.addAll(completedSnapshots);
        this.completedSnapshotHandleStore = completedSnapshotHandleStore;
        this.ioExecutor = executor;
        this.snapshotsCleaner = new SnapshotsCleaner();
    }

    public void add(final CompletedSnapshot completedSnapshot) throws Exception {
        addSnapshotAndSubsumeOldestOne(completedSnapshot, snapshotsCleaner, () -> {});
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
        completedSnapshots.addLast(snapshot);

        // Remove completed snapshot from queue and snapshotStateHandleStore, not discard.
        Optional<CompletedSnapshot> subsume =
                subsume(
                        completedSnapshots,
                        maxNumberOfSnapshotsToRetain,
                        completedSnapshot -> {
                            remove(
                                    completedSnapshot.getTableBucket(),
                                    completedSnapshot.getSnapshotID());
                            snapshotsCleaner.addSubsumedSnapshot(completedSnapshot);
                        });

        findLowest(completedSnapshots)
                .ifPresent(
                        id -> {
                            // unregister the unused kv file, which will then cause the kv file
                            // deletion
                            sharedKvFileRegistry.unregisterUnusedKvFile(id);
                            snapshotsCleaner.cleanSubsumedSnapshots(
                                    id, Collections.emptySet(), postCleanup, ioExecutor);
                        });
        return subsume.orElse(null);
    }

    public List<CompletedSnapshot> getAllSnapshots() {
        return new ArrayList<>(completedSnapshots);
    }

    private static Optional<CompletedSnapshot> subsume(
            Deque<CompletedSnapshot> snapshots, int numRetain, SubsumeAction subsumeAction) {
        if (snapshots.isEmpty()) {
            return Optional.empty();
        }

        CompletedSnapshot latest = snapshots.peekLast();
        Optional<CompletedSnapshot> lastSubsumedSnapshot = Optional.empty();
        Iterator<CompletedSnapshot> iterator = snapshots.iterator();
        while (snapshots.size() > numRetain && iterator.hasNext()) {
            CompletedSnapshot next = iterator.next();
            if (canSubsume(next, latest)) {
                // always return the subsumed snapshot with larger snapshot id.
                if (!lastSubsumedSnapshot.isPresent()
                        || next.getSnapshotID() > lastSubsumedSnapshot.get().getSnapshotID()) {
                    lastSubsumedSnapshot = Optional.of(next);
                }
                iterator.remove();
                try {
                    subsumeAction.subsume(next);
                } catch (Exception e) {
                    LOG.warn("Fail to subsume the old snapshot.", e);
                }
            }
        }
        return lastSubsumedSnapshot;
    }

    @FunctionalInterface
    interface SubsumeAction {
        void subsume(CompletedSnapshot snapshot) throws Exception;
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
        return Optional.ofNullable(completedSnapshots.peekLast());
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
                return new CompletedSnapshotHandle(filePath, snapshot.getLogOffset());
            } catch (Exception e) {
                latestException = e;
            }
        }
        throw new Exception(
                "Could not open output stream for storing kv to a retrievable kv handle.",
                latestException);
    }
}
