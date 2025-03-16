/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.kv.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Delegate class responsible for snapshots cleaning and counting the number of snapshots yet to
 * clean.
 */
public class SnapshotsCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotsCleaner.class);

    private final Object lock = new Object();

    /** All subsumed snapshots. */
    @GuardedBy("lock")
    private final List<CompletedSnapshot> subsumedSnapshots = new ArrayList<>();

    /**
     * Add one subsumed snapshot to SnapshotsCleaner, the subsumed snapshot would be discarded at
     * {@link #cleanSubsumedSnapshots(long, Set, Runnable, Executor)}.
     *
     * @param completedSnapshot which is subsumed.
     */
    public void addSubsumedSnapshot(CompletedSnapshot completedSnapshot) {
        synchronized (lock) {
            subsumedSnapshots.add(completedSnapshot);
        }
    }

    /**
     * Clean Snapshot that is not in the given {@param stillInUse}.
     *
     * @param upTo lowest SnapshotID which is still valid.
     * @param stillInUse the state of those Snapshots are still referenced.
     * @param postCleanAction post action after cleaning.
     * @param executor is used to perform the cleanup logic.
     */
    public void cleanSubsumedSnapshots(
            long upTo, Set<Long> stillInUse, Runnable postCleanAction, Executor executor) {
        synchronized (lock) {
            Iterator<CompletedSnapshot> iterator = subsumedSnapshots.iterator();
            while (iterator.hasNext()) {
                CompletedSnapshot snapshot = iterator.next();
                if (snapshot.getSnapshotID() < upTo
                        && !stillInUse.contains(snapshot.getSnapshotID())) {
                    try {
                        LOG.debug("Try to discard snapshot {}.", snapshot.getSnapshotID());
                        cleanSnapshot(snapshot, postCleanAction, executor);
                        iterator.remove();
                    } catch (Exception e) {
                        LOG.warn("Fail to discard the old snapshot {}.", snapshot, e);
                    }
                }
            }
        }
    }

    public void cleanSnapshot(
            CompletedSnapshot snapshot, Runnable postCleanAction, Executor executor) {
        LOG.debug("Clean snapshot {}.", snapshot.getSnapshotID());
        CompletableFuture<Void> discardFuture = snapshot.discardAsync(executor);
        discardFuture.handle(
                (Object outerIgnored, Throwable outerThrowable) -> {
                    if (outerThrowable != null) {
                        LOG.warn(
                                "Could not properly discard completed Snapshot {}.",
                                snapshot.getSnapshotID(),
                                outerThrowable);
                    }
                    postCleanAction.run();
                    return null;
                });
    }
}
