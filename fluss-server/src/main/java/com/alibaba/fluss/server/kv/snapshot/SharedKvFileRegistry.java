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

import com.alibaba.fluss.utils.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This registry manages kv files for a table bucket that is shared across (incremental) snapshots,
 * and is responsible for deleting shared kv files that is no longer used in any valid snapshot.
 *
 * <p>A {@code SharedKvFileRegistry} will be deployed for each table bucket to keep track of usage
 * of {@link KvFileHandle}s by a key that (logically) identifies them.
 */
public class SharedKvFileRegistry implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SharedKvFileRegistry.class);

    private final Map<SharedKvFileRegistryKey, SharedKvEntry> registeredKvEntries;

    /** This flag indicates whether or not the registry is open or if close() was called. */
    private boolean open;

    /** Executor for async kv deletion. */
    private final Executor asyncDisposalExecutor;

    public SharedKvFileRegistry() {
        this(Executors.directExecutor());
    }

    public SharedKvFileRegistry(Executor asyncDisposalExecutor) {
        this.registeredKvEntries = new HashMap<>();
        this.asyncDisposalExecutor = checkNotNull(asyncDisposalExecutor);
        this.open = true;
    }

    public KvFileHandle registerReference(
            final SharedKvFileRegistryKey registrationKey,
            final KvFileHandle newHandle,
            final long snapshotID) {

        checkNotNull(newHandle, "Kv handle should not be null.");

        SharedKvEntry entry;

        synchronized (registeredKvEntries) {
            checkState(open, "Attempt to register kv file to closed SharedKvRegistry.");

            entry = registeredKvEntries.get(registrationKey);
            if (entry == null) {
                checkState(
                        !isPlaceholder(newHandle),
                        "Attempt to reference unknown kv file: " + registrationKey);

                LOG.trace("Registered new kv file {} under key {}.", newHandle, registrationKey);
                entry = new SharedKvEntry(newHandle, snapshotID);
                registeredKvEntries.put(registrationKey, entry);

                // no further handling
                return entry.kvFileHandle;
            } else if (Objects.equals(entry.kvFileHandle, newHandle)) {
                LOG.trace(
                        "Duplicated registration under key {} with the new object: {}.",
                        registrationKey,
                        newHandle);
            } else if (isPlaceholder(newHandle)) {
                LOG.trace(
                        "Duplicated registration under key {} with a placeholder (normal case)",
                        registrationKey);
            } else {
                // maybe a bug
                LOG.warn(
                        "Unexpected registration under key {} with the new object: {}.",
                        registrationKey,
                        newHandle);
            }
        }

        LOG.trace(
                "Updating last snapshot for {} from {} to {}",
                registrationKey,
                entry.lastUsedSnapshotID,
                snapshotID);
        entry.advanceLastUsingSnapshotID(snapshotID);
        return entry.kvFileHandle;
    }

    public void unregisterUnusedKvFile(long lowestSnapshotID) {
        // delete kv files that aren't used
        LOG.debug(
                "Discard kv files created before snapshot {} and not used afterwards",
                lowestSnapshotID);
        List<KvFileHandle> subsumed = new ArrayList<>();
        // Iterate over all the registered kv file handles.
        // Using a simple loop and NOT index by snapshotID because:
        // 1. Maintaining index leads to the same time complexity and worse memory complexity
        // 2. Most of the entries are expected to be carried to the next snapshot
        synchronized (registeredKvEntries) {
            Iterator<SharedKvEntry> it = registeredKvEntries.values().iterator();
            while (it.hasNext()) {
                SharedKvEntry entry = it.next();
                if (entry.lastUsedSnapshotID < lowestSnapshotID) {
                    subsumed.add(entry.kvFileHandle);
                    it.remove();
                }
            }
        }
        LOG.trace("Discard {} kv files asynchronously", subsumed.size());
        for (KvFileHandle handle : subsumed) {
            scheduleAsyncDelete(handle);
        }
    }

    public void registerAll(KvSnapshotHandle kvSnapshotHandle, long snapshotID) {
        if (kvSnapshotHandle == null) {
            return;
        }

        synchronized (registeredKvEntries) {
            kvSnapshotHandle.registerKvFileHandles(this, snapshotID);
        }
    }

    public void registerAllAfterRestored(CompletedSnapshot completedSnapshot) {
        registerAll(completedSnapshot.getKvSnapshotHandle(), completedSnapshot.getSnapshotID());
    }

    private void scheduleAsyncDelete(KvFileHandle kvFileHandle) {
        // We do the small optimization to not issue discards for placeholders, which are NOPs.
        if (kvFileHandle != null && !isPlaceholder(kvFileHandle)) {
            LOG.debug("Scheduled delete of kv handle {}.", kvFileHandle);
            AsyncDisposalRunnable asyncDisposalRunnable = new AsyncDisposalRunnable(kvFileHandle);
            asyncDisposalExecutor.execute(asyncDisposalRunnable);
        }
    }

    private boolean isPlaceholder(KvFileHandle kvFileHandle) {
        return kvFileHandle instanceof PlaceholderKvFileHandler;
    }

    @Override
    public void close() throws Exception {
        synchronized (registeredKvEntries) {
            open = false;
        }
    }

    /** Encapsulates the operation the delete state handles asynchronously. */
    private static final class AsyncDisposalRunnable implements Runnable {

        private final KvFileHandle toDispose;

        public AsyncDisposalRunnable(KvFileHandle toDispose) {
            this.toDispose = checkNotNull(toDispose);
        }

        @Override
        public void run() {
            try {
                toDispose.discard();
            } catch (Exception e) {
                LOG.warn(
                        "A problem occurred during asynchronous disposal of a shared kv object: {}",
                        toDispose,
                        e);
            }
        }
    }

    private static final class SharedKvEntry {

        private final long createdBySnapshotID;
        private long lastUsedSnapshotID;
        /** The shared kv file handle. */
        KvFileHandle kvFileHandle;

        SharedKvEntry(KvFileHandle kvFileHandle, long snapshotID) {
            this.kvFileHandle = kvFileHandle;
            this.createdBySnapshotID = snapshotID;
            this.lastUsedSnapshotID = snapshotID;
        }

        private void advanceLastUsingSnapshotID(long snapshotID) {
            lastUsedSnapshotID = Math.max(snapshotID, lastUsedSnapshotID);
        }

        @Override
        public String toString() {
            return "SharedKvEntry{"
                    + "createdBySnapshotID="
                    + createdBySnapshotID
                    + ", lastUsedSnapshotID="
                    + lastUsedSnapshotID
                    + ", kvFileHandle="
                    + kvFileHandle
                    + '}';
        }
    }
}
