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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.server.utils.SnapshotUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * A handle to the snapshot of a kv tablet. It contains the share file handles and the private file
 * handles from which we can rebuild the kv tablet.
 */
public class KvSnapshotHandle {

    private static final Logger LOG = LoggerFactory.getLogger(KvSnapshotHandle.class);

    /** The shared file(like data file) handles of the kv snapshot. */
    private final List<KvFileHandleAndLocalPath> sharedFileHandles;
    /** The private file(like meta file) handles of the kv snapshot. */
    private final List<KvFileHandleAndLocalPath> privateFileHandles;

    /** The size of the incremental snapshot. */
    private final long incrementalSize;

    /**
     * Once the shared states are registered, it is the {@link SharedKvFileRegistry}'s
     * responsibility to cleanup those shared states. But in the cases where the state handle is
     * discarded before performing the registration, the handle should delete all the shared states
     * created by it.
     *
     * <p>This variable is not null iff the handles was registered.
     */
    private transient SharedKvFileRegistry sharedKvFileRegistry;

    public KvSnapshotHandle(
            List<KvFileHandleAndLocalPath> sharedFileHandles,
            List<KvFileHandleAndLocalPath> privateFileHandles,
            long incrementalSize) {
        this.sharedFileHandles = sharedFileHandles;
        this.privateFileHandles = privateFileHandles;
        this.incrementalSize = incrementalSize;
    }

    public List<KvFileHandleAndLocalPath> getSharedKvFileHandles() {
        return sharedFileHandles;
    }

    public List<KvFileHandleAndLocalPath> getPrivateFileHandles() {
        return privateFileHandles;
    }

    public long getIncrementalSize() {
        return incrementalSize;
    }

    /**
     * Returns the total size of all the snapshot. This includes the size of the shared file
     * handles, the size of the private file handles, and the size of the persisted size of this
     * snapshot.
     *
     * @return the size of the snapshot.
     */
    public long getSnapshotSize() {
        long snapshotSize = 0L;

        for (KvFileHandleAndLocalPath handleAndLocalPath : privateFileHandles) {
            snapshotSize += handleAndLocalPath.getKvFileHandle().getSize();
        }

        for (KvFileHandleAndLocalPath handleAndLocalPath : sharedFileHandles) {
            snapshotSize += handleAndLocalPath.getKvFileHandle().getSize();
        }

        return snapshotSize;
    }

    public void discard() {
        SharedKvFileRegistry registry = this.sharedKvFileRegistry;
        final boolean isRegistered = (registry != null);

        try {
            SnapshotUtil.bestEffortDiscardAllKvFiles(
                    privateFileHandles.stream()
                            .map(KvFileHandleAndLocalPath::getKvFileHandle)
                            .collect(Collectors.toList()));
        } catch (Exception e) {
            LOG.warn("Could not properly discard misc file states.", e);
        }

        if (!isRegistered) {
            try {
                SnapshotUtil.bestEffortDiscardAllKvFiles(
                        sharedFileHandles.stream()
                                .map(KvFileHandleAndLocalPath::getKvFileHandle)
                                .collect(Collectors.toSet()));
            } catch (Exception e) {
                LOG.warn("Could not properly discard new sst file states.", e);
            }
        }
    }

    public void registerKvFileHandles(SharedKvFileRegistry registry, long snapshotID) {
        checkState(
                sharedKvFileRegistry != registry,
                "The kv file handle has already registered its shared kv files to the given registry.");

        sharedKvFileRegistry = checkNotNull(registry);

        for (KvFileHandleAndLocalPath handleAndLocalPath : sharedFileHandles) {
            registry.registerReference(
                    SharedKvFileRegistryKey.fromKvFileHandle(handleAndLocalPath.getKvFileHandle()),
                    handleAndLocalPath.getKvFileHandle(),
                    snapshotID);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvSnapshotHandle that = (KvSnapshotHandle) o;
        return incrementalSize == that.incrementalSize
                && Objects.equals(sharedFileHandles, that.sharedFileHandles)
                && Objects.equals(privateFileHandles, that.privateFileHandles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sharedFileHandles, privateFileHandles, incrementalSize);
    }
}
