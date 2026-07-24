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

package org.apache.fluss.kv.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

    /**
     * Discards only the private files of this snapshot. Use this when shared files are managed
     * externally by a registry.
     */
    public void discardPrivateFiles() {
        try {
            SnapshotUtil.bestEffortDiscardAllKvFiles(
                    privateFileHandles.stream()
                            .map(KvFileHandleAndLocalPath::getKvFileHandle)
                            .collect(Collectors.toList()));
        } catch (Exception e) {
            LOG.warn("Could not properly discard private file states.", e);
        }
    }

    /**
     * Discards all files (private + shared). Use this for failed or aborted snapshots that were
     * never registered with a shared file registry.
     */
    public void discardAll() {
        discardPrivateFiles();

        try {
            SnapshotUtil.bestEffortDiscardAllKvFiles(
                    sharedFileHandles.stream()
                            .map(KvFileHandleAndLocalPath::getKvFileHandle)
                            .collect(Collectors.toSet()));
        } catch (Exception e) {
            LOG.warn("Could not properly discard shared sst file states.", e);
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
