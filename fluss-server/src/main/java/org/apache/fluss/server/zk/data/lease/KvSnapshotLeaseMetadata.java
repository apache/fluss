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

package org.apache.fluss.server.zk.data.lease;

import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.metrics.registry.MetricRegistry.LOG;

/** The zkNode data of kv snapshot lease. */
public class KvSnapshotLeaseMetadata {
    private final long expirationTime;
    private final Map<Long, FsPath> tableIdToRemoteMetadataFilePath;

    public KvSnapshotLeaseMetadata(
            long expirationTime, Map<Long, FsPath> tableIdToRemoteMetadataFilePath) {
        this.expirationTime = expirationTime;
        this.tableIdToRemoteMetadataFilePath = tableIdToRemoteMetadataFilePath;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public Map<Long, FsPath> getTableIdToRemoteMetadataFilePath() {
        return tableIdToRemoteMetadataFilePath;
    }

    public void discard() {
        // delete all remote metadata file.
        tableIdToRemoteMetadataFilePath.values().forEach(this::delete);
    }

    private void delete(FsPath fsPath) {
        try {
            FileSystem fileSystem = fsPath.getFileSystem();
            if (fileSystem.exists(fsPath)) {
                fileSystem.delete(fsPath, false);
            }
        } catch (IOException e) {
            LOG.warn(
                    "Error deleting remote file path of kv snapshot lease metadata at {}",
                    fsPath,
                    e);
        }
    }

    @Override
    public String toString() {
        return "KvSnapshotLeaseMetadata{"
                + "expirationTime="
                + expirationTime
                + ", tableIdToRemoteMetadataFilePath="
                + tableIdToRemoteMetadataFilePath
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvSnapshotLeaseMetadata that = (KvSnapshotLeaseMetadata) o;
        return expirationTime == that.expirationTime
                && tableIdToRemoteMetadataFilePath.equals(that.tableIdToRemoteMetadataFilePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expirationTime, tableIdToRemoteMetadataFilePath);
    }
}
