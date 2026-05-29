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

package org.apache.fluss.flink.action.orphan;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Shared utility methods for the orphan files cleanup action. */
@Internal
public final class OrphanCleanUtils {

    private OrphanCleanUtils() {}

    /**
     * Constructs a {@link PhysicalTablePath} from a table path and an optional partition. Returns
     * the non-partitioned form when {@code partitionInfo} is null.
     */
    public static PhysicalTablePath physicalPath(
            TablePath tablePath, @Nullable PartitionInfo partitionInfo) {
        if (partitionInfo == null) {
            return PhysicalTablePath.of(tablePath);
        }
        return PhysicalTablePath.of(tablePath, partitionInfo.getPartitionName());
    }

    /**
     * Enumerates all {@link TableBucket} instances for a table (or a single partition of that
     * table).
     */
    public static List<TableBucket> enumerateBuckets(
            TableInfo tableInfo, @Nullable PartitionInfo partitionInfo) {
        int n = tableInfo.getNumBuckets();
        List<TableBucket> buckets = new ArrayList<TableBucket>(n);
        long tableId = tableInfo.getTableId();
        for (int b = 0; b < n; b++) {
            if (partitionInfo == null) {
                buckets.add(new TableBucket(tableId, b));
            } else {
                buckets.add(new TableBucket(tableId, partitionInfo.getPartitionId(), b));
            }
        }
        return buckets;
    }

    /**
     * Resolves the effective remote data directory for a table/partition target using the
     * three-level fallback: partition-level → table-level → cluster-level.
     */
    @Nullable
    public static String resolveRemoteDataDir(
            TableInfo tableInfo,
            @Nullable PartitionInfo partitionInfo,
            @Nullable String clusterRemoteDataDir) {
        if (partitionInfo != null && partitionInfo.getRemoteDataDir() != null) {
            return partitionInfo.getRemoteDataDir();
        }
        if (tableInfo.getRemoteDataDir() != null) {
            return tableInfo.getRemoteDataDir();
        }
        return clusterRemoteDataDir;
    }

    /**
     * Resolves the cluster-level {@code remote.data.dir} by querying the coordinator's runtime
     * configuration.
     */
    @Nullable
    public static String resolveClusterRemoteDataDir(Admin admin) throws Exception {
        Collection<ConfigEntry> entries = admin.describeClusterConfigs().get();
        Map<String, String> map = new HashMap<String, String>();
        for (ConfigEntry entry : entries) {
            map.put(entry.key(), entry.value());
        }
        return map.get(ConfigOptions.REMOTE_DATA_DIR.key());
    }

    /** Constructs a remote sub-directory path, normalizing trailing slashes on the root. */
    public static FsPath remoteSubDir(String remoteDataDir, String subDir) {
        return new FsPath(normalizeRoot(remoteDataDir) + "/" + subDir);
    }

    /** Strips a trailing slash from a remote data directory string. */
    public static String normalizeRoot(String remoteDataDir) {
        return remoteDataDir.endsWith("/")
                ? remoteDataDir.substring(0, remoteDataDir.length() - 1)
                : remoteDataDir;
    }

    /**
     * Lists the entries of a directory, returning {@code null} on {@link IOException} (directory
     * does not exist or is inaccessible).
     */
    @Nullable
    public static FileStatus[] listStatuses(FileSystem fs, FsPath dir) {
        try {
            return fs.listStatus(dir);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Returns the {@link FileSystem} for a path if the path exists, or {@code null} otherwise.
     *
     * @throws IOException if resolving the filesystem itself fails
     */
    @Nullable
    public static FileSystem getFileSystemIfExists(FsPath dir) throws IOException {
        FileSystem fs = dir.getFileSystem();
        return fs.exists(dir) ? fs : null;
    }

    /** Formats a bucket-scope key for audit/logging purposes. */
    public static String bucketScopeKey(long tableId, Long partitionId, int bucketId) {
        return tableId + ":" + partitionId + ":" + bucketId;
    }
}
