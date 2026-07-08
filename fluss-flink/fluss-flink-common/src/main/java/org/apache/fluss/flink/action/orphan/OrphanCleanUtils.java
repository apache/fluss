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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
     * three-level fallback: partition-level → table-level → cluster-level. At least one level is
     * always set because the coordinator assigns a {@code remoteDataDir} to every table at creation
     * time via {@code RemoteDirSelector.nextDataDir()}.
     */
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
        return checkNotNull(
                clusterRemoteDataDir,
                "No remote data directory resolvable: partition, table, "
                        + "and cluster levels are all null. This should not happen because the "
                        + "coordinator requires remote.data.dir or remote.data.dirs at startup.");
    }

    /**
     * Resolves the cluster-level {@code remote.data.dir} by querying the coordinator's runtime
     * configuration. Returns {@code null} when the cluster uses {@code remote.data.dirs}
     * (multi-directory mode) without the legacy single {@code remote.data.dir}.
     */
    @Nullable
    public static String resolveClusterRemoteDataDir(Admin admin) throws Exception {
        return resolveClusterRemoteDataDir(fetchClusterConfigMap(admin));
    }

    /** Extracts the single-root {@code remote.data.dir} from a pre-fetched config map. */
    @Nullable
    public static String resolveClusterRemoteDataDir(Map<String, String> configMap) {
        return configMap.get(ConfigOptions.REMOTE_DATA_DIR.key());
    }

    /**
     * Resolves all cluster-level remote data directories by querying the coordinator's runtime
     * configuration. Reads both the single-root {@code remote.data.dir} and the multi-root {@code
     * remote.data.dirs}, deduplicates by normalized form, and returns the union as the canonical
     * root list.
     *
     * <p>This is the authoritative source for determining what storage roots the cleanup action is
     * allowed to touch.
     *
     * @return list of normalized roots (no trailing slash); never {@code null}, may be empty if the
     *     cluster has neither config set (which should not happen because the coordinator requires
     *     at least one remote data dir at startup).
     */
    public static List<String> resolveClusterRemoteDataDirs(Admin admin) throws Exception {
        return resolveClusterRemoteDataDirs(fetchClusterConfigMap(admin));
    }

    /** Extracts all remote data roots from a pre-fetched config map. */
    public static List<String> resolveClusterRemoteDataDirs(Map<String, String> configMap) {
        Configuration conf = Configuration.fromMap(configMap);
        LinkedHashSet<String> roots = new LinkedHashSet<String>();
        String singleDir = conf.get(ConfigOptions.REMOTE_DATA_DIR);
        if (singleDir != null && !singleDir.isEmpty()) {
            roots.add(normalizeRoot(singleDir));
        }
        List<String> multiDirs = conf.get(ConfigOptions.REMOTE_DATA_DIRS);
        if (multiDirs != null) {
            for (String dir : multiDirs) {
                if (dir != null && !dir.isEmpty()) {
                    roots.add(normalizeRoot(dir));
                }
            }
        }
        return new ArrayList<String>(roots);
    }

    /**
     * Fetches the coordinator's runtime configuration as a key-value map. Use this once and pass
     * the result to the map-based overloads of {@link #resolveClusterRemoteDataDir(Map)} and {@link
     * #resolveClusterRemoteDataDirs(Map)} to avoid duplicate RPCs.
     */
    public static Map<String, String> fetchClusterConfigMap(Admin admin) throws Exception {
        Collection<ConfigEntry> entries = admin.describeClusterConfigs().get();
        Map<String, String> map = new HashMap<String, String>();
        for (ConfigEntry entry : entries) {
            if (entry.value() != null) {
                map.put(entry.key(), entry.value());
            }
        }
        return map;
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

    /** Formats a bucket-scope key for audit/logging purposes. */
    public static String bucketScopeKey(long tableId, Long partitionId, int bucketId) {
        return tableId + ":" + partitionId + ":" + bucketId;
    }
}
