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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.InsufficientKvLeaderReplicaCapacityException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.ServerInfo;

import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Manages the in-memory cluster capacity for KV leader replicas. */
public class KvLeaderReplicaCapacityManager implements ServerReconfigurable {

    /** Negative capacity means the automatic capacity limit is disabled. */
    public static final long CAPACITY_LIMIT_DISABLED = -1L;

    private final Object lock = new Object();
    private final CoordinatorMetadataCache metadataCache;

    private long leaderReplicaMemoryReservedBytes;
    private long currentKvLeaderReplicaCount;

    public KvLeaderReplicaCapacityManager(
            Configuration conf, CoordinatorMetadataCache metadataCache) {
        this.metadataCache = checkNotNull(metadataCache, "metadataCache should not be null.");
        this.leaderReplicaMemoryReservedBytes = getLeaderReplicaMemoryReservedBytes(conf);
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        long memoryReservedBytes = getLeaderReplicaMemoryReservedBytes(newConfig);
        if (memoryReservedBytes <= 0) {
            throw new ConfigException(
                    ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED.key()
                            + " must be greater than 0.");
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        long memoryReservedBytes = getLeaderReplicaMemoryReservedBytes(newConfig);
        synchronized (lock) {
            leaderReplicaMemoryReservedBytes = memoryReservedBytes;
        }
    }

    /**
     * Checks whether the requested replicas fit the current capacity and reserves them if they do.
     */
    public void checkAndIncrease(long newKvLeaderReplicaCount) {
        if (newKvLeaderReplicaCount <= 0) {
            return;
        }

        synchronized (lock) {
            long capacity = getKvLeaderReplicaCapacityLocked();
            long newTotal = currentKvLeaderReplicaCount + newKvLeaderReplicaCount;
            if (capacity != CAPACITY_LIMIT_DISABLED && newTotal > capacity) {
                throw new InsufficientKvLeaderReplicaCapacityException(
                        String.format(
                                "Not enough KV leader replica capacity. "
                                        + "currentKvLeaderReplicaCount=%s, "
                                        + "newKvLeaderReplicaCount=%s, "
                                        + "kvLeaderReplicaCapacity=%s.",
                                currentKvLeaderReplicaCount, newKvLeaderReplicaCount, capacity));
            }

            currentKvLeaderReplicaCount = newTotal;
        }
    }

    /** Releases replicas that were removed from metadata or reserved by a failed creation. */
    public void decrease(long removedKvLeaderReplicaCount) {
        if (removedKvLeaderReplicaCount <= 0) {
            return;
        }

        synchronized (lock) {
            currentKvLeaderReplicaCount =
                    Math.max(0, currentKvLeaderReplicaCount - removedKvLeaderReplicaCount);
        }
    }

    /** Rebuilds the current KV leader replica count from metadata. */
    public void rebuildCurrentCount(MetadataManager metadataManager) {
        long rebuiltCount = 0;
        List<String> databases = metadataManager.listDatabases();
        for (String database : databases) {
            List<String> tables = metadataManager.listTables(database);
            for (String table : tables) {
                TablePath tablePath = TablePath.of(database, table);
                TableInfo tableInfo = metadataManager.getTable(tablePath);
                rebuiltCount += getKvLeaderReplicaCount(tableInfo, metadataManager);
            }
        }

        synchronized (lock) {
            currentKvLeaderReplicaCount = rebuiltCount;
        }
    }

    /** Returns the current in-memory KV leader replica count. */
    public long getKvLeaderReplicaCount() {
        synchronized (lock) {
            return currentKvLeaderReplicaCount;
        }
    }

    /** Returns the current cluster KV leader replica capacity, or -1 if disabled. */
    public long getKvLeaderReplicaCapacity() {
        synchronized (lock) {
            return getKvLeaderReplicaCapacityLocked();
        }
    }

    @VisibleForTesting
    long getLeaderReplicaMemoryReservedBytes() {
        synchronized (lock) {
            return leaderReplicaMemoryReservedBytes;
        }
    }

    private long getKvLeaderReplicaCapacityLocked() {
        Set<ServerInfo> liveTabletServers = metadataCache.getLiveTabletServerInfos();
        int liveTabletServerCount = liveTabletServers.size();
        if (liveTabletServerCount == 0) {
            return CAPACITY_LIMIT_DISABLED;
        }

        int knownMemoryTabletServerCount = 0;
        long knownMemoryBytes = 0;
        for (ServerInfo serverInfo : liveTabletServers) {
            if (serverInfo.resource().hasMemory()) {
                knownMemoryTabletServerCount++;
                knownMemoryBytes += serverInfo.resource().getMemoryBytes();
            }
        }

        if (knownMemoryTabletServerCount * 2 <= liveTabletServerCount) {
            return CAPACITY_LIMIT_DISABLED;
        }

        long averageKnownMemoryBytes = knownMemoryBytes / knownMemoryTabletServerCount;
        long totalMemoryBytes =
                knownMemoryBytes
                        + averageKnownMemoryBytes
                                * (liveTabletServerCount - knownMemoryTabletServerCount);
        return totalMemoryBytes / leaderReplicaMemoryReservedBytes;
    }

    private static long getLeaderReplicaMemoryReservedBytes(Configuration conf) {
        return conf.get(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED).getBytes();
    }

    private static long getKvLeaderReplicaCount(
            TableInfo tableInfo, MetadataManager metadataManager) {
        if (!tableInfo.hasPrimaryKey()) {
            return 0;
        }
        if (!tableInfo.isPartitioned()) {
            return tableInfo.getNumBuckets();
        }
        return (long) metadataManager.getPartitions(tableInfo.getTablePath()).size()
                * tableInfo.getNumBuckets();
    }
}
