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
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InsufficientKvLeaderReplicaCapacityException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Controls replica creation admission based on in-memory cluster capacity. */
public class ReplicaCapacityController implements ServerReconfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaCapacityController.class);

    /** Negative capacity means the automatic capacity limit is disabled. */
    public static final long CAPACITY_LIMIT_DISABLED = -1L;

    private final Object lock = new Object();
    private final CoordinatorMetadataCache metadataCache;

    private long kvLeaderReplicaMemoryReservedBytes;
    private long currentKvLeaderReplicaCount;

    public ReplicaCapacityController(
            Configuration conf,
            CoordinatorMetadataCache metadataCache,
            CoordinatorMetricGroup coordinatorMetricGroup) {
        this(conf, metadataCache);
        registerMetrics(
                checkNotNull(coordinatorMetricGroup, "coordinatorMetricGroup should not be null."));
    }

    @VisibleForTesting
    ReplicaCapacityController(Configuration conf, CoordinatorMetadataCache metadataCache) {
        this.metadataCache = checkNotNull(metadataCache, "metadataCache should not be null.");
        long memoryReservedBytes = getKvLeaderReplicaMemoryReservedBytes(conf);
        validateKvLeaderReplicaMemoryReservedBytes(memoryReservedBytes);
        this.kvLeaderReplicaMemoryReservedBytes = memoryReservedBytes;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        long memoryReservedBytes = getKvLeaderReplicaMemoryReservedBytes(newConfig);
        validateKvLeaderReplicaMemoryReservedBytes(memoryReservedBytes);
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        long memoryReservedBytes = getKvLeaderReplicaMemoryReservedBytes(newConfig);
        synchronized (lock) {
            kvLeaderReplicaMemoryReservedBytes = memoryReservedBytes;
        }
    }

    /**
     * Checks whether the requested KV leader replicas fit the current capacity and reserves them if
     * they do.
     */
    public void checkAndIncreaseKvLeaderReplicaCount(long newKvLeaderReplicaCount) {
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

    /** Releases KV leader replicas removed from metadata or reserved by a failed creation. */
    public void decreaseKvLeaderReplicaCount(long removedKvLeaderReplicaCount) {
        if (removedKvLeaderReplicaCount <= 0) {
            return;
        }

        synchronized (lock) {
            currentKvLeaderReplicaCount =
                    Math.max(0, currentKvLeaderReplicaCount - removedKvLeaderReplicaCount);
        }
    }

    /** Rebuilds the current KV leader replica count from metadata. */
    public void rebuildCurrentKvLeaderReplicaCount(MetadataManager metadataManager) {
        long rebuiltCount = 0;
        List<String> databases = metadataManager.listDatabases();
        for (String database : databases) {
            List<String> tables;
            try {
                tables = metadataManager.listTables(database);
            } catch (DatabaseNotExistException e) {
                continue;
            } catch (FlussRuntimeException e) {
                if (!metadataManager.databaseExists(database)) {
                    continue;
                }
                throw e;
            }
            for (String table : tables) {
                TablePath tablePath = TablePath.of(database, table);
                TableInfo tableInfo;
                try {
                    tableInfo = metadataManager.getTable(tablePath);
                } catch (TableNotExistException e) {
                    continue;
                } catch (FlussRuntimeException e) {
                    if (!metadataManager.tableExists(tablePath)) {
                        continue;
                    }
                    throw e;
                }
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
    long getKvLeaderReplicaMemoryReservedBytes() {
        synchronized (lock) {
            return kvLeaderReplicaMemoryReservedBytes;
        }
    }

    private void registerMetrics(CoordinatorMetricGroup coordinatorMetricGroup) {
        coordinatorMetricGroup.gauge(
                MetricNames.KV_LEADER_REPLICA_COUNT, this::getKvLeaderReplicaCount);
        coordinatorMetricGroup.gauge(
                MetricNames.KV_LEADER_REPLICA_CAPACITY, this::getKvLeaderReplicaCapacity);
    }

    private long getKvLeaderReplicaCapacityLocked() {
        if (kvLeaderReplicaMemoryReservedBytes == 0) {
            return CAPACITY_LIMIT_DISABLED;
        }

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
        long capacity = totalMemoryBytes / kvLeaderReplicaMemoryReservedBytes;
        LOG.debug(
                "Calculated KV leader replica capacity: liveTabletServerCount={}, "
                        + "knownMemoryTabletServerCount={}, knownMemoryBytes={}, "
                        + "averageKnownMemoryBytes={}, estimatedTotalMemoryBytes={}, "
                        + "kvLeaderReplicaMemoryReservedBytes={}, kvLeaderReplicaCapacity={}.",
                liveTabletServerCount,
                knownMemoryTabletServerCount,
                knownMemoryBytes,
                averageKnownMemoryBytes,
                totalMemoryBytes,
                kvLeaderReplicaMemoryReservedBytes,
                capacity);
        return capacity;
    }

    private static long getKvLeaderReplicaMemoryReservedBytes(Configuration conf) {
        return conf.get(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED).getBytes();
    }

    private static void validateKvLeaderReplicaMemoryReservedBytes(long memoryReservedBytes) {
        if (memoryReservedBytes < 0) {
            throw new ConfigException(
                    ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED.key()
                            + " must be greater than or equal to 0.");
        }
    }

    private static long getKvLeaderReplicaCount(
            TableInfo tableInfo, MetadataManager metadataManager) {
        if (!tableInfo.hasPrimaryKey()) {
            return 0;
        }
        if (!tableInfo.isPartitioned()) {
            return tableInfo.getNumBuckets();
        }
        try {
            return (long) metadataManager.getPartitions(tableInfo.getTablePath()).size()
                    * tableInfo.getNumBuckets();
        } catch (FlussRuntimeException e) {
            if (!metadataManager.tableExists(tableInfo.getTablePath())) {
                return 0;
            }
            throw e;
        }
    }
}
