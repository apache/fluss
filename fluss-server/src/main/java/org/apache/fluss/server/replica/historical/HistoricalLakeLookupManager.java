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

package org.apache.fluss.server.replica.historical;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LakeStorageNotConfiguredException;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Handles server-side point lookup for historical partitions stored in lake storage.
 *
 * <p>Creating a lake table lookuper may initialize catalog, table, and query state and allocate
 * local lookup files, so lookupers are cached and reused. The cache is keyed by table ID rather
 * than table path to prevent a deleted and recreated table from reusing the old table's lookuper. A
 * cached lookuper is replaced when its schema ID or lake configuration version no longer matches
 * the current request. Active lookups can finish on the old lookuper, which is closed after its
 * last lookup releases it. A new required lake snapshot refreshes the cached lookuper in place.
 *
 * <p>Up to ten table lookupers are cached. Each lookuper receives one tenth of the server-level
 * disk budget, and Caffeine evicts lookupers when the table limit is exceeded.
 *
 * <p>Historical lookup cache I/O participates in TabletServer disk write protection. Existing cache
 * hits remain available when the data disk is write-locked, while lookups that need to download new
 * cache files are rejected until the disk recovers.
 *
 * <p>A lookuper is closed when replaced, explicitly invalidated by a replica lifecycle event,
 * evicted when the table limit is exceeded, the manager shuts down, or after the configured idle
 * expiration. Caffeine expiration is scheduled on the shared TabletServer scheduler, allowing idle
 * resources to be released even if no subsequent lookup accesses the cache.
 */
class HistoricalLakeLookupManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalLakeLookupManager.class);

    private static final String LOOKUPER_CACHE_EXPIRATION_TASK_NAME =
            "historical-lookuper-cache-expiration";
    private static final String LOOKUP_CACHE_DISK_SIZE_TASK_NAME =
            "historical-lookup-cache-disk-size";
    private static final Duration LOOKUP_CACHE_DISK_SIZE_CHECK_INTERVAL = Duration.ofMinutes(3);
    // TODO: Share one Paimon IOManager disk budget across all table lookupers and evict cached
    // entries by data file instead of reserving fixed per-table capacity. See
    // https://github.com/apache/fluss/issues/3955.
    private static final int MAX_CACHED_TABLES = 10;

    private volatile Configuration conf;
    private volatile long lakeConfigVersion;
    private final @Nullable PluginManager pluginManager;
    private final Counter capacityEvictions;
    private final Cache<Long, CachedLakeTableLookuper> lakeTableLookupers;
    private final ConcurrentMap<Long, Long> requiredLakeSnapshotIds = new ConcurrentHashMap<>();
    private final File historicalLookupCacheRootDir;
    private final long dataDirVolumeBytes;
    // TODO: Introduce a minimum lookup cache disk ratio (default 0.01). When disk usage is high,
    // evict cached entries down to the minimum ratio instead of clearing the entire cache; allow
    // the cache to grow back to the maximum ratio after disk usage recovers.
    private final Runnable diskWriteGuard;

    private volatile long lookupCacheMaxDiskBytesPerTable;
    private volatile long lookupCacheDiskSize;

    private volatile boolean started;

    /** Creates a historical lake lookup manager. */
    HistoricalLakeLookupManager(
            Configuration conf,
            @Nullable PluginManager pluginManager,
            LocalDiskManager localDiskManager,
            File dataDir,
            long dataDirVolumeBytes,
            Scheduler scheduler) {
        this(
                conf,
                pluginManager,
                dataDir,
                dataDirVolumeBytes,
                Ticker.systemTicker(),
                createCacheScheduler(scheduler),
                checkNotNull(localDiskManager, "localDiskManager must not be null.")
                        ::ensureWritable);
    }

    @VisibleForTesting
    HistoricalLakeLookupManager(
            Configuration conf,
            @Nullable PluginManager pluginManager,
            File dataDir,
            long dataDirVolumeBytes,
            Ticker ticker,
            com.github.benmanes.caffeine.cache.Scheduler cacheScheduler,
            Runnable diskWriteGuard) {
        this.conf = checkNotNull(conf, "conf must not be null.");
        this.pluginManager = pluginManager;
        this.historicalLookupCacheRootDir =
                FlussPaths.historicalLookupRootDir(
                        checkNotNull(dataDir, "dataDir must not be null."));
        checkArgument(dataDirVolumeBytes > 0, "dataDirVolumeBytes must be greater than 0.");
        this.dataDirVolumeBytes = dataDirVolumeBytes;
        this.diskWriteGuard = checkNotNull(diskWriteGuard, "diskWriteGuard must not be null.");
        this.lookupCacheMaxDiskBytesPerTable =
                cacheBytesPerTable(
                        conf.get(
                                ConfigOptions
                                        .SERVER_HISTORICAL_PARTITION_LOOKUP_CACHE_MAX_DISK_RATIO));
        this.capacityEvictions = new ThreadSafeSimpleCounter();
        this.lakeTableLookupers =
                Caffeine.newBuilder()
                        .maximumSize(MAX_CACHED_TABLES)
                        .expireAfterAccess(
                                conf.get(
                                        ConfigOptions
                                                .SERVER_HISTORICAL_PARTITION_LOOKUPER_CACHE_EXPIRE_AFTER_ACCESS))
                        .ticker(checkNotNull(ticker, "ticker must not be null."))
                        .scheduler(checkNotNull(cacheScheduler, "cacheScheduler must not be null."))
                        .executor(Runnable::run)
                        .removalListener(this::onLookuperRemoved)
                        .build();
    }

    private static com.github.benmanes.caffeine.cache.Scheduler createCacheScheduler(
            Scheduler scheduler) {
        checkNotNull(scheduler, "scheduler must not be null.");
        // Schedule expiration maintenance so idle lookupers are closed even if no more lookups
        // arrive.
        return (executor, command, delay, timeUnit) ->
                scheduler.scheduleOnce(
                        LOOKUPER_CACHE_EXPIRATION_TASK_NAME,
                        () -> executor.execute(command),
                        timeUnit.toMillis(delay));
    }

    /**
     * Attempts to clean lookup cache files left by a previous TabletServer process.
     *
     * <p>The cache root under this server's first data directory is removed and recreated before
     * lookups are accepted.
     */
    synchronized void startup(Scheduler scheduler) {
        checkNotNull(scheduler, "scheduler must not be null.");
        if (started) {
            return;
        }
        try {
            FileUtils.deleteDirectory(historicalLookupCacheRootDir);
        } catch (IOException e) {
            LOG.warn(
                    "Failed to clean historical lookup cache directory {}.",
                    historicalLookupCacheRootDir,
                    e);
        }
        try {
            Files.createDirectories(historicalLookupCacheRootDir.toPath());
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    "Failed to create historical lookup cache directory: "
                            + historicalLookupCacheRootDir,
                    e);
        }
        scheduler.schedule(
                LOOKUP_CACHE_DISK_SIZE_TASK_NAME,
                this::updateLookupCacheDiskSize,
                0L,
                LOOKUP_CACHE_DISK_SIZE_CHECK_INTERVAL.toMillis());
        started = true;
    }

    /** Looks up a batch of keys from one historical lake partition. */
    List<byte[]> lookup(
            LookupDataForBucket lookupData,
            TableInfo tableInfo,
            SchemaInfo schemaInfo,
            ResolvedPartitionSpec originalPartitionSpec,
            LakeTableLookuper.LookupMetricRecorder lookupMetricRecorder)
            throws Exception {
        LakeTableLookuper.LookupMetricRecorder checkedMetricRecorder =
                checkNotNull(lookupMetricRecorder, "lookupMetricRecorder must not be null.");
        checkState(started, "Historical lake lookup manager has not been started.");
        LookupContext context =
                createLookupContext(
                        lookupData,
                        tableInfo,
                        schemaInfo,
                        originalPartitionSpec,
                        checkedMetricRecorder);
        CachedLakeTableLookuper cachedLookuper = acquireLookuper(context, tableInfo);
        try {
            List<byte[]> values = new ArrayList<>(lookupData.keys().size());
            for (byte[] key : lookupData.keys()) {
                values.add(cachedLookuper.lookuper.lookup(key, context.lookupContext));
            }
            return values;
        } finally {
            cachedLookuper.release();
        }
    }

    @Override
    public void close() {
        lakeTableLookupers.invalidateAll();
        lakeTableLookupers.cleanUp();
        requiredLakeSnapshotIds.clear();
    }

    /** Invalidates the cached lake lookuper for the given table. */
    void invalidateTableLookuper(long tableId) {
        requiredLakeSnapshotIds.remove(tableId);
        lakeTableLookupers.invalidate(tableId);
    }

    /** Records the required opaque lake snapshot ID for the next in-place file refresh. */
    void requireLakeSnapshot(long tableId, long snapshotId) {
        requiredLakeSnapshotIds.put(tableId, snapshotId);
    }

    /** Returns the number of table lookupers currently cached. */
    int cachedTableCount() {
        return lakeTableLookupers.asMap().size();
    }

    /** Returns the counter for table lookuper evictions caused by the cached table limit. */
    Counter capacityEvictions() {
        return capacityEvictions;
    }

    /** Applies dynamic historical lookup configuration changes. */
    void reconfigure(Configuration newConf) {
        checkNotNull(newConf, "newConf must not be null.");
        boolean lakeConfigChanged;
        boolean cacheLimitChanged;
        boolean expirationChanged;
        Duration newExpiration =
                newConf.get(
                        ConfigOptions
                                .SERVER_HISTORICAL_PARTITION_LOOKUPER_CACHE_EXPIRE_AFTER_ACCESS);
        synchronized (this) {
            long newMaxBytesPerTable =
                    cacheBytesPerTable(
                            newConf.get(
                                    ConfigOptions
                                            .SERVER_HISTORICAL_PARTITION_LOOKUP_CACHE_MAX_DISK_RATIO));
            cacheLimitChanged = newMaxBytesPerTable != lookupCacheMaxDiskBytesPerTable;
            lookupCacheMaxDiskBytesPerTable = newMaxBytesPerTable;

            lakeConfigChanged = hasLakeConfigChanged(conf, newConf);
            expirationChanged =
                    !newExpiration.equals(
                            conf.get(
                                    ConfigOptions
                                            .SERVER_HISTORICAL_PARTITION_LOOKUPER_CACHE_EXPIRE_AFTER_ACCESS));
            // Publish the configuration before its version. A lookup that observes the new version
            // must also observe the matching configuration snapshot.
            conf = newConf;
            if (lakeConfigChanged) {
                lakeConfigVersion++;
            }
        }
        if (expirationChanged) {
            lakeTableLookupers
                    .policy()
                    .expireAfterAccess()
                    .get()
                    .setExpiresAfter(newExpiration.toMillis(), TimeUnit.MILLISECONDS);
        }
        if (lakeConfigChanged || cacheLimitChanged) {
            // Do not invalidate while holding this monitor: lookuper creation holds a cache key
            // lock before preparing the lookup directory under the same monitor. Invalidation
            // closes inactive lookupers immediately and active lookupers after their last lookup
            // releases them. After a cache limit change, the next lookup creates a Paimon lookuper
            // with the updated per-table limit.
            lakeTableLookupers.invalidateAll();
            lakeTableLookupers.cleanUp();
        }
    }

    private void onLookuperRemoved(
            Long ignored, @Nullable CachedLakeTableLookuper cachedLookuper, RemovalCause cause) {
        if (cachedLookuper == null) {
            return;
        }
        if (cause == RemovalCause.SIZE) {
            capacityEvictions.inc();
            LOG.info(
                    "Evicted historical lookup cache for table {} (table ID {}) because the cache retains at most {} tables.",
                    cachedLookuper.tablePath,
                    cachedLookuper.tableId,
                    MAX_CACHED_TABLES);
        }
        cachedLookuper.invalidate();
    }

    private LookupContext createLookupContext(
            LookupDataForBucket lookupData,
            TableInfo tableInfo,
            SchemaInfo schemaInfo,
            ResolvedPartitionSpec originalPartitionSpec,
            LakeTableLookuper.LookupMetricRecorder lookupMetricRecorder) {
        TableBucket tableBucket = lookupData.tableBucket();
        TablePath tablePath = tableInfo.getLakeTablePath();
        LakeTableLookuper.LookupContext lookupContext =
                new LakeTableLookuper.LookupContext(
                        originalPartitionSpec,
                        tableBucket.getBucket(),
                        (short) schemaInfo.getSchemaId(),
                        schemaInfo.getSchema().getRowType(),
                        lookupMetricRecorder);
        return new LookupContext(
                tableInfo.getTableId(), schemaInfo.getSchemaId(), tablePath, lookupContext);
    }

    LakeTableLookuper createLakeTableLookuper(
            TablePath tablePath,
            String ioTmpDir,
            TableConfig tableConfig,
            long cacheSizeBytes,
            Configuration clusterConf) {
        DataLakeFormat dataLakeFormat = clusterConf.get(ConfigOptions.DATALAKE_FORMAT);
        if (dataLakeFormat == null) {
            throw new LakeStorageNotConfiguredException(
                    "Historical lookup requires cluster lake storage to be configured.");
        }
        if (dataLakeFormat != DataLakeFormat.PAIMON) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Historical lookup only supports Paimon lake storage, but cluster uses %s.",
                            dataLakeFormat));
        }

        Map<String, String> lakeProperties = extractLakeProperties(clusterConf);
        if (lakeProperties == null) {
            throw new LakeStorageNotConfiguredException(
                    "Historical lookup requires cluster lake storage properties to be configured.");
        }

        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat.toString(), pluginManager);
        LakeStorage lakeStorage =
                lakeStoragePlugin.createLakeStorage(Configuration.fromMap(lakeProperties));
        return lakeStorage.createLakeTableLookuper(
                tablePath,
                new LakeStorage.LookuperContext(
                        ioTmpDir, tableConfig, cacheSizeBytes, diskWriteGuard));
    }

    private static boolean hasLakeConfigChanged(Configuration currentConf, Configuration newConf) {
        return currentConf.get(ConfigOptions.DATALAKE_FORMAT)
                        != newConf.get(ConfigOptions.DATALAKE_FORMAT)
                || !Objects.equals(
                        extractLakeProperties(currentConf), extractLakeProperties(newConf));
    }

    private long cacheBytesPerTable(double ratio) {
        checkArgument(ratio > 0.0 && ratio <= 1.0, "ratio must be within (0.0, 1.0].");
        long totalCacheBytes =
                Math.min(dataDirVolumeBytes, (long) Math.ceil(dataDirVolumeBytes * ratio));
        return Math.max(1L, totalCacheBytes / MAX_CACHED_TABLES);
    }

    /** Returns the most recently sampled historical lookup cache footprint, in bytes. */
    long lookupCacheDiskSize() {
        return lookupCacheDiskSize;
    }

    private void updateLookupCacheDiskSize() {
        if (!historicalLookupCacheRootDir.exists()) {
            lookupCacheDiskSize = 0L;
            return;
        }
        try (Stream<Path> paths = Files.walk(historicalLookupCacheRootDir.toPath())) {
            lookupCacheDiskSize =
                    paths.filter(Files::isRegularFile)
                            .mapToLong(HistoricalLakeLookupManager::fileSize)
                            .sum();
        } catch (IOException | UncheckedIOException e) {
            LOG.warn(
                    "Failed to calculate historical lookup cache usage under {}. Keeping the last sampled value of {} bytes.",
                    historicalLookupCacheRootDir,
                    lookupCacheDiskSize,
                    e);
        }
    }

    private static long fileSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void closeLookuper(CachedLakeTableLookuper cachedLookuper) {
        closeLookuper(cachedLookuper.lookuper, cachedLookuper.tableLookupDir);
    }

    private static void closeLookuper(LakeTableLookuper lookuper, File tableLookupDir) {
        try {
            IOUtils.closeQuietly(lookuper, "historical lake table lookuper");
        } finally {
            deleteTableLookupDirIfEmpty(tableLookupDir);
        }
    }

    private static void deleteTableLookupDirIfEmpty(File tableLookupDir) {
        if (FileUtils.isDirectoryEmpty(tableLookupDir)) {
            try {
                Files.deleteIfExists(tableLookupDir.toPath());
            } catch (IOException e) {
                LOG.debug(
                        "Failed to delete empty historical lookup directory {}.",
                        tableLookupDir,
                        e);
            }
        }
    }

    private CachedLakeTableLookuper acquireLookuper(LookupContext context, TableInfo tableInfo) {
        long currentLakeConfigVersion = lakeConfigVersion;
        Configuration currentConf = conf;
        long cacheSizeBytes = lookupCacheMaxDiskBytesPerTable;
        return lakeTableLookupers
                .asMap()
                .compute(
                        context.tableId,
                        (ignored, currentLookuper) -> {
                            // Read inside the per-table atomic update to avoid refreshing back to a
                            // snapshot captured while waiting for another lookup to finish updating
                            // this lookuper.
                            Long requiredLakeSnapshotId =
                                    requiredLakeSnapshotIds.get(context.tableId);
                            CachedLakeTableLookuper selectedLookuper = currentLookuper;
                            // Create the lookuper lazily, and recreate it after schema,
                            // lake configuration, or server cache size changes so it
                            // reloads lake table/query state and uses the current
                            // settings.
                            if (selectedLookuper == null
                                    || selectedLookuper.schemaId != context.schemaId
                                    || selectedLookuper.lakeConfigVersion
                                            != currentLakeConfigVersion
                                    || selectedLookuper.cacheSizeBytes != cacheSizeBytes) {
                                File tableLookupDir =
                                        FlussPaths.historicalLookupTableDir(
                                                historicalLookupCacheRootDir,
                                                context.tablePath,
                                                context.tableId);
                                LakeTableLookuper lookuper =
                                        createLakeTableLookuper(
                                                context.tablePath,
                                                tableLookupDir.getAbsolutePath(),
                                                tableInfo.getTableConfig(),
                                                cacheSizeBytes,
                                                currentConf);
                                selectedLookuper =
                                        new CachedLakeTableLookuper(
                                                context.tableId,
                                                context.tablePath,
                                                context.schemaId,
                                                currentLakeConfigVersion,
                                                cacheSizeBytes,
                                                requiredLakeSnapshotId,
                                                tableLookupDir,
                                                lookuper);
                            }
                            // Pin the lookuper before leaving the atomic cache update.
                            // Eviction or invalidation can then defer closing it until
                            // this lookup releases it.
                            selectedLookuper.acquire(requiredLakeSnapshotId);
                            return selectedLookuper;
                        });
    }

    private static final class LookupContext {
        private final long tableId;
        private final int schemaId;
        private final TablePath tablePath;
        private final LakeTableLookuper.LookupContext lookupContext;

        private LookupContext(
                long tableId,
                int schemaId,
                TablePath tablePath,
                LakeTableLookuper.LookupContext lookupContext) {
            this.tableId = tableId;
            this.schemaId = schemaId;
            this.tablePath = tablePath;
            this.lookupContext = lookupContext;
        }
    }

    private static final class CachedLakeTableLookuper {
        private final long tableId;
        private final TablePath tablePath;
        private final int schemaId;
        private final long lakeConfigVersion;
        private final long cacheSizeBytes;
        /** The opaque lake snapshot ID covered by the last file refresh, or null if none. */
        private @Nullable Long lakeSnapshotId;

        private final File tableLookupDir;
        private final LakeTableLookuper lookuper;
        private int activeLookups;
        private boolean invalidated;
        private boolean closed;

        private CachedLakeTableLookuper(
                long tableId,
                TablePath tablePath,
                int schemaId,
                long lakeConfigVersion,
                long cacheSizeBytes,
                @Nullable Long lakeSnapshotId,
                File tableLookupDir,
                LakeTableLookuper lookuper) {
            this.tableId = tableId;
            this.tablePath = tablePath;
            this.schemaId = schemaId;
            this.lakeConfigVersion = lakeConfigVersion;
            this.cacheSizeBytes = cacheSizeBytes;
            this.lakeSnapshotId = lakeSnapshotId;
            this.tableLookupDir = tableLookupDir;
            this.lookuper = lookuper;
        }

        private synchronized void acquire(@Nullable Long requiredLakeSnapshotId) {
            if (invalidated) {
                throw new IllegalStateException("Lake table lookuper has been invalidated.");
            }
            if (!Objects.equals(lakeSnapshotId, requiredLakeSnapshotId)) {
                lookuper.requestRefresh();
                lakeSnapshotId = requiredLakeSnapshotId;
            }
            activeLookups++;
        }

        private void release() {
            synchronized (this) {
                if (activeLookups <= 0) {
                    throw new IllegalStateException("Lake table lookuper is not acquired.");
                }
                activeLookups--;
            }
            closeIfUnused();
        }

        private void invalidate() {
            synchronized (this) {
                invalidated = true;
            }
            closeIfUnused();
        }

        private void closeIfUnused() {
            boolean shouldClose;
            synchronized (this) {
                shouldClose = invalidated && activeLookups == 0 && !closed;
                if (shouldClose) {
                    closed = true;
                }
            }
            if (shouldClose) {
                closeLookuper(this);
            }
        }
    }
}
