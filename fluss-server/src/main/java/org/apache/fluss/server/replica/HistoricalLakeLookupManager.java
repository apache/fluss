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

package org.apache.fluss.server.replica;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.exception.InvalidPartitionException;
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
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.server.replica.HistoricalLookupCacheBudgetManager.Reservation;
import org.apache.fluss.utils.ExecutorUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
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
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Handles server-side point lookup for historical partitions stored in lake storage.
 *
 * <p>Accepted requests run on a dedicated executor whose threads are started lazily and released
 * when idle. A semaphore bounds the total number of accepted historical lookup tasks so slow lake
 * storage cannot create an unbounded request backlog.
 *
 * <p>Creating a lake table lookuper may initialize catalog, table, and query state and allocate
 * local lookup files, so lookupers are cached and reused. The cache is keyed by table ID rather
 * than table path to prevent a deleted and recreated table from reusing the old table's lookuper. A
 * cached lookuper is replaced when its schema ID or lake configuration version no longer matches
 * the current request. Active lookups can finish on the old lookuper, which is closed after its
 * last lookup releases it.
 *
 * <p>A lookuper is closed when replaced, explicitly invalidated by a replica lifecycle event,
 * evicted to admit another table within the configured disk budget, the manager shuts down, or
 * after the configured idle expiration. Caffeine expiration is scheduled on the shared TabletServer
 * scheduler, allowing idle resources to be released even if no subsequent lookup accesses the
 * cache.
 */
class HistoricalLakeLookupManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalLakeLookupManager.class);

    private static final String PAIMON_LOOKUP_DIR_NAME = "paimon-lookup";
    private static final String LOOKUPER_CACHE_EXPIRATION_TASK_NAME =
            "historical-lookuper-cache-expiration";
    private static final Duration HISTORICAL_PARTITION_THREAD_KEEP_ALIVE = Duration.ofMinutes(10);
    private static final Duration HISTORICAL_PARTITION_EXECUTOR_SHUTDOWN_TIMEOUT =
            Duration.ofSeconds(10);
    private static final String HISTORICAL_PARTITION_THREAD_NAME_PREFIX = "historical-partition-io";

    private volatile Configuration conf;
    private volatile long lakeConfigVersion;
    private final @Nullable PluginManager pluginManager;
    private final int serverId;
    private final Ticker ticker;
    private final HistoricalLookupCacheBudgetManager budgetManager;
    private final Counter capacityEvictions;
    private final Semaphore lookupPermits;
    // Accepted lookup futures tracked so close() can cancel tasks left after executor shutdown.
    private final Set<CompletableFuture<LookupResultForBucket>> pendingLookups;
    private final Cache<Long, CachedLakeTableLookuper> lakeTableLookupers;
    private final ExecutorService historicalPartitionExecutor;
    private @Nullable File paimonLookupTempDir;

    HistoricalLakeLookupManager(
            Configuration conf,
            @Nullable PluginManager pluginManager,
            int serverId,
            Scheduler scheduler) {
        this(
                conf,
                pluginManager,
                null,
                serverId,
                Ticker.systemTicker(),
                createCacheScheduler(scheduler));
    }

    @VisibleForTesting
    HistoricalLakeLookupManager(
            Configuration conf,
            @Nullable PluginManager pluginManager,
            @Nullable ExecutorService historicalPartitionExecutor,
            int serverId,
            Ticker ticker,
            com.github.benmanes.caffeine.cache.Scheduler cacheScheduler) {
        this.conf = checkNotNull(conf, "conf must not be null.");
        this.pluginManager = pluginManager;
        this.serverId = serverId;
        this.ticker = checkNotNull(ticker, "ticker must not be null.");
        this.budgetManager =
                new HistoricalLookupCacheBudgetManager(
                        conf.get(
                                        ConfigOptions
                                                .SERVER_HISTORICAL_PARTITION_LOOKUP_CACHE_MAX_DISK_SIZE)
                                .getBytes());
        this.capacityEvictions = new ThreadSafeSimpleCounter();
        int maxQueuedHistoricalRequests =
                conf.get(ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS);
        checkArgument(
                maxQueuedHistoricalRequests > 0,
                "%s must be greater than 0.",
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
        int maxThreadPoolSize =
                conf.get(ConfigOptions.SERVER_HISTORICAL_PARTITION_THREAD_POOL_MAX_SIZE);
        checkArgument(
                maxThreadPoolSize > 0,
                "%s must be greater than 0.",
                ConfigOptions.SERVER_HISTORICAL_PARTITION_THREAD_POOL_MAX_SIZE.key());
        this.historicalPartitionExecutor =
                historicalPartitionExecutor == null
                        ? createHistoricalPartitionExecutor(maxThreadPoolSize)
                        : historicalPartitionExecutor;
        this.lakeTableLookupers =
                Caffeine.newBuilder()
                        .expireAfterAccess(
                                conf.get(
                                        ConfigOptions
                                                .SERVER_HISTORICAL_PARTITION_LOOKUPER_CACHE_EXPIRE_AFTER_ACCESS))
                        .ticker(this.ticker)
                        .scheduler(checkNotNull(cacheScheduler, "cacheScheduler must not be null."))
                        .executor(Runnable::run)
                        .removalListener(
                                (Long ignored,
                                        CachedLakeTableLookuper cachedLookuper,
                                        RemovalCause ignoredCause) -> {
                                    if (cachedLookuper != null) {
                                        onLookuperRemoved(cachedLookuper);
                                    }
                                })
                        .build();
        this.lookupPermits = new Semaphore(maxQueuedHistoricalRequests);
        this.pendingLookups = ConcurrentHashMap.newKeySet();
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

    CompletableFuture<LookupResultForBucket> lookup(
            LookupDataForBucket lookupData, TableInfo tableInfo, SchemaInfo schemaInfo) {
        TableBucket tableBucket = lookupData.tableBucket();
        if (!lookupPermits.tryAcquire()) {
            return CompletableFuture.completedFuture(
                    new LookupResultForBucket(
                            tableBucket,
                            null,
                            lookupData.originalPartitionName(),
                            ApiError.fromThrowable(
                                    new HistoricalPartitionThrottledException(
                                            "Historical lookup is throttled for "
                                                    + tableBucket
                                                    + "."))));
        }

        CompletableFuture<LookupResultForBucket> future;
        try {
            future = submitLookup(lookupData, tableInfo, schemaInfo);
        } catch (RuntimeException e) {
            lookupPermits.release();
            throw e;
        }
        future.whenComplete(
                (ignored, error) -> {
                    pendingLookups.remove(future);
                    lookupPermits.release();
                });
        return future;
    }

    @Override
    public void close() {
        ExecutorUtils.gracefulShutdown(
                HISTORICAL_PARTITION_EXECUTOR_SHUTDOWN_TIMEOUT.toMillis(),
                TimeUnit.MILLISECONDS,
                historicalPartitionExecutor);
        pendingLookups.forEach(future -> future.cancel(true));
        lakeTableLookupers.invalidateAll();
        lakeTableLookupers.cleanUp();
    }

    private CompletableFuture<LookupResultForBucket> submitLookup(
            LookupDataForBucket lookupData, TableInfo tableInfo, SchemaInfo schemaInfo) {
        CompletableFuture<LookupResultForBucket> future =
                CompletableFuture.supplyAsync(
                        () -> lookupInternal(lookupData, tableInfo, schemaInfo),
                        historicalPartitionExecutor);
        pendingLookups.add(future);
        return future;
    }

    private ExecutorService createHistoricalPartitionExecutor(int maxThreadPoolSize) {
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        maxThreadPoolSize,
                        maxThreadPoolSize,
                        HISTORICAL_PARTITION_THREAD_KEEP_ALIVE.toMillis(),
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory(HISTORICAL_PARTITION_THREAD_NAME_PREFIX));
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    void invalidateTableLookuper(long tableId) {
        lakeTableLookupers.invalidate(tableId);
    }

    int cachedTableCount() {
        return lakeTableLookupers.asMap().size();
    }

    Counter capacityEvictions() {
        return capacityEvictions;
    }

    synchronized void reconfigure(Configuration newConf) {
        checkNotNull(newConf, "newConf must not be null.");
        boolean lakeConfigChanged = hasLakeConfigChanged(conf, newConf);
        // Publish the configuration before its version. A lookup that observes the new version
        // must also observe the matching configuration snapshot.
        conf = newConf;
        if (lakeConfigChanged) {
            lakeConfigVersion++;
        }
    }

    private LookupResultForBucket lookupInternal(
            LookupDataForBucket lookupData, TableInfo tableInfo, SchemaInfo schemaInfo) {
        TableBucket tableBucket = lookupData.tableBucket();
        CachedLakeTableLookuper cachedLookuper = null;
        try {
            LookupContext context = createLookupContext(lookupData, tableInfo, schemaInfo);
            long currentLakeConfigVersion = lakeConfigVersion;
            Configuration currentConf = conf;
            TableConfig tableConfig = tableInfo.getTableConfig();
            long cacheSizeBytes =
                    tableConfig.getHistoricalPartitionLookupCacheMaxDiskSize().getBytes();
            cachedLookuper =
                    acquireCachedLookuper(
                            context,
                            tableConfig,
                            currentConf,
                            currentLakeConfigVersion,
                            cacheSizeBytes);
            List<byte[]> values = new ArrayList<>(lookupData.keys().size());
            for (byte[] key : lookupData.keys()) {
                values.add(cachedLookuper.lookuper.lookup(key, context.lookupContext));
            }
            return new LookupResultForBucket(
                    tableBucket, values, lookupData.originalPartitionName(), ApiError.NONE);
        } catch (Exception e) {
            return new LookupResultForBucket(
                    tableBucket,
                    null,
                    lookupData.originalPartitionName(),
                    ApiError.fromThrowable(e));
        } finally {
            if (cachedLookuper != null) {
                cachedLookuper.release();
            }
        }
    }

    private CachedLakeTableLookuper acquireCachedLookuper(
            LookupContext context,
            TableConfig tableConfig,
            Configuration clusterConf,
            long currentLakeConfigVersion,
            long cacheSizeBytes) {
        CachedLakeTableLookuper cachedLookuper =
                tryAcquireCachedLookuper(
                        context,
                        tableConfig,
                        clusterConf,
                        currentLakeConfigVersion,
                        cacheSizeBytes);
        if (cachedLookuper != null) {
            return cachedLookuper;
        }

        int maxEvictions = lakeTableLookupers.asMap().size();
        for (int evictions = 0; evictions < maxEvictions; evictions++) {
            // Evict only after compute releases the target table's cache lock. Updating a
            // different table mapping from inside compute can deadlock with a concurrent
            // replacement performing the inverse update.
            if (!evictLeastRecentlyUsed(context.tableId)) {
                break;
            }
            cachedLookuper =
                    tryAcquireCachedLookuper(
                            context,
                            tableConfig,
                            clusterConf,
                            currentLakeConfigVersion,
                            cacheSizeBytes);
            if (cachedLookuper != null) {
                return cachedLookuper;
            }
        }
        throw capacityThrottledException(context, cacheSizeBytes);
    }

    /**
     * Makes one atomic attempt to acquire a matching cached lookuper without evicting other tables.
     *
     * @return the acquired lookuper, or {@code null} if its capacity cannot be reserved
     */
    private @Nullable CachedLakeTableLookuper tryAcquireCachedLookuper(
            LookupContext context,
            TableConfig tableConfig,
            Configuration clusterConf,
            long currentLakeConfigVersion,
            long cacheSizeBytes) {
        CachedLakeTableLookuper cachedLookuper =
                lakeTableLookupers
                        .asMap()
                        .compute(
                                context.tableId,
                                (ignored, currentLookuper) -> {
                                    CachedLakeTableLookuper selectedLookuper = currentLookuper;
                                    // Create the lookuper lazily, and recreate it after schema or
                                    // lake configuration changes so it reloads lake table/query
                                    // state and uses the current configuration.
                                    if (!matchesLookupConfiguration(
                                            selectedLookuper, context, currentLakeConfigVersion)) {
                                        selectedLookuper =
                                                tryCreateCachedLookuper(
                                                        context,
                                                        tableConfig,
                                                        clusterConf,
                                                        currentLakeConfigVersion,
                                                        cacheSizeBytes,
                                                        currentLookuper);
                                        if (selectedLookuper == null) {
                                            // Preserve the current mapping and leave compute before
                                            // attempting to evict another table.
                                            return currentLookuper;
                                        }
                                    }
                                    // Pin the lookuper before leaving the atomic cache update.
                                    // Eviction or invalidation can then defer closing it until this
                                    // lookup releases it.
                                    selectedLookuper.acquire(ticker.read());
                                    return selectedLookuper;
                                });
        return matchesLookupConfiguration(cachedLookuper, context, currentLakeConfigVersion)
                ? cachedLookuper
                : null;
    }

    private static boolean matchesLookupConfiguration(
            @Nullable CachedLakeTableLookuper cachedLookuper,
            LookupContext context,
            long currentLakeConfigVersion) {
        return cachedLookuper != null
                && cachedLookuper.schemaId == context.schemaId
                && cachedLookuper.lakeConfigVersion == currentLakeConfigVersion;
    }

    /**
     * Creates a lookuper after atomically reserving its configured cache capacity.
     *
     * @return the new cached lookuper, or {@code null} if the capacity cannot be reserved
     */
    private @Nullable CachedLakeTableLookuper tryCreateCachedLookuper(
            LookupContext context,
            TableConfig tableConfig,
            Configuration clusterConf,
            long currentLakeConfigVersion,
            long cacheSizeBytes,
            @Nullable CachedLakeTableLookuper currentLookuper) {
        File tableLookupDir =
                FlussPaths.historicalLookupTableDir(
                        getOrPreparePaimonLookupTempDir(clusterConf),
                        context.tablePath,
                        context.tableId);
        if (currentLookuper == null) {
            // A cache miss must obtain capacity before creating any local lookup resources.
            Reservation reservation = budgetManager.tryReserve(context.tableId, cacheSizeBytes);
            if (reservation == null) {
                return null;
            }
            try {
                LakeTableLookuper lookuper =
                        createLakeTableLookuper(
                                context.tablePath,
                                tableLookupDir.getAbsolutePath(),
                                tableConfig,
                                clusterConf);
                return new CachedLakeTableLookuper(
                        context.tableId,
                        context.tablePath,
                        context.schemaId,
                        currentLakeConfigVersion,
                        cacheSizeBytes,
                        tableLookupDir,
                        reservation,
                        lookuper);
            } catch (Throwable throwable) {
                budgetManager.release(reservation);
                throw throwable;
            }
        }

        // Build the replacement first so a creation failure leaves the current lookuper and its
        // reservation unchanged in the cache.
        LakeTableLookuper lookuper =
                createLakeTableLookuper(
                        context.tablePath,
                        tableLookupDir.getAbsolutePath(),
                        tableConfig,
                        clusterConf);
        // Replace the reservation atomically: the old and replacement cache sizes never count
        // against the global budget at the same time.
        Reservation reservation =
                budgetManager.tryReplace(currentLookuper.reservation, cacheSizeBytes);
        if (reservation == null) {
            // The candidate was never published, while the current lookuper remains usable.
            closeLookuper(lookuper, tableLookupDir);
            return null;
        }
        return new CachedLakeTableLookuper(
                context.tableId,
                context.tablePath,
                context.schemaId,
                currentLakeConfigVersion,
                cacheSizeBytes,
                tableLookupDir,
                reservation,
                lookuper);
    }

    /**
     * Evicts one eligible cached lookuper using best-effort LRU order.
     *
     * <p>Candidates are ordered by their last-access timestamps without blocking concurrent
     * lookups. A candidate accessed after it is ordered may therefore still be evicted.
     */
    private boolean evictLeastRecentlyUsed(long excludedTableId) {
        List<CachedLakeTableLookuper> candidates =
                new ArrayList<>(lakeTableLookupers.asMap().values());
        candidates.sort(Comparator.comparingLong(CachedLakeTableLookuper::lastAccessNanos));
        for (CachedLakeTableLookuper candidate : candidates) {
            if (candidate.tableId == excludedTableId) {
                continue;
            }
            // Claim this victim so concurrent admission threads cannot evict it twice. A false
            // result means it was already invalidated or claimed by another eviction.
            if (!candidate.markEvictionPending()) {
                continue;
            }

            // The snapshot may be stale after expiration or replacement. Compare-and-remove
            // prevents this eviction from removing a newer lookuper for the same table.
            boolean removed = lakeTableLookupers.asMap().remove(candidate.tableId, candidate);
            if (!removed) {
                candidate.clearEvictionPending();
                continue;
            }

            // The direct Caffeine executor normally invokes the listener inline. Repeat the
            // transition explicitly so admission does not depend on listener scheduling.
            onLookuperRemoved(candidate);
            capacityEvictions.inc();
            LOG.info(
                    "Evicted historical lookup cache for table {} (table ID {}, cache size {} bytes, reserved {} of {} bytes).",
                    candidate.tablePath,
                    candidate.tableId,
                    candidate.cacheSizeBytes,
                    budgetManager.reservedBytes(),
                    budgetManager.maxBytes());
            return true;
        }
        return false;
    }

    private HistoricalPartitionThrottledException capacityThrottledException(
            LookupContext context, long cacheSizeBytes) {
        return new HistoricalPartitionThrottledException(
                String.format(
                        "Historical lookup cache capacity is unavailable for table %s (table ID %s): requested %s bytes, reserved %s of %s bytes across %s cached tables.",
                        context.tablePath,
                        context.tableId,
                        cacheSizeBytes,
                        budgetManager.reservedBytes(),
                        budgetManager.maxBytes(),
                        cachedTableCount()));
    }

    private void onLookuperRemoved(CachedLakeTableLookuper cachedLookuper) {
        budgetManager.release(cachedLookuper.reservation);
        cachedLookuper.invalidate();
    }

    private LookupContext createLookupContext(
            LookupDataForBucket lookupData, TableInfo tableInfo, SchemaInfo schemaInfo) {
        TableBucket tableBucket = lookupData.tableBucket();
        String originalPartitionName = lookupData.originalPartitionName();
        if (originalPartitionName == null) {
            throw new InvalidPartitionException(
                    "Historical lookup request must carry the original partition name.");
        }

        TablePath tablePath = tableInfo.getTablePath();

        ResolvedPartitionSpec originalPartitionSpec;
        try {
            originalPartitionSpec =
                    ResolvedPartitionSpec.fromPartitionName(
                            tableInfo.getPartitionKeys(), originalPartitionName);
        } catch (RuntimeException e) {
            throw new InvalidPartitionException(
                    String.format(
                            "Invalid original partition name %s for historical lookup on table %s.",
                            originalPartitionName, tablePath));
        }

        LakeTableLookuper.LookupContext lookupContext =
                new LakeTableLookuper.LookupContext(
                        originalPartitionSpec,
                        tableBucket.getBucket(),
                        (short) schemaInfo.getSchemaId(),
                        schemaInfo.getSchema().getRowType());
        return new LookupContext(
                tableInfo.getTableId(), schemaInfo.getSchemaId(), tablePath, lookupContext);
    }

    LakeTableLookuper createLakeTableLookuper(
            TablePath tablePath,
            String ioTmpDir,
            TableConfig tableConfig,
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
                tablePath, new LakeStorage.LookuperContext(ioTmpDir, tableConfig));
    }

    private static boolean hasLakeConfigChanged(Configuration currentConf, Configuration newConf) {
        return currentConf.get(ConfigOptions.DATALAKE_FORMAT)
                        != newConf.get(ConfigOptions.DATALAKE_FORMAT)
                || !Objects.equals(
                        extractLakeProperties(currentConf), extractLakeProperties(newConf));
    }

    private synchronized File getOrPreparePaimonLookupTempDir(Configuration clusterConf) {
        if (paimonLookupTempDir == null) {
            paimonLookupTempDir = preparePaimonLookupTempDir(clusterConf, serverId);
        }
        return paimonLookupTempDir;
    }

    private static File preparePaimonLookupTempDir(Configuration conf, int serverId) {
        File paimonLookupTempDir =
                new File(
                        new File(conf.get(ConfigOptions.SERVER_IO_TMP_DIR), PAIMON_LOOKUP_DIR_NAME),
                        String.valueOf(serverId));
        try {
            // A crashed server cannot close the Paimon IOManager, so lookup cache files may be
            // left behind. Clean only this server's directory before creating the first table
            // lookuper; cleaning in each table lookuper would delete files used by other tables.
            FileUtils.deleteDirectory(paimonLookupTempDir);
            Files.createDirectories(paimonLookupTempDir.toPath());
            return paimonLookupTempDir;
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    "Failed to prepare Paimon lookup temporary directory: " + paimonLookupTempDir,
                    e);
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
        private final File tableLookupDir;
        private final Reservation reservation;
        private final LakeTableLookuper lookuper;
        private long lastAccessNanos;
        private int activeLookups;
        private boolean evictionPending;
        private boolean invalidated;
        private boolean closed;

        private CachedLakeTableLookuper(
                long tableId,
                TablePath tablePath,
                int schemaId,
                long lakeConfigVersion,
                long cacheSizeBytes,
                File tableLookupDir,
                Reservation reservation,
                LakeTableLookuper lookuper) {
            this.tableId = tableId;
            this.tablePath = tablePath;
            this.schemaId = schemaId;
            this.lakeConfigVersion = lakeConfigVersion;
            this.cacheSizeBytes = cacheSizeBytes;
            this.tableLookupDir = tableLookupDir;
            this.reservation = reservation;
            this.lookuper = lookuper;
        }

        private synchronized void acquire(long accessNanos) {
            if (invalidated) {
                throw new IllegalStateException("Lake table lookuper has been invalidated.");
            }
            lastAccessNanos = accessNanos;
            activeLookups++;
        }

        private synchronized long lastAccessNanos() {
            return lastAccessNanos;
        }

        private synchronized boolean markEvictionPending() {
            if (invalidated || evictionPending) {
                return false;
            }
            evictionPending = true;
            return true;
        }

        private synchronized void clearEvictionPending() {
            evictionPending = false;
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
