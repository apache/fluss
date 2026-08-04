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
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.LookupDataForBucket;
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
 * evicted after the cache reaches ten tables, the manager shuts down, or after three hours without
 * access. Caffeine expiration is scheduled on the shared TabletServer scheduler, allowing idle
 * resources to be released even if no subsequent lookup accesses the cache.
 */
class HistoricalLakeLookupManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalLakeLookupManager.class);

    private static final String PAIMON_LOOKUP_DIR_NAME = "paimon-lookup";
    private static final String LOOKUPER_CACHE_EXPIRATION_TASK_NAME =
            "historical-lookuper-cache-expiration";
    private static final Duration LOOKUPER_CACHE_EXPIRATION = Duration.ofHours(3);
    private static final Duration HISTORICAL_PARTITION_THREAD_KEEP_ALIVE = Duration.ofMinutes(10);
    private static final Duration HISTORICAL_PARTITION_EXECUTOR_SHUTDOWN_TIMEOUT =
            Duration.ofSeconds(10);
    private static final int MAX_CACHED_LOOKUPERS = 10;
    private static final String HISTORICAL_PARTITION_THREAD_NAME_PREFIX = "historical-partition-io";

    // TODO: MAX_CACHED_LOOKUPERS and the 2GB per-table limit configured by
    // PaimonLakeTableLookuper through Paimon's "lookup.cache-max-disk-size" option are hard-coded.
    // Make them configurable, and prefer a Paimon IOManager-level global disk limit shared by all
    // table lookupers because fixed per-table limits can underutilize cache for hot tables while
    // reserving too much for cold tables.

    private volatile Configuration conf;
    private volatile long lakeConfigVersion;
    private final @Nullable PluginManager pluginManager;
    private final int serverId;
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
                        .expireAfterAccess(LOOKUPER_CACHE_EXPIRATION)
                        .maximumSize(MAX_CACHED_LOOKUPERS)
                        .ticker(checkNotNull(ticker, "ticker must not be null."))
                        .scheduler(checkNotNull(cacheScheduler, "cacheScheduler must not be null."))
                        .executor(Runnable::run)
                        .removalListener(
                                (Long ignored,
                                        CachedLakeTableLookuper cachedLookuper,
                                        RemovalCause ignoredCause) -> {
                                    if (cachedLookuper != null) {
                                        cachedLookuper.invalidate();
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
            cachedLookuper =
                    lakeTableLookupers
                            .asMap()
                            .compute(
                                    context.tableId,
                                    (ignored, currentLookuper) -> {
                                        CachedLakeTableLookuper selectedLookuper = currentLookuper;
                                        // Create the lookuper lazily, and recreate it after schema
                                        // or lake configuration changes so it reloads lake
                                        // table/query state and uses the current configuration.
                                        if (selectedLookuper == null
                                                || selectedLookuper.schemaId != context.schemaId
                                                || selectedLookuper.lakeConfigVersion
                                                        != currentLakeConfigVersion) {
                                            selectedLookuper =
                                                    createCachedLookuper(
                                                            context,
                                                            tableInfo.getTableConfig(),
                                                            currentConf,
                                                            currentLakeConfigVersion);
                                        }
                                        // Pin the lookuper before leaving the atomic cache update.
                                        // Eviction or invalidation can then defer closing it until
                                        // this lookup releases it.
                                        selectedLookuper.acquire();
                                        return selectedLookuper;
                                    });
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

    private CachedLakeTableLookuper createCachedLookuper(
            LookupContext context,
            TableConfig tableConfig,
            Configuration clusterConf,
            long currentLakeConfigVersion) {
        File tableLookupDir =
                FlussPaths.historicalLookupTableDir(
                        getOrPreparePaimonLookupTempDir(clusterConf),
                        context.tablePath,
                        context.tableId);
        LakeTableLookuper lookuper =
                createLakeTableLookuper(
                        context.tablePath,
                        tableLookupDir.getAbsolutePath(),
                        tableConfig,
                        clusterConf);
        return new CachedLakeTableLookuper(
                context.schemaId, currentLakeConfigVersion, tableLookupDir, lookuper);
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
        try {
            IOUtils.closeQuietly(cachedLookuper.lookuper, "historical lake table lookuper");
        } finally {
            deleteTableLookupDirIfEmpty(cachedLookuper.tableLookupDir);
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
        private final int schemaId;
        private final long lakeConfigVersion;
        private final File tableLookupDir;
        private final LakeTableLookuper lookuper;
        private int activeLookups;
        private boolean invalidated;
        private boolean closed;

        private CachedLakeTableLookuper(
                int schemaId,
                long lakeConfigVersion,
                File tableLookupDir,
                LakeTableLookuper lookuper) {
            this.schemaId = schemaId;
            this.lakeConfigVersion = lakeConfigVersion;
            this.tableLookupDir = tableLookupDir;
            this.lookuper = lookuper;
        }

        private synchronized void acquire() {
            if (invalidated) {
                throw new IllegalStateException("Lake table lookuper has been invalidated.");
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
