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
import org.apache.fluss.exception.HistoricalLookupThrottledException;
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
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Handles server-side point lookup for historical partitions stored in lake storage.
 *
 * <p>Accepted requests run on the TabletServer I/O executor. A semaphore bounds the total number of
 * accepted historical lookup tasks so slow lake storage cannot create an unbounded request backlog.
 *
 * <p>Creating a lake table lookuper may initialize catalog, table, and query state and allocate
 * local lookup files, so lookupers are cached and reused. The cache is keyed by table ID rather
 * than table path to prevent a deleted and recreated table from reusing the old table's lookuper. A
 * cached lookuper is replaced when its schema ID no longer matches the requested table schema.
 *
 * <p>A lookuper is closed when replaced, explicitly invalidated by a replica lifecycle event,
 * evicted after the cache reaches ten tables, the manager shuts down, or after three hours without
 * access. Caffeine expiration is scheduled on the shared TabletServer scheduler, allowing idle
 * resources to be released even if no subsequent lookup accesses the cache.
 */
class HistoricalLakeLookupManager implements AutoCloseable {

    private static final String PAIMON_LOOKUP_DIR_NAME = "paimon-lookup";
    private static final String LOOKUPER_CACHE_EXPIRATION_TASK_NAME =
            "historical-lookuper-cache-expiration";
    private static final Duration LOOKUPER_CACHE_EXPIRATION = Duration.ofHours(3);
    private static final int MAX_CACHED_LOOKUPERS = 10;

    // TODO: MAX_CACHED_LOOKUPERS and the 2GB per-table limit configured by
    // PaimonLakeTableLookuper through Paimon's "lookup.cache-max-disk-size" option are hard-coded.
    // Make them configurable, and prefer a Paimon IOManager-level global disk limit shared by all
    // table lookupers because fixed per-table limits can underutilize cache for hot tables while
    // reserving too much for cold tables.

    private final Configuration conf;
    private final @Nullable PluginManager pluginManager;
    private final ExecutorService ioExecutor;
    private final int serverId;
    private final Semaphore lookupPermits;
    private final Cache<Long, CachedLakeTableLookuper> lakeTableLookupers;
    private @Nullable String paimonLookupTempDir;

    HistoricalLakeLookupManager(
            Configuration conf,
            @Nullable PluginManager pluginManager,
            ExecutorService ioExecutor,
            int serverId,
            Scheduler scheduler) {
        this(
                conf,
                pluginManager,
                ioExecutor,
                serverId,
                Ticker.systemTicker(),
                createCacheScheduler(scheduler));
    }

    @VisibleForTesting
    HistoricalLakeLookupManager(
            Configuration conf,
            @Nullable PluginManager pluginManager,
            ExecutorService ioExecutor,
            int serverId,
            Ticker ticker,
            com.github.benmanes.caffeine.cache.Scheduler cacheScheduler) {
        this.conf = checkNotNull(conf, "conf must not be null.");
        this.pluginManager = pluginManager;
        this.ioExecutor = checkNotNull(ioExecutor, "ioExecutor must not be null.");
        this.serverId = serverId;
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
        int maxQueuedHistoricalRequests =
                conf.get(ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS);
        checkArgument(
                maxQueuedHistoricalRequests > 0,
                "%s must be greater than 0.",
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
        this.lookupPermits = new Semaphore(maxQueuedHistoricalRequests);
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
                            ApiError.fromThrowable(
                                    new HistoricalLookupThrottledException(
                                            "Historical lookup is throttled for "
                                                    + tableBucket
                                                    + "."))));
        }

        CompletableFuture<LookupResultForBucket> future;
        try {
            future =
                    CompletableFuture.supplyAsync(
                            () -> lookupInternal(lookupData, tableInfo, schemaInfo), ioExecutor);
        } catch (RuntimeException e) {
            lookupPermits.release();
            throw e;
        }
        future.whenComplete((ignored, error) -> lookupPermits.release());
        return future;
    }

    @Override
    public void close() {
        lakeTableLookupers.invalidateAll();
        lakeTableLookupers.cleanUp();
    }

    void invalidateTableLookuper(long tableId) {
        lakeTableLookupers.invalidate(tableId);
    }

    private LookupResultForBucket lookupInternal(
            LookupDataForBucket lookupData, TableInfo tableInfo, SchemaInfo schemaInfo) {
        TableBucket tableBucket = lookupData.tableBucket();
        CachedLakeTableLookuper cachedLookuper = null;
        try {
            LookupContext context = createLookupContext(lookupData, tableInfo, schemaInfo);
            cachedLookuper =
                    lakeTableLookupers
                            .asMap()
                            .compute(
                                    context.tableId,
                                    (ignored, currentLookuper) -> {
                                        CachedLakeTableLookuper selectedLookuper = currentLookuper;
                                        // Create the lookuper lazily, and recreate it after schema
                                        // evolution so it reloads lake table/query state and
                                        // encodes values with the requested schema.
                                        if (selectedLookuper == null
                                                || selectedLookuper.schemaId != context.schemaId) {
                                            LakeTableLookuper newLookuper =
                                                    createLakeTableLookuper(
                                                            context.tablePath,
                                                            getOrPreparePaimonLookupTempDir(),
                                                            tableInfo.getTableConfig());
                                            selectedLookuper =
                                                    new CachedLakeTableLookuper(
                                                            context.schemaId, newLookuper);
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
            return new LookupResultForBucket(tableBucket, values);
        } catch (Exception e) {
            return new LookupResultForBucket(tableBucket, ApiError.fromThrowable(e));
        } finally {
            if (cachedLookuper != null) {
                cachedLookuper.release();
            }
        }
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

    @VisibleForTesting
    LakeTableLookuper createLakeTableLookuper(
            TablePath tablePath, String ioTmpDir, TableConfig tableConfig) {
        DataLakeFormat dataLakeFormat = conf.get(ConfigOptions.DATALAKE_FORMAT);
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

        Map<String, String> lakeProperties = extractLakeProperties(conf);
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

    private synchronized String getOrPreparePaimonLookupTempDir() {
        if (paimonLookupTempDir == null) {
            paimonLookupTempDir = preparePaimonLookupTempDir(conf, serverId);
        }
        return paimonLookupTempDir;
    }

    private static String preparePaimonLookupTempDir(Configuration conf, int serverId) {
        File paimonLookupTempDir =
                new File(
                        new File(conf.get(ConfigOptions.IO_TMP_DIR), PAIMON_LOOKUP_DIR_NAME),
                        String.valueOf(serverId));
        try {
            // A crashed server cannot close the Paimon IOManager, so lookup cache files may be
            // left behind. Clean only this server's directory before creating the first table
            // lookuper; cleaning in each table lookuper would delete files used by other tables.
            FileUtils.deleteDirectory(paimonLookupTempDir);
            Files.createDirectories(paimonLookupTempDir.toPath());
            return paimonLookupTempDir.getAbsolutePath();
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    "Failed to prepare Paimon lookup temporary directory: " + paimonLookupTempDir,
                    e);
        }
    }

    private static void closeLookuper(CachedLakeTableLookuper cachedLookuper) {
        IOUtils.closeQuietly(cachedLookuper.lookuper, "historical lake table lookuper");
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
        private final LakeTableLookuper lookuper;
        private int activeLookups;
        private boolean invalidated;
        private boolean closed;

        private CachedLakeTableLookuper(int schemaId, LakeTableLookuper lookuper) {
            this.schemaId = schemaId;
            this.lookuper = lookuper;
        }

        private synchronized void acquire() {
            if (invalidated) {
                throw new IllegalStateException("Lake table lookuper has been invalidated.");
            }
            activeLookups++;
        }

        private synchronized void release() {
            if (activeLookups <= 0) {
                throw new IllegalStateException("Lake table lookuper is not acquired.");
            }
            activeLookups--;
            closeIfUnused();
        }

        private synchronized void invalidate() {
            invalidated = true;
            closeIfUnused();
        }

        private void closeIfUnused() {
            if (invalidated && activeLookups == 0 && !closed) {
                closed = true;
                closeLookuper(this);
            }
        }
    }
}
