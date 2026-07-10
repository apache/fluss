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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.HistoricalLookupThrottledException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.LakeStorageNotConfiguredException;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Handles server-side point lookup for historical partitions stored in lake storage. */
class HistoricalLakeLookupManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalLakeLookupManager.class);

    private final Configuration conf;
    private final @Nullable PluginManager pluginManager;
    private final ExecutorService ioExecutor;
    private final Semaphore lookupPermits;
    // todo: consider add cache expire
    private final ConcurrentHashMap<TablePath, LakeTableLookuper> lakeTableLookupers =
            new ConcurrentHashMap<>();

    HistoricalLakeLookupManager(
            Configuration conf, @Nullable PluginManager pluginManager, ExecutorService ioExecutor) {
        this.conf = checkNotNull(conf, "conf must not be null.");
        this.pluginManager = pluginManager;
        this.ioExecutor = checkNotNull(ioExecutor, "ioExecutor must not be null.");
        int maxQueuedHistoricalRequests =
                conf.get(ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS);
        checkArgument(
                maxQueuedHistoricalRequests > 0,
                "%s must be greater than 0.",
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
        this.lookupPermits = new Semaphore(maxQueuedHistoricalRequests);
    }

    CompletableFuture<LookupResultForBucket> lookup(
            LookupDataForBucket lookupData, TableInfo tableInfo) {
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
                            () -> lookupInternal(lookupData, tableInfo), ioExecutor);
        } catch (RuntimeException e) {
            lookupPermits.release();
            throw e;
        }
        future.whenComplete((ignored, error) -> lookupPermits.release());
        return future;
    }

    @Override
    public void close() {
        for (LakeTableLookuper lookuper : lakeTableLookupers.values()) {
            IOUtils.closeQuietly(lookuper, "historical lake table lookuper");
        }
        lakeTableLookupers.clear();
    }

    private LookupResultForBucket lookupInternal(
            LookupDataForBucket lookupData, TableInfo tableInfo) {
        TableBucket tableBucket = lookupData.tableBucket();
        try {
            LookupContext context = createLookupContext(lookupData, tableInfo);
            LakeTableLookuper lookuper =
                    lakeTableLookupers.computeIfAbsent(
                            context.tablePath, this::createLakeTableLookuper);
            List<byte[]> values = new ArrayList<>(lookupData.keys().size());
            for (byte[] key : lookupData.keys()) {
                values.add(lookuper.lookup(key, context.lookupContext));
            }
            return new LookupResultForBucket(tableBucket, values);
        } catch (Exception e) {
            return new LookupResultForBucket(tableBucket, ApiError.fromThrowable(e));
        }
    }

    private LookupContext createLookupContext(LookupDataForBucket lookupData, TableInfo tableInfo) {
        TableBucket tableBucket = lookupData.tableBucket();
        String originalPartitionName = lookupData.partitionName();
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
                        (short) tableInfo.getSchemaInfo().getSchemaId(),
                        tableInfo.getRowType());
        return new LookupContext(tablePath, lookupContext);
    }

    private LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
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
                tablePath, new LakeStorage.LookuperContext(conf.get(ConfigOptions.IO_TMP_DIR)));
    }

    private static final class LookupContext {
        private final TablePath tablePath;
        private final LakeTableLookuper.LookupContext lookupContext;

        private LookupContext(TablePath tablePath, LakeTableLookuper.LookupContext lookupContext) {
            this.tablePath = tablePath;
            this.lookupContext = lookupContext;
        }
    }
}
