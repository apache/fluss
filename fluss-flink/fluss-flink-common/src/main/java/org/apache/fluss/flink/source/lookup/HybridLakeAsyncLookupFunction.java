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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

/**
 * An async lookup function that first looks up Fluss and falls back to a lake point lookup when the
 * requested Fluss partition is absent.
 *
 * <p>The lake fallback currently supports Paimon tables only. It is deliberately a point lookup: no
 * lake source or lake split planner is involved.
 */
public class HybridLakeAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(HybridLakeAsyncLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final LookupNormalizer lookupNormalizer;
    private final FlussLookupRuntime flussLookupRuntime;
    private final org.apache.fluss.types.RowType flussLookupRowType;
    private final LookupResultConverter lookupResultConverter;

    // Auto-created partitions cached as absent are expired and will never become live again.
    private final Map<PartitionSpec, Boolean> partitionExistenceCache = new ConcurrentHashMap<>();

    private final LakeLookupRuntime lakeLookupRuntime;
    private Admin admin;
    private PartitionGetter partitionGetter;

    public HybridLakeAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            int[] primaryKeyIndexes,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            Map<String, String> tableOptions,
            Duration lakeFallbackTimeout,
            int lakeFallbackExecutorThreads,
            int lakeFallbackMaxConcurrency) {
        this.tablePath = tablePath;
        this.lookupNormalizer = lookupNormalizer;

        int[] resolvedProjection =
                projection == null
                        ? IntStream.range(0, flinkRowType.getFieldCount()).toArray()
                        : projection;
        this.lookupResultConverter =
                new LookupResultConverter(
                        FlinkConversions.toFlussRowType(
                                FlinkUtils.projectRowType(flinkRowType, resolvedProjection)),
                        resolvedProjection);
        org.apache.fluss.types.RowType flussFullRowType =
                FlinkConversions.toFlussRowType(flinkRowType);
        this.flussLookupRowType = flussFullRowType.project(primaryKeyIndexes);

        this.flussLookupRuntime =
                new FlussLookupRuntime(
                        flussConfig, tablePath, flinkRowType, lookupNormalizer, false);
        this.lakeLookupRuntime =
                new LakeLookupRuntime(
                        flussLookupRuntime,
                        flussConfig,
                        tablePath,
                        flussFullRowType,
                        primaryKeyIndexes,
                        tableOptions,
                        lakeFallbackTimeout,
                        lakeFallbackExecutorThreads,
                        lakeFallbackMaxConcurrency);
    }

    @Override
    public void open(@Nullable FunctionContext context) {
        LOG.info("Starting hybrid lake async lookup function for table {}.", tablePath);
        flussLookupRuntime.open();
        TableInfo tableInfo = flussLookupRuntime.getTableInfo();
        partitionGetter = new PartitionGetter(flussLookupRowType, tableInfo.getPartitionKeys());
        admin = flussLookupRuntime.getAdmin();
        initializePartitionExistenceCache();

        lakeLookupRuntime.open();
        LOG.info("Finished opening hybrid lake async lookup function for table {}.", tablePath);
    }

    private void initializePartitionExistenceCache() {
        partitionExistenceCache.clear();
        try {
            List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitionExistenceCache.put(partitionInfo.getPartitionSpec(), true);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while initializing partitions for table " + tablePath + ".", e);
        } catch (ExecutionException e) {
            throw new RuntimeException(
                    "Failed to initialize partitions for table " + tablePath + ".", e.getCause());
        }
    }

    private synchronized boolean getOrRefreshPartitionExistence(PartitionSpec partitionSpec) {
        Boolean partitionExists = partitionExistenceCache.get(partitionSpec);
        if (partitionExists != null) {
            return partitionExists;
        }
        try {
            partitionExists = !admin.listPartitionInfos(tablePath, partitionSpec).get().isEmpty();
            partitionExistenceCache.put(partitionSpec, partitionExists);
            return partitionExists;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to refresh partition "
                            + partitionSpec
                            + " for table "
                            + tablePath
                            + ".",
                    e.getCause());
        }
    }

    private void markPartitionInvalid(PartitionSpec partitionSpec) {
        partitionExistenceCache.put(partitionSpec, false);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        LookupNormalizer.RemainingFilter remainingFilter =
                lookupNormalizer.createRemainingFilter(keyRow);
        try {
            PartitionSpec partitionSpec =
                    partitionGetter
                            .getResolvedPartitionSpec(new FlinkAsFlussRow(normalizedKeyRow))
                            .toPartitionSpec();
            Boolean partitionExists = partitionExistenceCache.get(partitionSpec);
            if (partitionExists == null) {
                partitionExists = getOrRefreshPartitionExistence(partitionSpec);
            }
            if (partitionExists) {
                return lookupFlussAsync(normalizedKeyRow, partitionSpec, remainingFilter);
            } else {
                return lookupLakeAsync(normalizedKeyRow, remainingFilter);
            }
        } catch (Throwable t) {
            return FutureUtils.completedExceptionally(t);
        }
    }

    private CompletableFuture<Collection<RowData>> lookupFlussAsync(
            RowData normalizedKeyRow,
            PartitionSpec partitionSpec,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter) {
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        try {
            flussLookupRuntime
                    .lookup(normalizedKeyRow)
                    .whenComplete(
                            (result, throwable) -> {
                                try {
                                    if (throwable != null) {
                                        if (ExceptionUtils.findThrowable(
                                                        throwable, PartitionNotExistException.class)
                                                .isPresent()) {
                                            markPartitionInvalid(partitionSpec);
                                            lookupLakeAsync(normalizedKeyRow, remainingFilter)
                                                    .whenComplete(
                                                            (rows, error) ->
                                                                    FutureUtils.doForward(
                                                                            rows, error, future));
                                            return;
                                        }
                                        LOG.error(
                                                "Fluss async lookup failed for table {}.",
                                                tablePath,
                                                throwable);
                                        future.completeExceptionally(
                                                new RuntimeException(
                                                        "Execution of Fluss async lookup failed: "
                                                                + throwable.getMessage(),
                                                        throwable));
                                        return;
                                    }

                                    if (result != null && !result.getRowList().isEmpty()) {
                                        future.complete(
                                                lookupResultConverter.convert(
                                                        result.getRowList(), remainingFilter));
                                    } else {
                                        future.complete(Collections.emptyList());
                                    }
                                } catch (Throwable t) {
                                    future.completeExceptionally(t);
                                }
                            });
        } catch (Throwable t) {
            if (ExceptionUtils.findThrowable(t, PartitionNotExistException.class).isPresent()) {
                markPartitionInvalid(partitionSpec);
                return lookupLakeAsync(normalizedKeyRow, remainingFilter);
            } else {
                future.completeExceptionally(t);
            }
        }
        return future;
    }

    private CompletableFuture<Collection<RowData>> lookupLakeAsync(
            RowData normalizedKeyRow, @Nullable LookupNormalizer.RemainingFilter remainingFilter) {
        return lakeLookupRuntime
                .lookup(normalizedKeyRow)
                .thenApply(
                        result ->
                                lookupResultConverter.convert(
                                        result.getRowList(), remainingFilter));
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing hybrid lake async lookup function for table {}.", tablePath);
        lakeLookupRuntime.close();
        flussLookupRuntime.close();
    }
}
