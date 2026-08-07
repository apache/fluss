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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private final LookupResultConverter lookupResultConverter;

    // Auto-created partitions cached as absent are expired and will never become live again.
    private final Map<PartitionSpec, Boolean> partitionExistenceCache = new ConcurrentHashMap<>();

    private final LakeLookupRuntime lakeLookupRuntime;
    private final Duration lakeFallbackTimeout;
    private final int lakeFallbackExecutorThreads;
    private final int lakeFallbackMaxConcurrency;
    private transient ThreadPoolExecutor lakeLookupExecutor;
    private transient ScheduledExecutorService timeoutExecutor;
    private transient Admin admin;

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

        this.lakeFallbackTimeout = lakeFallbackTimeout;
        this.lakeFallbackExecutorThreads = lakeFallbackExecutorThreads;
        this.lakeFallbackMaxConcurrency = lakeFallbackMaxConcurrency;

        int[] resolvedProjection =
                projection == null
                        ? IntStream.range(0, flinkRowType.getFieldCount()).toArray()
                        : projection;
        this.lookupResultConverter =
                new LookupResultConverter(
                        FlinkConversions.toFlussRowType(
                                FlinkUtils.projectRowType(flinkRowType, resolvedProjection)),
                        resolvedProjection);

        this.flussLookupRuntime =
                new FlussLookupRuntime(
                        flussConfig, tablePath, flinkRowType, lookupNormalizer, false);
        this.lakeLookupRuntime =
                new LakeLookupRuntime(
                        flussConfig,
                        tablePath,
                        FlinkConversions.toFlussRowType(flinkRowType),
                        primaryKeyIndexes,
                        tableOptions);
    }

    @Override
    public void open(@Nullable FunctionContext context) {
        LOG.info("Starting hybrid lake async lookup function for table {}.", tablePath);
        flussLookupRuntime.open();
        admin = flussLookupRuntime.getAdmin();
        initializePartitionExistenceCache();

        TableInfo tableInfo = flussLookupRuntime.getTableInfo();
        lakeLookupRuntime.open(tableInfo);

        lakeLookupExecutor = createLakeLookupExecutor();
        timeoutExecutor =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("fluss-lake-fallback-timeout"));
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
        LakeLookupRuntime.LakeLookupKey lakeLookupKey =
                lakeLookupRuntime.createLookupKey(normalizedKeyRow);

        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        lookupByPartition(normalizedKeyRow, lakeLookupKey, remainingFilter, future);
        return future;
    }

    private void lookupByPartition(
            RowData normalizedKeyRow,
            LakeLookupRuntime.LakeLookupKey lakeLookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future) {
        try {
            PartitionSpec partitionSpec = lakeLookupKey.getPartitionSpec().toPartitionSpec();
            Boolean partitionExists = partitionExistenceCache.get(partitionSpec);
            if (partitionExists == null) {
                partitionExists = getOrRefreshPartitionExistence(partitionSpec);
            }
            if (partitionExists) {
                lookupFlussAsync(normalizedKeyRow, lakeLookupKey, remainingFilter, future);
            } else {
                lookupLakeAsync(lakeLookupKey, remainingFilter, future);
            }
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    private void lookupFlussAsync(
            RowData normalizedKeyRow,
            LakeLookupRuntime.LakeLookupKey lakeLookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future) {
        PartitionSpec partitionSpec = lakeLookupKey.getPartitionSpec().toPartitionSpec();
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
                                            lookupLakeAsync(lakeLookupKey, remainingFilter, future);
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
                lookupLakeAsync(lakeLookupKey, remainingFilter, future);
            } else {
                future.completeExceptionally(t);
            }
        }
    }

    private ThreadPoolExecutor createLakeLookupExecutor() {
        int queueCapacity = lakeFallbackMaxConcurrency - lakeFallbackExecutorThreads;
        BlockingQueue<Runnable> queue =
                queueCapacity == 0
                        ? new SynchronousQueue<>()
                        : new ArrayBlockingQueue<>(queueCapacity);
        return new ThreadPoolExecutor(
                lakeFallbackExecutorThreads,
                lakeFallbackExecutorThreads,
                0L,
                TimeUnit.MILLISECONDS,
                queue,
                new ExecutorThreadFactory("fluss-lake-fallback-lookup"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private void lookupLakeAsync(
            LakeLookupRuntime.LakeLookupKey lakeLookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future) {
        ScheduledFuture<?> timeoutTask;
        try {
            timeoutTask =
                    timeoutExecutor.schedule(
                            () ->
                                    completeLakeFallbackExceptionally(
                                            future,
                                            new TimeoutException(
                                                    "Lake fallback lookup timed out after "
                                                            + lakeFallbackTimeout)),
                            lakeFallbackTimeout.toMillis(),
                            TimeUnit.MILLISECONDS);
            future.whenComplete((ignored, ignoredError) -> timeoutTask.cancel(false));
        } catch (RejectedExecutionException e) {
            completeLakeFallbackExceptionally(
                    future, new RuntimeException("Lake fallback timeout executor is closed.", e));
            return;
        }

        try {
            lakeLookupExecutor.execute(
                    () -> {
                        try {
                            Collection<RowData> rows = lookupLake(lakeLookupKey, remainingFilter);
                            completeLakeFallbackSuccessfully(future, rows);
                        } catch (Throwable t) {
                            completeLakeFallbackExceptionally(
                                    future,
                                    new RuntimeException(
                                            "Execution of lake fallback lookup failed: "
                                                    + t.getMessage(),
                                            t));
                        }
                    });
        } catch (RejectedExecutionException e) {
            completeLakeFallbackExceptionally(
                    future,
                    new RuntimeException("Lake fallback lookup executor is overloaded.", e));
        }
    }

    private void completeLakeFallbackSuccessfully(
            CompletableFuture<Collection<RowData>> future, Collection<RowData> rows) {
        future.complete(rows);
    }

    private void completeLakeFallbackExceptionally(
            CompletableFuture<Collection<RowData>> future, Throwable throwable) {
        future.completeExceptionally(throwable);
    }

    private Collection<RowData> lookupLake(
            LakeLookupRuntime.LakeLookupKey lakeLookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter)
            throws Exception {
        InternalRow row = lakeLookupRuntime.lookup(lakeLookupKey);
        if (row == null) {
            return Collections.emptyList();
        }
        return lookupResultConverter.convert(Collections.singletonList(row), remainingFilter);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing hybrid lake async lookup function for table {}.", tablePath);
        if (lakeLookupExecutor != null) {
            lakeLookupExecutor.shutdownNow();
        }
        if (timeoutExecutor != null) {
            timeoutExecutor.shutdownNow();
        }
        lakeLookupRuntime.close();
        flussLookupRuntime.close();
    }
}
