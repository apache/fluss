/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.KvStateLookupResult.Status;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.utils.concurrent.Scheduler;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Coordinates lookup, write, and lifecycle operations for historical partitions. */
@Internal
public final class HistoricalPartitionManager implements AutoCloseable {

    private final HistoricalPartitionTaskExecutor taskExecutor;
    private final HistoricalLakeLookupManager lakeLookupManager;

    /** Creates a historical partition manager from the tablet server dependencies. */
    public HistoricalPartitionManager(
            Configuration conf,
            @Nullable PluginManager pluginManager,
            LocalDiskManager localDiskManager,
            File dataDir,
            long dataDirVolumeBytes,
            Scheduler scheduler) {
        this(
                new HistoricalPartitionTaskExecutor(conf),
                new HistoricalLakeLookupManager(
                        conf,
                        pluginManager,
                        localDiskManager,
                        dataDir,
                        dataDirVolumeBytes,
                        scheduler));
    }

    @VisibleForTesting
    HistoricalPartitionManager(
            HistoricalPartitionTaskExecutor taskExecutor,
            HistoricalLakeLookupManager lakeLookupManager) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.lakeLookupManager =
                checkNotNull(lakeLookupManager, "lakeLookupManager must not be null");
    }

    /** Starts the resources used by historical partition operations. */
    public void startup(Scheduler scheduler) {
        lakeLookupManager.startup(scheduler);
    }

    /** Looks up historical keys from the local overlay and then lake storage. */
    public CompletableFuture<LookupResultForBucket> lookup(
            Replica replica,
            LookupDataForBucket lookupData,
            LakeTableLookuper.LookupMetricRecorder lookupMetricRecorder) {
        TableBucket tableBucket = lookupData.tableBucket();
        try {
            LakeTableLookuper.LookupMetricRecorder checkedMetricRecorder =
                    checkNotNull(lookupMetricRecorder, "lookupMetricRecorder must not be null.");
            return taskExecutor.submit(
                    () -> lookupInternal(replica, lookupData, checkedMetricRecorder),
                    () ->
                            new LookupResultForBucket(
                                    tableBucket,
                                    null,
                                    lookupData.originalPartitionName(),
                                    ApiError.fromThrowable(
                                            new HistoricalPartitionThrottledException(
                                                    "Historical lookup is throttled for "
                                                            + tableBucket
                                                            + "."))));
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(
                    new LookupResultForBucket(
                            tableBucket,
                            null,
                            lookupData.originalPartitionName(),
                            ApiError.fromThrowable(e)));
        }
    }

    /** Writes records to the local overlay of a historical partition. */
    public CompletableFuture<PutKvResultForBucket> put(
            Replica replica,
            PutKvDataForBucket putData,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            int requiredAcks) {
        try {
            String originalPartitionName =
                    checkNotNull(
                            putData.originalPartitionName(),
                            "originalPartitionName must not be null");
            HistoricalWriteKey orderingKey =
                    new HistoricalWriteKey(putData.tableBucket(), originalPartitionName);
            return taskExecutor.submitOrdered(
                    orderingKey,
                    () -> {
                        try {
                            LogAppendInfo appendInfo =
                                    processPut(
                                            replica,
                                            putData,
                                            targetColumns,
                                            mergeMode,
                                            requiredAcks);
                            return new PutKvResultForBucket(
                                    putData.tableBucket(), appendInfo.lastOffset() + 1);
                        } catch (Throwable t) {
                            return new PutKvResultForBucket(
                                    putData.tableBucket(), ApiError.fromThrowable(t));
                        }
                    },
                    () ->
                            new PutKvResultForBucket(
                                    putData.tableBucket(),
                                    ApiError.fromThrowable(
                                            new HistoricalPartitionThrottledException(
                                                    "Historical write is throttled for "
                                                            + putData.tableBucket()
                                                            + "."))));
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(
                    new PutKvResultForBucket(putData.tableBucket(), ApiError.fromThrowable(e)));
        }
    }

    /** Applies dynamic historical lookup configuration changes. */
    public void reconfigure(Configuration newConf) {
        lakeLookupManager.reconfigure(newConf);
    }

    /** Invalidates the cached lake lookuper for the given table. */
    public void invalidateTableLookuper(long tableId) {
        lakeLookupManager.invalidateTableLookuper(tableId);
    }

    /** Returns the number of accepted historical operations that have not completed. */
    public int numInflightRequests() {
        return taskExecutor.numInflightRequests();
    }

    /** Returns the current disk usage of the historical lake lookup cache. */
    public long lookupCacheDiskSize() {
        return lakeLookupManager.lookupCacheDiskSize();
    }

    /** Returns the number of cached historical lake table lookupers. */
    public int cachedTableCount() {
        return lakeLookupManager.cachedTableCount();
    }

    /** Returns the counter for lookuper evictions caused by the table cache capacity. */
    public Counter capacityEvictions() {
        return lakeLookupManager.capacityEvictions();
    }

    @VisibleForTesting
    LogAppendInfo processPut(
            Replica replica,
            PutKvDataForBucket putData,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            int requiredAcks)
            throws Exception {
        TableInfo tableInfo = replica.getTableInfo();
        String originalPartitionName =
                checkNotNull(
                        putData.originalPartitionName(), "originalPartitionName must not be null");
        ResolvedPartitionSpec originalPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(
                        tableInfo.getPartitionKeys(), originalPartitionName);
        return replica.putHistoricalRecordsToLeader(
                putData.records(),
                targetColumns,
                mergeMode,
                originalPartitionName,
                primaryKey ->
                        lakeLookupManager.lookupValue(
                                tableInfo,
                                replica.getLatestSchemaInfo(),
                                originalPartitionSpec,
                                putData.tableBucket().getBucket(),
                                primaryKey,
                                replica.tableMetrics()::recordHistoricalLakeLookup),
                requiredAcks);
    }

    @Override
    public void close() {
        taskExecutor.close();
        lakeLookupManager.close();
    }

    private LookupResultForBucket lookupInternal(
            Replica replica,
            LookupDataForBucket lookupData,
            LakeTableLookuper.LookupMetricRecorder lookupMetricRecorder) {
        TableBucket tableBucket = lookupData.tableBucket();
        String originalPartitionName = lookupData.originalPartitionName();
        try {
            TableInfo tableInfo = replica.getTableInfo();
            if (originalPartitionName == null) {
                throw new InvalidPartitionException(
                        "Historical lookup request must carry the original partition name.");
            }
            ResolvedPartitionSpec originalPartitionSpec =
                    ResolvedPartitionSpec.fromPartitionName(
                            tableInfo.getPartitionKeys(), originalPartitionName);

            List<KvStateLookupResult> localResults =
                    replica.lookupHistoricalLocal(originalPartitionName, lookupData.keys());
            List<byte[]> missingKeys = new ArrayList<>();
            for (int i = 0; i < localResults.size(); i++) {
                KvStateLookupResult localResult = localResults.get(i);
                // Only a true local miss falls back to lake. A local value or tombstone is
                // authoritative and must not be overwritten by an older lake value.
                if (localResult.status() == Status.NOT_FOUND) {
                    missingKeys.add(lookupData.keys().get(i));
                }
            }

            List<byte[]> lakeValues = Collections.emptyList();
            if (!missingKeys.isEmpty()) {
                // Look up all local misses together. Results preserve the order of missingKeys.
                lakeValues =
                        lakeLookupManager.lookup(
                                new LookupDataForBucket(
                                        tableBucket, missingKeys, originalPartitionName),
                                tableInfo,
                                replica.getLatestSchemaInfo(),
                                originalPartitionSpec,
                                lookupMetricRecorder);
            }

            Iterator<byte[]> lakeValueIterator = lakeValues.iterator();
            List<byte[]> values = new ArrayList<>(localResults.size());
            for (KvStateLookupResult localResult : localResults) {
                // Consume one lake value for each NOT_FOUND result; local values and tombstones
                // keep their original positions without advancing the lake iterator.
                values.add(
                        localResult.status() == Status.NOT_FOUND
                                ? lakeValueIterator.next()
                                : localResult.value());
            }
            return new LookupResultForBucket(
                    tableBucket, values, originalPartitionName, ApiError.NONE);
        } catch (Exception e) {
            return new LookupResultForBucket(
                    tableBucket, null, originalPartitionName, ApiError.fromThrowable(e));
        }
    }

    private static final class HistoricalWriteKey {
        private final TableBucket tableBucket;
        private final String originalPartitionName;

        private HistoricalWriteKey(TableBucket tableBucket, String originalPartitionName) {
            this.tableBucket = tableBucket;
            this.originalPartitionName = originalPartitionName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof HistoricalWriteKey)) {
                return false;
            }
            HistoricalWriteKey that = (HistoricalWriteKey) o;
            return tableBucket.equals(that.tableBucket)
                    && originalPartitionName.equals(that.originalPartitionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableBucket, originalPartitionName);
        }
    }
}
