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
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.KvStateLookupResult.Status;
import org.apache.fluss.server.kv.historical.HistoricalValueLookup;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.ByteArrayWrapper;
import org.apache.fluss.utils.concurrent.Scheduler;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
                                    lookupData.originalPartitionName(),
                                    ApiError.fromThrowable(
                                            new HistoricalPartitionThrottledException(
                                                    "Historical lookup is throttled for "
                                                            + tableBucket
                                                            + " (original partition "
                                                            + lookupData.originalPartitionName()
                                                            + ")."))));
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(
                    new LookupResultForBucket(
                            tableBucket,
                            lookupData.originalPartitionName(),
                            ApiError.fromThrowable(e)));
        }
    }

    /** Writes records to the local overlay of a historical partition. */
    public CompletableFuture<PutKvResultForBucket> putKv(
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
            return taskExecutor.submitOrdered(
                    putData.tableBucket(),
                    () -> {
                        try {
                            LogAppendInfo appendInfo =
                                    processPut(
                                            replica,
                                            putData,
                                            targetColumns,
                                            mergeMode,
                                            requiredAcks);
                            return PutKvResultForBucket.historicalSuccess(
                                    putData.tableBucket(),
                                    appendInfo.lastOffset() + 1,
                                    originalPartitionName);
                        } catch (Throwable t) {
                            return PutKvResultForBucket.historicalFailure(
                                    putData.tableBucket(),
                                    ApiError.fromThrowable(t),
                                    originalPartitionName);
                        }
                    },
                    () ->
                            PutKvResultForBucket.historicalFailure(
                                    putData.tableBucket(),
                                    ApiError.fromThrowable(
                                            new HistoricalPartitionThrottledException(
                                                    "Historical write is throttled for "
                                                            + putData.tableBucket()
                                                            + " (original partition "
                                                            + originalPartitionName
                                                            + ").")),
                                    originalPartitionName));
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(
                    PutKvResultForBucket.historicalFailure(
                            putData.tableBucket(),
                            ApiError.fromThrowable(e),
                            putData.originalPartitionName()));
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

    /** Requires future fallback lookups to reload after the given lake snapshot notification. */
    public void requireLakeSnapshot(long tableId, long lakeSnapshotId) {
        lakeLookupManager.requireLakeSnapshot(tableId, lakeSnapshotId);
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
        // The public put path holds the TableBucket ordering slot until processPut returns, so
        // local state cannot be changed by a later historical write between resolve and apply.
        int expectedLeaderEpoch = replica.getLeaderEpoch();
        List<byte[]> keysRequiringLakeLookup =
                replica.findKeysRequiringLakeLookup(
                        putData.records(),
                        targetColumns,
                        mergeMode,
                        originalPartitionName,
                        expectedLeaderEpoch,
                        requiredAcks);

        Map<ByteArrayWrapper, KvStateLookupResult> lakeResults = new HashMap<>();
        if (!keysRequiringLakeLookup.isEmpty()) {
            List<byte[]> lakeValues =
                    lakeLookupManager.lookup(
                            new LookupDataForBucket(
                                    putData.tableBucket(),
                                    keysRequiringLakeLookup,
                                    originalPartitionName),
                            tableInfo,
                            replica.getLatestSchemaInfo(),
                            originalPartitionSpec,
                            replica.tableMetrics()::recordHistoricalLakeLookup);
            for (int i = 0; i < keysRequiringLakeLookup.size(); i++) {
                byte[] lakeValue = lakeValues.get(i);
                lakeResults.put(
                        new ByteArrayWrapper(keysRequiringLakeLookup.get(i)),
                        lakeValue == null
                                ? KvStateLookupResult.notFound()
                                : KvStateLookupResult.present(lakeValue));
            }
        }

        HistoricalValueLookup memoizedLakeLookup =
                primaryKey -> {
                    KvStateLookupResult result =
                            checkNotNull(
                                    lakeResults.get(new ByteArrayWrapper(primaryKey)),
                                    "No resolved lake value for a historical write key");
                    return result.value();
                };

        return replica.putHistoricalRecordsToLeader(
                putData.records(),
                targetColumns,
                mergeMode,
                originalPartitionName,
                memoizedLakeLookup,
                expectedLeaderEpoch,
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
            List<ByteArraySlice> values = new ArrayList<>(localResults.size());
            KvValueLayout localValueLayout =
                    KvValueLayout.fromTableConfig(tableInfo.getTableConfig());
            for (KvStateLookupResult localResult : localResults) {
                // Consume one lake value for each NOT_FOUND result; local values and tombstones
                // keep their original positions without advancing the lake iterator.
                if (localResult.status() == Status.NOT_FOUND) {
                    values.add(KvValueLayout.PLAIN.toValueBodySlice(lakeValueIterator.next()));
                } else {
                    values.add(localValueLayout.toValueBodySlice(localResult.value()));
                }
            }
            return new LookupResultForBucket(tableBucket, values, originalPartitionName);
        } catch (Exception e) {
            return new LookupResultForBucket(
                    tableBucket, originalPartitionName, ApiError.fromThrowable(e));
        }
    }
}
