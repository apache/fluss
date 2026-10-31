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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.InvalidBucketRoutingException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.RequiredApiVersionNotSupportedException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogReqForTable;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForTable;
import org.apache.fluss.rpc.messages.PbListOffsetsRespForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.FetchLogReadPreference;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeListOffsetsRequest;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.getFetchLogResultForBucket;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** fetcher to fetch log. */
@Internal
public class LogFetcher implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(LogFetcher.class);

    /** Per-table read contexts keyed by tableId. */
    @GuardedBy("this")
    private final Map<Long, TableReadContext> tableReadContexts;

    private final ChunkedAllocationManager.ChunkedFactory chunkedFactory;
    private final int maxFetchBytes;
    private final int maxBucketFetchBytes;
    private final int minFetchBytes;
    private final int maxFetchWaitMs;
    private final boolean isCheckCrcs;
    private final LogScannerStatus logScannerStatus;
    private final LogFetchBuffer logFetchBuffer;
    private final LogFetchCollector logFetchCollector;
    private final ArrowLogFetchCollector arrowLogFetchCollector;
    private final RemoteLogDownloader remoteLogDownloader;
    private final FetchLogReadPreference readPreference;

    @GuardedBy("this")
    private final Set<Integer> nodesWithPendingFetchRequests;

    @GuardedBy("this")
    private boolean isClosed = false;

    private final MetadataUpdater metadataUpdater;
    private final ScannerMetricGroup scannerMetricGroup;
    private final LogRecordReadContext.SchemaResolution schemaResolution;

    @GuardedBy("this")
    private final Map<TableBucket, LegacyFetchState> legacySubscriptions = new HashMap<>();

    /** Create a multi-table LogFetcher without an initial table. */
    public LogFetcher(
            String scannerName,
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater,
            ScannerMetricGroup scannerMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            LogRecordReadContext.SchemaResolution schemaResolution) {
        this.tableReadContexts = new HashMap<>();
        this.chunkedFactory = new ChunkedAllocationManager.ChunkedFactory();
        this.logScannerStatus = logScannerStatus;
        this.maxFetchBytes =
                (int) conf.get(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES).getBytes();
        this.maxBucketFetchBytes =
                (int)
                        conf.get(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET)
                                .getBytes();
        this.minFetchBytes =
                (int) conf.get(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MIN_BYTES).getBytes();
        this.maxFetchWaitMs =
                (int) conf.get(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_WAIT_MAX_TIME).toMillis();

        this.isCheckCrcs = conf.getBoolean(ConfigOptions.CLIENT_SCANNER_LOG_CHECK_CRC);
        this.logFetchBuffer = new LogFetchBuffer();
        this.nodesWithPendingFetchRequests = new HashSet<>();
        this.metadataUpdater = metadataUpdater;
        this.logFetchCollector = new LogFetchCollector(logScannerStatus, conf, metadataUpdater);
        this.arrowLogFetchCollector =
                new ArrowLogFetchCollector(logScannerStatus, conf, metadataUpdater);
        this.scannerMetricGroup = scannerMetricGroup;
        this.readPreference = conf.get(ConfigOptions.CLIENT_SCANNER_LOG_READ_PREFERENCE);
        this.remoteLogDownloader =
                new RemoteLogDownloader(
                        scannerName, conf, remoteFileDownloader, scannerMetricGroup);
        remoteLogDownloader.start();
        this.schemaResolution = schemaResolution;
    }

    /**
     * Register a new table for multi-table fetching. If the table is already registered, this is a
     * no-op.
     *
     * <p>The fetcher monitor protects registration against asynchronous fetch response callbacks.
     */
    public synchronized void registerTable(TableScanSpec spec, SchemaGetter schemaGetter) {
        TableInfo tableInfo = spec.getTableInfo();
        long tableId = tableInfo.getTableId();
        if (!tableReadContexts.containsKey(tableId)) {
            TableReadContext tableReadContext =
                    new TableReadContext(
                            tableInfo,
                            schemaResolution,
                            spec.getProjection(),
                            spec.getFilter(),
                            schemaGetter,
                            chunkedFactory);
            tableReadContexts.put(tableId, tableReadContext);
        }
    }

    synchronized void onSubscriptionChanged(TableBucket bucket, boolean discardBuffered) {
        legacySubscriptions.remove(bucket);
        if (discardBuffered) {
            logFetchBuffer.discardBucket(bucket);
        }
    }

    synchronized void unregisterTable(long tableId) {
        TableReadContext tableReadContext = tableReadContexts.remove(tableId);
        if (tableReadContext == null) {
            return;
        }
        Set<TableBucket> retainedBuckets =
                new HashSet<>(logScannerStatus.fetchableBuckets(ignored -> true));
        logFetchBuffer.retainAll(retainedBuckets);
        tableReadContext.close();
    }

    /**
     * Return whether we have any completed fetches that are fetch-able. This method is thread-safe.
     *
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    public boolean hasAvailableFetches() {
        return !logFetchBuffer.isEmpty();
    }

    public ScanRecords collectFetch() {
        logFetchBuffer.discardObsolete();
        return logFetchCollector.collectFetch(logFetchBuffer);
    }

    public ArrowScanRecords collectArrowFetch() {
        logFetchBuffer.discardObsolete();
        return arrowLogFetchCollector.collectFetch(logFetchBuffer);
    }

    /**
     * Set up a fetch request for any node that we have assigned buckets for which doesn't already
     * have an in-flight fetch or pending fetch data.
     */
    public void sendFetches() {
        logFetchBuffer.discardObsolete();
        checkAndUpdateMetadata(fetchableBuckets());
        synchronized (this) {
            // Recompute after metadata update because response callbacks can populate the buffer.
            // Don't perform heavy I/O operations or synchronous waits here.
            Map<Integer, FetchLogRequest> fetchRequestMap =
                    prepareFetchLogRequests(fetchableBuckets());
            fetchRequestMap.forEach(
                    (nodeId, fetchLogRequest) -> {
                        LOG.debug("Adding pending request for node id {}", nodeId);
                        nodesWithPendingFetchRequests.add(nodeId);
                        sendFetchRequest(nodeId, fetchLogRequest);
                    });
        }
    }

    /**
     * @param deadlineNanos the deadline time to wait until
     * @return false if the waiting time detectably elapsed before return from the method, else true
     */
    public boolean awaitNotEmpty(long deadlineNanos) {
        long retryDeadlineNanos = Long.MAX_VALUE;
        synchronized (this) {
            for (Map.Entry<TableBucket, LegacyFetchState> entry : legacySubscriptions.entrySet()) {
                LegacyFetchState legacy = entry.getValue();
                if (legacy.resolving || legacy.failed || legacy.retryAfterNanos == 0L) {
                    continue;
                }
                if (logScannerStatus.matchesRequestedRange(
                        entry.getKey(), LogScanner.EARLIEST_OFFSET, legacy.stoppingOffset)) {
                    retryDeadlineNanos = Math.min(retryDeadlineNanos, legacy.retryAfterNanos);
                }
            }
        }
        long actualDeadlineNanos = Math.min(deadlineNanos, retryDeadlineNanos);
        try {
            boolean available = logFetchBuffer.awaitNotEmpty(actualDeadlineNanos);
            return available
                    || (retryDeadlineNanos < deadlineNanos
                            && System.nanoTime() >= retryDeadlineNanos);
        } catch (InterruptedException e) {
            LOG.trace("Interrupted during fetching", e);
            // true for interrupted
            return true;
        }
    }

    public void wakeup() {
        logFetchBuffer.wakeup();
    }

    private void checkAndUpdateMetadata(List<TableBucket> tableBuckets) {
        // Group buckets (without a leader) by tableId to support multi-table fetching.
        Map<Long, List<TableBucket>> bucketsByTable = new HashMap<>();
        for (TableBucket tb : tableBuckets) {
            if (getTableBucketLeader(tb) != null) {
                continue;
            }
            bucketsByTable.computeIfAbsent(tb.getTableId(), k -> new ArrayList<>()).add(tb);
        }

        for (Map.Entry<Long, List<TableBucket>> entry : bucketsByTable.entrySet()) {
            long tableId = entry.getKey();
            List<TableBucket> buckets = entry.getValue();
            TableReadContext tableReadContext;
            synchronized (this) {
                tableReadContext = tableReadContexts.get(tableId);
            }
            checkNotNull(tableReadContext, "Table read context for table id " + tableId);
            boolean isPartitioned = tableReadContext.isPartitioned;
            TablePath tablePath = tableReadContext.tablePath;
            try {
                if (isPartitioned) {
                    List<Long> partitionIds = new ArrayList<>();
                    for (TableBucket tb : buckets) {
                        partitionIds.add(tb.getPartitionId());
                    }
                    if (!partitionIds.isEmpty()) {
                        metadataUpdater.updateMetadata(
                                Collections.singleton(tablePath), null, partitionIds);
                    }
                } else {
                    metadataUpdater.updateTableOrPartitionMetadata(tablePath, null);
                }
            } catch (Exception e) {
                if (e instanceof PartitionNotExistException) {
                    LOG.warn(
                            "Receive PartitionNotExistException when update metadata, ignore it",
                            e);
                } else {
                    throw e;
                }
            }
        }
    }

    @VisibleForTesting
    synchronized void sendFetchRequest(int destination, FetchLogRequest fetchLogRequest) {
        FetchRequestContext fetchRequestContext = createFetchRequestContext(fetchLogRequest);
        // TODO cache the tablet server gateway.
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);
        if (gateway == null) {
            handleFetchLogException(
                    destination,
                    fetchRequestContext,
                    new LeaderNotAvailableException(
                            "Server " + destination + " is not found in metadata cache."));
        } else {
            final long requestStartTime = System.currentTimeMillis();
            scannerMetricGroup.fetchRequestCount().inc();

            try {
                gateway.fetchLog(fetchLogRequest)
                        .whenComplete(
                                (fetchLogResponse, e) -> {
                                    if (e != null) {
                                        handleFetchLogException(
                                                destination, fetchRequestContext, e);
                                    } else {
                                        handleFetchLogResponse(
                                                destination,
                                                requestStartTime,
                                                fetchLogResponse,
                                                fetchRequestContext);
                                    }
                                })
                        .whenComplete(
                                (ignored, callbackError) -> {
                                    if (callbackError != null) {
                                        LOG.error(
                                                "Unexpected error while handling FETCH_LOG "
                                                        + "response from node {}.",
                                                destination,
                                                ExceptionUtils.stripCompletionException(
                                                        callbackError));
                                    }
                                });
            } catch (Throwable t) {
                handleFetchLogException(destination, fetchRequestContext, t);
            }
        }
    }

    private FetchRequestContext createFetchRequestContext(FetchLogRequest fetchLogRequest) {
        Map<Long, TableReadContext> requestedTableContexts = new HashMap<>();
        Set<Long> tableIdsInFetchRequest = new HashSet<>();
        Set<TablePartition> tablePartitionsInFetchRequest = new HashSet<>();
        Map<TableBucket, LogScannerStatus.SubscriptionSnapshot> subscriptions = new HashMap<>();
        for (PbFetchLogReqForTable fetchTableRequest : fetchLogRequest.getTablesReqsList()) {
            long tableId = fetchTableRequest.getTableId();
            TableReadContext tableReadContext = tableReadContexts.get(tableId);
            if (tableReadContext == null) {
                LOG.warn(
                        "Skip fetch log request for table id {} because its read context is not "
                                + "registered. The table may have already been unregistered.",
                        tableId);
                continue;
            }
            requestedTableContexts.put(tableId, tableReadContext);
            if (!tableReadContext.isPartitioned) {
                tableIdsInFetchRequest.add(tableId);
            }
            for (PbFetchLogReqForBucket fetchLogReqForBucket :
                    fetchTableRequest.getBucketsReqsList()) {
                Long partitionId =
                        fetchLogReqForBucket.hasPartitionId()
                                ? fetchLogReqForBucket.getPartitionId()
                                : null;
                TableBucket bucket =
                        new TableBucket(tableId, partitionId, fetchLogReqForBucket.getBucketId());
                if (tableReadContext.isPartitioned) {
                    tablePartitionsInFetchRequest.add(
                            new TablePartition(tableId, fetchLogReqForBucket.getPartitionId()));
                }
                LogScannerStatus.SubscriptionSnapshot snapshot =
                        logScannerStatus.subscriptionSnapshot(bucket);
                if (snapshot == null || snapshot.offset != fetchLogReqForBucket.getFetchOffset()) {
                    LOG.warn(
                            "FETCH_LOG request context mismatch: bucket={}, "
                                    + "requestOffset={}, subscriptionOffset={}",
                            bucket,
                            fetchLogReqForBucket.getFetchOffset(),
                            snapshot == null ? null : snapshot.offset);
                    continue;
                }
                if (fetchLogReqForBucket.hasStoppingOffset()) {
                    if (!snapshot.isBounded()
                            || snapshot.stoppingOffset
                                    != fetchLogReqForBucket.getStoppingOffset()) {
                        continue;
                    }
                } else if (snapshot.isBounded()) {
                    LegacyFetchState legacy = legacySubscriptions.get(bucket);
                    if (legacy == null || legacy.stoppingOffset != snapshot.stoppingOffset) {
                        continue;
                    }
                }
                subscriptions.put(bucket, snapshot);
            }
        }
        return new FetchRequestContext(
                requestedTableContexts,
                new TableOrPartitions(
                        tableIdsInFetchRequest.isEmpty() ? null : tableIdsInFetchRequest,
                        tablePartitionsInFetchRequest.isEmpty()
                                ? null
                                : tablePartitionsInFetchRequest),
                subscriptions);
    }

    /** A helper class to hold table ids or table partitions. */
    @VisibleForTesting
    static class TableOrPartitions {
        private final @Nullable Set<Long> tableIds;
        private final @Nullable Set<TablePartition> tablePartitions;

        TableOrPartitions(
                @Nullable Set<Long> tableIds, @Nullable Set<TablePartition> tablePartitions) {
            this.tableIds = tableIds;
            this.tablePartitions = tablePartitions;
        }
    }

    @VisibleForTesting
    void invalidTableOrPartitions(TableOrPartitions tableOrPartitions) {
        Set<PhysicalTablePath> physicalTablePaths =
                metadataUpdater.getPhysicalTablePathByIds(
                        tableOrPartitions.tableIds, tableOrPartitions.tablePartitions);
        metadataUpdater.invalidPhysicalTableBucketMeta(physicalTablePaths);
    }

    private synchronized void handleFetchLogException(
            int destination, FetchRequestContext fetchRequestContext, Throwable e) {
        try {
            if (isClosed) {
                return;
            }
            Throwable cause = ExceptionUtils.stripCompletionException(e);
            if (cause instanceof RequiredApiVersionNotSupportedException) {
                boolean fallbackRequired = false;

                for (Map.Entry<TableBucket, LogScannerStatus.SubscriptionSnapshot> entry :
                        fetchRequestContext.subscriptions.entrySet()) {
                    TableBucket bucket = entry.getKey();
                    LogScannerStatus.SubscriptionSnapshot snapshot = entry.getValue();

                    if (!snapshot.isBounded()
                            || !logScannerStatus.matchesRequestedRange(
                                    bucket, snapshot.offset, snapshot.stoppingOffset)) {
                        continue;
                    }

                    LegacyFetchState legacy = legacySubscriptions.get(bucket);
                    if (legacy == null || legacy.stoppingOffset != snapshot.stoppingOffset) {
                        legacySubscriptions.put(
                                bucket, new LegacyFetchState(snapshot.stoppingOffset));
                    }
                    fallbackRequired = true;
                }
                if (fallbackRequired) {
                    logFetchBuffer.requestFetchRetry();
                    return;
                }
            }
            // if is invalid metadata exception, we need to clear table bucket meta
            // to enable another round of log fetch to request new medata
            LOG.error("Failed to fetch log from node {}", destination, cause);
            if (cause instanceof InvalidMetadataException) {
                LOG.warn(
                        "Invalid metadata error in fetch log request. "
                                + "Going to request metadata update.",
                        cause);
                invalidTableOrPartitions(fetchRequestContext.tableOrPartitions);
            }
        } finally {
            LOG.debug("Removing pending request for node: {}", destination);
            nodesWithPendingFetchRequests.remove(destination);
        }
    }

    /** Implements the core logic for a successful fetch log response. */
    private synchronized void handleFetchLogResponse(
            int destination,
            long requestStartTime,
            FetchLogResponse fetchLogResponse,
            FetchRequestContext fetchRequestContext) {
        // Capture the parsed ByteBuf for buffer lifecycle management. The response may
        // have been lazily parsed from the network buffer. Each DefaultCompletedFetch
        // that references the buffer's records data must retain it. We release the base
        // reference in the finally block.
        ByteBuf parsedByteBuf = fetchLogResponse.getParsedByteBuf();
        try {
            if (isClosed) {
                return;
            }

            // update fetch metrics only when request success
            scannerMetricGroup.updateFetchLatency(System.currentTimeMillis() - requestStartTime);
            scannerMetricGroup.bytesPerRequest().update(fetchLogResponse.totalSize());

            for (PbFetchLogRespForTable respForTable : fetchLogResponse.getTablesRespsList()) {
                long tableId = respForTable.getTableId();
                TableReadContext trc = fetchRequestContext.requestedTableContexts.get(tableId);
                if (trc == null || tableReadContexts.get(tableId) != trc) {
                    LOG.debug(
                            "Ignoring stale or unsolicited fetch log response for table id {}.",
                            tableId);
                    continue;
                }
                TablePath tablePathForResp = trc.tablePath;
                for (PbFetchLogRespForBucket respForBucket : respForTable.getBucketsRespsList()) {
                    TableBucket tb =
                            new TableBucket(
                                    tableId,
                                    respForBucket.hasPartitionId()
                                            ? respForBucket.getPartitionId()
                                            : null,
                                    respForBucket.getBucketId());

                    LogScannerStatus.SubscriptionSnapshot snapshot =
                            fetchRequestContext.subscriptions.get(tb);
                    if (snapshot == null) {
                        continue;
                    }
                    if (!logScannerStatus.matchesRequestedRange(
                            tb, snapshot.offset, snapshot.stoppingOffset)) {
                        continue;
                    }
                    FetchLogResultForBucket fetchResultForBucket =
                            getFetchLogResultForBucket(tb, tablePathForResp, respForBucket);

                    // if error code is not NONE, it means the fetch log request failed, we need to
                    // clear table bucket meta for InvalidMetadataException.
                    if (fetchResultForBucket.getErrorCode() != Errors.NONE.code()) {
                        ApiError error = ApiError.fromErrorMessage(respForBucket);
                        handleFetchLogExceptionForBucket(tb, destination, error);
                    }

                    boolean resolveStart =
                            snapshot.isBounded()
                                    && snapshot.offset == LogScanner.EARLIEST_OFFSET
                                    && fetchResultForBucket.succeeded();
                    long logStartOffset = fetchResultForBucket.getLogStartOffset();

                    if (resolveStart && logStartOffset < 0) {
                        enqueueFailure(
                                tb,
                                snapshot,
                                new FetchException(
                                        "Missing or invalid bounded log start offset for " + tb));
                        continue;
                    }
                    if (fetchResultForBucket.fetchFromRemote()) {
                        long start = snapshot.offset;
                        if (resolveStart) {
                            enqueueStartResolution(
                                    tb,
                                    trc,
                                    snapshot,
                                    logStartOffset,
                                    fetchResultForBucket.getHighWatermark());
                            start = logStartOffset;
                        }
                        if (!snapshot.isBounded() || start < snapshot.stoppingOffset) {
                            pendRemoteFetches(
                                    trc,
                                    fetchResultForBucket.remoteLogFetchInfo(),
                                    start,
                                    fetchResultForBucket.getHighWatermark(),
                                    snapshot);
                        }
                    } else {
                        LogRecords logRecords = fetchResultForBucket.recordsOrEmpty();
                        boolean hasRecords = !MemoryLogRecords.EMPTY.equals(logRecords);
                        if (hasRecords
                                || fetchResultForBucket.getErrorCode() != Errors.NONE.code()
                                || fetchResultForBucket.hasFilteredEndOffset()
                                || resolveStart) {
                            // Retain the parsed buffer so it stays alive while
                            // this CompletedFetch's records are being consumed.
                            if (hasRecords && parsedByteBuf != null) {
                                parsedByteBuf.retain();
                            }
                            CompletedFetch completed;
                            try {
                                completed =
                                        new DefaultCompletedFetch(
                                                tb,
                                                tablePathForResp,
                                                fetchResultForBucket,
                                                trc.readContext,
                                                logScannerStatus,
                                                // skipping CRC check if projection push downed as
                                                // the data is pruned
                                                isCheckCrcs,
                                                snapshot.offset,
                                                hasRecords ? parsedByteBuf : null);
                            } catch (Throwable failure) {
                                if (hasRecords && parsedByteBuf != null) {
                                    parsedByteBuf.release();
                                }
                                throw failure;
                            }
                            completed.bindSubscription(snapshot, logStartOffset);
                            logFetchBuffer.add(completed);
                        }
                    }
                }
            }
        } finally {
            // Release the base reference from the network buffer. Any CompletedFetch
            // objects created above hold their own retained references, keeping the
            // buffer alive until they are drained.
            if (parsedByteBuf != null) {
                parsedByteBuf.release();
            }
            LOG.debug("Removing pending request for node: {}", destination);
            nodesWithPendingFetchRequests.remove(destination);
        }
    }

    private void handleFetchLogExceptionForBucket(TableBucket tb, int destination, ApiError error) {
        ApiException exception = error.error().exception();
        LOG.error("Failed to fetch log from node {} for bucket {}", destination, tb, exception);
        if (exception instanceof InvalidMetadataException
                || exception instanceof InvalidBucketRoutingException) {
            LOG.warn(
                    "Metadata or bucket routing error in fetch log request. "
                            + "Going to request metadata update.",
                    exception);
            long tableId = tb.getTableId();
            TableOrPartitions tableOrPartitions;
            if (tb.getPartitionId() == null) {
                tableOrPartitions = new TableOrPartitions(Collections.singleton(tableId), null);
            } else {
                tableOrPartitions =
                        new TableOrPartitions(
                                null,
                                Collections.singleton(
                                        new TablePartition(tableId, tb.getPartitionId())));
            }
            invalidTableOrPartitions(tableOrPartitions);
        }
    }

    private void pendRemoteFetches(
            TableReadContext tableReadContext,
            RemoteLogFetchInfo remoteLogFetchInfo,
            long firstFetchOffset,
            long highWatermark,
            LogScannerStatus.SubscriptionSnapshot subscription) {
        checkNotNull(remoteLogFetchInfo);
        FsPath remoteLogTabletDir = new FsPath(remoteLogFetchInfo.remoteLogTabletDir());
        List<RemoteLogSegment> remoteLogSegments = remoteLogFetchInfo.remoteLogSegmentList();
        int posInLogSegment = remoteLogFetchInfo.firstStartPos();
        long fetchOffset = firstFetchOffset;
        for (int i = 0; i < remoteLogSegments.size(); i++) {
            RemoteLogSegment segment = remoteLogSegments.get(i);
            if (subscription.isBounded()
                    && segment.remoteLogStartOffset() >= subscription.stoppingOffset) {
                break;
            }
            if (i > 0) {
                posInLogSegment = 0;
                fetchOffset = segment.remoteLogStartOffset();
            }
            RemoteLogDownloadFuture downloadFuture =
                    remoteLogDownloader.requestRemoteLog(remoteLogTabletDir, segment);
            RemotePendingFetch pendingFetch =
                    new RemotePendingFetch(
                            segment,
                            downloadFuture,
                            tableReadContext.tablePath,
                            posInLogSegment,
                            fetchOffset,
                            highWatermark,
                            tableReadContext.remoteReadContext,
                            logScannerStatus,
                            isCheckCrcs,
                            subscription);
            logFetchBuffer.pend(pendingFetch);
            downloadFuture.whenComplete(
                    throwable -> logFetchBuffer.tryComplete(pendingFetch, throwable));
        }
    }

    @VisibleForTesting
    Map<Integer, FetchLogRequest> prepareFetchLogRequests(List<TableBucket> fetchableBuckets) {
        // Outer key: leaderId, inner key: tableId -> list of bucket requests
        Map<Integer, Map<Long, List<PbFetchLogReqForBucket>>> fetchReqsByLeaderAndTable =
                new HashMap<>();
        int readyForFetchCount = 0;
        for (TableBucket tb : fetchableBuckets) {
            LogScannerStatus.SubscriptionSnapshot snapshot =
                    logScannerStatus.subscriptionSnapshot(tb);
            if (snapshot == null) {
                LOG.debug(
                        "Skipping fetch request for bucket {} because the bucket has been "
                                + "unsubscribed.",
                        tb);
                continue;
            }
            long offset = snapshot.offset;
            if (snapshot.isBounded()
                    && (snapshot.stoppingOffset == 0 || offset >= snapshot.stoppingOffset)) {
                continue;
            }
            LegacyFetchState legacy = legacySubscriptions.get(tb);
            if (legacy != null
                    && (!snapshot.isBounded()
                            || legacy.stoppingOffset != snapshot.stoppingOffset)) {
                legacySubscriptions.remove(tb);
                legacy = null;
            }
            if (legacy != null) {
                if (legacy.failed) {
                    continue;
                }
                if (offset == LogScanner.EARLIEST_OFFSET) {
                    resolveLegacyStart(tb, snapshot, legacy);
                    continue;
                }
            }

            // TODO add select preferred read replica, currently we can only read from leader.

            Integer leader = getTableBucketLeader(tb);
            if (leader == null) {
                LOG.trace(
                        "Skipping fetch request for bucket {} because leader is not available.",
                        tb);
            } else if (nodesWithPendingFetchRequests.contains(leader)) {
                LOG.trace(
                        "Skipping fetch request for bucket {} because previous request "
                                + "to server {} has not been processed.",
                        tb,
                        leader);
            } else {
                PbFetchLogReqForBucket fetchLogReqForBucket =
                        new PbFetchLogReqForBucket()
                                .setBucketId(tb.getBucket())
                                .setFetchOffset(offset)
                                .setMaxFetchBytes(maxBucketFetchBytes);

                if (snapshot.isBounded() && legacy == null) {
                    fetchLogReqForBucket.setStoppingOffset(snapshot.stoppingOffset);
                }

                if (tb.getPartitionId() != null) {
                    fetchLogReqForBucket.setPartitionId(tb.getPartitionId());
                }
                metadataUpdater
                        .getCluster()
                        .getBucketCount(TableOrPartition.of(tb.getTableId(), tb.getPartitionId()))
                        .ifPresent(fetchLogReqForBucket::setRoutingBucketCount);
                fetchReqsByLeaderAndTable
                        .computeIfAbsent(leader, k -> new HashMap<>())
                        .computeIfAbsent(tb.getTableId(), k -> new ArrayList<>())
                        .add(fetchLogReqForBucket);
                readyForFetchCount++;
            }
        }

        if (readyForFetchCount == 0) {
            return Collections.emptyMap();
        } else {
            Map<Integer, FetchLogRequest> fetchLogRequests = new HashMap<>();
            for (Map.Entry<Integer, Map<Long, List<PbFetchLogReqForBucket>>> leaderEntry :
                    fetchReqsByLeaderAndTable.entrySet()) {
                int leaderId = leaderEntry.getKey();
                Map<Long, List<PbFetchLogReqForBucket>> tableReqs = leaderEntry.getValue();

                FetchLogRequest fetchLogRequest =
                        new FetchLogRequest()
                                .setFollowerServerId(-1)
                                .setMaxBytes(maxFetchBytes)
                                .setMinBytes(minFetchBytes)
                                .setMaxWaitMs(maxFetchWaitMs)
                                .setReadPreference(readPreference.value());

                List<PbFetchLogReqForTable> tablesReqs = new ArrayList<>();
                for (Map.Entry<Long, List<PbFetchLogReqForBucket>> tableEntry :
                        tableReqs.entrySet()) {
                    long tableId = tableEntry.getKey();
                    List<PbFetchLogReqForBucket> bucketReqs = tableEntry.getValue();
                    TableReadContext trc = tableReadContexts.get(tableId);
                    if (trc == null) {
                        LOG.warn(
                                "Skip fetch log request for table id {} because its read context "
                                        + "is not registered. The table may have already been "
                                        + "unregistered.",
                                tableId);
                        continue;
                    }

                    PbFetchLogReqForTable reqForTable =
                            new PbFetchLogReqForTable().setTableId(tableId);
                    if (trc.readContext.isProjectionPushDowned()) {
                        assert trc.projection != null;
                        reqForTable
                                .setProjectionPushdownEnabled(true)
                                .setProjectedFields(trc.projection.getProjectionInOrder());
                    } else {
                        reqForTable.setProjectionPushdownEnabled(false);
                    }
                    if (trc.cachedPbPredicate != null) {
                        reqForTable.setFilterPredicate(trc.cachedPbPredicate);
                        reqForTable.setFilterSchemaId(trc.filterSchemaId);
                    }
                    reqForTable.addAllBucketsReqs(bucketReqs);
                    tablesReqs.add(reqForTable);
                }
                if (tablesReqs.isEmpty()) {
                    continue;
                }
                fetchLogRequest.addAllTablesReqs(tablesReqs);
                fetchLogRequests.put(leaderId, fetchLogRequest);
            }
            return fetchLogRequests;
        }
    }

    private void enqueueStartResolution(
            TableBucket tb,
            TableReadContext context,
            LogScannerStatus.SubscriptionSnapshot snapshot,
            long start,
            long highWatermark) {
        CompletedFetch completed =
                new DefaultCompletedFetch(
                        tb,
                        context.tablePath,
                        FetchLogResultForBucket.empty(tb, highWatermark, -1L),
                        context.readContext,
                        logScannerStatus,
                        isCheckCrcs,
                        snapshot.offset,
                        null);
        completed.bindSubscription(snapshot, start);
        logFetchBuffer.add(completed);
    }

    private void enqueueFailure(
            TableBucket tb, LogScannerStatus.SubscriptionSnapshot snapshot, Throwable failure) {
        TableReadContext context = tableReadContexts.get(tb.getTableId());
        if (context == null
                || !logScannerStatus.matchesRequestedRange(
                        tb, snapshot.offset, snapshot.stoppingOffset)) {
            return;
        }
        CompletedFetch completed =
                new DefaultCompletedFetch(
                        tb,
                        context.tablePath,
                        FetchLogResultForBucket.empty(tb, -1L, -1L),
                        context.readContext,
                        logScannerStatus,
                        isCheckCrcs,
                        snapshot.offset,
                        null);
        completed.bindSubscription(snapshot, -1L);
        completed.setFailure(failure);
        logFetchBuffer.add(completed);
    }

    private void resolveLegacyStart(
            TableBucket tb,
            LogScannerStatus.SubscriptionSnapshot snapshot,
            LegacyFetchState legacy) {
        long now = System.nanoTime();
        if (legacy.resolving || legacy.failed || now < legacy.retryAfterNanos) {
            return;
        }
        Integer leader = getTableBucketLeader(tb);
        if (leader == null) {
            legacy.retryAfterNanos = System.nanoTime() + MILLISECONDS.toNanos(100);
            return;
        }
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            legacy.retryAfterNanos = System.nanoTime() + MILLISECONDS.toNanos(100);
            return;
        }
        legacy.resolving = true;
        legacy.retryAfterNanos = 0L;
        try {
            gateway.listOffsets(
                            makeListOffsetsRequest(
                                    tb.getTableId(),
                                    tb.getPartitionId(),
                                    Collections.singletonList(tb.getBucket()),
                                    new OffsetSpec.EarliestSpec(),
                                    metadataUpdater.getCluster()))
                    .whenComplete(
                            (response, error) ->
                                    handleLegacyStart(tb, snapshot, legacy, response, error));
        } catch (Throwable error) {
            handleLegacyStart(tb, snapshot, legacy, null, error);
        }
    }

    private synchronized void handleLegacyStart(
            TableBucket tb,
            LogScannerStatus.SubscriptionSnapshot snapshot,
            LegacyFetchState legacy,
            ListOffsetsResponse response,
            Throwable error) {
        if (isClosed
                || legacySubscriptions.get(tb) != legacy
                || !logScannerStatus.matchesRequestedRange(
                        tb, snapshot.offset, snapshot.stoppingOffset)) {
            return;
        }
        legacy.resolving = false;
        try {
            if (error == null) {
                if (response == null || response.getBucketsRespsCount() != 1) {
                    throw new FetchException("Missing or duplicate ListOffsets result for " + tb);
                }
                PbListOffsetsRespForBucket bucketResponse = response.getBucketsRespAt(0);
                if (bucketResponse.getBucketId() != tb.getBucket()) {
                    throw new FetchException("Unexpected ListOffsets bucket for " + tb);
                }
                ApiError apiError = ApiError.fromErrorMessage(bucketResponse);
                if (apiError.isFailure()) {
                    throw apiError.exception();
                }
                if (!bucketResponse.hasOffset() || bucketResponse.getOffset() < 0) {
                    throw new FetchException("Invalid earliest offset for " + tb);
                }
                TableReadContext context = tableReadContexts.get(tb.getTableId());
                if (context != null) {
                    enqueueStartResolution(tb, context, snapshot, bucketResponse.getOffset(), -1L);
                }
                return;
            }
        } catch (RuntimeException failure) {
            error = failure;
        }
        Throwable cause = ExceptionUtils.stripCompletionException(error);
        if (cause instanceof InvalidMetadataException
                || cause instanceof InvalidBucketRoutingException) {
            handleFetchLogExceptionForBucket(tb, -1, ApiError.fromThrowable(cause));
        } else if (!(cause instanceof RetriableException)
                && !(cause instanceof IOException)
                && !(cause instanceof TimeoutException)) {
            legacy.failed = true;
            enqueueFailure(tb, snapshot, cause);
            return;
        }
        legacy.retryAfterNanos = System.nanoTime() + MILLISECONDS.toNanos(100);
        logFetchBuffer.requestFetchRetry();
    }

    private static final class LegacyFetchState {
        private final long stoppingOffset;
        private boolean resolving;
        private boolean failed;
        private long retryAfterNanos;

        private LegacyFetchState(long stoppingOffset) {
            this.stoppingOffset = stoppingOffset;
        }
    }

    private List<TableBucket> fetchableBuckets() {
        // This is the set of buckets we have in our buffer
        Set<TableBucket> exclude = logFetchBuffer.bufferedBuckets();

        if (exclude == null) {
            return Collections.emptyList();
        }

        return logScannerStatus.fetchableBuckets(tableBucket -> !exclude.contains(tableBucket));
    }

    private Integer getTableBucketLeader(TableBucket tableBucket) {
        Optional<BucketLocation> bucketLocationOpt = metadataUpdater.getBucketLocation(tableBucket);
        if (bucketLocationOpt.isPresent()) {
            BucketLocation bucketLocation = bucketLocationOpt.get();
            if (bucketLocation.getLeader() != null) {
                return bucketLocation.getLeader();
            }
        }

        return null;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!isClosed) {
            isClosed = true;
            IOUtils.closeQuietly(logFetchBuffer, "logFetchBuffer");
            IOUtils.closeQuietly(remoteLogDownloader, "remoteLogDownloader");
            for (TableReadContext tableReadContext : tableReadContexts.values()) {
                tableReadContext.close();
            }
            chunkedFactory.close();
            LOG.info("LogFetcher is closed.");
        }
    }

    @VisibleForTesting
    LogScannerStatus getLogScannerStatus() {
        return logScannerStatus;
    }

    @VisibleForTesting
    int getCompletedFetchesSize() {
        return logFetchBuffer.bufferedBuckets().size();
    }

    @VisibleForTesting
    synchronized int getRegisteredTableCount() {
        return tableReadContexts.size();
    }

    @VisibleForTesting
    int getQueuedRemoteSegmentCount() {
        return remoteLogDownloader.getSizeOfSegmentsToFetch();
    }

    /** Request-scoped context snapshot. */
    private static class FetchRequestContext {
        private final Map<Long, TableReadContext> requestedTableContexts;
        private final TableOrPartitions tableOrPartitions;
        private final Map<TableBucket, LogScannerStatus.SubscriptionSnapshot> subscriptions;

        private FetchRequestContext(
                Map<Long, TableReadContext> requestedTableContexts,
                TableOrPartitions tableOrPartitions,
                Map<TableBucket, LogScannerStatus.SubscriptionSnapshot> subscriptions) {
            this.requestedTableContexts = requestedTableContexts;
            this.tableOrPartitions = tableOrPartitions;
            this.subscriptions = subscriptions;
        }
    }

    /** Per-table context holding read contexts, projection, and predicate info. */
    @VisibleForTesting
    static class TableReadContext {
        final TablePath tablePath;
        final boolean isPartitioned;
        final LogRecordReadContext readContext;
        final LogRecordReadContext remoteReadContext;
        @Nullable final Projection projection;
        @Nullable final org.apache.fluss.rpc.messages.PbPredicate cachedPbPredicate;
        final int filterSchemaId;

        TableReadContext(
                TableInfo tableInfo,
                LogRecordReadContext.SchemaResolution schemaResolution,
                @Nullable Projection projection,
                @Nullable Predicate recordBatchFilter,
                SchemaGetter schemaGetter,
                ChunkedAllocationManager.ChunkedFactory chunkedFactory) {
            this.tablePath = tableInfo.getTablePath();
            this.isPartitioned = tableInfo.isPartitioned();
            // Share the LogFetcher-owned chunked factory so all tables reuse one chunk
            // pool and LogFetcher.close() releases the chunks after contexts are closed.
            this.readContext =
                    LogRecordReadContext.createReadContext(
                            tableInfo,
                            false,
                            schemaResolution,
                            projection,
                            schemaGetter,
                            chunkedFactory);
            this.remoteReadContext =
                    LogRecordReadContext.createReadContext(
                            tableInfo,
                            true,
                            schemaResolution,
                            projection,
                            schemaGetter,
                            chunkedFactory);
            this.projection = projection;
            this.cachedPbPredicate =
                    recordBatchFilter != null
                            ? PredicateMessageUtils.toPbPredicate(
                                    recordBatchFilter, tableInfo.getRowType())
                            : null;
            this.filterSchemaId = tableInfo.getSchemaId();
        }

        void close() {
            readContext.close();
            remoteReadContext.close();
        }
    }
}
