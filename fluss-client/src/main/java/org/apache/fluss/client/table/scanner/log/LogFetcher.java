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
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
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
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogReqForTable;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForTable;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
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

    @GuardedBy("this")
    private final Set<Integer> nodesWithPendingFetchRequests;

    @GuardedBy("this")
    private boolean isClosed = false;

    private final MetadataUpdater metadataUpdater;
    private final ScannerMetricGroup scannerMetricGroup;
    private final LogRecordReadContext.SchemaResolution schemaResolution;

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
     * <p><b>Threading contract:</b> {@code tableReadContexts} is a plain {@link HashMap} and the
     * mutation below is intentionally unsynchronized. This is safe because callers are expected to
     * be single-threaded: scanner-facing entry points (e.g. {@code AbstractLogScanner}) enforce a
     * single-thread access guard, so registration and concurrent reads from {@code
     * tableReadContexts} cannot race. Do not invoke this method from multiple threads without
     * adding external synchronization.
     */
    public void registerTable(TableScanSpec spec, SchemaGetter schemaGetter) {
        TableInfo tableInfo = spec.getTableInfo();
        long tableId = tableInfo.getTableId();
        if (!tableReadContexts.containsKey(tableId)) {
            tableReadContexts.put(
                    tableId,
                    new TableReadContext(
                            tableInfo,
                            schemaResolution,
                            spec.getProjection(),
                            spec.getFilter(),
                            schemaGetter));
        }
    }

    void unregisterTable(long tableId) {
        TableReadContext tableReadContext = tableReadContexts.remove(tableId);
        if (tableReadContext != null) {
            tableReadContext.close();
        }
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
        return logFetchCollector.collectFetch(logFetchBuffer);
    }

    public ArrowScanRecords collectArrowFetch() {
        return arrowLogFetchCollector.collectFetch(logFetchBuffer);
    }

    /**
     * Set up a fetch request for any node that we have assigned buckets for which doesn't already
     * have an in-flight fetch or pending fetch data.
     */
    public void sendFetches() {
        List<TableBucket> fetchable = fetchableBuckets();
        checkAndUpdateMetadata(fetchable);
        synchronized (this) {
            // NOTE: Don't perform heavy I/O operations or synchronous waits inside this lock to
            // avoid blocking the future complete of FetchLogResponse.
            Map<Integer, FetchLogRequest> fetchRequestMap = prepareFetchLogRequests(fetchable);
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
        try {
            return logFetchBuffer.awaitNotEmpty(deadlineNanos);
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
            TableReadContext tableReadContext = tableReadContexts.get(tableId);
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
    void sendFetchRequest(int destination, FetchLogRequest fetchLogRequest) {
        TableOrPartitions tableOrPartitionsInFetchRequest =
                getTableOrPartitionsInFetchRequest(fetchLogRequest);
        // TODO cache the tablet server gateway.
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);
        if (gateway == null) {
            handleFetchLogException(
                    destination,
                    tableOrPartitionsInFetchRequest,
                    new LeaderNotAvailableException(
                            "Server " + destination + " is not found in metadata cache."));
        } else {
            final long requestStartTime = System.currentTimeMillis();
            scannerMetricGroup.fetchRequestCount().inc();

            gateway.fetchLog(fetchLogRequest)
                    .whenComplete(
                            (fetchLogResponse, e) -> {
                                if (e != null) {
                                    handleFetchLogException(
                                            destination, tableOrPartitionsInFetchRequest, e);
                                } else {
                                    handleFetchLogResponse(
                                            destination, requestStartTime, fetchLogResponse);
                                }
                            });
        }
    }

    private TableOrPartitions getTableOrPartitionsInFetchRequest(FetchLogRequest fetchLogRequest) {
        Set<Long> tableIdsInFetchRequest = new HashSet<>();
        Set<TablePartition> tablePartitionsInFetchRequest = new HashSet<>();
        for (PbFetchLogReqForTable fetchTableRequest : fetchLogRequest.getTablesReqsList()) {
            long tableId = fetchTableRequest.getTableId();
            TableReadContext trc = tableReadContexts.get(tableId);
            boolean partitioned = trc != null && trc.isPartitioned;
            if (!partitioned) {
                tableIdsInFetchRequest.add(tableId);
            } else {
                for (PbFetchLogReqForBucket fetchLogReqForBucket :
                        fetchTableRequest.getBucketsReqsList()) {
                    tablePartitionsInFetchRequest.add(
                            new TablePartition(tableId, fetchLogReqForBucket.getPartitionId()));
                }
            }
        }
        return new TableOrPartitions(
                tableIdsInFetchRequest.isEmpty() ? null : tableIdsInFetchRequest,
                tablePartitionsInFetchRequest.isEmpty() ? null : tablePartitionsInFetchRequest);
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

    private void handleFetchLogException(
            int destination, TableOrPartitions tableOrPartitionsInFetchRequest, Throwable e) {
        try {
            if (isClosed) {
                return;
            }

            LOG.error("Failed to fetch log from node {}", destination, e);
            // if is invalid metadata exception, we need to clear table bucket meta
            // to enable another round of log fetch to request new medata
            if (e instanceof InvalidMetadataException) {
                LOG.warn(
                        "Invalid metadata error in fetch log request. "
                                + "Going to request metadata update.",
                        e);
                invalidTableOrPartitions(tableOrPartitionsInFetchRequest);
            }
        } finally {
            LOG.debug("Removing pending request for node: {}", destination);
            nodesWithPendingFetchRequests.remove(destination);
        }
    }

    /** Implements the core logic for a successful fetch log response. */
    private synchronized void handleFetchLogResponse(
            int destination, long requestStartTime, FetchLogResponse fetchLogResponse) {
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
                TableReadContext trc = tableReadContexts.get(tableId);
                // Fallback tablePath for unknown tableId (should not happen in practice)
                TablePath tablePathForResp =
                        trc != null ? trc.tablePath : TablePath.of("unknown", "unknown");
                for (PbFetchLogRespForBucket respForBucket : respForTable.getBucketsRespsList()) {
                    TableBucket tb =
                            new TableBucket(
                                    tableId,
                                    respForBucket.hasPartitionId()
                                            ? respForBucket.getPartitionId()
                                            : null,
                                    respForBucket.getBucketId());
                    FetchLogResultForBucket fetchResultForBucket =
                            getFetchLogResultForBucket(tb, tablePathForResp, respForBucket);

                    // if error code is not NONE, it means the fetch log request failed, we need to
                    // clear table bucket meta for InvalidMetadataException.
                    if (fetchResultForBucket.getErrorCode() != Errors.NONE.code()) {
                        ApiError error = ApiError.fromErrorMessage(respForBucket);
                        handleFetchLogExceptionForBucket(tb, destination, error);
                    }

                    Long fetchOffset = logScannerStatus.getBucketOffset(tb);
                    // if the offset is null, it means the bucket has been unsubscribed,
                    // we just set a Long.MAX_VALUE as the next fetch offset
                    if (fetchOffset == null) {
                        LOG.debug(
                                "Ignoring fetch log response for bucket {} because the bucket has been "
                                        + "unsubscribed.",
                                tb);
                    } else {
                        if (fetchResultForBucket.fetchFromRemote()) {
                            pendRemoteFetches(
                                    tablePathForResp,
                                    trc != null ? trc.remoteReadContext : null,
                                    fetchResultForBucket.remoteLogFetchInfo(),
                                    fetchOffset,
                                    fetchResultForBucket.getHighWatermark());
                        } else {
                            LogRecords logRecords = fetchResultForBucket.recordsOrEmpty();
                            boolean hasRecords = !MemoryLogRecords.EMPTY.equals(logRecords);
                            if (hasRecords
                                    || fetchResultForBucket.getErrorCode() != Errors.NONE.code()
                                    || fetchResultForBucket.hasFilteredEndOffset()) {
                                // Retain the parsed buffer so it stays alive while
                                // this CompletedFetch's records are being consumed.
                                if (hasRecords && parsedByteBuf != null) {
                                    parsedByteBuf.retain();
                                }
                                logFetchBuffer.add(
                                        new DefaultCompletedFetch(
                                                tb,
                                                tablePathForResp,
                                                fetchResultForBucket,
                                                trc != null
                                                        ? trc.readContext
                                                        : tableReadContexts
                                                                .values()
                                                                .iterator()
                                                                .next()
                                                                .readContext,
                                                logScannerStatus,
                                                // skipping CRC check if projection push downed as
                                                // the data is pruned
                                                isCheckCrcs,
                                                fetchOffset,
                                                hasRecords ? parsedByteBuf : null));
                            }
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
        if (exception instanceof InvalidMetadataException) {
            LOG.warn(
                    "Invalid metadata error in fetch log request. "
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
            TablePath tablePath,
            @Nullable LogRecordReadContext remoteReadContext,
            RemoteLogFetchInfo remoteLogFetchInfo,
            long firstFetchOffset,
            long highWatermark) {
        checkNotNull(remoteLogFetchInfo);
        FsPath remoteLogTabletDir = new FsPath(remoteLogFetchInfo.remoteLogTabletDir());
        List<RemoteLogSegment> remoteLogSegments = remoteLogFetchInfo.remoteLogSegmentList();
        int posInLogSegment = remoteLogFetchInfo.firstStartPos();
        long fetchOffset = firstFetchOffset;
        for (int i = 0; i < remoteLogSegments.size(); i++) {
            RemoteLogSegment segment = remoteLogSegments.get(i);
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
                            tablePath,
                            posInLogSegment,
                            fetchOffset,
                            highWatermark,
                            remoteReadContext,
                            logScannerStatus,
                            isCheckCrcs);
            logFetchBuffer.pend(pendingFetch);
            downloadFuture.onComplete(() -> logFetchBuffer.tryComplete(segment.tableBucket()));
        }
    }

    @VisibleForTesting
    Map<Integer, FetchLogRequest> prepareFetchLogRequests(List<TableBucket> fetchableBuckets) {
        // Outer key: leaderId, inner key: tableId -> list of bucket requests
        Map<Integer, Map<Long, List<PbFetchLogReqForBucket>>> fetchReqsByLeaderAndTable =
                new HashMap<>();
        int readyForFetchCount = 0;
        for (TableBucket tb : fetchableBuckets) {
            Long offset = logScannerStatus.getBucketOffset(tb);
            if (offset == null) {
                LOG.debug(
                        "Skipping fetch request for bucket {} because the bucket has been "
                                + "unsubscribed.",
                        tb);
                continue;
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
                if (tb.getPartitionId() != null) {
                    fetchLogReqForBucket.setPartitionId(tb.getPartitionId());
                }
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
                                .setMaxWaitMs(maxFetchWaitMs);

                List<PbFetchLogReqForTable> tablesReqs = new ArrayList<>();
                for (Map.Entry<Long, List<PbFetchLogReqForBucket>> tableEntry :
                        tableReqs.entrySet()) {
                    long tableId = tableEntry.getKey();
                    List<PbFetchLogReqForBucket> bucketReqs = tableEntry.getValue();
                    TableReadContext trc = tableReadContexts.get(tableId);

                    PbFetchLogReqForTable reqForTable =
                            new PbFetchLogReqForTable().setTableId(tableId);
                    if (trc != null && trc.readContext.isProjectionPushDowned()) {
                        assert trc.projection != null;
                        reqForTable
                                .setProjectionPushdownEnabled(true)
                                .setProjectedFields(trc.projection.getProjectionInOrder());
                    } else {
                        reqForTable.setProjectionPushdownEnabled(false);
                    }
                    if (trc != null && trc.cachedPbPredicate != null) {
                        reqForTable.setFilterPredicate(trc.cachedPbPredicate);
                        reqForTable.setFilterSchemaId(trc.filterSchemaId);
                    }
                    reqForTable.addAllBucketsReqs(bucketReqs);
                    tablesReqs.add(reqForTable);
                }
                fetchLogRequest.addAllTablesReqs(tablesReqs);
                fetchLogRequests.put(leaderId, fetchLogRequest);
            }
            return fetchLogRequests;
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
            IOUtils.closeQuietly(logFetchBuffer, "logFetchBuffer");
            IOUtils.closeQuietly(remoteLogDownloader, "remoteLogDownloader");
            for (TableReadContext tableReadContext : tableReadContexts.values()) {
                tableReadContext.close();
            }
            chunkedFactory.close();
            isClosed = true;
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
    int getRegisteredTableCount() {
        return tableReadContexts.size();
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
                SchemaGetter schemaGetter) {
            this.tablePath = tableInfo.getTablePath();
            this.isPartitioned = tableInfo.isPartitioned();
            this.readContext =
                    LogRecordReadContext.createReadContext(
                            tableInfo, false, schemaResolution, projection, schemaGetter);
            this.remoteReadContext =
                    LogRecordReadContext.createReadContext(
                            tableInfo, true, schemaResolution, projection, schemaGetter);
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
