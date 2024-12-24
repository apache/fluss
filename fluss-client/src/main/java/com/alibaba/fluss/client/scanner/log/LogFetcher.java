/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.metrics.ScannerMetricGroup;
import com.alibaba.fluss.client.scanner.RemoteFileDownloader;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.InvalidMetadataException;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.remote.RemoteLogFetchInfo;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.PbFetchLogReqForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogReqForTable;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForTable;
import com.alibaba.fluss.utils.IOUtils;
import com.alibaba.fluss.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.getFetchLogResultForBucket;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** fetcher to fetch log. */
@Internal
public class LogFetcher implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(LogFetcher.class);

    private final TablePath tablePath;
    private final boolean isPartitioned;
    private final LogRecordReadContext readContext;
    // TODO this context can be merge with readContext. Introduce it only because log remote read
    //  currently can only do project when generate scanRecord instead of doing project while read
    //  bytes from remote file.
    private final LogRecordReadContext remoteReadContext;
    @Nullable private final Projection projection;
    private final RpcClient rpcClient;
    private final int maxFetchBytes;
    private final int maxBucketFetchBytes;
    private final boolean isCheckCrcs;
    private final LogScannerStatus logScannerStatus;
    private final LogFetchBuffer logFetchBuffer;
    private final LogFetchCollector logFetchCollector;
    private final RemoteLogDownloader remoteLogDownloader;

    @GuardedBy("this")
    private final Set<Integer> nodesWithPendingFetchRequests;

    @GuardedBy("this")
    private boolean isClosed = false;

    private final MetadataUpdater metadataUpdater;
    private final ScannerMetricGroup scannerMetricGroup;

    public LogFetcher(
            TableInfo tableInfo,
            @Nullable Projection projection,
            RpcClient rpcClient,
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater,
            ScannerMetricGroup scannerMetricGroup,
            RemoteFileDownloader remoteFileDownloader) {
        this.tablePath = tableInfo.getTablePath();
        this.isPartitioned = tableInfo.getTableDescriptor().isPartitioned();
        this.readContext = LogRecordReadContext.createReadContext(tableInfo, false, projection);
        this.remoteReadContext =
                LogRecordReadContext.createReadContext(tableInfo, true, projection);
        this.projection = projection;
        this.rpcClient = rpcClient;
        this.logScannerStatus = logScannerStatus;
        this.maxFetchBytes = (int) conf.get(ConfigOptions.LOG_FETCH_MAX_BYTES).getBytes();
        this.maxBucketFetchBytes =
                (int) conf.get(ConfigOptions.LOG_FETCH_MAX_BYTES_FOR_BUCKET).getBytes();
        this.isCheckCrcs = conf.getBoolean(ConfigOptions.CLIENT_SCANNER_LOG_CHECK_CRC);
        this.logFetchBuffer = new LogFetchBuffer();
        this.nodesWithPendingFetchRequests = new HashSet<>();
        this.metadataUpdater = metadataUpdater;
        this.logFetchCollector =
                new LogFetchCollector(tablePath, logScannerStatus, conf, metadataUpdater);
        this.scannerMetricGroup = scannerMetricGroup;
        this.remoteLogDownloader =
                new RemoteLogDownloader(tablePath, conf, remoteFileDownloader, scannerMetricGroup);
    }

    /**
     * Return whether we have any completed fetches that are fetch-able. This method is thread-safe.
     *
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    public boolean hasAvailableFetches() {
        return !logFetchBuffer.isEmpty();
    }

    public Map<TableBucket, List<ScanRecord>> collectFetch() {
        return logFetchCollector.collectFetch(logFetchBuffer);
    }

    /**
     * Set up a fetch request for any node that we have assigned buckets for which doesn't already
     * have an in-flight fetch or pending fetch data.
     */
    public synchronized void sendFetches() {
        Map<Integer, FetchLogRequest> fetchRequestMap = prepareFetchLogRequests();
        fetchRequestMap.forEach(
                (nodeId, fetchLogRequest) -> {
                    LOG.debug("Adding pending request for node id {}", nodeId);
                    nodesWithPendingFetchRequests.add(nodeId);
                    sendFetchRequest(nodeId, fetchLogRequest);
                });
    }

    /**
     * @param deadlineNanos the deadline time to wait util
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

    private void sendFetchRequest(int destination, FetchLogRequest fetchLogRequest) {
        // TODO cache the tablet server gateway.
        TabletServerGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> metadataUpdater.getTabletServer(destination),
                        rpcClient,
                        TabletServerGateway.class);

        final long requestStartTime = System.currentTimeMillis();
        scannerMetricGroup.fetchRequestCount().inc();

        TableOrPartitions tableOrPartitionsInFetchRequest =
                getTableOrPartitionsInFetchRequest(fetchLogRequest);

        gateway.fetchLog(fetchLogRequest)
                .whenComplete(
                        (fetchLogResponse, e) ->
                                handleFetchLogResponse(
                                        destination,
                                        requestStartTime,
                                        fetchLogResponse,
                                        tableOrPartitionsInFetchRequest,
                                        e));
    }

    private TableOrPartitions getTableOrPartitionsInFetchRequest(FetchLogRequest fetchLogRequest) {
        Set<Long> tableIdsInFetchRequest = null;
        Set<TablePartition> tablePartitionsInFetchRequest = null;
        if (!isPartitioned) {
            tableIdsInFetchRequest =
                    fetchLogRequest.getTablesReqsList().stream()
                            .map(PbFetchLogReqForTable::getTableId)
                            .collect(Collectors.toSet());
        } else {
            tablePartitionsInFetchRequest = new HashSet<>();
            // iterate over table requests
            for (PbFetchLogReqForTable fetchTableRequest : fetchLogRequest.getTablesReqsList()) {
                for (PbFetchLogReqForBucket fetchLogReqForBucket :
                        fetchTableRequest.getBucketsReqsList()) {
                    tablePartitionsInFetchRequest.add(
                            new TablePartition(
                                    fetchTableRequest.getTableId(),
                                    fetchLogReqForBucket.getPartitionId()));
                }
            }
        }
        return new TableOrPartitions(tableIdsInFetchRequest, tablePartitionsInFetchRequest);
    }

    private static class TableOrPartitions {
        private final @Nullable Set<Long> tableIds;
        private final @Nullable Set<TablePartition> tablePartitions;

        private TableOrPartitions(
                @Nullable Set<Long> tableIds, @Nullable Set<TablePartition> tablePartitions) {
            this.tableIds = tableIds;
            this.tablePartitions = tablePartitions;
        }
    }

    /** Implements the core logic for a successful fetch log response. */
    private synchronized void handleFetchLogResponse(
            int destination,
            long requestStartTime,
            FetchLogResponse fetchLogResponse,
            TableOrPartitions tableOrPartitionsInFetchRequest,
            @Nullable Throwable e) {
        try {
            if (isClosed) {
                return;
            }

            if (e != null) {
                LOG.error("Failed to fetch log from node {}", destination, e);

                // if is invalid metadata exception, we need to clear table bucket meta
                // to enable another round of log fetch to request new medata
                if (e instanceof InvalidMetadataException) {
                    Collection<PhysicalTablePath> physicalTablePaths =
                            metadataUpdater.getPhysicalTablePathByIds(
                                    tableOrPartitionsInFetchRequest.tableIds,
                                    tableOrPartitionsInFetchRequest.tablePartitions);
                    LOG.warn(
                            "Received invalid metadata error in fetch log request. "
                                    + "Going to request metadata update.",
                            e);
                    metadataUpdater.invalidPhysicalTableBucketMeta(physicalTablePaths);
                }
                return;
            }

            // update fetch metrics only when request success
            scannerMetricGroup.updateFetchLatency(System.currentTimeMillis() - requestStartTime);
            scannerMetricGroup.bytesPerRequest().update(fetchLogResponse.totalSize());

            for (PbFetchLogRespForTable respForTable : fetchLogResponse.getTablesRespsList()) {
                long tableId = respForTable.getTableId();
                for (PbFetchLogRespForBucket respForBucket : respForTable.getBucketsRespsList()) {
                    TableBucket tb =
                            new TableBucket(
                                    tableId,
                                    respForBucket.hasPartitionId()
                                            ? respForBucket.getPartitionId()
                                            : null,
                                    respForBucket.getBucketId());
                    FetchLogResultForBucket fetchResultForBucket =
                            getFetchLogResultForBucket(tb, tablePath, respForBucket);
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
                                    fetchResultForBucket.remoteLogFetchInfo(),
                                    fetchOffset,
                                    fetchResultForBucket.getHighWatermark());
                        } else {
                            LogRecords logRecords = fetchResultForBucket.recordsOrEmpty();
                            if (!MemoryLogRecords.EMPTY.equals(logRecords)) {
                                // In oder to not signal notEmptyCondition, add completed fetch to
                                // buffer until log records is not empty.
                                DefaultCompletedFetch completedFetch =
                                        new DefaultCompletedFetch(
                                                tb,
                                                fetchResultForBucket,
                                                readContext,
                                                logScannerStatus,
                                                // skipping CRC check if projection push downed as
                                                // the data is pruned
                                                isCheckCrcs,
                                                fetchOffset);
                                logFetchBuffer.add(completedFetch);
                            }
                        }
                    }
                }
            }
        } finally {
            LOG.debug("Removing pending request for node: {}", destination);
            nodesWithPendingFetchRequests.remove(destination);
        }
    }

    private void pendRemoteFetches(
            RemoteLogFetchInfo remoteLogFetchInfo, long firstFetchOffset, long highWatermark) {
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
            PendingFetch pendingFetch =
                    new RemotePendingFetch(
                            segment,
                            downloadFuture,
                            posInLogSegment,
                            fetchOffset,
                            highWatermark,
                            remoteReadContext,
                            logScannerStatus,
                            isCheckCrcs);
            logFetchBuffer.pend(pendingFetch);
            downloadFuture.onComplete(logFetchBuffer::tryComplete);
        }
    }

    private Map<Integer, FetchLogRequest> prepareFetchLogRequests() {
        Map<Integer, List<PbFetchLogReqForBucket>> fetchLogReqForBuckets = new HashMap<>();
        int readyForFetchCount = 0;
        Long tableId = null;
        for (TableBucket tb : fetchableBuckets()) {
            if (tableId == null) {
                tableId = tb.getTableId();
            }
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
                // try to get the latest metadata info of this table because the leader for this
                // bucket is unknown.
                metadataUpdater.updateTableOrPartitionMetadata(tablePath, tb.getPartitionId());
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
                fetchLogReqForBuckets
                        .computeIfAbsent(leader, key -> new ArrayList<>())
                        .add(fetchLogReqForBucket);
                readyForFetchCount++;
            }
        }

        if (readyForFetchCount == 0) {
            return Collections.emptyMap();
        } else {
            Map<Integer, FetchLogRequest> fetchLogRequests = new HashMap<>();
            long finalTableId = tableId;
            fetchLogReqForBuckets.forEach(
                    (leaderId, reqForBuckets) -> {
                        FetchLogRequest fetchLogRequest =
                                new FetchLogRequest()
                                        .setFollowerServerId(-1)
                                        .setMaxBytes(maxFetchBytes);
                        PbFetchLogReqForTable reqForTable =
                                new PbFetchLogReqForTable().setTableId(finalTableId);
                        if (readContext.isProjectionPushDowned()) {
                            assert projection != null;
                            reqForTable
                                    .setProjectionPushdownEnabled(true)
                                    .setProjectedFields(projection.getProjectionInOrder());
                        } else {
                            reqForTable.setProjectionPushdownEnabled(false);
                        }
                        reqForTable.addAllBucketsReqs(reqForBuckets);
                        fetchLogRequest.addAllTablesReqs(Collections.singletonList(reqForTable));
                        fetchLogRequests.put(leaderId, fetchLogRequest);
                    });
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
        metadataUpdater.checkAndUpdateMetadata(tablePath, tableBucket);
        if (metadataUpdater.getBucketLocation(tableBucket).isPresent()) {
            BucketLocation bucketLocation = metadataUpdater.getBucketLocation(tableBucket).get();
            if (bucketLocation.getLeader() != null) {
                return bucketLocation.getLeader().id();
            }
        }

        return null;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!isClosed) {
            IOUtils.closeQuietly(logFetchBuffer, "logFetchBuffer");
            IOUtils.closeQuietly(remoteLogDownloader, "remoteLogDownloader");
            readContext.close();
            remoteReadContext.close();
            isClosed = true;
            LOG.info("Fetcher for {} is closed.", tablePath);
        }
    }

    @VisibleForTesting
    int getCompletedFetchesSize() {
        return logFetchBuffer.bufferedBuckets().size();
    }
}
