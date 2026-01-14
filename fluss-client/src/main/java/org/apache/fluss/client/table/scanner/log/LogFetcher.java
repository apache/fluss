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
import org.apache.fluss.client.table.scanner.ScanRecord;
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
import java.util.stream.Collectors;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.getFetchLogResultForBucket;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
    private final int maxFetchBytes;
    private final int maxBucketFetchBytes;
    private final int minFetchBytes;
    private final int maxFetchWaitMs;
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
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater,
            ScannerMetricGroup scannerMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            SchemaGetter schemaGetter) {
        this.tablePath = tableInfo.getTablePath();
        this.isPartitioned = tableInfo.isPartitioned();
        this.readContext =
                LogRecordReadContext.createReadContext(tableInfo, false, projection, schemaGetter);
        this.remoteReadContext =
                LogRecordReadContext.createReadContext(tableInfo, true, projection, schemaGetter);
        this.projection = projection;
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
        this.logFetchCollector =
                new LogFetchCollector(tablePath, logScannerStatus, conf, metadataUpdater);
        this.scannerMetricGroup = scannerMetricGroup;
        this.remoteLogDownloader =
                new RemoteLogDownloader(tablePath, conf, remoteFileDownloader, scannerMetricGroup);
        remoteLogDownloader.start();
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
    public void sendFetches() {
        checkAndUpdateMetadata(fetchableBuckets());
        synchronized (this) {
            // NOTE: Don't perform heavy I/O operations or synchronous waits inside this lock to
            // avoid blocking the future complete of FetchLogResponse.
            Map<Integer, FetchLogRequest> fetchRequestMap = prepareFetchLogRequests();
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
        // If the table is partitioned table, check if we need update partition metadata.
        List<Long> partitionIds = isPartitioned ? new ArrayList<>() : null;
        // If the table is none-partitioned table, check if we need update table metadata.
        boolean needUpdate = false;
        for (TableBucket tb : tableBuckets) {
            if (getTableBucketLeader(tb) != null) {
                continue;
            }

            if (isPartitioned) {
                partitionIds.add(tb.getPartitionId());
            } else {
                needUpdate = true;
                break;
            }
        }

        try {
            if (isPartitioned && !partitionIds.isEmpty()) {
                metadataUpdater.updateMetadata(
                        Collections.singleton(tablePath), null, partitionIds);
            } else if (needUpdate) {
                metadataUpdater.updateTableOrPartitionMetadata(tablePath, null);
            }
        } catch (Exception e) {
            if (e instanceof PartitionNotExistException) {
                // ignore this exception, this is probably happen because the partition is deleted.
                // The fetcher can also work fine. The caller like flink can remove the partition
                // from fetch list when receive exception.
                LOG.warn("Receive PartitionNotExistException when update metadata, ignore it", e);
            } else {
                throw e;
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
        try {
            if (isClosed) {
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
                                    fetchResultForBucket.remoteLogFetchInfo(),
                                    fetchOffset,
                                    fetchResultForBucket.getHighWatermark());
                        } else {
                            LogRecords logRecords = fetchResultForBucket.recordsOrEmpty();
                            if (!MemoryLogRecords.EMPTY.equals(logRecords)
                                    || fetchResultForBucket.getErrorCode() != Errors.NONE.code()) {
                                // In oder to not signal notEmptyCondition, add completed
                                // fetch to buffer until log records is not empty.
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
            RemotePendingFetch pendingFetch =
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
            downloadFuture.onComplete(() -> logFetchBuffer.tryComplete(segment.tableBucket()));
        }
    }

    @VisibleForTesting
    Map<Integer, FetchLogRequest> prepareFetchLogRequests() {
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
                                        .setMaxBytes(maxFetchBytes)
                                        .setMinBytes(minFetchBytes)
                                        .setMaxWaitMs(maxFetchWaitMs);
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
