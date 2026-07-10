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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbScanReqForBucket;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.SchemaUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;

/**
 * A {@link BatchScanner} that streams every live row of a single primary-key bucket from the tablet
 * server's RocksDB state via a sequence of {@code ScanKv} RPCs. The scan has snapshot isolation:
 * rows reflect the KV state at the moment the server opened the snapshot; concurrent writes after
 * that point are invisible.
 *
 * <p>The next continuation RPC is issued immediately after each response so the caller's
 * deserialize/process work overlaps the network round-trip; one in-flight RPC at a time.
 */
@Internal
@NotThreadSafe
public final class KvBatchScanner implements BatchScanner {

    private static final Logger LOG = LoggerFactory.getLogger(KvBatchScanner.class);

    @VisibleForTesting static final int MAX_OPEN_RETRIES = 3;
    @VisibleForTesting static final long BASE_RETRY_DELAY_MS = 100L;

    private final TablePath tablePath;
    private final TableBucket bucket;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final int targetSchemaId;
    private final int batchSizeBytes;
    @Nullable private final int[] projectedColumns;
    private final Map<Short, int[]> schemaMappingCache = new HashMap<>();
    private final ValueRecordReadContext readContext;

    @Nullable private TabletServerGateway gateway;
    @Nullable private byte[] scannerId;
    @Nullable private CompletableFuture<ScanKvResponse> inFlight;

    private int callSeqId = 0;
    private int openRetries = 0;
    private boolean drained = false;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public KvBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            int batchSizeBytes,
            @Nullable int[] projectedColumns) {
        this.tablePath = tableInfo.getTablePath();
        this.bucket = tableBucket;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.targetSchemaId = tableInfo.getSchemaId();
        this.batchSizeBytes = Math.max(1, batchSizeBytes);
        this.projectedColumns = projectedColumns;
        this.readContext =
                ValueRecordReadContext.createReadContext(
                        schemaGetter,
                        tableInfo.getTableConfig().getKvFormat(),
                        KvValueLayout.forKvFormatVersion(
                                tableInfo
                                        .getTableConfig()
                                        .getKvFormatVersion()
                                        .orElse(KV_FORMAT_VERSION_2)));
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (closed.get() || drained) {
            return null;
        }
        if (inFlight == null) {
            try {
                openScanner();
            } catch (Exception e) {
                terminate();
                throw new IOException("Failed to open scanner for bucket " + bucket, e);
            }
        }

        final ScanKvResponse response;
        try {
            response = inFlight.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return CloseableIterator.emptyIterator();
        } catch (Exception e) {
            // TODO: we should retry continuation requests on transient networks errors, but
            //  for simplicity we currently fail the whole scan. Retrying here would require robust
            //  handling of duplicate continuations (e.g. call_seq_id rollback) to avoid skipping or
            //  repeating rows.
            terminate();
            throw new IOException(e);
        }
        inFlight = null;

        if (response.hasErrorCode() && response.getErrorCode() != 0) {
            return handleErrorResponse(response);
        }

        if (response.hasScannerId()) {
            scannerId = response.getScannerId();
        }

        boolean hasMore = response.hasHasMoreResults() && response.isHasMoreResults();
        if (hasMore) {
            sendContinuation();
        } else {
            drained = true;
        }

        if (!response.hasRecords()) {
            return drained ? null : CloseableIterator.emptyIterator();
        }
        return CloseableIterator.wrap(parseRecords(response).iterator());
    }

    @Override
    public void close() throws IOException {
        terminate();
    }

    private void openScanner() {
        metadataUpdater.checkAndUpdateMetadata(tablePath, bucket);
        int leader = metadataUpdater.leaderFor(tablePath, bucket);
        gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "Leader for bucket " + bucket + " is not available. Please retry the scan.");
        }

        PbScanReqForBucket bucketReq =
                new PbScanReqForBucket()
                        .setTableId(bucket.getTableId())
                        .setBucketId(bucket.getBucket());
        if (bucket.getPartitionId() != null) {
            bucketReq.setPartitionId(bucket.getPartitionId());
        }

        ScanKvRequest request =
                new ScanKvRequest()
                        .setBucketScanReq(bucketReq)
                        .setBatchSizeBytes(batchSizeBytes)
                        .setCallSeqId(callSeqId);
        inFlight = gateway.scanKv(request);
    }

    private void sendContinuation() {
        callSeqId++;
        ScanKvRequest request =
                new ScanKvRequest()
                        .setScannerId(scannerId)
                        .setBatchSizeBytes(batchSizeBytes)
                        .setCallSeqId(callSeqId);
        inFlight = gateway.scanKv(request);
    }

    private CloseableIterator<InternalRow> handleErrorResponse(ScanKvResponse response)
            throws IOException {
        Errors error = Errors.forCode(response.getErrorCode());
        String message = response.hasErrorMessage() ? response.getErrorMessage() : null;

        if (error == Errors.TOO_MANY_SCANNERS
                && scannerId == null
                && openRetries < MAX_OPEN_RETRIES) {
            long delayMs = BASE_RETRY_DELAY_MS * (1L << openRetries);
            openRetries++;
            LOG.debug(
                    "Too many scanners for bucket {}; retrying open in {} ms (attempt {}/{}).",
                    bucket,
                    delayMs,
                    openRetries,
                    MAX_OPEN_RETRIES);
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                terminate();
                throw new IOException(
                        "Interrupted while backing off before scanner open retry for bucket "
                                + bucket,
                        ie);
            }
            try {
                openScanner();
            } catch (Exception e) {
                terminate();
                throw new IOException(
                        "Failed to open scanner for bucket " + bucket + " on retry", e);
            }
            return CloseableIterator.emptyIterator();
        }

        // Refresh metadata so the next caller-driven attempt resolves the new leader. Auto-
        // restarting here would silently swap the RocksDB snapshot, breaking snapshot isolation.
        if (error == Errors.NOT_LEADER_OR_FOLLOWER) {
            try {
                metadataUpdater.checkAndUpdateMetadata(tablePath, bucket);
            } catch (Exception refreshFailure) {
                LOG.debug(
                        "Metadata refresh after NotLeaderOrFollower failed for bucket {}.",
                        bucket,
                        refreshFailure);
            }
            terminate();
            throw new IOException(error.exception(message));
        }

        // The server-side session is already gone; skip close_scanner.
        if (error == Errors.SCANNER_EXPIRED || error == Errors.UNKNOWN_SCANNER_ID) {
            scannerId = null;
            closed.set(true);
            drained = true;
            throw new IOException(error.exception(message));
        }

        terminate();
        throw new IOException(error.exception(message));
    }

    private void terminate() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (inFlight != null) {
            inFlight.cancel(true);
            inFlight = null;
        }
        sendBestEffortClose();
    }

    private void sendBestEffortClose() {
        // After natural EOF the server already closed the session; sending close_scanner is
        // unnecessary and would be a stale request.
        if (scannerId == null || gateway == null || drained) {
            return;
        }
        try {
            gateway.scanKv(
                            new ScanKvRequest()
                                    .setScannerId(scannerId)
                                    .setBatchSizeBytes(batchSizeBytes)
                                    .setCloseScanner(true))
                    .whenComplete(
                            (resp, ex) -> {
                                if (ex != null) {
                                    LOG.debug(
                                            "close_scanner RPC failed for scanner of bucket {};"
                                                    + " server-side TTL will reclaim resources.",
                                            bucket,
                                            ex);
                                }
                            });
        } catch (Throwable t) {
            LOG.debug("close_scanner RPC dispatch failed for bucket {}.", bucket, t);
        }
    }

    private List<InternalRow> parseRecords(ScanKvResponse response) {
        DefaultValueRecordBatch valueRecords =
                DefaultValueRecordBatch.pointToBytes(response.getRecords());
        int recordCount = valueRecords.getRecordCount();
        if (recordCount == 0) {
            return Collections.emptyList();
        }
        List<InternalRow> rows = new ArrayList<>(recordCount);
        for (ValueRecord record : valueRecords.records(readContext)) {
            InternalRow row = record.getRow();
            if (targetSchemaId != record.schemaId()) {
                int[] indexMapping =
                        schemaMappingCache.computeIfAbsent(
                                record.schemaId(),
                                sourceSchemaId ->
                                        SchemaUtil.getIndexMapping(
                                                schemaGetter.getSchema(sourceSchemaId),
                                                schemaGetter.getSchema(targetSchemaId)));
                row = ProjectedRow.from(indexMapping).replaceRow(row);
            }
            if (projectedColumns != null) {
                row = ProjectedRow.from(projectedColumns).replaceRow(row);
            }
            rows.add(row);
        }
        return rows;
    }

    @VisibleForTesting
    boolean isClosed() {
        return closed.get();
    }

    @VisibleForTesting
    boolean isDrained() {
        return drained;
    }

    @VisibleForTesting
    int openRetries() {
        return openRetries;
    }
}
