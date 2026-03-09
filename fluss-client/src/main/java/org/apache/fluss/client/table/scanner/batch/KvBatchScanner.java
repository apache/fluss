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
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbScanReqForBucket;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.SchemaUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link BatchScanner} that streams all live rows from a single KV bucket by iterating the tablet
 * server's RocksDB instance via a sequence of ScanKv RPCs.
 *
 * <p>The remote scanner is opened lazily on the first {@link #pollBatch} call. Once the bucket is
 * exhausted the scanner closes itself on the server. If the caller needs to abort early it must
 * call {@link #close} explicitly.
 *
 * <p>After each response with {@code has_more_results = true} the next RPC is fired immediately,
 * overlapping network latency with the caller's row processing. At most one request is in-flight at
 * any time.
 *
 * <p>Not reusable and not thread-safe.
 */
public class KvBatchScanner implements BatchScanner {
    private static final int BATCH_SIZE_BYTES = 4 * 1024 * 1024;

    private final TablePath tablePath;
    private final TableBucket tableBucket;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final int targetSchemaId;

    /** Cache for schema evolution index mappings. */
    private final Map<Short, int[]> schemaMappingCache = new HashMap<>();

    /** Reused across all batches; schemaGetter and kvFormat never change. */
    private final ValueRecordReadContext readContext;

    private boolean done = false;
    @Nullable private TabletServerGateway gateway;
    @Nullable private byte[] scannerId;

    /** Monotonically increasing ID sent with each continuation request, starting at 0. */
    private int callSeqId = 0;

    @Nullable private CompletableFuture<ScanKvResponse> prefetchFuture;

    public KvBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater) {
        this.tablePath = tableInfo.getTablePath();
        this.tableBucket = tableBucket;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.targetSchemaId = tableInfo.getSchemaId();
        this.readContext =
                ValueRecordReadContext.createReadContext(
                        schemaGetter, tableInfo.getTableConfig().getKvFormat());
    }

    /**
     * Returns the next batch of rows.
     *
     * <ul>
     *   <li>Returns an empty iterator if the in-flight RPC has not completed within {@code
     *       timeout}.
     *   <li>Returns {@code null} when the bucket is exhausted or the scanner has been closed.
     * </ul>
     */
    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (done) {
            return null;
        }
        if (prefetchFuture == null) {
            try {
                openScanner();
            } catch (Exception e) {
                done = true;
                // TODO: handle LeaderNotAvailableException with retry (see LimitBatchScanner).
                throw new IOException("Failed to open scanner for bucket " + tableBucket, e);
            }
        }

        ScanKvResponse response;
        try {
            response = prefetchFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return CloseableIterator.emptyIterator();
        } catch (Exception e) {
            throw new IOException(e);
        }
        prefetchFuture = null;

        if (response.hasScannerId()) {
            scannerId = response.getScannerId();
        }

        boolean hasMore = response.hasHasMoreResults() && response.isHasMoreResults();
        if (hasMore) {
            sendContinuation();
        } else {
            done = true;
        }

        if (!response.hasRecords()) {
            // Empty last batch or empty bucket.
            return done ? null : CloseableIterator.emptyIterator();
        }
        List<InternalRow> rows = parseRecords(response);
        return CloseableIterator.wrap(rows.iterator());
    }

    /**
     * Releases all resources. Cancels any in-flight prefetch and sends a {@code close_scanner=true}
     * RPC to free the server-side session immediately. Idempotent.
     */
    @Override
    public void close() throws IOException {
        if (done) {
            return;
        }
        done = true;
        if (prefetchFuture != null) {
            prefetchFuture.cancel(true);
            prefetchFuture = null;
        }
        if (scannerId != null && gateway != null) {
            // Fire-and-forget: the server will close the session on receipt.
            gateway.scanKv(new ScanKvRequest().setScannerId(scannerId).setCloseScanner(true));
        }
    }

    private void openScanner() {
        if (tableBucket.getPartitionId() != null) {
            metadataUpdater.checkAndUpdateMetadata(tablePath, tableBucket);
        }
        int leader = metadataUpdater.leaderFor(tablePath, tableBucket);
        gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }

        PbScanReqForBucket bucketReq =
                new PbScanReqForBucket()
                        .setTableId(tableBucket.getTableId())
                        .setBucketId(tableBucket.getBucket());
        if (tableBucket.getPartitionId() != null) {
            bucketReq.setPartitionId(tableBucket.getPartitionId());
        }

        ScanKvRequest request =
                new ScanKvRequest().setBucketScanReq(bucketReq).setBatchSizeBytes(BATCH_SIZE_BYTES);
        prefetchFuture = gateway.scanKv(request);
    }

    private void sendContinuation() {
        ScanKvRequest request =
                new ScanKvRequest()
                        .setScannerId(scannerId)
                        .setBatchSizeBytes(BATCH_SIZE_BYTES)
                        .setCallSeqId(callSeqId++);
        prefetchFuture = gateway.scanKv(request);
    }

    private List<InternalRow> parseRecords(ScanKvResponse response) {
        List<InternalRow> rows = new ArrayList<>();
        ByteBuffer recordsBuffer = ByteBuffer.wrap(response.getRecords());
        DefaultValueRecordBatch valueRecords =
                DefaultValueRecordBatch.pointToByteBuffer(recordsBuffer);
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
            rows.add(row);
        }
        return rows;
    }
}
