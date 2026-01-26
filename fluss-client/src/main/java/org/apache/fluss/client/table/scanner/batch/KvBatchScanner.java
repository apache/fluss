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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbScanReqForBucket;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.messages.ScannerKeepAliveRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.SchemaUtil;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A {@link BatchScanner} implementation that scans records from a primary key table. */
public class KvBatchScanner implements BatchScanner {
    private static final Logger LOG = LoggerFactory.getLogger(KvBatchScanner.class);

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    @Nullable private final int[] projectedFields;
    @Nullable private final Long limit;
    private final int targetSchemaId;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final KvFormat kvFormat;
    private final int batchSizeBytes;

    private final Map<Short, int[]> schemaProjectionCache = new HashMap<>();

    private byte[] scannerId;
    private int callSeqId = 0;
    private boolean hasMoreResults = true;
    private boolean isClosed = false;

    private CompletableFuture<ScanKvResponse> inFlightRequest;
    private ScheduledExecutorService keepAliveExecutor;

    public KvBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            @Nullable Long limit) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.projectedFields = projectedFields;
        this.limit = limit;
        this.targetSchemaId = tableInfo.getSchemaId();
        this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        this.batchSizeBytes =
                (int)
                        tableInfo
                                .getTableConfig()
                                .getConfiguration()
                                .get(ConfigOptions.CLIENT_SCANNER_KV_FETCH_MAX_BYTES)
                                .getBytes();

        RowType rowType = tableInfo.getRowType();
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (isClosed || (!hasMoreResults && inFlightRequest == null)) {
            return null;
        }

        try {
            if (inFlightRequest == null) {
                sendRequest();
            }

            ScanKvResponse response =
                    inFlightRequest.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            inFlightRequest = null;

            if (response.hasErrorCode() && response.getErrorCode() != Errors.NONE.code()) {
                throw Errors.forCode(response.getErrorCode()).exception(response.getErrorMessage());
            }

            this.scannerId = response.getScannerId();
            this.hasMoreResults = response.isHasMoreResults();
            this.callSeqId++;

            List<InternalRow> rows = parseScanKvResponse(response);

            // pipeline: send next request if there are more results
            if (hasMoreResults) {
                sendRequest();
            }

            return CloseableIterator.wrap(rows.iterator());
        } catch (java.util.concurrent.TimeoutException e) {
            return CloseableIterator.emptyIterator();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void sendRequest() {
        int leader = metadataUpdater.leaderFor(tableInfo.getTablePath(), tableBucket);
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }

        ScanKvRequest request = new ScanKvRequest();
        request.setBatchSizeBytes(batchSizeBytes);
        if (scannerId == null) {
            PbScanReqForBucket bucketScanReq = request.setBucketScanReq();
            bucketScanReq.setTableId(tableBucket.getTableId()).setBucketId(tableBucket.getBucket());
            if (tableBucket.getPartitionId() != null) {
                bucketScanReq.setPartitionId(tableBucket.getPartitionId());
            }
            if (limit != null) {
                bucketScanReq.setLimit(limit);
            }
            request.setCallSeqId(0);
        } else {
            request.setScannerId(scannerId).setCallSeqId(callSeqId);
        }

        inFlightRequest = gateway.scanKv(request);
    }

    private List<InternalRow> parseScanKvResponse(ScanKvResponse response) {
        if (!response.hasRecords()) {
            return Collections.emptyList();
        }

        List<InternalRow> scanRows = new ArrayList<>();
        ByteBuffer recordsBuffer = ByteBuffer.wrap(response.getRecords());
        DefaultValueRecordBatch valueRecords =
                DefaultValueRecordBatch.pointToByteBuffer(recordsBuffer);
        ValueRecordReadContext readContext =
                ValueRecordReadContext.createReadContext(schemaGetter, kvFormat);

        for (ValueRecord record : valueRecords.records(readContext)) {
            InternalRow row = record.getRow();
            if (targetSchemaId != record.schemaId()) {
                int[] indexMapping =
                        schemaProjectionCache.computeIfAbsent(
                                record.schemaId(),
                                sourceSchemaId ->
                                        SchemaUtil.getIndexMapping(
                                                schemaGetter.getSchema(sourceSchemaId),
                                                schemaGetter.getSchema(targetSchemaId)));
                row = ProjectedRow.from(indexMapping).replaceRow(row);
            }
            scanRows.add(maybeProject(row));
        }
        return scanRows;
    }

    private InternalRow maybeProject(InternalRow originRow) {
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            newRow.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow projectedRow = ProjectedRow.from(projectedFields);
            projectedRow.replaceRow(newRow);
            return projectedRow;
        } else {
            return newRow;
        }
    }

    public void startKeepAlivePeriodically(int keepAliveIntervalMs) {
        if (keepAliveExecutor != null) {
            return;
        }

        keepAliveExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("kv-scanner-keep-alive-" + tableBucket));
        keepAliveExecutor.scheduleAtFixedRate(
                this::sendKeepAlive,
                keepAliveIntervalMs,
                keepAliveIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    private void sendKeepAlive() {
        if (isClosed || scannerId == null || !hasMoreResults) {
            return;
        }

        try {
            int leader = metadataUpdater.leaderFor(tableInfo.getTablePath(), tableBucket);
            TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
            if (gateway != null) {
                gateway.scannerKeepAlive(new ScannerKeepAliveRequest().setScannerId(scannerId));
            }
        } catch (Exception e) {
            LOG.warn("Failed to send keep alive for scanner {}", tableBucket, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;

        if (keepAliveExecutor != null) {
            keepAliveExecutor.shutdownNow();
        }

        if (scannerId != null && hasMoreResults) {
            // Close scanner on server
            int leader = metadataUpdater.leaderFor(tableInfo.getTablePath(), tableBucket);
            TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
            if (gateway != null) {
                gateway.scanKv(
                        new ScanKvRequest()
                                .setScannerId(scannerId)
                                .setCallSeqId(callSeqId)
                                .setCloseScanner(true));
            }
        }

        if (inFlightRequest != null) {
            inFlightRequest.cancel(true);
        }
    }
}
