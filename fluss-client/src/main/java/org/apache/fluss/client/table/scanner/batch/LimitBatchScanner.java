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

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.LogOffsetOutOfRangeException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.rpc.messages.PbListOffsetsRespForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.SchemaUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** A {@link BatchScanner} implementation that scans a limited number of records from a table. */
public class LimitBatchScanner implements BatchScanner {

    private final TableInfo tableInfo;
    @Nullable private final int[] projectedFields;
    private final int limit;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final CompletableFuture<LimitScanResponse> scanFuture;
    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;
    private final int targetSchemaId;
    /** The chunked allocation manager factory to reuse memory for arrow log write batch. */
    private final ChunkedAllocationManager.ChunkedFactory chunkedFactory;

    /**
     * A cache for schema projection mapping from source schema to target. Use HashMap here, because
     * LimitBatchScanner is used in single thread only.
     */
    private final Map<Short, int[]> schemaProjectionCache = new HashMap<>();

    private boolean endOfInput;
    private final TableBucket tableBucket;
    @Nullable private final Supplier<LogScanner> logScannerFactory;
    private final TabletServerGateway gateway;
    private final Cluster cluster;
    @Nullable private CompletableFuture<Long> endOffset;
    @Nullable private CompletableFuture<Long> startOffset;
    @Nullable private LogScanner fallbackScanner;
    private final Deque<InternalRow> tail = new ArrayDeque<>();
    private long nextOffset;

    public LimitBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            int limit,
            @Nullable Supplier<LogScanner> logScannerFactory) {
        this.tableBucket = tableBucket;
        this.logScannerFactory = logScannerFactory;
        this.tableInfo = tableInfo;
        this.projectedFields = projectedFields;
        this.limit = limit;
        this.targetSchemaId = tableInfo.getSchemaId();
        this.schemaGetter = schemaGetter;
        RowType rowType = tableInfo.getRowType();
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            // getRecords() returns a heap copy, so rows don't reference the released buffer.
            this.fieldGetters[i] =
                    InternalRow.createDeepFieldGetter(rowType.getTypeAt(i), i, false);
        }

        LimitScanRequest limitScanRequest =
                new LimitScanRequest()
                        .setTableId(tableBucket.getTableId())
                        .setBucketId(tableBucket.getBucket())
                        .setLimit(limit);

        metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
        cluster = metadataUpdater.getCluster();
        if (tableBucket.getPartitionId() != null) {
            limitScanRequest.setPartitionId(tableBucket.getPartitionId());
        }
        cluster.getBucketCount(
                        TableOrPartition.of(tableBucket.getTableId(), tableBucket.getPartitionId()))
                .ifPresent(limitScanRequest::setRoutingBucketCount);

        // because that rocksdb is not suitable to projection, thus do it in client.
        int leader = metadataUpdater.leaderFor(tableInfo.getTablePath(), tableBucket);
        gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            // TODO handle this exception, like retry.
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }
        this.scanFuture = gateway.limitScan(limitScanRequest);

        this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        this.endOfInput = false;
        this.chunkedFactory = new ChunkedAllocationManager.ChunkedFactory();
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }
        if (endOffset != null) {
            return pollRetainedTail(timeout);
        }
        LimitScanResponse response;
        try {
            response = scanFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // poll next time
            return CloseableIterator.emptyIterator();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (Exception e) {
            return handleLimitScanFailure(ExceptionUtils.stripExecutionException(e));
        }

        ByteBuf parsedByteBuf = response.getParsedByteBuf();
        try {
            if (response.hasErrorCode()) {
                return handleLimitScanFailure(ApiError.fromErrorMessage(response).exception());
            }
            List<InternalRow> scanRows = parseLimitScanResponse(response);
            endOfInput = true;
            return CloseableIterator.wrap(scanRows.iterator());
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (parsedByteBuf != null) {
                parsedByteBuf.release();
            }
        }
    }

    private CloseableIterator<InternalRow> handleLimitScanFailure(Throwable failure)
            throws IOException {
        if (logScannerFactory == null || !(failure instanceof LogOffsetOutOfRangeException)) {
            throw new IOException(failure);
        }
        endOffset = requestOffset(new OffsetSpec.LatestSpec());
        startOffset = requestOffset(new OffsetSpec.EarliestSpec());
        return CloseableIterator.emptyIterator();
    }

    private CompletableFuture<Long> requestOffset(OffsetSpec spec) {
        return gateway.listOffsets(
                        ClientRpcMessageUtils.makeListOffsetsRequest(
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                Collections.singletonList(tableBucket.getBucket()),
                                spec,
                                cluster))
                .thenApply(
                        response -> {
                            PbListOffsetsRespForBucket offset =
                                    response.getBucketsRespsList().get(0);
                            if (offset.hasErrorCode()) {
                                throw ApiError.fromErrorMessage(offset).exception();
                            }
                            return offset.getOffset();
                        });
    }

    private CloseableIterator<InternalRow> pollRetainedTail(Duration timeout) throws IOException {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            long end = endOffset.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            if (fallbackScanner == null) {
                nextOffset =
                        startOffset.get(
                                Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (nextOffset < end) {
                    fallbackScanner = logScannerFactory.get();
                    if (tableBucket.getPartitionId() == null) {
                        fallbackScanner.subscribe(tableBucket.getBucket(), nextOffset);
                    } else {
                        fallbackScanner.subscribe(
                                tableBucket.getPartitionId(), tableBucket.getBucket(), nextOffset);
                    }
                }
            }
            if (nextOffset < end) {
                ScanRecords records =
                        fallbackScanner.poll(
                                Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
                for (LogRecord record : records.records(tableBucket)) {
                    if (record.logOffset() >= end) {
                        break;
                    }
                    if (tail.size() == limit) {
                        tail.removeFirst();
                    }
                    tail.addLast(maybeProject(record.getRow()));
                }
                Long consumed = records.consumedUpToOffset(tableBucket);
                if (consumed != null) {
                    nextOffset = consumed;
                }
            }
            if (nextOffset >= end) {
                endOfInput = true;
                return CloseableIterator.wrap(tail.iterator());
            }
            return CloseableIterator.emptyIterator();
        } catch (TimeoutException e) {
            return CloseableIterator.emptyIterator();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private List<InternalRow> parseLimitScanResponse(LimitScanResponse limitScanResponse) {
        if (!limitScanResponse.hasRecords()) {
            return Collections.emptyList();
        }
        List<InternalRow> scanRows = new ArrayList<>();
        ByteBuffer recordsBuffer = ByteBuffer.wrap(limitScanResponse.getRecords());
        if (tableInfo.hasPrimaryKey()) {
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
        } else {
            LogRecordReadContext readContext =
                    LogRecordReadContext.createReadContext(
                            tableInfo,
                            false,
                            LogRecordReadContext.SchemaResolution.TARGET,
                            null,
                            schemaGetter,
                            chunkedFactory);
            LogRecords records = MemoryLogRecords.pointToByteBuffer(recordsBuffer);
            for (LogRecordBatch logRecordBatch : records.batches()) {
                // A batch of log record maybe little more than limit, thus we need slice the
                // last limit number.
                try (CloseableIterator<LogRecord> logRecordIterator =
                        logRecordBatch.records(readContext)) {
                    while (logRecordIterator.hasNext()) {
                        scanRows.add(maybeProject(logRecordIterator.next().getRow()));
                    }
                }
            }
        }
        if (scanRows.size() > limit) {
            scanRows = scanRows.subList(scanRows.size() - limit, scanRows.size());
        }
        return scanRows;
    }

    private InternalRow maybeProject(InternalRow originRow) {
        // TODO: currently, we have to deep copy the row to avoid the underlying ArrowBatch is
        //  released, we should return the originRow directly and lazily deserialize ArrowBatch in
        //  the future
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

    @Override
    public void close() throws IOException {
        scanFuture.cancel(true);
        try {
            if (fallbackScanner != null) {
                fallbackScanner.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            chunkedFactory.close();
        }
    }
}
