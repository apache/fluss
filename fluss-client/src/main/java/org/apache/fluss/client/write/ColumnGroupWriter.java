/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.writer.AppendColumnsResult;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidColumnGroupOffsetException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.UnknownColumnGroupException;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogColumnsReqForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogColumnsRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogColumnsRequest;
import org.apache.fluss.rpc.messages.ProduceLogColumnsResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.types.RowType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Writes column-group rows (FIP-45 log enrichment via append columns) for one table.
 *
 * <p>Each call encodes the rows of one column group for a contiguous range of base-log offsets into
 * a standard Arrow log record batch and sends it to the bucket leader with a {@link
 * ProduceLogColumnsRequest}. The server stamps the base offsets into the batch header, so the bytes
 * written here are exactly the bytes later served to readers.
 *
 * <p>TODO (WP7): batching, one in-flight request per bucket, leader-change retries and resync on
 * {@code expected_source_offset} are not implemented; every call is one request.
 */
@Internal
@ThreadSafe
public final class ColumnGroupWriter implements AutoCloseable {

    private final TablePath tablePath;
    private final TableInfo tableInfo;
    private final MetadataUpdater metadataUpdater;
    private final int acks;
    private final int requestTimeoutMs;
    private final int batchBufferSize;
    private final BufferAllocator allocator;
    private final Map<String, ArrowWriterPool> writerPools = new ConcurrentHashMap<>();

    public ColumnGroupWriter(
            TablePath tablePath,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            Configuration conf,
            int acks) {
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.metadataUpdater = metadataUpdater;
        this.acks = acks;
        this.requestTimeoutMs = (int) conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis();
        this.batchBufferSize =
                (int) conf.get(ConfigOptions.CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes();
        this.allocator = BufferAllocatorUtil.createBufferAllocator(null);
    }

    /** Appends {@code rows} of {@code columnGroup} at base offsets starting at {@code first}. */
    public CompletableFuture<AppendColumnsResult> appendColumns(
            String columnGroup,
            TableBucket bucket,
            long firstSourceOffset,
            List<InternalRow> rows) {
        Schema schema = tableInfo.getSchema();
        if (!schema.getColumnGroups().containsKey(columnGroup)) {
            throw new UnknownColumnGroupException(
                    "Unknown column group '" + columnGroup + "' on table " + tablePath);
        }
        RowType groupRowType = schema.getColumnGroupRowType(columnGroup);
        for (InternalRow row : rows) {
            if (row.getFieldCount() != groupRowType.getFieldCount()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column group '%s' has %d columns but a row with %d fields was given.",
                                columnGroup, groupRowType.getFieldCount(), row.getFieldCount()));
            }
        }
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("No rows to append for column group " + columnGroup);
        }

        final BytesView records;
        try {
            records = encode(columnGroup, groupRowType, rows);
        } catch (Exception e) {
            CompletableFuture<AppendColumnsResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new FlussRuntimeException("Failed to encode column group rows.", e));
            return failed;
        }

        TabletServerGateway gateway;
        try {
            gateway = leaderGateway(bucket);
        } catch (Exception e) {
            CompletableFuture<AppendColumnsResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        ProduceLogColumnsRequest request =
                new ProduceLogColumnsRequest()
                        .setAcks(acks)
                        .setTableId(tableInfo.getTableId())
                        .setTimeoutMs(requestTimeoutMs)
                        .setColumnGroup(columnGroup);
        PbProduceLogColumnsReqForBucket bucketReq =
                request.addBucketsReq()
                        .setBucketId(bucket.getBucket())
                        .setFirstSourceOffset(firstSourceOffset);
        if (bucket.getPartitionId() != null) {
            bucketReq.setPartitionId(bucket.getPartitionId());
        }
        bucketReq.setRecordsBytesView(records);

        return gateway.produceLogColumns(request).thenApply(response -> toResult(bucket, response));
    }

    private TabletServerGateway leaderGateway(TableBucket bucket) {
        int leader;
        try {
            leader = metadataUpdater.leaderFor(tablePath, bucket);
        } catch (Exception e) {
            metadataUpdater.checkAndUpdateMetadata(tablePath, bucket);
            leader = metadataUpdater.leaderFor(tablePath, bucket);
        }
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "No tablet server gateway for leader " + leader + " of bucket " + bucket);
        }
        return gateway;
    }

    private BytesView encode(String columnGroup, RowType groupRowType, List<InternalRow> rows)
            throws Exception {
        ArrowWriterPool pool =
                writerPools.computeIfAbsent(columnGroup, g -> new ArrowWriterPool(allocator));
        ArrowWriter arrowWriter =
                pool.getOrCreateWriter(
                        tableInfo.getTableId(),
                        tableInfo.getSchemaId(),
                        batchBufferSize,
                        groupRowType,
                        tableInfo.getTableConfig().getArrowCompressionInfo());
        int pageSize = Math.max(4096, Math.min(batchBufferSize, 1024 * 1024));
        UnmanagedPagedOutputView outputView = new UnmanagedPagedOutputView(pageSize);
        MemoryLogRecordsArrowBuilder builder =
                MemoryLogRecordsArrowBuilder.builder(
                        tableInfo.getSchemaId(), arrowWriter, outputView, true, null);
        try {
            for (InternalRow row : rows) {
                if (builder.isFull()) {
                    throw new IllegalArgumentException(
                            "Too many column group rows for one request; split the rows into "
                                    + "smaller batches (limit "
                                    + ConfigOptions.CLIENT_WRITER_REQUEST_MAX_SIZE.key()
                                    + ").");
                }
                builder.append(ChangeType.APPEND_ONLY, row);
            }
            builder.close();
            return builder.build();
        } finally {
            builder.recycleArrowWriter();
        }
    }

    private static AppendColumnsResult toResult(
            TableBucket bucket, ProduceLogColumnsResponse response) {
        if (response.getBucketsRespsCount() == 0) {
            throw new FlussRuntimeException(
                    "Empty produceLogColumns response for bucket " + bucket);
        }
        PbProduceLogColumnsRespForBucket resp = response.getBucketsRespAt(0);
        if (resp.hasErrorCode() && resp.getErrorCode() != Errors.NONE.code()) {
            Errors error = Errors.forCode(resp.getErrorCode());
            String message =
                    "Column group write for bucket "
                            + bucket
                            + " failed: "
                            + resp.getErrorMessage();
            if (error == Errors.INVALID_COLUMN_GROUP_OFFSET) {
                long expected =
                        resp.hasExpectedSourceOffset() ? resp.getExpectedSourceOffset() : -1L;
                throw new InvalidColumnGroupOffsetException(message, expected);
            }
            throw error.exception(message);
        }
        return new AppendColumnsResult(resp.getLogEndOffset(), resp.getHighWatermark());
    }

    @Override
    public void close() {
        for (ArrowWriterPool pool : writerPools.values()) {
            pool.close();
        }
        writerPools.clear();
        allocator.close();
    }
}
