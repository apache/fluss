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
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
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
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.SchemaUtil;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    /**
     * A cache for schema projection mapping from source schema to target. Use HashMap here, because
     * LimitBatchScanner is used in single thread only.
     */
    private final Map<Short, int[]> schemaProjectionCache = new HashMap<>();

    private boolean endOfInput;

    public LimitBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            int limit) {
        this.tableInfo = tableInfo;
        this.projectedFields = projectedFields;
        this.limit = limit;
        this.targetSchemaId = tableInfo.getSchemaId();
        this.schemaGetter = schemaGetter;
        RowType rowType = tableInfo.getRowType();
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }

        LimitScanRequest limitScanRequest =
                new LimitScanRequest()
                        .setTableId(tableBucket.getTableId())
                        .setBucketId(tableBucket.getBucket())
                        .setLimit(limit);

        if (tableBucket.getPartitionId() != null) {
            limitScanRequest.setPartitionId(tableBucket.getPartitionId());
            metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
        }

        // because that rocksdb is not suitable to projection, thus do it in client.
        int leader = metadataUpdater.leaderFor(tableInfo.getTablePath(), tableBucket);
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            // TODO handle this exception, like retry.
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }
        this.scanFuture = gateway.limitScan(limitScanRequest);

        this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        this.endOfInput = false;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }
        try {
            LimitScanResponse response = scanFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            List<InternalRow> scanRows = parseLimitScanResponse(response);
            endOfInput = true;
            return CloseableIterator.wrap(scanRows.iterator());
        } catch (TimeoutException e) {
            // poll next time
            return CloseableIterator.emptyIterator();
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
                    LogRecordReadContext.createReadContext(tableInfo, false, null, schemaGetter);
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
    }
}
