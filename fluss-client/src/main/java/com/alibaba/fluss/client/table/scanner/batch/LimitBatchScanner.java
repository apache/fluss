/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner.batch;

import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.ValueRecord;
import com.alibaba.fluss.record.ValueRecordReadContext;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.row.decode.RowDecoder;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.LimitScanRequest;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private final ValueDecoder kvValueDecoder;

    private boolean endOfInput;

    public LimitBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            int limit) {
        this.tableInfo = tableInfo;
        this.projectedFields = projectedFields;
        this.limit = limit;

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
        int leader = metadataUpdater.leaderFor(tableBucket);
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);

        this.scanFuture = gateway.limitScan(limitScanRequest);

        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                rowType.getChildren().toArray(new DataType[0])));
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
                    new ValueRecordReadContext(kvValueDecoder.getRowDecoder());
            for (ValueRecord record : valueRecords.records(readContext)) {
                scanRows.add(maybeProject(record.getRow()));
            }
        } else {
            LogRecordReadContext readContext =
                    LogRecordReadContext.createReadContext(tableInfo, false, null);
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
