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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordBatch;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.ScanSystemViewRequest;
import org.apache.fluss.rpc.messages.ScanSystemViewResponse;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link BatchScanner} that reads data from a system view by sending a {@code
 * ScanSystemViewRequest} to the coordinator server.
 *
 * <p>System views have no buckets or partitions — the coordinator returns all matching rows in a
 * single response. This scanner is always bounded (one-shot request/response).
 */
public class SystemViewBatchScanner implements BatchScanner {

    private final CompletableFuture<ScanSystemViewResponse> scanFuture;
    private final RowType responseRowType;
    private boolean endOfInput;

    public SystemViewBatchScanner(
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            @Nullable Predicate filterPredicate) {
        this.endOfInput = false;

        TablePath tablePath = tableInfo.getTablePath();
        RowType rowType = tableInfo.getRowType();

        // Build RowType with sequential field IDs for predicate serialization
        List<DataField> fieldsWithIds = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField f = rowType.getFields().get(i);
            fieldsWithIds.add(new DataField(f.getName(), f.getType(), i));
        }
        RowType fullRowType = new RowType(rowType.isNullable(), fieldsWithIds);

        // Determine the row type of the response (projected if applicable)
        this.responseRowType =
                projectedFields != null ? fullRowType.project(projectedFields) : fullRowType;

        ScanSystemViewRequest request = new ScanSystemViewRequest();
        request.setDatabaseName(tablePath.getDatabaseName());
        request.setViewName(tablePath.getTableName());
        request.setSchemaId(String.valueOf(tableInfo.getSchemaId()));

        if (projectedFields != null) {
            request.setProjectedFields(projectedFields);
        }

        if (filterPredicate != null) {
            request.setFilterPredicate(
                    PredicateMessageUtils.toPbPredicate(filterPredicate, fullRowType));
        }

        CoordinatorGateway coordinatorGateway = metadataUpdater.newCoordinatorServerClient();
        this.scanFuture = coordinatorGateway.scanSystemView(request);
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }
        try {
            ScanSystemViewResponse response =
                    scanFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (response.hasErrorCode()) {
                throw new FlussRuntimeException(
                        "Failed to scan system view: " + response.getErrorMessage());
            }
            List<InternalRow> rows = decodeResponse(response);
            endOfInput = true;
            return CloseableIterator.wrap(rows.iterator());
        } catch (TimeoutException e) {
            return CloseableIterator.emptyIterator();
        } catch (FlussRuntimeException e) {
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private List<InternalRow> decodeResponse(ScanSystemViewResponse response) {
        if (!response.hasRecords()) {
            return new ArrayList<>();
        }

        byte[] recordBytes = response.getRecords();
        DataType[] fieldTypes = responseRowType.getFieldTypes().toArray(new DataType[0]);
        RowDecoder decoder = RowDecoder.create(KvFormat.INDEXED, fieldTypes);
        ValueRecordBatch.ReadContext readContext = schemaId -> decoder;

        DefaultValueRecordBatch batch = DefaultValueRecordBatch.pointToBytes(recordBytes);

        List<InternalRow> rows = new ArrayList<>();
        for (ValueRecord record : batch.records(readContext)) {
            InternalRow row = record.getRow();
            if (row != null) {
                rows.add(row);
            }
        }
        return rows;
    }

    @Override
    public void close() throws IOException {
        scanFuture.cancel(true);
    }
}
