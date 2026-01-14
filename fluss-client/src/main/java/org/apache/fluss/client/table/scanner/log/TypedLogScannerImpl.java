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

import org.apache.fluss.client.converter.RowToPojoConverter;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.TypedScanRecord;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapter that converts {@link InternalRow} records from a {@link LogScanner} into POJOs of type T.
 */
public class TypedLogScannerImpl<T> implements TypedLogScanner<T> {

    private final LogScanner delegate;
    private final RowToPojoConverter<T> converter;

    public TypedLogScannerImpl(
            LogScanner delegate, Class<T> pojoClass, TableInfo tableInfo, int[] projectedColumns) {
        this.delegate = delegate;
        RowType tableSchema = tableInfo.getRowType();
        RowType projection =
                projectedColumns == null ? tableSchema : tableSchema.project(projectedColumns);
        this.converter = RowToPojoConverter.of(pojoClass, tableSchema, projection);
    }

    @Override
    public TypedScanRecords<T> poll(Duration timeout) {
        ScanRecords records = delegate.poll(timeout);
        if (records == null || records.isEmpty()) {
            return TypedScanRecords.empty();
        }
        Map<TableBucket, List<TypedScanRecord<T>>> out = new HashMap<>();
        for (TableBucket bucket : records.buckets()) {
            List<ScanRecord> list = records.records(bucket);
            List<TypedScanRecord<T>> converted = new ArrayList<>(list.size());
            for (ScanRecord r : list) {
                InternalRow row = r.getRow();
                T pojo = converter.fromRow(row);
                converted.add(new TypedScanRecord<>(r, pojo));
            }
            out.put(bucket, converted);
        }
        return new TypedScanRecords<>(out);
    }

    @Override
    public void subscribeFromBeginning(int bucket) {
        delegate.subscribeFromBeginning(bucket);
    }

    @Override
    public void subscribeFromBeginning(long partitionId, int bucket) {
        delegate.subscribeFromBeginning(partitionId, bucket);
    }

    @Override
    public void subscribe(int bucket, long offset) {
        delegate.subscribe(bucket, offset);
    }

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {
        delegate.subscribe(partitionId, bucket, offset);
    }

    @Override
    public void unsubscribe(long partitionId, int bucket) {
        delegate.unsubscribe(partitionId, bucket);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
