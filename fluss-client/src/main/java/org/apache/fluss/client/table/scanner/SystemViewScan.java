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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.SystemViewBatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.TypedLogScanner;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A {@link Scan} implementation for system views.
 *
 * <p>System views are virtual tables with no independent storage, no buckets, and no partitions.
 * They only support bounded batch scans via {@link #createBatchScanner()}. Streaming scans and
 * bucket-based batch scans are not supported.
 */
public class SystemViewScan implements Scan {

    private final TableInfo tableInfo;
    private final FlussConnection conn;
    @Nullable private final int[] projectedColumns;
    @Nullable private final Predicate filterPredicate;

    public SystemViewScan(TableInfo tableInfo, FlussConnection conn) {
        this(tableInfo, conn, null, null);
    }

    private SystemViewScan(
            TableInfo tableInfo,
            FlussConnection conn,
            @Nullable int[] projectedColumns,
            @Nullable Predicate filterPredicate) {
        this.tableInfo = tableInfo;
        this.conn = conn;
        this.projectedColumns = projectedColumns;
        this.filterPredicate = filterPredicate;
    }

    @Override
    public Scan project(@Nullable int[] projectedColumns) {
        return new SystemViewScan(tableInfo, conn, projectedColumns, filterPredicate);
    }

    @Override
    public Scan project(List<String> projectedColumnNames) {
        int[] columnIndexes = new int[projectedColumnNames.size()];
        RowType rowType = tableInfo.getRowType();
        for (int i = 0; i < projectedColumnNames.size(); i++) {
            int index = rowType.getFieldIndex(projectedColumnNames.get(i));
            if (index < 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Field '%s' not found in system view schema. "
                                        + "Available fields: %s, View: %s",
                                projectedColumnNames.get(i),
                                rowType.getFieldNames(),
                                tableInfo.getTablePath()));
            }
            columnIndexes[i] = index;
        }
        return new SystemViewScan(tableInfo, conn, columnIndexes, filterPredicate);
    }

    @Override
    public Scan limit(int rowNumber) {
        throw new UnsupportedOperationException(
                "System views do not support limit pushdown. View: " + tableInfo.getTablePath());
    }

    @Override
    public Scan filter(@Nullable Predicate predicate) {
        return new SystemViewScan(tableInfo, conn, projectedColumns, predicate);
    }

    @Override
    public LogScanner createLogScanner() {
        throw new UnsupportedOperationException(
                "System views do not support streaming log scan. "
                        + "Use createBatchScanner() instead. View: "
                        + tableInfo.getTablePath());
    }

    @Override
    public <T> TypedLogScanner<T> createTypedLogScanner(Class<T> pojoClass) {
        throw new UnsupportedOperationException(
                "System views do not support streaming log scan. "
                        + "Use createBatchScanner() instead. View: "
                        + tableInfo.getTablePath());
    }

    @Override
    public BatchScanner createBatchScanner(TableBucket tableBucket) {
        throw new UnsupportedOperationException(
                "System views do not support bucket-based batch scan. "
                        + "Use createBatchScanner() (no args) instead. View: "
                        + tableInfo.getTablePath());
    }

    @Override
    public BatchScanner createBatchScanner(TableBucket tableBucket, long snapshotId) {
        throw new UnsupportedOperationException(
                "System views do not support snapshot batch scan. "
                        + "Use createBatchScanner() (no args) instead. View: "
                        + tableInfo.getTablePath());
    }

    @Override
    public BatchScanner createBatchScanner() {
        return new SystemViewBatchScanner(
                tableInfo, conn.getMetadataUpdater(), projectedColumns, filterPredicate);
    }
}
