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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.KvSnapshotBatchScanner;
import org.apache.fluss.client.table.scanner.batch.LimitBatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** API for configuring and creating {@link LogScanner} and {@link BatchScanner}. */
public class TableScan implements Scan {

    private final FlussConnection conn;
    private final TableInfo tableInfo;

    /** The projected fields to do projection. No projection if is null. */
    @Nullable private final int[] projectedColumns;
    /** The limited row number to read. No limit if is null. */
    @Nullable private final Integer limit;

    public TableScan(FlussConnection conn, TableInfo tableInfo) {
        this(conn, tableInfo, null, null);
    }

    private TableScan(
            FlussConnection conn,
            TableInfo tableInfo,
            @Nullable int[] projectedColumns,
            @Nullable Integer limit) {
        this.conn = conn;
        this.tableInfo = tableInfo;
        this.projectedColumns = projectedColumns;
        this.limit = limit;
    }

    @Override
    public Scan project(@Nullable int[] projectedColumns) {
        return new TableScan(conn, tableInfo, projectedColumns, limit);
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
                                "Field '%s' not found in table schema. Available fields: %s, Table: %s",
                                projectedColumnNames.get(i),
                                rowType.getFieldNames(),
                                tableInfo.getTablePath()));
            }
            columnIndexes[i] = index;
        }
        return new TableScan(conn, tableInfo, columnIndexes, limit);
    }

    @Override
    public Scan limit(int rowNumber) {
        return new TableScan(conn, tableInfo, projectedColumns, rowNumber);
    }

    @Override
    public LogScanner createLogScanner() {
        if (limit != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "LogScanner doesn't support limit pushdown. Table: %s, requested limit: %d",
                            tableInfo.getTablePath(), limit));
        }
        return new LogScannerImpl(
                conn.getConfiguration(),
                tableInfo,
                conn.getMetadataUpdater(),
                conn.getClientMetricGroup(),
                conn.getOrCreateRemoteFileDownloader(),
                projectedColumns);
    }

    @Override
    public BatchScanner createBatchScanner(TableBucket tableBucket) {
        if (limit == null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Currently, BatchScanner is only available when limit is set. Table: %s, bucket: %s",
                            tableInfo.getTablePath(), tableBucket));
        }
        return new LimitBatchScanner(
                tableInfo, tableBucket, conn.getMetadataUpdater(), projectedColumns, limit);
    }

    @Override
    public BatchScanner createBatchScanner(TableBucket tableBucket, long snapshotId) {
        if (limit != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Currently, SnapshotBatchScanner doesn't support limit pushdown. Table: %s, bucket: %s, snapshot ID: %d, requested limit: %d",
                            tableInfo.getTablePath(), tableBucket, snapshotId, limit));
        }
        String scannerTmpDir =
                conn.getConfiguration().getString(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR);
        Admin admin = conn.getAdmin();
        final KvSnapshotMetadata snapshotMeta;
        try {
            snapshotMeta = admin.getKvSnapshotMetadata(tableBucket, snapshotId).get();
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to get snapshot metadata for table bucket %s, snapshot ID: %d, Table: %s",
                            tableBucket, snapshotId, tableInfo.getTablePath()),
                    e);
        }

        return new KvSnapshotBatchScanner(
                tableInfo.getRowType(),
                tableBucket,
                snapshotMeta.getSnapshotFiles(),
                projectedColumns,
                scannerTmpDir,
                tableInfo.getTableConfig().getKvFormat(),
                conn.getOrCreateRemoteFileDownloader());
    }
}
