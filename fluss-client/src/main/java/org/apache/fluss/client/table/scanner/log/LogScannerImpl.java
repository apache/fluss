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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.Projection;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.ConcurrentModificationException;

/**
 * The default impl of {@link LogScanner}.
 *
 * <p>The {@link LogScannerImpl} is NOT thread-safe. It is the responsibility of the user to ensure
 * that multithreaded access is properly synchronized. Un-synchronized access will result in {@link
 * ConcurrentModificationException}.
 *
 * @since 0.1
 */
@PublicEvolving
public class LogScannerImpl extends AbstractLogScanner<ScanRecords> implements LogScanner {

    private final TablePath tablePath;
    private final MetadataUpdater metadataUpdater;
    private final long tableId;
    private final boolean isPartitionedTable;

    private final TableInfo tableInfo;
    private final @Nullable Projection projection;
    private final @Nullable Predicate recordBatchFilter;
    private final SchemaGetter schemaGetter;

    public LogScannerImpl(
            Configuration conf,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            ClientMetricGroup clientMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            @Nullable int[] projectedFields,
            SchemaGetter schemaGetter,
            @Nullable Predicate recordBatchFilter) {
        this(
                conf,
                tableInfo,
                metadataUpdater,
                new LogScannerStatus(),
                new ScannerMetricGroup(clientMetricGroup, tableInfo.getTablePath()),
                remoteFileDownloader,
                projectedFields,
                schemaGetter,
                recordBatchFilter);
    }

    private LogScannerImpl(
            Configuration conf,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            LogScannerStatus logScannerStatus,
            ScannerMetricGroup scannerMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            @Nullable int[] projectedFields,
            SchemaGetter schemaGetter,
            @Nullable Predicate recordBatchFilter) {
        super(
                tableInfo.getTablePath().toString(),
                logScannerStatus,
                new LogFetcher(
                        tableInfo.getTablePath().toString(),
                        logScannerStatus,
                        conf,
                        metadataUpdater,
                        scannerMetricGroup,
                        remoteFileDownloader,
                        LogRecordReadContext.SchemaResolution.DYNAMIC),
                scannerMetricGroup);
        this.tablePath = tableInfo.getTablePath();
        this.tableId = tableInfo.getTableId();
        this.isPartitionedTable = tableInfo.isPartitioned();
        // add this table to metadata updater.
        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
        this.metadataUpdater = metadataUpdater;
        this.projection = sanityProjection(projectedFields, tableInfo);
        this.tableInfo = tableInfo;
        this.recordBatchFilter = recordBatchFilter;
        this.schemaGetter = schemaGetter;
    }

    /**
     * Check if the projected fields are valid and returns {@link Projection} if the projection is
     * not null.
     */
    @Nullable
    private static Projection sanityProjection(
            @Nullable int[] projectedFields, TableInfo tableInfo) {
        RowType tableRowType = tableInfo.getRowType();
        if (projectedFields != null) {
            for (int projectedField : projectedFields) {
                if (projectedField < 0 || projectedField >= tableRowType.getFieldCount()) {
                    throw new IllegalArgumentException(
                            "Projected field index "
                                    + projectedField
                                    + " is out of bound for schema "
                                    + tableRowType);
                }
            }
            return Projection.of(projectedFields);
        } else {
            return null;
        }
    }

    @Override
    protected ScanRecords emptyResult() {
        return ScanRecords.EMPTY;
    }

    @Override
    protected ScanRecords toResult(ScanRecords scanRecords) {
        return scanRecords;
    }

    @Override
    protected String notSubscribedMessage() {
        return "LogScanner is not subscribed any buckets.";
    }

    @Override
    public void subscribe(int bucket, long offset) {
        if (isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is a partitioned table, please use "
                            + "\"subscribe(long partitionId, int bucket, long offset)\" to "
                            + "subscribe a partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            this.logFetcher.registerTable(
                    new TableScanSpec(tableInfo, projection, recordBatchFilter), schemaGetter);
            this.metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
            this.logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is not a partitioned table, please use "
                            + "\"subscribe(int bucket, long offset)\" to "
                            + "subscribe a non-partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            this.logFetcher.registerTable(
                    new TableScanSpec(tableInfo, projection, recordBatchFilter), schemaGetter);
            // we make assumption that the partition id must belong to the current table
            // if we can't find the partition id from the table path, we'll consider the table
            // is not exist
            this.metadataUpdater.checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(partitionId));
            this.logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(long partitionId, int bucket) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "Can't unsubscribe a partition for a non-partitioned table.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            this.logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(int bucket) {
        if (isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is a partitioned table, please use "
                            + "\"unsubscribe(long partitionId, int bucket)\" to "
                            + "unsubscribe a partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            this.logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }
}
