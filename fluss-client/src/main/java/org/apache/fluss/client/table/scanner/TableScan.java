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
import org.apache.fluss.client.table.BucketCalculator;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.CompositeBatchScanner;
import org.apache.fluss.client.table.scanner.batch.KvBatchScanner;
import org.apache.fluss.client.table.scanner.batch.KvSnapshotBatchScanner;
import org.apache.fluss.client.table.scanner.batch.LimitBatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.TypedLogScanner;
import org.apache.fluss.client.table.scanner.log.TypedLogScannerImpl;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.OrderedKeyEncoder;
import org.apache.fluss.row.ordered.OrderedKeyCodec;
import org.apache.fluss.shaded.guava32.com.google.common.primitives.UnsignedBytes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** API for configuring and creating {@link LogScanner} and {@link BatchScanner}. */
public class TableScan implements Scan {

    private final FlussConnection conn;
    private final TableInfo tableInfo;
    private final SchemaGetter schemaGetter;

    /** The projected fields to do projection. No projection if is null. */
    @Nullable private final int[] projectedColumns;

    /** The limited row number to read. No limit if is null. */
    @Nullable private final Integer limit;

    /** The record batch filter to apply. No filter if is null. */
    @Nullable private final Predicate recordBatchFilter;

    @Nullable private final List<String> equalityColumns;
    @Nullable private final String rangeColumn;

    public TableScan(FlussConnection conn, TableInfo tableInfo, SchemaGetter schemaGetter) {
        this(conn, tableInfo, schemaGetter, null, null, null, null, null);
    }

    private TableScan(
            FlussConnection conn,
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            @Nullable int[] projectedColumns,
            @Nullable Integer limit,
            @Nullable Predicate recordBatchFilter,
            @Nullable List<String> equalityColumns,
            @Nullable String rangeColumn) {
        this.conn = conn;
        this.tableInfo = tableInfo;
        this.projectedColumns = projectedColumns == null ? null : projectedColumns.clone();
        this.limit = limit;
        this.schemaGetter = schemaGetter;
        this.recordBatchFilter = recordBatchFilter;
        this.equalityColumns =
                equalityColumns == null
                        ? null
                        : Collections.unmodifiableList(new ArrayList<>(equalityColumns));
        this.rangeColumn = rangeColumn;
    }

    @Override
    public Scan project(@Nullable int[] projectedColumns) {
        return new TableScan(
                conn,
                tableInfo,
                schemaGetter,
                projectedColumns,
                limit,
                recordBatchFilter,
                equalityColumns,
                rangeColumn);
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
        return new TableScan(
                conn,
                tableInfo,
                schemaGetter,
                columnIndexes,
                limit,
                recordBatchFilter,
                equalityColumns,
                rangeColumn);
    }

    @Override
    public Scan limit(int rowNumber) {
        return new TableScan(
                conn,
                tableInfo,
                schemaGetter,
                projectedColumns,
                rowNumber,
                recordBatchFilter,
                equalityColumns,
                rangeColumn);
    }

    @Override
    public Scan filter(@Nullable Predicate predicate) {
        return new TableScan(
                conn,
                tableInfo,
                schemaGetter,
                projectedColumns,
                limit,
                predicate,
                equalityColumns,
                rangeColumn);
    }

    @Override
    public Scan equalBy(List<String> columnNames) {
        checkNotNull(columnNames, "columnNames");
        return new TableScan(
                conn,
                tableInfo,
                schemaGetter,
                projectedColumns,
                limit,
                recordBatchFilter,
                columnNames,
                rangeColumn);
    }

    @Override
    public Scan rangeBy(String columnName) {
        return new TableScan(
                conn,
                tableInfo,
                schemaGetter,
                projectedColumns,
                limit,
                recordBatchFilter,
                equalityColumns,
                checkNotNull(columnName, "columnName"));
    }

    @Override
    public BatchScanner createBatchScanner(InternalRow equalityKey, ScanRange range) {
        checkNotNull(equalityKey, "equalityKey");
        checkNotNull(range, "range");
        checkArgument(tableInfo.hasPrimaryKey(), "Range scans require a primary key table.");
        checkArgument(
                tableInfo.getTableConfig().getKvFormatVersion().orElse(1)
                        == ConfigOptions.KV_FORMAT_VERSION_3,
                "Range scans require table.kv.format-version=3.");
        checkArgument(recordBatchFilter == null, "Range scans cannot be combined with filter.");
        checkArgument(
                equalityColumns != null && !equalityColumns.isEmpty() && rangeColumn != null,
                "Configure equalBy and rangeBy before creating a range scanner.");
        checkArgument(limit == null || limit >= 0, "Range scan limit must be nonnegative.");
        checkArgument(
                equalityKey.getFieldCount() == equalityColumns.size(),
                "Equality key arity does not match equalBy.");
        checkArgument(
                new HashSet<>(equalityColumns).size() == equalityColumns.size()
                        && !equalityColumns.contains(rangeColumn),
                "Duplicate condition fields.");
        RowType equalityType = tableInfo.getRowType().project(equalityColumns);
        checkArgument(
                equalityColumns.containsAll(tableInfo.getPartitionKeys()),
                "Range scans require all partition fields.");
        for (int i = 0; i < equalityColumns.size(); i++) {
            checkArgument(!equalityKey.isNullAt(i), "Null equality keys are not supported.");
        }
        List<String> prefixColumns = new ArrayList<>(equalityColumns);
        prefixColumns.removeAll(tableInfo.getPartitionKeys());
        List<String> primary = tableInfo.getPhysicalPrimaryKeys();
        int count = prefixColumns.size();
        checkArgument(
                count < primary.size()
                        && new HashSet<>(primary.subList(0, count))
                                .equals(new HashSet<>(prefixColumns))
                        && primary.get(count).equals(rangeColumn),
                "Equality fields must form a primary-key prefix followed by the range field.");
        checkArgument(
                prefixColumns.containsAll(tableInfo.getBucketKeys()),
                "Equality fields must contain all bucket keys.");
        prefixColumns = primary.subList(0, count);
        byte[] prefix =
                OrderedKeyEncoder.createKeyEncoder(equalityType, prefixColumns)
                        .encodeKey(equalityKey);
        RowType endpointType =
                tableInfo.getRowType().project(Collections.singletonList(rangeColumn));
        OrderedKeyEncoder endpointEncoder =
                OrderedKeyEncoder.createKeyEncoder(
                        endpointType, Collections.singletonList(rangeColumn));
        byte[] lowerValue = range.lower == null ? null : endpointEncoder.encodeKey(range.lower);
        byte[] upperValue = range.upper == null ? null : endpointEncoder.encodeKey(range.upper);
        if (lowerValue != null && upperValue != null) {
            checkArgument(
                    UnsignedBytes.lexicographicalComparator().compare(lowerValue, upperValue) <= 0,
                    "Range lower bound exceeds upper bound.");
        }
        byte[] start = lowerValue == null ? prefix : concatenate(prefix, lowerValue);
        byte[] end =
                upperValue == null
                        ? OrderedKeyCodec.prefixUpperBound(prefix)
                        : concatenate(prefix, upperValue);
        if (lowerValue != null && !range.lowerInclusive) {
            start = OrderedKeyCodec.prefixUpperBound(start);
        }
        if (upperValue != null && range.upperInclusive) {
            end = OrderedKeyCodec.prefixUpperBound(end);
        }
        Long partitionId = null;
        int bucketCount = tableInfo.getNumBuckets();
        if (tableInfo.isPartitioned()) {
            String name =
                    new PartitionGetter(equalityType, tableInfo.getPartitionKeys())
                            .getPartition(equalityKey);
            PhysicalTablePath path = PhysicalTablePath.of(tableInfo.getTablePath(), name);
            try {
                conn.getMetadataUpdater().checkAndUpdatePartitionMetadata(path);
                Cluster cluster = conn.getMetadataUpdater().getCluster();
                partitionId = cluster.getPartitionIdOrElseThrow(path);
                bucketCount = cluster.getBucketCountOrFallback(tableInfo, partitionId);
            } catch (PartitionNotExistException e) {
                return emptyScanner();
            }
        }
        if (start == null
                || (end != null
                        && UnsignedBytes.lexicographicalComparator().compare(start, end) >= 0)
                || (limit != null && limit == 0)) {
            return emptyScanner();
        }
        byte[] bucketKey =
                KeyEncoder.ofBucketKeyEncoder(
                                equalityType,
                                tableInfo.getBucketKeys(),
                                tableInfo.getTableConfig().getDataLakeFormat().orElse(null))
                        .encodeKey(equalityKey);
        int bucket =
                BucketCalculator.bucketingFunctionFor(tableInfo).bucketing(bucketKey, bucketCount);
        return new KvBatchScanner(
                tableInfo,
                new TableBucket(tableInfo.getTableId(), partitionId, bucket),
                schemaGetter,
                conn.getMetadataUpdater(),
                kvBatchSizeBytes(),
                projectedColumns,
                start,
                end,
                limit,
                bucketCount);
    }

    private static byte[] concatenate(byte[] prefix, byte[] suffix) {
        byte[] bytes = Arrays.copyOf(prefix, prefix.length + suffix.length);
        System.arraycopy(suffix, 0, bytes, prefix.length, suffix.length);
        return bytes;
    }

    private static BatchScanner emptyScanner() {
        return new BatchScanner() {
            @Override
            public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
                return null;
            }

            @Override
            public void close() {}
        };
    }

    private void rejectUnboundRange() {
        checkArgument(
                equalityColumns == null && rangeColumn == null,
                "Range configuration requires createBatchScanner(equalityKey, range).");
    }

    @Override
    public LogScanner createLogScanner() {
        rejectUnboundRange();
        if (limit != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "LogScanner doesn't support limit pushdown. Table: %s, requested limit: %d",
                            tableInfo.getTablePath(), limit));
        }

        if (recordBatchFilter != null
                && tableInfo.getTableConfig().getLogFormat() != LogFormat.ARROW) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Filter pushdown is only supported for ARROW log format. "
                                    + "Table: %s, current log format: %s",
                            tableInfo.getTablePath(), tableInfo.getTableConfig().getLogFormat()));
        }

        return new LogScannerImpl(
                conn.getConfiguration(),
                tableInfo,
                conn.getMetadataUpdater(),
                conn.getClientMetricGroup(),
                conn.getOrCreateRemoteFileDownloader(),
                projectedColumns,
                schemaGetter,
                recordBatchFilter);
    }

    @Override
    public <T> TypedLogScanner<T> createTypedLogScanner(Class<T> pojoClass) {
        LogScanner base = createLogScanner();
        return new TypedLogScannerImpl<>(base, pojoClass, tableInfo, projectedColumns);
    }

    @Override
    public BatchScanner createBatchScanner(TableBucket tableBucket) {
        rejectUnboundRange();
        if (recordBatchFilter != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "BatchScanner doesn't support filter pushdown. Table: %s, bucket: %s",
                            tableInfo.getTablePath(), tableBucket));
        }
        if (tableInfo.hasPrimaryKey() && limit == null) {
            return new KvBatchScanner(
                    tableInfo,
                    tableBucket,
                    schemaGetter,
                    conn.getMetadataUpdater(),
                    kvBatchSizeBytes(),
                    projectedColumns);
        }
        if (limit == null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "BatchScanner over a Log Table requires limit to be set. Table: %s, bucket: %s",
                            tableInfo.getTablePath(), tableBucket));
        }
        return new LimitBatchScanner(
                tableInfo,
                tableBucket,
                schemaGetter,
                conn.getMetadataUpdater(),
                projectedColumns,
                limit);
    }

    private int kvBatchSizeBytes() {
        return (int)
                conn.getConfiguration()
                        .get(ConfigOptions.CLIENT_SCANNER_KV_FETCH_MAX_BYTES)
                        .getBytes();
    }

    @Override
    public BatchScanner createBatchScanner(TableBucket tableBucket, long snapshotId) {
        rejectUnboundRange();
        if (recordBatchFilter != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "SnapshotBatchScanner doesn't support filter pushdown. Table: %s, bucket: %s, snapshot ID: %d",
                            tableInfo.getTablePath(), tableBucket, snapshotId));
        }
        if (limit != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Currently, SnapshotBatchScanner doesn't support limit pushdown. Table: %s, bucket: %s, snapshot ID: %d, requested limit: %d",
                            tableInfo.getTablePath(), tableBucket, snapshotId, limit));
        }
        String scannerTmpDir =
                conn.getConfiguration().getString(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR);
        KvValueLayout kvValueLayout = KvValueLayout.fromTableConfig(tableInfo.getTableConfig());
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
                tableInfo.getSchemaId(),
                tableInfo.getSchema(),
                schemaGetter,
                tableBucket,
                snapshotMeta.getSnapshotFiles(),
                projectedColumns,
                scannerTmpDir,
                kvValueLayout,
                tableInfo.getTableConfig().getKvFormat(),
                conn.getOrCreateRemoteFileDownloader());
    }

    @Override
    public BatchScanner createBatchScanner() throws IOException {
        rejectUnboundRange();
        if (recordBatchFilter != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "BatchScanner doesn't support filter pushdown. Table: %s",
                            tableInfo.getTablePath()));
        }
        int bucketCount = tableInfo.getNumBuckets();
        List<TableBucket> tableBuckets;
        if (tableInfo.isPartitioned()) {
            List<PartitionInfo> partitionInfos;
            try {
                partitionInfos = conn.getAdmin().listPartitionInfos(tableInfo.getTablePath()).get();
            } catch (Exception e) {
                throw new IOException(
                        "Failed to list partition infos for table" + tableInfo.getTablePath(), e);
            }
            tableBuckets =
                    partitionInfos.stream()
                            .flatMap(
                                    partitionInfo ->
                                            IntStream.range(0, partitionInfo.getBucketCount())
                                                    .mapToObj(
                                                            bucketId ->
                                                                    new TableBucket(
                                                                            tableInfo.getTableId(),
                                                                            partitionInfo
                                                                                    .getPartitionId(),
                                                                            bucketId)))
                            .collect(Collectors.toList());
        } else {
            tableBuckets =
                    IntStream.range(0, bucketCount)
                            .mapToObj(bucketId -> new TableBucket(tableInfo.getTableId(), bucketId))
                            .collect(Collectors.toList());
        }

        List<BatchScanner> scanners =
                tableBuckets.stream().map(this::createBatchScanner).collect(Collectors.toList());
        return new CompositeBatchScanner(scanners, limit);
    }
}
