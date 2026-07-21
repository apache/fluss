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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.deserializer.BinlogDeserializationSchema;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** A Flink table source for the $binlog virtual table. */
public class BinlogFlinkTableSource
        implements ScanTableSource, SupportsProjectionPushDown, SupportsFilterPushDown {

    private final TablePath tablePath;
    private final Configuration flussConfig;
    // The binlog output type (includes metadata + nested before/after ROW columns)
    private final org.apache.flink.table.types.logical.RowType binlogOutputType;
    // The data columns type extracted from the 'before' nested ROW
    private final org.apache.flink.table.types.logical.RowType dataColumnsType;
    private final boolean isPartitioned;
    private final boolean streaming;
    private final FlinkConnectorOptionsUtils.StartupOptions startupOptions;
    private final long scanPartitionDiscoveryIntervalMs;
    private final int splitPerAssignmentBatchSize;
    private final Map<String, String> tableOptions;

    // Projection pushdown. Top-level projection over the binlog row [_change_type, _log_offset,
    // _commit_timestamp, before, after]; the underlying data scan stays full because before/after
    // are whole nested ROWs (nested pruning is out of scope).
    @Nullable private int[] projectedTopLevel;
    private LogicalType producedDataType;

    @Nullable private Predicate partitionFilters;

    public BinlogFlinkTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            org.apache.flink.table.types.logical.RowType binlogOutputType,
            boolean isPartitioned,
            boolean streaming,
            FlinkConnectorOptionsUtils.StartupOptions startupOptions,
            long scanPartitionDiscoveryIntervalMs,
            Map<String, String> tableOptions) {
        this(
                tablePath,
                flussConfig,
                binlogOutputType,
                isPartitioned,
                streaming,
                startupOptions,
                scanPartitionDiscoveryIntervalMs,
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                tableOptions);
    }

    public BinlogFlinkTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            org.apache.flink.table.types.logical.RowType binlogOutputType,
            boolean isPartitioned,
            boolean streaming,
            FlinkConnectorOptionsUtils.StartupOptions startupOptions,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            Map<String, String> tableOptions) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.binlogOutputType = binlogOutputType;
        this.isPartitioned = isPartitioned;
        this.streaming = streaming;
        this.startupOptions = startupOptions;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.splitPerAssignmentBatchSize = splitPerAssignmentBatchSize;
        this.tableOptions = tableOptions;

        // Extract data columns from the 'before' nested ROW type (index 3)
        // The binlog schema is: [_change_type, _log_offset, _commit_timestamp, before, after]
        this.dataColumnsType =
                (org.apache.flink.table.types.logical.RowType) binlogOutputType.getTypeAt(3);
        this.producedDataType = binlogOutputType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // Create the Fluss row type for the data columns (the original table columns). The data
        // scan always stays full because the projected before/after columns are whole nested ROWs.
        RowType flussRowType = FlinkConversions.toFlussRowType(dataColumnsType);

        // Determine the offsets initializer based on startup mode
        OffsetsInitializer offsetsInitializer;
        switch (startupOptions.startupMode) {
            case EARLIEST:
            case FULL:
                // For binlog, read all log records from the beginning
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case LATEST:
                offsetsInitializer = OffsetsInitializer.latest();
                break;
            case TIMESTAMP:
                offsetsInitializer =
                        OffsetsInitializer.timestamp(startupOptions.startupTimestampMs);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported startup mode: " + startupOptions.startupMode);
        }

        // Create the source with the binlog deserialization schema
        FlinkSource<RowData> source =
                new FlinkSource<>(
                        flussConfig,
                        tablePath,
                        false,
                        isPartitioned,
                        flussRowType,
                        null,
                        null,
                        offsetsInitializer,
                        scanPartitionDiscoveryIntervalMs,
                        splitPerAssignmentBatchSize,
                        new BinlogDeserializationSchema(projectedTopLevel),
                        streaming,
                        partitionFilters,
                        LeaseContext.DEFAULT);

        return SourceProvider.of(source);
    }

    @Override
    public DynamicTableSource copy() {
        BinlogFlinkTableSource copy =
                new BinlogFlinkTableSource(
                        tablePath,
                        flussConfig,
                        binlogOutputType,
                        isPartitioned,
                        streaming,
                        startupOptions,
                        scanPartitionDiscoveryIntervalMs,
                        splitPerAssignmentBatchSize,
                        tableOptions);
        copy.producedDataType = producedDataType;
        copy.projectedTopLevel = projectedTopLevel;
        copy.partitionFilters = partitionFilters;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "FlussBinlogTableSource";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        // Top-level projection over [_change_type, _log_offset, _commit_timestamp, before, after].
        // The data scan stays full (before/after are whole nested ROWs); the deserialization schema
        // emits only the projected top-level columns.
        this.projectedTopLevel =
                Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        this.producedDataType = producedDataType.getLogicalType();
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // The $binlog data and partition columns are nested inside the before/after ROW columns,
        // and the leading columns are non-pushable metadata (_change_type, _log_offset,
        // _commit_timestamp). No top-level filter is convertible to a Fluss predicate, so nothing
        // is pushed down; all filters are returned to Flink. Implemented for interface parity with
        // the normal table.
        return Result.of(Collections.emptyList(), filters);
    }

    @VisibleForTesting
    @Nullable
    int[] getProjectedTopLevel() {
        return projectedTopLevel;
    }

    @VisibleForTesting
    LogicalType getProducedDataType() {
        return producedDataType;
    }

    @VisibleForTesting
    @Nullable
    Predicate getPartitionFilters() {
        return partitionFilters;
    }
}
