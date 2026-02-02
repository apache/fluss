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
import org.apache.fluss.flink.source.deserializer.BinlogDeserializationSchema;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.Map;

/** A Flink table source for the $binlog virtual table. */
public class BinlogFlinkTableSource implements ScanTableSource {

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
    private final Map<String, String> tableOptions;

    // Projection pushdown
    @Nullable private int[] projectedFields;
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
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.binlogOutputType = binlogOutputType;
        this.isPartitioned = isPartitioned;
        this.streaming = streaming;
        this.startupOptions = startupOptions;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
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
        // Create the Fluss row type for the data columns (the original table columns)
        RowType flussRowType = FlinkConversions.toFlussRowType(dataColumnsType);
        if (projectedFields != null) {
            flussRowType = flussRowType.project(projectedFields);
        }

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
                        projectedFields,
                        offsetsInitializer,
                        scanPartitionDiscoveryIntervalMs,
                        new BinlogDeserializationSchema(),
                        streaming,
                        partitionFilters,
                        null);

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
                        tableOptions);
        copy.producedDataType = producedDataType;
        copy.projectedFields = projectedFields;
        copy.partitionFilters = partitionFilters;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "FlussBinlogTableSource";
    }

    // TODO: Implement projection pushdown handling for nested before/after columns
    // TODO: Implement filter pushdown
}
