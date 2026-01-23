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
import org.apache.fluss.flink.source.deserializer.ChangelogDeserializationSchema;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A Flink table source for the $changelog virtual table. */
public class ChangelogFlinkTableSource implements ScanTableSource {

    private final TablePath tablePath;
    private final Configuration flussConfig;
    // The changelog output type (includes metadata columns: _change_type, _log_offset,
    // _commit_timestamp)
    private final org.apache.flink.table.types.logical.RowType changelogOutputType;
    private final org.apache.flink.table.types.logical.RowType dataColumnsType;
    private final int[] partitionKeyIndexes;
    private final boolean streaming;
    private final FlinkConnectorOptionsUtils.StartupOptions startupOptions;
    private final long scanPartitionDiscoveryIntervalMs;
    private final Map<String, String> tableOptions;

    // Projection pushdown
    @Nullable private int[] projectedFields;
    private LogicalType producedDataType;

    @Nullable private Predicate partitionFilters;

    private static final Set<String> METADATA_COLUMN_NAMES =
            new HashSet<>(
                    Arrays.asList(
                            TableDescriptor.CHANGE_TYPE_COLUMN,
                            TableDescriptor.LOG_OFFSET_COLUMN,
                            TableDescriptor.COMMIT_TIMESTAMP_COLUMN));

    public ChangelogFlinkTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            org.apache.flink.table.types.logical.RowType changelogOutputType,
            int[] partitionKeyIndexes,
            boolean streaming,
            FlinkConnectorOptionsUtils.StartupOptions startupOptions,
            long scanPartitionDiscoveryIntervalMs,
            Map<String, String> tableOptions) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        // The changelogOutputType already includes metadata columns from FlinkCatalog
        this.changelogOutputType = changelogOutputType;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.streaming = streaming;
        this.startupOptions = startupOptions;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.tableOptions = tableOptions;

        // Extract data columns by filtering out metadata columns by name
        this.dataColumnsType = extractDataColumnsType(changelogOutputType);
        this.producedDataType = changelogOutputType;
    }

    /**
     * Extracts the data columns type by removing the metadata columns from the changelog output
     * type.
     */
    private org.apache.flink.table.types.logical.RowType extractDataColumnsType(
            org.apache.flink.table.types.logical.RowType changelogType) {
        // Filter out metadata columns by name
        List<org.apache.flink.table.types.logical.RowType.RowField> dataFields =
                changelogType.getFields().stream()
                        .filter(field -> !METADATA_COLUMN_NAMES.contains(field.getName()))
                        .collect(Collectors.toList());

        return new org.apache.flink.table.types.logical.RowType(dataFields);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // The $changelog virtual table always produces INSERT-only records.
        // All change types (+I, -U, +U, -D) are flattened into regular rows
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // Create the Fluss row type for the data columns (without metadata)
        RowType flussRowType = FlinkConversions.toFlussRowType(dataColumnsType);
        if (projectedFields != null) {
            // Adjust projection to account for metadata columns
            // TODO: Handle projection properly with metadata columns
            flussRowType = flussRowType.project(projectedFields);
        }
        // to capture all change types (+I, -U, +U, -D).
        // FULL mode reads snapshot first (no change types), so we use EARLIEST for log-only
        // reading.
        // LATEST mode is supported for real-time changelog streaming from current position.
        OffsetsInitializer offsetsInitializer;
        switch (startupOptions.startupMode) {
            case EARLIEST:
            case FULL:
                // For changelog, FULL mode should read all log records from beginning
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

        // Create the source with the changelog deserialization schema
        FlinkSource<RowData> source =
                new FlinkSource<>(
                        flussConfig,
                        tablePath,
                        // Changelog/binlog virtual tables are purely log-based and don't have a
                        // primary key, setting hasPrimaryKey "false" to ensure the enumerator
                        // fetches log-only splits (not snapshot splits), which is the correct
                        // behavior for virtual tables.
                        false,
                        isPartitioned(),
                        flussRowType,
                        projectedFields,
                        offsetsInitializer,
                        scanPartitionDiscoveryIntervalMs,
                        new ChangelogDeserializationSchema(),
                        streaming,
                        partitionFilters,
                        null); // Lake source not supported

        if (!streaming) {
            // Batch mode - changelog virtual tables read from log, not data lake
            return new SourceProvider() {
                @Override
                public boolean isBounded() {
                    return true;
                }

                @Override
                public Source<RowData, ?, ?> createSource() {
                    return source;
                }
            };
        } else {
            return SourceProvider.of(source);
        }
    }

    @Override
    public DynamicTableSource copy() {
        ChangelogFlinkTableSource copy =
                new ChangelogFlinkTableSource(
                        tablePath,
                        flussConfig,
                        changelogOutputType,
                        partitionKeyIndexes,
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
        return "FlussChangelogTableSource";
    }

    // TODO: Implement projection pushdown handling for metadata columns
    // TODO: Implement filter pushdown

    private boolean isPartitioned() {
        return partitionKeyIndexes.length > 0;
    }
}
