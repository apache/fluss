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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.deserializer.ChangelogDeserializationSchema;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.MergeEngineType;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A Flink table source for the $changelog virtual table. */
public class ChangelogFlinkTableSource implements ScanTableSource {

    private static final String CHANGE_TYPE_COLUMN = "_change_type";
    private static final String LOG_OFFSET_COLUMN = "_log_offset";
    private static final String COMMIT_TIMESTAMP_COLUMN = "_commit_timestamp";

    private final TablePath tablePath;
    private final Configuration flussConfig;
    // The changelog output type (includes metadata columns: _change_type, _log_offset,
    // _commit_timestamp)
    private final org.apache.flink.table.types.logical.RowType changelogOutputType;
    private final org.apache.flink.table.types.logical.RowType dataColumnsType;
    private final int[] primaryKeyIndexes;
    private final int[] bucketKeyIndexes;
    private final int[] partitionKeyIndexes;
    private final boolean streaming;
    private final FlinkConnectorOptionsUtils.StartupOptions startupOptions;
    private final long scanPartitionDiscoveryIntervalMs;
    private final boolean isDataLakeEnabled;
    @Nullable private final MergeEngineType mergeEngineType;
    private final Map<String, String> tableOptions;

    // Projection pushdown
    @Nullable private int[] projectedFields;
    private LogicalType producedDataType;

    @Nullable private Predicate partitionFilters;

    /** Number of metadata columns prepended to the changelog schema. */
    private static final int NUM_METADATA_COLUMNS = 3;

    public ChangelogFlinkTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            org.apache.flink.table.types.logical.RowType changelogOutputType,
            int[] primaryKeyIndexes,
            int[] bucketKeyIndexes,
            int[] partitionKeyIndexes,
            boolean streaming,
            FlinkConnectorOptionsUtils.StartupOptions startupOptions,
            long scanPartitionDiscoveryIntervalMs,
            boolean isDataLakeEnabled,
            @Nullable MergeEngineType mergeEngineType,
            Map<String, String> tableOptions) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        // The changelogOutputType already includes metadata columns from FlinkCatalog
        this.changelogOutputType = changelogOutputType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.bucketKeyIndexes = bucketKeyIndexes;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.streaming = streaming;
        this.startupOptions = startupOptions;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.isDataLakeEnabled = isDataLakeEnabled;
        this.mergeEngineType = mergeEngineType;
        this.tableOptions = tableOptions;

        // Extract data columns by removing the first 3 metadata columns
        this.dataColumnsType = extractDataColumnsType(changelogOutputType);
        this.producedDataType = changelogOutputType;
    }

    /**
     * Extracts the data columns type by removing the metadata columns from the changelog output
     * type.
     */
    private org.apache.flink.table.types.logical.RowType extractDataColumnsType(
            org.apache.flink.table.types.logical.RowType changelogType) {
        List<org.apache.flink.table.types.logical.RowType.RowField> allFields =
                changelogType.getFields();

        // Skip the first NUM_METADATA_COLUMNS fields (metadata columns)
        List<org.apache.flink.table.types.logical.RowType.RowField> dataFields =
                allFields.subList(NUM_METADATA_COLUMNS, allFields.size());

        return new org.apache.flink.table.types.logical.RowType(new ArrayList<>(dataFields));
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
                        hasPrimaryKey(),
                        isPartitioned(),
                        flussRowType,
                        projectedFields,
                        offsetsInitializer,
                        scanPartitionDiscoveryIntervalMs,
                        new ChangelogDeserializationSchema(dataColumnsType),
                        streaming,
                        partitionFilters,
                        null); // Lake source not supported

        if (!streaming) {
            // Batch mode
            return new SourceProvider() {
                @Override
                public boolean isBounded() {
                    return true;
                }

                @Override
                public Source<RowData, ?, ?> createSource() {
                    if (!isDataLakeEnabled) {
                        throw new UnsupportedOperationException(
                                "Changelog virtual table requires data lake to be enabled for batch queries");
                    }
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
                        primaryKeyIndexes,
                        bucketKeyIndexes,
                        partitionKeyIndexes,
                        streaming,
                        startupOptions,
                        scanPartitionDiscoveryIntervalMs,
                        isDataLakeEnabled,
                        mergeEngineType,
                        tableOptions);
        copy.producedDataType = producedDataType;
        copy.projectedFields = projectedFields;
        copy.partitionFilters = partitionFilters;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "ChangelogFlinkTableSource";
    }

    // TODO: Implement projection pushdown handling for metadata columns
    // TODO: Implement filter pushdown

    private boolean hasPrimaryKey() {
        return primaryKeyIndexes.length > 0;
    }

    private boolean isPartitioned() {
        return partitionKeyIndexes.length > 0;
    }
}
