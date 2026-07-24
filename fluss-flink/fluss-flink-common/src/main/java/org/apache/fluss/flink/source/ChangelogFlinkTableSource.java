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
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.deserializer.ChangelogDeserializationSchema;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.PushdownUtils;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.PartitionPredicateVisitor;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.predicate.PredicateVisitor;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.utils.PredicateConverter.convertToFlussPredicate;

/** A Flink table source for the $changelog virtual table. */
public class ChangelogFlinkTableSource
        implements ScanTableSource, SupportsProjectionPushDown, SupportsFilterPushDown {

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
    private final int splitPerAssignmentBatchSize;
    private final Map<String, String> tableOptions;

    // Projection pushdown
    @Nullable private int[] projectedFields;
    private LogicalType producedDataType;
    // Data-column indices actually scanned from Fluss (derived from the virtual projection).
    @Nullable private int[] dataProjection;
    // Projection over the base changelog row [metadata columns + scanned data columns].
    @Nullable private int[] baseRowProjection;

    @Nullable private Predicate partitionFilters;
    // Filter over the scanned data columns pushed to the Fluss log scan (statistics-based).
    @Nullable private Predicate logRecordBatchFilter;

    // Number of leading changelog metadata columns: _change_type, _log_offset, _commit_timestamp.
    private static final int METADATA_COLUMN_COUNT = 3;

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
        this(
                tablePath,
                flussConfig,
                changelogOutputType,
                partitionKeyIndexes,
                streaming,
                startupOptions,
                scanPartitionDiscoveryIntervalMs,
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                tableOptions);
    }

    public ChangelogFlinkTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            org.apache.flink.table.types.logical.RowType changelogOutputType,
            int[] partitionKeyIndexes,
            boolean streaming,
            FlinkConnectorOptionsUtils.StartupOptions startupOptions,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            Map<String, String> tableOptions) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        // The changelogOutputType already includes metadata columns from FlinkCatalog
        this.changelogOutputType = changelogOutputType;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.streaming = streaming;
        this.startupOptions = startupOptions;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.splitPerAssignmentBatchSize = splitPerAssignmentBatchSize;
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
        if (dataProjection != null) {
            flussRowType = flussRowType.project(dataProjection);
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
                        dataProjection,
                        logRecordBatchFilter,
                        offsetsInitializer,
                        scanPartitionDiscoveryIntervalMs,
                        splitPerAssignmentBatchSize,
                        new ChangelogDeserializationSchema(baseRowProjection),
                        streaming,
                        partitionFilters,
                        LeaseContext.DEFAULT); // Lake source not supported

        return SourceProvider.of(source);
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
                        splitPerAssignmentBatchSize,
                        tableOptions);
        copy.producedDataType = producedDataType;
        copy.projectedFields = projectedFields;
        copy.dataProjection = dataProjection;
        copy.baseRowProjection = baseRowProjection;
        copy.partitionFilters = partitionFilters;
        copy.logRecordBatchFilter = logRecordBatchFilter;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "FlussChangelogTableSource";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        this.producedDataType = producedDataType.getLogicalType();

        // Derive the data-column indices actually scanned from Fluss (first-appearance order),
        // and the projection over the base changelog row [metadata columns + scanned data columns].
        List<Integer> dataColumns = new ArrayList<>();
        for (int virtualIndex : this.projectedFields) {
            if (virtualIndex >= METADATA_COLUMN_COUNT) {
                int dataIndex = virtualIndex - METADATA_COLUMN_COUNT;
                if (!dataColumns.contains(dataIndex)) {
                    dataColumns.add(dataIndex);
                }
            }
        }
        // When no data column is projected (metadata-only projection, e.g. SELECT _change_type),
        // normalize the scan projection to null so Fluss reads the full data row. A zero-column
        // scan produces no records, so the metadata columns must be emitted from a full data scan
        // and selected later during deserialization via baseRowProjection.
        this.dataProjection =
                dataColumns.isEmpty()
                        ? null
                        : dataColumns.stream().mapToInt(Integer::intValue).toArray();

        int[] base = new int[this.projectedFields.length];
        for (int i = 0; i < this.projectedFields.length; i++) {
            int virtualIndex = this.projectedFields[i];
            base[i] =
                    virtualIndex < METADATA_COLUMN_COUNT
                            ? virtualIndex
                            : METADATA_COLUMN_COUNT
                                    + dataColumns.indexOf(virtualIndex - METADATA_COLUMN_COUNT);
        }
        this.baseRowProjection = base;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        RowType dataFlussRowType = FlinkConversions.toFlussRowType(dataColumnsType);

        if (isPartitioned()) {
            // apply partition filter pushdown
            List<Predicate> converted = new ArrayList<>();
            RowType partitionRowType = dataFlussRowType.project(partitionKeyIndexes);
            PredicateVisitor<Boolean> checksOnlyPartitionKeys =
                    new PartitionPredicateVisitor(partitionRowType.getFieldNames());

            for (ResolvedExpression filter : filters) {
                Optional<Predicate> predicateOptional =
                        convertToFlussPredicate(partitionRowType, filter);
                if (predicateOptional.isPresent()) {
                    Predicate p = predicateOptional.get();
                    // partition pushdown can only guarantee to filter out partitions matching the
                    // predicate, but can't guarantee to filter out all data matching a
                    // non-partition filter in the partition
                    if (!p.visit(checksOnlyPartitionKeys)) {
                        remainingFilters.add(filter);
                    } else {
                        acceptedFilters.add(filter);
                    }
                    converted.add(p);
                } else {
                    remainingFilters.add(filter);
                }
            }
            partitionFilters = converted.isEmpty() ? null : PredicateBuilder.and(converted);
        }

        if (acceptedFilters.isEmpty() && remainingFilters.isEmpty()) {
            remainingFilters.addAll(filters);
        }

        // Data-column log filter pushdown (statistics-based). Metadata columns (_change_type,
        // _log_offset, _commit_timestamp) are resolved by name against the data columns and
        // therefore naturally fall out as non-pushable.
        Result recordBatchResult = pushdownRecordBatchFilter(remainingFilters, dataFlussRowType);
        acceptedFilters.addAll(recordBatchResult.getAcceptedFilters());

        // We cannot determine whether this source will ultimately be used as a scan source. Always
        // return all original filters as remaining so Flink applies them as a safety net.
        return Result.of(acceptedFilters, filters);
    }

    private Result pushdownRecordBatchFilter(
            List<ResolvedExpression> filters, RowType dataFlussRowType) {
        TableConfig tableConfig = new TableConfig(Configuration.fromMap(tableOptions));
        // Log filter pushdown is only supported for the ARROW log format. For INDEXED/COMPACTED
        // formats, TableScan#createLogScanner rejects a record-batch filter at runtime, so we skip
        // pushdown here and let Flink apply the filters as a post-scan safety net.
        if (tableConfig.getLogFormat() != LogFormat.ARROW) {
            this.logRecordBatchFilter = null;
            return Result.of(Collections.emptyList(), filters);
        }

        Set<String> availableStatsColumns =
                PushdownUtils.computeAvailableStatsColumns(dataFlussRowType, tableConfig);

        List<Predicate> pushdownPredicates = new ArrayList<>();
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        for (ResolvedExpression filter : filters) {
            Optional<Predicate> predicateOpt = convertToFlussPredicate(dataColumnsType, filter);
            if (predicateOpt.isPresent()
                    && PushdownUtils.canPredicateUseStatistics(
                            predicateOpt.get(), dataFlussRowType, availableStatsColumns)) {
                pushdownPredicates.add(predicateOpt.get());
                acceptedFilters.add(filter);
            }
            // All filters are kept as remaining so that Flink can still verify the results
            // after server-side filtering (safety net).
            remainingFilters.add(filter);
        }

        if (pushdownPredicates.isEmpty()) {
            this.logRecordBatchFilter = null;
        } else {
            this.logRecordBatchFilter =
                    pushdownPredicates.size() == 1
                            ? pushdownPredicates.get(0)
                            : PredicateBuilder.and(pushdownPredicates);
        }
        return Result.of(acceptedFilters, remainingFilters);
    }

    private boolean isPartitioned() {
        return partitionKeyIndexes.length > 0;
    }

    @VisibleForTesting
    @Nullable
    int[] getProjectedFields() {
        return projectedFields;
    }

    @VisibleForTesting
    @Nullable
    int[] getDataProjection() {
        return dataProjection;
    }

    @VisibleForTesting
    @Nullable
    int[] getBaseRowProjection() {
        return baseRowProjection;
    }

    @VisibleForTesting
    LogicalType getProducedDataType() {
        return producedDataType;
    }

    @VisibleForTesting
    @Nullable
    Predicate getLogRecordBatchFilter() {
        return logRecordBatchFilter;
    }

    @VisibleForTesting
    @Nullable
    Predicate getPartitionFilters() {
        return partitionFilters;
    }
}
