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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.lookup.FlinkAsyncLookupFunction;
import org.apache.fluss.flink.source.lookup.FlinkLookupFunction;
import org.apache.fluss.flink.source.lookup.LookupNormalizer;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.PushdownUtils;
import org.apache.fluss.flink.utils.PushdownUtils.FieldEqual;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.GreaterOrEqual;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.PartitionPredicateVisitor;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.predicate.PredicateVisitor;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.utils.LakeSourceUtils.createLakeSource;
import static org.apache.fluss.flink.utils.PredicateConverter.convertToFlussPredicate;
import static org.apache.fluss.flink.utils.PushdownUtils.ValueConversion.FLINK_INTERNAL_VALUE;
import static org.apache.fluss.flink.utils.PushdownUtils.extractFieldEquals;
import static org.apache.fluss.flink.utils.StringifyPredicateVisitor.stringifyPartitionPredicate;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Flink table source to scan Fluss data. */
public class FlinkTableSource
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsFilterPushDown,
                LookupTableSource,
                SupportsRowLevelModificationScan,
                SupportsLimitPushDown,
                SupportsAggregatePushDown {

    public static final Logger LOG = LoggerFactory.getLogger(FlinkTableSource.class);

    private final TablePath tablePath;
    private final Configuration flussConfig;
    // output type before projection pushdown
    private final org.apache.flink.table.types.logical.RowType tableOutputType;
    // will be empty if no primary key
    private final int[] primaryKeyIndexes;
    // will be empty if no bucket key
    private final int[] bucketKeyIndexes;
    // will be empty if no partition key
    private final int[] partitionKeyIndexes;
    private final boolean streaming;
    private final FlinkConnectorOptionsUtils.StartupOptions startupOptions;

    // options for lookup source
    private final int lookupMaxRetryTimes;
    private final boolean lookupAsync;
    @Nullable private final LookupCache cache;

    private final long scanPartitionDiscoveryIntervalMs;
    private final boolean isDataLakeEnabled;
    @Nullable private final MergeEngineType mergeEngineType;

    // table-level configuration
    private final Configuration tableConfig;

    // pre-computed available statistics columns
    private final Set<String> availableStatsColumns;

    // output type after projection pushdown
    private LogicalType producedDataType;

    // projection push down
    @Nullable private int[] projectedFields;

    @Nullable private GenericRowData singleRowFilter;

    // whether the scan is for row-level modification
    @Nullable private RowLevelModificationType modificationScanType;

    // count(*) push down
    private boolean selectRowCount = false;

    private long limit = -1;

    @Nullable private Predicate partitionFilters;

    private final Map<String, String> tableOptions;

    @Nullable private LakeSource<LakeSplit> lakeSource;
    private Predicate logRecordBatchFilter;

    public FlinkTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            Configuration tableConfig,
            org.apache.flink.table.types.logical.RowType tableOutputType,
            int[] primaryKeyIndexes,
            int[] bucketKeyIndexes,
            int[] partitionKeyIndexes,
            boolean streaming,
            FlinkConnectorOptionsUtils.StartupOptions startupOptions,
            int lookupMaxRetryTimes,
            boolean lookupAsync,
            @Nullable LookupCache cache,
            long scanPartitionDiscoveryIntervalMs,
            boolean isDataLakeEnabled,
            @Nullable MergeEngineType mergeEngineType,
            Map<String, String> tableOptions) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableOutputType = tableOutputType;
        this.producedDataType = tableOutputType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.bucketKeyIndexes = bucketKeyIndexes;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.streaming = streaming;
        this.startupOptions = checkNotNull(startupOptions, "startupOptions must not be null");

        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
        this.lookupAsync = lookupAsync;
        this.cache = cache;

        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.isDataLakeEnabled = isDataLakeEnabled;
        this.mergeEngineType = mergeEngineType;
        this.tableOptions = tableOptions;
        if (isDataLakeEnabled) {
            this.lakeSource =
                    checkNotNull(
                            createLakeSource(tablePath, tableOptions),
                            "LakeSource must not be null if enable datalake");
        }
        this.tableConfig = tableConfig;

        // Pre-compute available statistics columns to avoid repeated calculation
        RowType flussRowType = FlinkConversions.toFlussRowType(tableOutputType);
        this.availableStatsColumns = computeAvailableStatsColumns(flussRowType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!streaming) {
            return ChangelogMode.insertOnly();
        } else {
            if (hasPrimaryKey()) {
                // pk table
                if (mergeEngineType == MergeEngineType.FIRST_ROW) {
                    return ChangelogMode.insertOnly();
                } else {
                    // Check delete behavior configuration
                    Configuration tableConf = Configuration.fromMap(tableOptions);
                    DeleteBehavior deleteBehavior =
                            tableConf.get(ConfigOptions.TABLE_DELETE_BEHAVIOR);
                    if (deleteBehavior == DeleteBehavior.ALLOW) {
                        return ChangelogMode.all();
                    } else {
                        // If delete operations are ignored or disabled, only insert and update are
                        // relevant
                        return ChangelogMode.newBuilder()
                                .addContainedKind(RowKind.INSERT)
                                .addContainedKind(RowKind.UPDATE_BEFORE)
                                .addContainedKind(RowKind.UPDATE_AFTER)
                                .build();
                    }
                }
            } else {
                // append only
                return ChangelogMode.insertOnly();
            }
        }
    }

    private boolean hasPrimaryKey() {
        return primaryKeyIndexes.length > 0;
    }

    private boolean isPartitioned() {
        return partitionKeyIndexes.length > 0;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // handle single row filter scan
        if (singleRowFilter != null || limit > 0 || selectRowCount) {
            Collection<RowData> results;
            if (singleRowFilter != null) {
                results =
                        PushdownUtils.querySingleRow(
                                singleRowFilter,
                                tablePath,
                                flussConfig,
                                tableOutputType,
                                primaryKeyIndexes,
                                lookupMaxRetryTimes,
                                projectedFields);
            } else if (limit > 0) {
                results =
                        PushdownUtils.limitScan(
                                tablePath, flussConfig, tableOutputType, projectedFields, limit);
            } else {
                results =
                        Collections.singleton(
                                GenericRowData.of(
                                        PushdownUtils.countLogTable(tablePath, flussConfig)));
            }

            TypeInformation<RowData> resultTypeInfo =
                    scanContext.createTypeInformation(producedDataType);
            return new DataStreamScanProvider() {
                @Override
                public DataStream<RowData> produceDataStream(
                        ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                    return execEnv.fromCollection(results, resultTypeInfo);
                }

                @Override
                public boolean isBounded() {
                    return true;
                }
            };
        }

        // handle normal scan
        RowType flussRowType = FlinkConversions.toFlussRowType(tableOutputType);
        if (projectedFields != null) {
            flussRowType = flussRowType.project(projectedFields);
        }
        OffsetsInitializer offsetsInitializer;
        boolean enableLakeSource = lakeSource != null;
        switch (startupOptions.startupMode) {
            case EARLIEST:
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case LATEST:
                offsetsInitializer = OffsetsInitializer.latest();
                // since it's scan from latest, don't consider lake data
                enableLakeSource = false;
                break;
            case FULL:
                offsetsInitializer = OffsetsInitializer.full();
                break;
            case TIMESTAMP:
                offsetsInitializer =
                        OffsetsInitializer.timestamp(startupOptions.startupTimestampMs);
                if (hasPrimaryKey()) {
                    // Currently, for primary key tables, we do not consider lake data
                    // when reading from a given timestamp. This is because we will need
                    // to read the change log of primary key table.
                    // TODO: consider support it using paimon change log data?
                    enableLakeSource = false;
                } else {
                    if (enableLakeSource) {
                        enableLakeSource =
                                pushTimeStampFilterToLakeSource(lakeSource, flussRowType);
                    }
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported startup mode: " + startupOptions.startupMode);
        }

        FlinkSource<RowData> source =
                new FlinkSource<>(
                        flussConfig,
                        tablePath,
                        hasPrimaryKey(),
                        isPartitioned(),
                        flussRowType,
                        projectedFields,
                        logRecordBatchFilter,
                        offsetsInitializer,
                        scanPartitionDiscoveryIntervalMs,
                        new RowDataDeserializationSchema(),
                        streaming,
                        partitionFilters,
                        enableLakeSource ? lakeSource : null);

        if (!streaming) {
            // return a bounded source provide to make planner happy,
            // but this should throw exception when used to create source
            return new SourceProvider() {
                @Override
                public boolean isBounded() {
                    return true;
                }

                @Override
                public Source<RowData, ?, ?> createSource() {
                    if (modificationScanType != null) {
                        throw new UnsupportedOperationException(
                                "Currently, Fluss table only supports "
                                        + modificationScanType
                                        + " statement with conditions on primary key.");
                    }
                    if (!isDataLakeEnabled) {
                        throw new UnsupportedOperationException(
                                "Currently, Fluss only support queries on table with datalake enabled or point queries on primary key when it's in batch execution mode.");
                    }
                    return source;
                }
            };
        } else {
            return SourceProvider.of(source);
        }
    }

    private boolean pushTimeStampFilterToLakeSource(
            LakeSource<?> lakeSource, RowType flussRowType) {
        // will push timestamp to lake
        // we will have three additional system columns, __bucket, __offset, __timestamp
        // in lake, get the  __timestamp index in lake table
        final int timestampFieldIndex = flussRowType.getFieldCount() + 2;
        Predicate timestampFilter =
                new LeafPredicate(
                        GreaterOrEqual.INSTANCE,
                        DataTypes.TIMESTAMP_LTZ(),
                        timestampFieldIndex,
                        TIMESTAMP_COLUMN_NAME,
                        Collections.singletonList(
                                TimestampLtz.fromEpochMillis(startupOptions.startupTimestampMs)));
        List<Predicate> acceptedPredicates =
                lakeSource
                        .withFilters(Collections.singletonList(timestampFilter))
                        .acceptedPredicates();
        if (acceptedPredicates.isEmpty()) {
            LOG.warn(
                    "The lake source doesn't accept the filter {}, won't read data from lake.",
                    timestampFilter);
            return false;
        }
        checkState(
                acceptedPredicates.size() == 1
                        && acceptedPredicates.get(0).equals(timestampFilter));
        return true;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        LookupNormalizer lookupNormalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        context.getKeys(),
                        primaryKeyIndexes,
                        bucketKeyIndexes,
                        partitionKeyIndexes,
                        tableOutputType,
                        projectedFields);
        if (lookupAsync) {
            AsyncLookupFunction asyncLookupFunction =
                    new FlinkAsyncLookupFunction(
                            flussConfig,
                            tablePath,
                            tableOutputType,
                            lookupMaxRetryTimes,
                            lookupNormalizer,
                            projectedFields);
            if (cache != null) {
                return PartialCachingAsyncLookupProvider.of(asyncLookupFunction, cache);
            } else {
                return AsyncLookupFunctionProvider.of(asyncLookupFunction);
            }
        } else {
            LookupFunction lookupFunction =
                    new FlinkLookupFunction(
                            flussConfig,
                            tablePath,
                            tableOutputType,
                            lookupMaxRetryTimes,
                            lookupNormalizer,
                            projectedFields);
            if (cache != null) {
                return PartialCachingLookupProvider.of(lookupFunction, cache);
            } else {
                return LookupFunctionProvider.of(lookupFunction);
            }
        }
    }

    @Override
    public DynamicTableSource copy() {
        FlinkTableSource source =
                new FlinkTableSource(
                        tablePath,
                        flussConfig,
                        tableConfig,
                        tableOutputType,
                        primaryKeyIndexes,
                        bucketKeyIndexes,
                        partitionKeyIndexes,
                        streaming,
                        startupOptions,
                        lookupMaxRetryTimes,
                        lookupAsync,
                        cache,
                        scanPartitionDiscoveryIntervalMs,
                        isDataLakeEnabled,
                        mergeEngineType,
                        tableOptions);
        source.producedDataType = producedDataType;
        source.projectedFields = projectedFields;
        source.singleRowFilter = singleRowFilter;
        source.modificationScanType = modificationScanType;
        source.partitionFilters = partitionFilters;
        source.lakeSource = lakeSource;
        source.logRecordBatchFilter = logRecordBatchFilter;
        // Note: availableStatsColumns is already computed in the constructor
        return source;
    }

    @Override
    public String asSummaryString() {
        return "FlussTableSource";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        this.producedDataType = producedDataType.getLogicalType();
        if (lakeSource != null) {
            lakeSource.withProject(projectedFields);
        }
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {

        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        // primary pushdown
        // (1) batch execution mode,
        // (2) default (full) startup mode,
        // (3) the table is a pk table,
        // (4) all filters are pk field equal expression
        if (!streaming
                && startupOptions.startupMode == FlinkConnectorOptions.ScanStartupMode.FULL
                && hasPrimaryKey()
                && filters.size() == primaryKeyIndexes.length) {

            Map<Integer, LogicalType> primaryKeyTypes = getPrimaryKeyTypes();
            List<FieldEqual> fieldEquals =
                    extractFieldEquals(
                            filters,
                            primaryKeyTypes,
                            acceptedFilters,
                            remainingFilters,
                            FLINK_INTERNAL_VALUE);
            int[] keyRowProjection = getKeyRowProjection();
            HashSet<Integer> visitedPkFields = new HashSet<>();
            GenericRowData lookupRow = new GenericRowData(primaryKeyIndexes.length);
            for (FieldEqual fieldEqual : fieldEquals) {
                lookupRow.setField(keyRowProjection[fieldEqual.fieldIndex], fieldEqual.equalValue);
                visitedPkFields.add(fieldEqual.fieldIndex);
            }

            // if not all primary key fields are in condition, we skip to pushdown
            if (!visitedPkFields.equals(primaryKeyTypes.keySet())) {
                return Result.of(Collections.emptyList(), filters);
            }
            singleRowFilter = lookupRow;
            return Result.of(acceptedFilters, remainingFilters);
        }

        if (isPartitioned()) {
            // apply partition filter pushdown
            List<Predicate> converted = new ArrayList<>();

            RowType partitionRowType =
                    FlinkConversions.toFlussRowType(tableOutputType).project(partitionKeyIndexes);
            PredicateVisitor<Boolean> checksOnlyPartitionKeys =
                    new PartitionPredicateVisitor(partitionRowType.getFieldNames());

            for (ResolvedExpression filter : filters) {

                Optional<Predicate> predicateOptional =
                        convertToFlussPredicate(partitionRowType, filter);

                if (predicateOptional.isPresent()) {
                    Predicate p = predicateOptional.get();
                    // partition pushdown can only guarantee to filter out partitions matches the
                    // predicate, but can't guarantee to filter out all data matches to
                    // non-partition filter in the partition
                    if (!p.visit(checksOnlyPartitionKeys)) {
                        remainingFilters.add(filter);
                    } else {
                        acceptedFilters.add(filter);
                    }
                    // Convert literals in the predicate to partition string
                    converted.add(stringifyPartitionPredicate(p));
                } else {
                    remainingFilters.add(filter);
                }
            }
            partitionFilters = converted.isEmpty() ? null : PredicateBuilder.and(converted);

            // lake source is not null
            if (lakeSource != null) {
                List<Predicate> lakePredicates = new ArrayList<>();
                for (ResolvedExpression filter : filters) {
                    Optional<Predicate> predicateOptional =
                            convertToFlussPredicate(tableOutputType, filter);
                    predicateOptional.ifPresent(lakePredicates::add);
                }

                if (!lakePredicates.isEmpty()) {
                    final LakeSource.FilterPushDownResult filterPushDownResult =
                            lakeSource.withFilters(lakePredicates);
                    if (filterPushDownResult.acceptedPredicates().size() != lakePredicates.size()) {
                        LOG.info(
                                "LakeSource rejected some partition filters. Falling back to Flink-side filtering.");
                        // Flink will apply all filters to preserve correctness
                        return Result.of(Collections.emptyList(), filters);
                    }
                }
            }
        }

        if (acceptedFilters.isEmpty() && remainingFilters.isEmpty()) {
            remainingFilters.addAll(filters);
        }

        if (!hasPrimaryKey()) {
            Result recordBatchResult = pushdownRecordBatchFilter(remainingFilters);
            acceptedFilters.addAll(recordBatchResult.getAcceptedFilters());
            remainingFilters = recordBatchResult.getRemainingFilters();
        }
        return Result.of(acceptedFilters, remainingFilters);
    }

    private Result pushdownRecordBatchFilter(List<ResolvedExpression> filters) {
        // Use pre-computed available statistics columns
        LOG.trace("Statistics available columns: {}", availableStatsColumns);

        // Convert to fluss row type for predicate operations
        RowType flussRowType = FlinkConversions.toFlussRowType(tableOutputType);

        List<Predicate> pushdownPredicates = new ArrayList<>();
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        for (ResolvedExpression filter : filters) {
            java.util.Optional<Predicate> predicateOpt =
                    PredicateConverter.convert(tableOutputType, filter);

            if (predicateOpt.isPresent()) {
                Predicate predicate = predicateOpt.get();
                LOG.trace("Converted filter to predicate: {}", predicate);
                // Check if predicate can benefit from statistics
                if (canPredicateUseStatistics(predicate, flussRowType, availableStatsColumns)) {
                    pushdownPredicates.add(predicate);
                    acceptedFilters.add(filter);
                }
            }
            remainingFilters.add(filter);
        }

        if (!pushdownPredicates.isEmpty()) {
            Predicate merged =
                    pushdownPredicates.size() == 1
                            ? pushdownPredicates.get(0)
                            : org.apache.fluss.predicate.PredicateBuilder.and(pushdownPredicates);
            LOG.info("Accept merged predicate for record batch filter: {}", merged);
            this.logRecordBatchFilter = merged;
        } else {
            this.logRecordBatchFilter = null;
        }
        return Result.of(acceptedFilters, remainingFilters);
    }

    /**
     * Checks if a data type is binary.
     *
     * @param dataType the data type to check
     * @return true if it's a binary type
     */
    private boolean isBinaryType(org.apache.fluss.types.DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BINARY:
            case BYTES:
                return true;
            default:
                return false;
        }
    }

    /**
     * Checks if a predicate can benefit from statistics based on the available statistics columns.
     *
     * @param predicate the predicate to check
     * @param rowType the row type
     * @param availableStatsColumns the columns that have statistics available
     * @return true if the predicate can use statistics
     */
    private boolean canPredicateUseStatistics(
            Predicate predicate, RowType rowType, Set<String> availableStatsColumns) {

        class StatisticsUsageVisitor implements PredicateVisitor<Boolean> {
            @Override
            public Boolean visit(org.apache.fluss.predicate.LeafPredicate leaf) {
                // Check if the field referenced by this predicate has statistics available
                String fieldName = rowType.getFieldNames().get(leaf.index());
                // Check if statistics are available for this column
                return availableStatsColumns.contains(fieldName);
            }

            @Override
            public Boolean visit(CompoundPredicate compound) {
                // For compound predicates, all children must be able to use statistics
                for (Predicate child : compound.children()) {
                    if (!child.visit(this)) {
                        return false;
                    }
                }
                return true;
            }
        }

        return predicate.visit(new StatisticsUsageVisitor());
    }

    /**
     * Computes the available statistics columns based on table configuration. This method is called
     * once during construction to pre-compute the result.
     *
     * @param flussRowType the row type
     * @return set of column names that have statistics available
     */
    private Set<String> computeAvailableStatsColumns(RowType flussRowType) {
        Set<String> availableStatsColumns = new HashSet<>();

        // Get the configured statistics columns
        String columnsConfig = tableConfig.get(ConfigOptions.TABLE_STATISTICS_COLUMNS);

        // Check if statistics are enabled for the table
        if (null == columnsConfig || columnsConfig.isEmpty()) {
            LOG.debug("Statistics collection is disabled for the table");
            return availableStatsColumns;
        }

        if ("*".equals(columnsConfig)) {
            // Collect all non-binary columns
            for (int i = 0; i < flussRowType.getFieldCount(); i++) {
                org.apache.fluss.types.DataType fieldType = flussRowType.getTypeAt(i);
                if (!isBinaryType(fieldType)) {
                    availableStatsColumns.add(flussRowType.getFieldNames().get(i));
                }
            }
        } else {
            // Use user-specified columns (validate they exist and are non-binary)
            List<String> configuredColumns =
                    Arrays.stream(columnsConfig.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());

            for (String columnName : configuredColumns) {
                // Find the column in the row type
                int columnIndex = flussRowType.getFieldNames().indexOf(columnName);
                if (columnIndex >= 0) {
                    org.apache.fluss.types.DataType fieldType = flussRowType.getTypeAt(columnIndex);
                    if (!isBinaryType(fieldType)) {
                        availableStatsColumns.add(columnName);
                    } else {
                        LOG.trace(
                                "Configured statistics column '{}' is a binary type and will be ignored",
                                columnName);
                    }
                } else {
                    LOG.trace(
                            "Configured statistics column '{}' does not exist in table schema",
                            columnName);
                }
            }
        }

        return availableStatsColumns;
    }

    @Override
    public RowLevelModificationScanContext applyRowLevelModificationScan(
            RowLevelModificationType rowLevelModificationType,
            @Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
        modificationScanType = rowLevelModificationType;
        return null;
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean applyAggregates(
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            DataType dataType) {
        // Only supports 'select count(*)/count(1) from source' for log table now.
        if (streaming
                || aggregateExpressions.size() != 1
                || hasPrimaryKey()
                || groupingSets.size() > 1
                || (groupingSets.size() == 1 && groupingSets.get(0).length > 0)
                // The count pushdown feature is not supported when the data lake is enabled.
                // Otherwise, it'll cause miss count data in lake. But In the future, we can push
                // down count into lake.
                || isDataLakeEnabled) {
            return false;
        }

        FunctionDefinition functionDefinition = aggregateExpressions.get(0).getFunctionDefinition();
        if (!(functionDefinition
                        .getClass()
                        .getCanonicalName()
                        .equals(
                                "org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction")
                || functionDefinition
                        .getClass()
                        .getCanonicalName()
                        .equals(
                                "org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction"))) {
            return false;
        }
        selectRowCount = true;
        this.producedDataType = dataType.getLogicalType();
        return true;
    }

    private Map<Integer, LogicalType> getPrimaryKeyTypes() {
        Map<Integer, LogicalType> pkTypes = new HashMap<>();
        for (int index : primaryKeyIndexes) {
            pkTypes.put(index, tableOutputType.getTypeAt(index));
        }
        return pkTypes;
    }

    // projection from pk_field_index to index_in_pk
    private int[] getKeyRowProjection() {
        int[] projection = new int[tableOutputType.getFieldCount()];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            projection[primaryKeyIndexes[i]] = i;
        }
        return projection;
    }

    @VisibleForTesting
    @Nullable
    public LookupCache getCache() {
        return cache;
    }

    @VisibleForTesting
    public int[] getPrimaryKeyIndexes() {
        return primaryKeyIndexes;
    }

    @VisibleForTesting
    public int[] getBucketKeyIndexes() {
        return bucketKeyIndexes;
    }

    @VisibleForTesting
    public int[] getPartitionKeyIndexes() {
        return partitionKeyIndexes;
    }

    @VisibleForTesting
    @Nullable
    public Predicate getLogRecordBatchFilter() {
        return logRecordBatchFilter;
    }

    @VisibleForTesting
    @Nullable
    public GenericRowData getSingleRowFilter() {
        return singleRowFilter;
    }

    @VisibleForTesting
    @Nullable
    public Predicate getPartitionFilters() {
        return partitionFilters;
    }
}
