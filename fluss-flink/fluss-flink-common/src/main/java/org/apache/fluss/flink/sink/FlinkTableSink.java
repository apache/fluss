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

package org.apache.fluss.flink.sink;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.sink.writer.FlinkSinkWriter;
import org.apache.fluss.flink.utils.PushdownUtils;
import org.apache.fluss.flink.utils.PushdownUtils.FieldEqual;
import org.apache.fluss.flink.utils.PushdownUtils.ValueConversion;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.utils.PushdownUtils.extractFieldEquals;

/** A Flink {@link DynamicTableSink}. */
public class FlinkTableSink
        implements DynamicTableSink,
                SupportsPartitioning,
                SupportsDeletePushDown,
                SupportsRowLevelDelete,
                SupportsRowLevelUpdate {

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final RowType tableRowType;
    private final int[] primaryKeyIndexes;
    private final List<String> partitionKeys;
    private final boolean streaming;
    @Nullable private final MergeEngineType mergeEngineType;
    private final boolean sinkIgnoreDelete;
    private final DeleteBehavior tableDeleteBehavior;
    private final int numBucket;
    private final List<String> bucketKeys;
    private final DistributionMode distributionMode;
    private final @Nullable DataLakeFormat lakeFormat;

    private boolean appliedUpdates = false;
    @Nullable private GenericRow deleteRow;

    public FlinkTableSink(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            int[] primaryKeyIndexes,
            List<String> partitionKeys,
            boolean streaming,
            @Nullable MergeEngineType mergeEngineType,
            @Nullable DataLakeFormat lakeFormat,
            boolean sinkIgnoreDelete,
            DeleteBehavior tableDeleteBehavior,
            int numBucket,
            List<String> bucketKeys,
            DistributionMode distributionMode) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableRowType = tableRowType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.partitionKeys = partitionKeys;
        this.streaming = streaming;
        this.mergeEngineType = mergeEngineType;
        this.sinkIgnoreDelete = sinkIgnoreDelete;
        this.tableDeleteBehavior = tableDeleteBehavior;
        this.numBucket = numBucket;
        this.bucketKeys = bucketKeys;
        this.distributionMode = distributionMode;
        this.lakeFormat = lakeFormat;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (!streaming) {
            return ChangelogMode.insertOnly();
        } else {
            if (primaryKeyIndexes.length > 0 || sinkIgnoreDelete) {
                // primary-key table or ignore_delete mode can accept RowKind.DELETE
                ChangelogMode.Builder builder = ChangelogMode.newBuilder();
                for (RowKind kind : requestedMode.getContainedKinds()) {
                    // optimize out the update_before messages
                    if (kind != RowKind.UPDATE_BEFORE) {
                        builder.addContainedKind(kind);
                    }
                }
                return builder.build();
            } else {
                return ChangelogMode.insertOnly();
            }
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        int[] targetColumnIndexes = null;
        // skip applying partial-updates for UPDATE command as the Context#targetColumns
        // is not correct, see FLINK-36736
        if (!appliedUpdates
                && context.getTargetColumns().isPresent()
                // when no columns specified in insert into, the length of target columns
                // is 0, when no column specified, it's not partial update
                // see FLINK-36000
                && context.getTargetColumns().get().length != 0) {
            // is partial update, check whether partial update is supported or not
            if (context.getTargetColumns().get().length != tableRowType.getFieldCount()) {
                if (primaryKeyIndexes.length == 0) {
                    throw new ValidationException(
                            "Fluss table sink does not support partial updates for table without primary key. Please make sure the "
                                    + "number of specified columns in INSERT INTO matches columns of the Fluss table.");
                }
                if (mergeEngineType != null) {
                    throw new ValidationException(
                            String.format(
                                    "Table %s uses the '%s' merge engine which does not support partial updates. Please make sure the "
                                            + "number of specified columns in INSERT INTO matches columns of the Fluss table.",
                                    tablePath, mergeEngineType));
                }
                int[][] targetColumns = context.getTargetColumns().get();
                targetColumnIndexes = new int[targetColumns.length];
                for (int i = 0; i < targetColumns.length; i++) {
                    int[] column = targetColumns[i];
                    if (column.length != 1) {
                        throw new ValidationException(
                                "Fluss sink table doesn't support partial updates for nested columns.");
                    }
                    targetColumnIndexes[i] = column[0];
                }
                // check the target column contains the primary key columns
                for (int primaryKeyIndex : primaryKeyIndexes) {
                    if (Arrays.stream(targetColumnIndexes)
                            .noneMatch(targetColumIndex -> targetColumIndex == primaryKeyIndex)) {
                        throw new ValidationException(
                                String.format(
                                        "Fluss table sink does not support partial updates without fully specifying the primary key columns. "
                                                + "The insert columns are %s, but the primary key columns are %s. "
                                                + "Please make sure the specified columns in INSERT INTO contains "
                                                + "the primary key columns.",
                                        columns(targetColumnIndexes), columns(primaryKeyIndexes)));
                    }
                }
            }
            // else, it's full update, ignore the given target columns as we don't care the order
        }

        FlinkSink<RowData> flinkSink = getFlinkSink(targetColumnIndexes);
        // Use DataStreamSinkProvider rather than SinkV2Provider because later won't set default uid
        // for transforms added by addPreWriteTopology.
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                return flinkSink.apply(dataStream);
            }
        };
    }

    private FlinkSink<RowData> getFlinkSink(int[] targetColumnIndexes) {
        FlinkSink.SinkWriterBuilder<? extends FlinkSinkWriter, RowData> flinkSinkWriterBuilder =
                (primaryKeyIndexes.length > 0)
                        ? new FlinkSink.UpsertSinkWriterBuilder<>(
                                tablePath,
                                flussConfig,
                                tableRowType,
                                targetColumnIndexes,
                                numBucket,
                                bucketKeys,
                                partitionKeys,
                                lakeFormat,
                                distributionMode,
                                new RowDataSerializationSchema(false, sinkIgnoreDelete))
                        : new FlinkSink.AppendSinkWriterBuilder<>(
                                tablePath,
                                flussConfig,
                                tableRowType,
                                numBucket,
                                bucketKeys,
                                partitionKeys,
                                lakeFormat,
                                distributionMode,
                                new RowDataSerializationSchema(true, sinkIgnoreDelete));

        return new FlinkSink<>(flinkSinkWriterBuilder, tablePath);
    }

    private List<String> columns(int[] columnIndexes) {
        List<String> columns = new ArrayList<>();
        for (int columnIndex : columnIndexes) {
            columns.add(tableRowType.getFieldNames().get(columnIndex));
        }
        return columns;
    }

    @Override
    public DynamicTableSink copy() {
        FlinkTableSink sink =
                new FlinkTableSink(
                        tablePath,
                        flussConfig,
                        tableRowType,
                        primaryKeyIndexes,
                        partitionKeys,
                        streaming,
                        mergeEngineType,
                        lakeFormat,
                        sinkIgnoreDelete,
                        tableDeleteBehavior,
                        numBucket,
                        bucketKeys,
                        distributionMode);
        sink.appliedUpdates = appliedUpdates;
        sink.deleteRow = deleteRow;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "FlussTableSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // do nothing
    }

    @Override
    public boolean applyDeleteFilters(List<ResolvedExpression> filters) {
        validateUpdatableAndDeletable();
        if (filters.size() != primaryKeyIndexes.length) {
            // only supports delete on primary key
            return false;
        }

        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        Map<Integer, LogicalType> primaryKeyTypes = getPrimaryKeyTypes();
        List<FieldEqual> fieldEquals =
                extractFieldEquals(
                        filters,
                        primaryKeyTypes,
                        acceptedFilters,
                        remainingFilters,
                        ValueConversion.FLUSS_INTERNAL_VALUE);
        if (!remainingFilters.isEmpty()) {
            // only supports delete on primary key
            return false;
        }

        HashSet<Integer> visitedPkFields = new HashSet<>();
        GenericRow deleteRow = new GenericRow(tableRowType.getFieldCount());
        for (FieldEqual fieldEqual : fieldEquals) {
            deleteRow.setField(fieldEqual.fieldIndex, fieldEqual.equalValue);
            visitedPkFields.add(fieldEqual.fieldIndex);
        }

        // if not all primary key fields are in condition, we can't push down
        if (!visitedPkFields.equals(primaryKeyTypes.keySet())) {
            return false;
        }

        this.deleteRow = deleteRow;
        return true;
    }

    @Override
    public Optional<Long> executeDeletion() {
        if (deleteRow != null) {
            PushdownUtils.deleteSingleRow(deleteRow, tablePath, flussConfig);
            // return empty to indicate the number of deleted rows is unknown
            return Optional.empty();
        }
        throw new IllegalStateException(
                "Failed to execute DELETE statement as no deletion pushdown, this should never happen.");
    }

    @Override
    public RowLevelDeleteInfo applyRowLevelDelete(
            @Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
        throw new UnsupportedOperationException(
                "Currently, Fluss table only supports DELETE statement with conditions on primary key.");
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns,
            @Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
        validateUpdatableAndDeletable();
        Set<String> primaryKeys = getPrimaryKeyNames();
        updatedColumns.forEach(
                column -> {
                    if (primaryKeys.contains(column.getName())) {
                        String errMsg =
                                String.format(
                                        "Updates to primary keys are not supported, primaryKeys (%s), updatedColumns (%s)",
                                        primaryKeys,
                                        updatedColumns.stream()
                                                .map(Column::getName)
                                                .collect(Collectors.toList()));
                        throw new UnsupportedOperationException(errMsg);
                    }
                });

        appliedUpdates = true;
        return new RowLevelUpdateInfo() {
            @Override
            public Optional<List<Column>> requiredColumns() {
                // TODO: return primary-key columns to support partial-updates after
                //  FLINK-36735 is resolved.
                return Optional.empty();
            }

            @Override
            public RowLevelUpdateMode getRowLevelUpdateMode() {
                return RowLevelUpdateMode.UPDATED_ROWS;
            }
        };
    }

    private void validateUpdatableAndDeletable() {
        if (primaryKeyIndexes.length == 0) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Table %s is a Log Table. Log Table doesn't support DELETE and UPDATE statements.",
                            tablePath));
        }
        if (mergeEngineType != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Table %s uses the '%s' merge engine which does not support DELETE or UPDATE statements.",
                            tablePath, mergeEngineType));
        }

        // Check table-level delete behavior configuration
        if (tableDeleteBehavior == DeleteBehavior.DISABLE) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Table %s has delete behavior set to 'disable' which does not support DELETE statements.",
                            tablePath));
        }
    }

    private Map<Integer, LogicalType> getPrimaryKeyTypes() {
        Map<Integer, LogicalType> pkTypes = new HashMap<>();
        for (int index : primaryKeyIndexes) {
            pkTypes.put(index, tableRowType.getTypeAt(index));
        }
        return pkTypes;
    }

    private Set<String> getPrimaryKeyNames() {
        Set<String> pkNames = new HashSet<>();
        for (int index : primaryKeyIndexes) {
            pkNames.add(tableRowType.getFieldNames().get(index));
        }
        return pkNames;
    }

    @VisibleForTesting
    public List<String> getBucketKeys() {
        return bucketKeys;
    }
}
