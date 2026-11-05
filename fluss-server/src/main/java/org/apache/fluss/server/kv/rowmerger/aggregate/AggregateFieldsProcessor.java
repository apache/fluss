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

package org.apache.fluss.server.kv.rowmerger.aggregate;

import org.apache.fluss.exception.InvalidRecordException;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldAggregator;

import javax.annotation.Nullable;

import java.util.BitSet;
import java.util.List;

/**
 * Utility class for processing aggregated fields from old and new rows.
 *
 * <p>This class provides reusable methods for aggregating fields with different strategies (full
 * aggregation or partial aggregation with target columns). It handles schema evolution by matching
 * fields using column IDs.
 *
 * <p>Note: This class processes aggregation logic and delegates encoding to {@link RowEncoder}. The
 * class is designed to be stateless and thread-safe, with all methods being static.
 */
public final class AggregateFieldsProcessor {

    // A FieldGetter that always returns null, used for non-existent columns in schema evolution
    private static final InternalRow.FieldGetter NULL_FIELD_GETTER = row -> null;

    /** Functional interface for field-level operations (agg or retract). */
    @FunctionalInterface
    private interface FieldOperation {
        Object apply(FieldAggregator aggregator, Object accumulator, Object inputField);
    }

    // Allows processAllFields/processTargetFields to be reused for both aggregate and retract
    // paths without branching — the caller simply passes the appropriate operation.
    private static final FieldOperation AGGREGATE_OP = FieldAggregator::agg;

    /** Field operation: applies aggregator.retract(accumulator, input). */
    private static final FieldOperation RETRACT_OP = FieldAggregator::retract;

    // Private constructor to prevent instantiation
    private AggregateFieldsProcessor() {}

    // ======================== Public aggregate methods ========================

    /**
     * Aggregate all fields from old and new rows with explicit target schema (full aggregation).
     * Handles schema evolution by matching fields using column IDs across three potentially
     * different schemas.
     */
    public static void aggregateAllFieldsWithTargetSchema(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext oldContext,
            AggregationContext newInputContext,
            AggregationContext targetContext,
            RowEncoder encoder) {
        processAllFieldsWithTargetSchema(
                oldRow,
                newRow,
                oldContext,
                newInputContext,
                targetContext,
                encoder,
                AGGREGATE_OP,
                false);
    }

    /**
     * Encodes the first row written for a key, passing the fields whose aggregator requires it
     * through {@code agg(null, value)}.
     *
     * <p>For partial updates, non-target columns are set to null without invoking their
     * aggregators.
     *
     * @param row the first row written for the key
     * @param context context for the row schema
     * @param targetPosBitSet target column positions in the input schema, or null for a full-row
     *     write
     * @param encoder the row encoder to encode results (should match context)
     */
    public static void aggregateFirstRow(
            BinaryRow row,
            AggregationContext context,
            @Nullable BitSet targetPosBitSet,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();
        FieldAggregator[] aggregators = context.getAggregators();
        for (int idx = 0; idx < context.getFieldCount(); idx++) {
            if (targetPosBitSet != null && !targetPosBitSet.get(idx)) {
                encoder.encodeField(idx, null);
                continue;
            }
            Object field = fieldGetters[idx].getFieldOrNull(row);
            FieldAggregator aggregator = aggregators[idx];
            encoder.encodeField(
                    idx,
                    aggregator.requiresAggOnFirstWrite() ? aggregator.agg(null, field) : field);
        }
    }

    /**
     * Aggregate target fields from old and new rows with explicit target schema (partial
     * aggregation).
     *
     * <p>This variant allows specifying a different target schema for the output, which is useful
     * when the client's newValue uses an outdated schema but we want to output using the latest
     * server schema.
     *
     * <p>For target columns, aggregate with the aggregation function. For non-target columns, keep
     * the old value unchanged. For columns that don't exist in old schema, copy from newRow. For
     * columns that exist only in target schema, set to null.
     *
     * @param oldRow the old row
     * @param newRow the new row
     * @param oldContext context for the old row schema
     * @param newInputContext context for the new row schema (for reading newRow)
     * @param targetContext context for the target output schema
     * @param targetColumnIdBitSet BitSet marking target columns by column ID
     * @param encoder the row encoder to encode results (should match targetContext)
     */
    public static void aggregateTargetFieldsWithTargetSchema(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext oldContext,
            AggregationContext newInputContext,
            AggregationContext targetContext,
            BitSet targetColumnIdBitSet,
            RowEncoder encoder) {
        processTargetFieldsWithTargetSchema(
                oldRow,
                newRow,
                oldContext,
                newInputContext,
                targetContext,
                targetColumnIdBitSet,
                encoder,
                AGGREGATE_OP,
                false);
    }

    // ======================== Public retract methods ========================

    /**
     * Retract all fields with explicit target schema (full retract). Primary key columns are never
     * retracted; their old values are copied directly.
     */
    public static void retractAllFieldsWithTargetSchema(
            BinaryRow oldRow,
            BinaryRow retractRow,
            AggregationContext oldContext,
            AggregationContext retractInputContext,
            AggregationContext targetContext,
            RowEncoder encoder) {
        processAllFieldsWithTargetSchema(
                oldRow,
                retractRow,
                oldContext,
                retractInputContext,
                targetContext,
                encoder,
                RETRACT_OP,
                true);
    }

    /**
     * Retract target fields with explicit target schema (partial retract). For target columns
     * (excluding primary keys), retract with the aggregation function. For non-target columns and
     * primary key columns, keep the old value unchanged.
     */
    public static void retractTargetFieldsWithTargetSchema(
            BinaryRow oldRow,
            BinaryRow retractRow,
            AggregationContext oldContext,
            AggregationContext retractInputContext,
            AggregationContext targetContext,
            BitSet targetColumnIdBitSet,
            RowEncoder encoder) {
        processTargetFieldsWithTargetSchema(
                oldRow,
                retractRow,
                oldContext,
                retractInputContext,
                targetContext,
                targetColumnIdBitSet,
                encoder,
                RETRACT_OP,
                true);
    }

    // ======================== Public delete methods ========================

    /**
     * Encode a partial delete when schemas are identical. Set target columns (except primary key)
     * to null, keep other columns unchanged.
     */
    public static void encodePartialDeleteWithSameSchema(
            BinaryRow oldRow,
            AggregationContext context,
            BitSet targetPosBitSet,
            BitSet pkPosBitSet,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();

        for (int i = 0; i < oldRow.getFieldCount(); i++) {
            if (targetPosBitSet.get(i) && !pkPosBitSet.get(i)) {
                // Target column (not primary key): set to null
                encoder.encodeField(i, null);
            } else {
                // Non-target column or primary key: keep old value
                copyOldValueAndEncode(fieldGetters[i], oldRow, i, encoder);
            }
        }
    }

    /**
     * Encode a partial delete with schema evolution. Set target columns (except primary key) to
     * null, keep other columns unchanged.
     */
    public static void encodePartialDeleteWithDifferentSchema(
            BinaryRow oldRow,
            AggregationContext oldContext,
            AggregationContext targetContext,
            BitSet targetColumnIdBitSet,
            BitSet pkPosBitSet,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] oldFieldGetters = oldContext.getFieldGetters();
        List<Schema.Column> targetColumns = targetContext.getSchema().getColumns();

        for (int targetIdx = 0; targetIdx < targetColumns.size(); targetIdx++) {
            Schema.Column targetColumn = targetColumns.get(targetIdx);
            int columnId = targetColumn.getColumnId();

            // Find corresponding field in old schema using column ID
            Integer oldIdx = oldContext.getFieldIndex(columnId);

            // Check if this is a target column using columnId (not position)
            boolean isTargetColumn = targetColumnIdBitSet.get(columnId);
            boolean isPrimaryKey = pkPosBitSet.get(targetIdx);

            if (isTargetColumn && !isPrimaryKey) {
                // Target column (not primary key): set to null
                encoder.encodeField(targetIdx, null);
            } else if (oldIdx != null) {
                // Column exists in old schema: copy value from old row
                copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, targetIdx, encoder);
            } else {
                // New column that doesn't exist in old schema: set to null
                encoder.encodeField(targetIdx, null);
            }
        }
    }

    // ======================== Public utility methods ========================

    /** Check if there are any non-null fields in non-target columns. */
    public static boolean hasNonTargetNonNullField(BinaryRow row, BitSet targetPosBitSet) {
        int fieldCount = row.getFieldCount();
        // Use nextClearBit to iterate over non-target fields (bits not set in targetPosBitSet)
        for (int pos = targetPosBitSet.nextClearBit(0);
                pos < fieldCount;
                pos = targetPosBitSet.nextClearBit(pos + 1)) {
            if (!row.isNullAt(pos)) {
                return true;
            }
        }
        return false;
    }

    // ======================== Unified internal methods ========================

    /**
     * Process all fields with explicit target schema. Unified implementation for both aggregate and
     * retract operations.
     *
     * @param skipPrimaryKeys if true, primary key columns are copied directly (retract mode)
     */
    private static void processAllFieldsWithTargetSchema(
            BinaryRow oldRow,
            BinaryRow inputRow,
            AggregationContext oldContext,
            AggregationContext inputContext,
            AggregationContext targetContext,
            RowEncoder encoder,
            FieldOperation fieldOp,
            boolean skipPrimaryKeys) {
        // Fast path: all three schemas are the same
        if (targetContext == oldContext && targetContext == inputContext) {
            processAllFieldsWithSameSchema(
                    oldRow, inputRow, targetContext, encoder, fieldOp, skipPrimaryKeys);
            return;
        }

        // General path: iterate over target schema columns
        InternalRow.FieldGetter[] oldFieldGetters = oldContext.getFieldGetters();
        InternalRow.FieldGetter[] inputFieldGetters = inputContext.getFieldGetters();
        FieldAggregator[] targetAggregators = targetContext.getAggregators();
        List<Schema.Column> targetColumns = targetContext.getSchema().getColumns();
        BitSet pkBitSet = skipPrimaryKeys ? targetContext.getPrimaryKeyColsBitSet() : null;

        for (int targetIdx = 0; targetIdx < targetColumns.size(); targetIdx++) {
            Schema.Column targetColumn = targetColumns.get(targetIdx);
            int columnId = targetColumn.getColumnId();

            Integer oldIdx = oldContext.getFieldIndex(columnId);

            // Primary key columns: copy old value directly when in retract mode
            if (pkBitSet != null && pkBitSet.get(targetIdx)) {
                if (oldIdx != null) {
                    copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, targetIdx, encoder);
                } else {
                    encoder.encodeField(targetIdx, null);
                }
                continue;
            }

            Integer inputIdx = inputContext.getFieldIndex(columnId);

            // An older schema does not retract fields it cannot represent. Absence must not
            // become an explicit null retract or invoke a newly added unsupported function.
            if (skipPrimaryKeys && inputIdx == null) {
                if (oldIdx != null) {
                    copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, targetIdx, encoder);
                } else {
                    encoder.encodeField(targetIdx, null);
                }
                continue;
            }

            // Get field getters (use NULL_FIELD_GETTER if column doesn't exist in that schema)
            InternalRow.FieldGetter oldFieldGetter =
                    (oldIdx != null) ? oldFieldGetters[oldIdx] : NULL_FIELD_GETTER;
            InternalRow.FieldGetter inputFieldGetter =
                    (inputIdx != null) ? inputFieldGetters[inputIdx] : NULL_FIELD_GETTER;

            applyAndEncode(
                    oldFieldGetter,
                    inputFieldGetter,
                    oldRow,
                    inputRow,
                    targetAggregators[targetIdx],
                    targetIdx,
                    encoder,
                    fieldOp,
                    targetContext);
        }
    }

    /**
     * Process all fields when old and input schemas are identical. Fast path: field positions match
     * directly, no column ID lookup needed.
     */
    private static void processAllFieldsWithSameSchema(
            BinaryRow oldRow,
            BinaryRow inputRow,
            AggregationContext context,
            RowEncoder encoder,
            FieldOperation fieldOp,
            boolean skipPrimaryKeys) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();
        FieldAggregator[] aggregators = context.getAggregators();
        int fieldCount = context.getFieldCount();
        BitSet pkBitSet = skipPrimaryKeys ? context.getPrimaryKeyColsBitSet() : null;

        for (int idx = 0; idx < fieldCount; idx++) {
            if (pkBitSet != null && pkBitSet.get(idx)) {
                // Primary key columns: copy old value directly
                copyOldValueAndEncode(fieldGetters[idx], oldRow, idx, encoder);
            } else {
                applyAndEncode(
                        fieldGetters[idx],
                        fieldGetters[idx],
                        oldRow,
                        inputRow,
                        aggregators[idx],
                        idx,
                        encoder,
                        fieldOp,
                        context);
            }
        }
    }

    /**
     * Process target fields with explicit target schema. Unified implementation for both aggregate
     * and retract partial operations.
     *
     * @param skipPrimaryKeys if true, primary key columns are always copied (retract mode)
     */
    private static void processTargetFieldsWithTargetSchema(
            BinaryRow oldRow,
            BinaryRow inputRow,
            AggregationContext oldContext,
            AggregationContext inputContext,
            AggregationContext targetContext,
            BitSet targetColumnIdBitSet,
            RowEncoder encoder,
            FieldOperation fieldOp,
            boolean skipPrimaryKeys) {
        // Fast path: all three schemas are the same
        if (targetContext == oldContext && targetContext == inputContext) {
            processTargetFieldsWithSameSchema(
                    oldRow,
                    inputRow,
                    targetContext,
                    targetColumnIdBitSet,
                    encoder,
                    fieldOp,
                    skipPrimaryKeys);
            return;
        }

        // General path: iterate over target schema columns
        InternalRow.FieldGetter[] oldFieldGetters = oldContext.getFieldGetters();
        InternalRow.FieldGetter[] inputFieldGetters = inputContext.getFieldGetters();
        FieldAggregator[] targetAggregators = targetContext.getAggregators();
        List<Schema.Column> targetColumns = targetContext.getSchema().getColumns();
        BitSet pkBitSet = skipPrimaryKeys ? targetContext.getPrimaryKeyColsBitSet() : null;

        for (int targetIdx = 0; targetIdx < targetColumns.size(); targetIdx++) {
            Schema.Column targetColumn = targetColumns.get(targetIdx);
            int columnId = targetColumn.getColumnId();

            Integer oldIdx = oldContext.getFieldIndex(columnId);

            // Primary key columns: always copy old value when in retract mode
            if (pkBitSet != null && pkBitSet.get(targetIdx)) {
                if (oldIdx != null) {
                    copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, targetIdx, encoder);
                } else {
                    encoder.encodeField(targetIdx, null);
                }
                continue;
            }

            if (targetColumnIdBitSet.get(columnId)) {
                Integer inputIdx = inputContext.getFieldIndex(columnId);
                if (skipPrimaryKeys && inputIdx == null) {
                    if (oldIdx != null) {
                        copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, targetIdx, encoder);
                    } else {
                        encoder.encodeField(targetIdx, null);
                    }
                    continue;
                }
                InternalRow.FieldGetter oldFieldGetter =
                        (oldIdx != null) ? oldFieldGetters[oldIdx] : NULL_FIELD_GETTER;
                InternalRow.FieldGetter inputFieldGetter =
                        (inputIdx != null) ? inputFieldGetters[inputIdx] : NULL_FIELD_GETTER;
                applyAndEncode(
                        oldFieldGetter,
                        inputFieldGetter,
                        oldRow,
                        inputRow,
                        targetAggregators[targetIdx],
                        targetIdx,
                        encoder,
                        fieldOp,
                        targetContext);
            } else if (oldIdx != null) {
                copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, targetIdx, encoder);
            } else {
                encoder.encodeField(targetIdx, null);
            }
        }
    }

    /**
     * Process target fields when old and input schemas are identical. Fast path: field positions
     * match directly, no column ID lookup needed.
     */
    private static void processTargetFieldsWithSameSchema(
            BinaryRow oldRow,
            BinaryRow inputRow,
            AggregationContext context,
            BitSet targetColumnIdBitSet,
            RowEncoder encoder,
            FieldOperation fieldOp,
            boolean skipPrimaryKeys) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();
        FieldAggregator[] aggregators = context.getAggregators();
        List<Schema.Column> columns = context.getSchema().getColumns();
        int fieldCount = context.getFieldCount();
        BitSet pkBitSet = skipPrimaryKeys ? context.getPrimaryKeyColsBitSet() : null;

        for (int idx = 0; idx < fieldCount; idx++) {
            // Primary key columns: always copy old value when in retract mode
            if (pkBitSet != null && pkBitSet.get(idx)) {
                copyOldValueAndEncode(fieldGetters[idx], oldRow, idx, encoder);
                continue;
            }

            int columnId = columns.get(idx).getColumnId();

            if (targetColumnIdBitSet.get(columnId)) {
                applyAndEncode(
                        fieldGetters[idx],
                        fieldGetters[idx],
                        oldRow,
                        inputRow,
                        aggregators[idx],
                        idx,
                        encoder,
                        fieldOp,
                        context);
            } else {
                copyOldValueAndEncode(fieldGetters[idx], oldRow, idx, encoder);
            }
        }
    }

    /** Apply a field operation (agg or retract) and encode the result. */
    private static void applyAndEncode(
            InternalRow.FieldGetter oldFieldGetter,
            InternalRow.FieldGetter inputFieldGetter,
            BinaryRow oldRow,
            BinaryRow inputRow,
            FieldAggregator aggregator,
            int targetIdx,
            RowEncoder encoder,
            FieldOperation fieldOp,
            AggregationContext context) {
        Object accumulator = oldFieldGetter.getFieldOrNull(oldRow);
        Object inputField = inputFieldGetter.getFieldOrNull(inputRow);
        Object result;
        try {
            result = fieldOp.apply(aggregator, accumulator, inputField);
        } catch (ArithmeticException e) {
            if (fieldOp != RETRACT_OP) {
                throw e;
            }
            Schema.Column column = context.getSchema().getColumns().get(targetIdx);
            throw new InvalidRecordException(
                    String.format(
                            "Cannot retract field '%s' using %s: %s",
                            column.getName(),
                            column.getAggFunction().orElse(AggFunctions.LAST_VALUE_IGNORE_NULLS()),
                            e.getMessage()),
                    e);
        }
        encoder.encodeField(targetIdx, result);
    }

    /** Copy and encode a field value from old row. */
    private static void copyOldValueAndEncode(
            InternalRow.FieldGetter fieldGetter,
            BinaryRow oldRow,
            int targetIdx,
            RowEncoder encoder) {
        encoder.encodeField(targetIdx, fieldGetter.getFieldOrNull(oldRow));
    }
}
