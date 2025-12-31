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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldAggregator;

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

    // Private constructor to prevent instantiation
    private AggregateFieldsProcessor() {}

    /**
     * Aggregate all fields from old and new rows with explicit target schema (full aggregation).
     *
     * <p>This variant allows specifying a different target schema for the output, which is useful
     * when the client's newValue uses an outdated schema but we want to output using the latest
     * server schema.
     *
     * <p>This method handles schema evolution by matching fields using column IDs across three
     * potentially different schemas: old row schema, new row schema, and target output schema.
     *
     * @param oldRow the old row
     * @param newRow the new row
     * @param oldContext context for the old row schema
     * @param newInputContext context for the new row schema (for reading newRow)
     * @param targetContext context for the target output schema
     * @param encoder the row encoder to encode results (should match targetContext)
     */
    public static void aggregateAllFieldsWithTargetSchema(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext oldContext,
            AggregationContext newInputContext,
            AggregationContext targetContext,
            RowEncoder encoder) {
        // Fast path: all three schemas are the same
        if (targetContext == oldContext && targetContext == newInputContext) {
            aggregateAllFieldsWithSameSchema(oldRow, newRow, targetContext, encoder);
            return;
        }

        // General path: iterate over target schema columns and aggregate using column ID matching
        InternalRow.FieldGetter[] oldFieldGetters = oldContext.getFieldGetters();
        InternalRow.FieldGetter[] newFieldGetters = newInputContext.getFieldGetters();
        FieldAggregator[] targetAggregators = targetContext.getAggregators();
        List<Schema.Column> targetColumns = targetContext.getSchema().getColumns();

        for (int targetIdx = 0; targetIdx < targetColumns.size(); targetIdx++) {
            Schema.Column targetColumn = targetColumns.get(targetIdx);
            int columnId = targetColumn.getColumnId();

            // Find corresponding fields in old and new schemas using column ID
            Integer oldIdx = oldContext.getFieldIndex(columnId);
            Integer newIdx = newInputContext.getFieldIndex(columnId);

            // Get field getters (use NULL_FIELD_GETTER if column doesn't exist in that schema)
            InternalRow.FieldGetter oldFieldGetter =
                    (oldIdx != null) ? oldFieldGetters[oldIdx] : NULL_FIELD_GETTER;
            InternalRow.FieldGetter newFieldGetter =
                    (newIdx != null) ? newFieldGetters[newIdx] : NULL_FIELD_GETTER;

            // Aggregate and encode using target schema's aggregator
            aggregateAndEncode(
                    oldFieldGetter,
                    newFieldGetter,
                    oldRow,
                    newRow,
                    targetAggregators[targetIdx],
                    targetIdx,
                    encoder);
        }
    }

    /**
     * Aggregate and encode a single field.
     *
     * @param oldFieldGetter getter for the old field
     * @param newFieldGetter getter for the new field
     * @param oldRow the old row
     * @param newRow the new row
     * @param aggregator the aggregator for this field
     * @param targetIdx the target index to encode
     * @param encoder the row encoder
     */
    private static void aggregateAndEncode(
            InternalRow.FieldGetter oldFieldGetter,
            InternalRow.FieldGetter newFieldGetter,
            BinaryRow oldRow,
            BinaryRow newRow,
            FieldAggregator aggregator,
            int targetIdx,
            RowEncoder encoder) {
        Object accumulator = oldFieldGetter.getFieldOrNull(oldRow);
        Object inputField = newFieldGetter.getFieldOrNull(newRow);
        Object mergedField = aggregator.agg(accumulator, inputField);
        encoder.encodeField(targetIdx, mergedField);
    }

    /**
     * Copy and encode a field value from old row.
     *
     * @param fieldGetter getter for the field
     * @param oldRow the old row
     * @param targetIdx the target index to encode
     * @param encoder the row encoder
     */
    private static void copyOldValueAndEncode(
            InternalRow.FieldGetter fieldGetter,
            BinaryRow oldRow,
            int targetIdx,
            RowEncoder encoder) {
        encoder.encodeField(targetIdx, fieldGetter.getFieldOrNull(oldRow));
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
        // Fast path: all three schemas are the same
        if (targetContext == oldContext && targetContext == newInputContext) {
            aggregateTargetFieldsWithSameSchema(
                    oldRow, newRow, targetContext, targetColumnIdBitSet, encoder);
            return;
        }

        // General path: iterate over target schema columns
        InternalRow.FieldGetter[] oldFieldGetters = oldContext.getFieldGetters();
        InternalRow.FieldGetter[] newFieldGetters = newInputContext.getFieldGetters();
        FieldAggregator[] targetAggregators = targetContext.getAggregators();
        List<Schema.Column> targetColumns = targetContext.getSchema().getColumns();

        for (int targetIdx = 0; targetIdx < targetColumns.size(); targetIdx++) {
            Schema.Column targetColumn = targetColumns.get(targetIdx);
            int columnId = targetColumn.getColumnId();

            // Find corresponding fields in old and new schemas using column ID
            Integer oldIdx = oldContext.getFieldIndex(columnId);
            Integer newIdx = newInputContext.getFieldIndex(columnId);

            if (targetColumnIdBitSet.get(columnId)) {
                // Target column: aggregate and encode
                InternalRow.FieldGetter oldFieldGetter =
                        (oldIdx != null) ? oldFieldGetters[oldIdx] : NULL_FIELD_GETTER;
                InternalRow.FieldGetter newFieldGetter =
                        (newIdx != null) ? newFieldGetters[newIdx] : NULL_FIELD_GETTER;
                aggregateAndEncode(
                        oldFieldGetter,
                        newFieldGetter,
                        oldRow,
                        newRow,
                        targetAggregators[targetIdx],
                        targetIdx,
                        encoder);
            } else if (oldIdx != null) {
                // Non-target column that exists in old schema: copy old value
                copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, targetIdx, encoder);
            } else {
                // Non-target column that doesn't exist in old schema: set to null
                // NOTE: In partial aggregation, non-target columns should not use values from
                // newRow, even if they exist in newRow's schema, as only target columns are
                // aggregated
                encoder.encodeField(targetIdx, null);
            }
        }
    }

    /**
     * Aggregate all fields when old and new schemas are identical.
     *
     * <p>Fast path: field positions match directly, no column ID lookup needed.
     */
    private static void aggregateAllFieldsWithSameSchema(
            BinaryRow oldRow, BinaryRow newRow, AggregationContext context, RowEncoder encoder) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();
        FieldAggregator[] aggregators = context.getAggregators();
        int fieldCount = context.getFieldCount();

        for (int idx = 0; idx < fieldCount; idx++) {
            aggregateAndEncode(
                    fieldGetters[idx],
                    fieldGetters[idx],
                    oldRow,
                    newRow,
                    aggregators[idx],
                    idx,
                    encoder);
        }
    }

    /**
     * Aggregate target fields when old and new schemas are identical.
     *
     * <p>Fast path: field positions match directly, no column ID lookup needed.
     */
    private static void aggregateTargetFieldsWithSameSchema(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext context,
            BitSet targetColumnIdBitSet,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();
        FieldAggregator[] aggregators = context.getAggregators();
        List<Schema.Column> columns = context.getSchema().getColumns();
        int fieldCount = context.getFieldCount();

        for (int idx = 0; idx < fieldCount; idx++) {
            int columnId = columns.get(idx).getColumnId();

            if (targetColumnIdBitSet.get(columnId)) {
                // Target column: aggregate and encode
                aggregateAndEncode(
                        fieldGetters[idx],
                        fieldGetters[idx],
                        oldRow,
                        newRow,
                        aggregators[idx],
                        idx,
                        encoder);
            } else {
                // Non-target column: encode old value
                copyOldValueAndEncode(fieldGetters[idx], oldRow, idx, encoder);
            }
        }
    }

    /**
     * Encode a partial delete when old and new schemas are identical.
     *
     * <p>Set target columns (except primary key) to null, keep other columns unchanged.
     *
     * <p>Fast path: field positions match directly, no column ID lookup needed.
     *
     * @param oldRow the old row to partially delete
     * @param context the aggregation context for encoding
     * @param targetPosBitSet BitSet marking target column positions
     * @param pkPosBitSet BitSet marking primary key positions
     * @param encoder the row encoder to encode results
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
     * Encode a partial delete when old and new schemas differ (schema evolution).
     *
     * <p>Set target columns (except primary key) to null, keep other columns unchanged. For new
     * columns that don't exist in old schema, set to null.
     *
     * <p>Slow path: requires column ID matching to find corresponding fields between schemas.
     *
     * @param oldRow the old row to partially delete
     * @param oldContext context for the old schema
     * @param targetContext context for the target schema
     * @param targetColumnIdBitSet BitSet marking target columns by column ID
     * @param pkPosBitSet BitSet marking primary key positions in target schema
     * @param encoder the row encoder to encode results
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

    /**
     * Check if there are any non-null fields in non-target columns.
     *
     * @param row the row to check
     * @param targetPosBitSet BitSet marking target column positions
     * @return true if at least one non-target column has a non-null value
     */
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
}
