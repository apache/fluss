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

package org.apache.fluss.server.kv.rowmerger;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldAggregator;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldPrimaryKeyAgg;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

/**
 * A row merger that aggregates rows with the same primary key using field-level aggregate
 * functions.
 *
 * <p>Each field can have its own aggregate function (e.g., sum, max, min, last_value, etc.). This
 * allows for flexible aggregation semantics at the field level.
 */
public class AggregateRowMerger implements RowMerger {

    private final RowType schema;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final FieldAggregator[] aggregators;
    private final DeleteBehavior deleteBehavior;
    private final boolean removeRecordOnDelete;
    private final int fieldCount;

    // Reusable GenericRow for aggregation accumulator
    private final GenericRow accumulatorRow;

    // Row encoder for converting GenericRow to target kv format (e.g., CompactedRow)
    private final RowEncoder rowEncoder;

    public AggregateRowMerger(
            RowType schema,
            FieldAggregator[] aggregators,
            boolean removeRecordOnDelete,
            @Nullable DeleteBehavior deleteBehavior,
            KvFormat kvFormat) {
        this.schema = schema;
        this.aggregators = aggregators;
        this.removeRecordOnDelete = removeRecordOnDelete;
        this.deleteBehavior = deleteBehavior != null ? deleteBehavior : DeleteBehavior.IGNORE;
        this.fieldCount = schema.getFieldCount();

        // Create field getters for accessing field values
        this.fieldGetters = new InternalRow.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            this.fieldGetters[i] =
                    InternalRow.createFieldGetter(schema.getFields().get(i).getType(), i);
        }

        this.accumulatorRow = new GenericRow(fieldCount);
        // Create row encoder for the target kv format
        this.rowEncoder = RowEncoder.create(kvFormat, schema);
    }

    @Override
    public BinaryValue merge(BinaryValue oldValue, BinaryValue newValue) {
        // First write: no existing row
        if (oldValue == null || oldValue.row == null) {
            return newValue;
        }

        BinaryRow oldRow = oldValue.row;
        BinaryRow newRow = newValue.row;

        // Apply aggregation for each field
        for (int i = 0; i < fieldCount; i++) {
            FieldAggregator aggregator = aggregators[i];

            // Get the current accumulator value from oldRow
            Object accumulator = fieldGetters[i].getFieldOrNull(oldRow);

            // Get the new field value
            Object inputField = fieldGetters[i].getFieldOrNull(newRow);

            // Apply aggregate function
            Object mergedField = aggregator.agg(accumulator, inputField);

            // Update accumulator
            accumulatorRow.setField(i, mergedField);
        }

        // Convert GenericRow to BinaryRow using the target kv format encoder
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldCount; i++) {
            rowEncoder.encodeField(i, accumulatorRow.getField(i));
        }
        BinaryRow mergedRow = rowEncoder.finishRow();

        // Return with the new schema id
        return new BinaryValue(newValue.schemaId, mergedRow);
    }

    @Override
    public BinaryValue delete(BinaryValue oldValue) {
        if (removeRecordOnDelete) {
            return null; // Remove the entire row
        }
        throw new UnsupportedOperationException(
                "DELETE is not supported for aggregate merge engine. "
                        + "Configure 'table.aggregation.remove-record-on-delete' to true if needed.");
    }

    @Override
    public DeleteBehavior deleteBehavior() {
        return deleteBehavior;
    }

    @Override
    public RowMerger configureTargetColumns(
            @Nullable int[] targetColumns, short schemaId, Schema schema) {
        if (targetColumns == null) {
            return this;
        }
        // Create a partial aggregation merger
        return new PartialAggregateRowMerger(
                this.schema,
                aggregators,
                targetColumns,
                removeRecordOnDelete,
                deleteBehavior,
                rowEncoder,
                schemaId);
    }

    /**
     * A merger that partially aggregates specified columns with the new row while keeping other
     * columns unchanged.
     */
    private static class PartialAggregateRowMerger implements RowMerger {
        private final int fieldCount;
        private final InternalRow.FieldGetter[] fieldGetters;
        private final FieldAggregator[] aggregators;
        private final java.util.BitSet targetColumnsBitSet;
        private final DeleteBehavior deleteBehavior;
        private final boolean removeRecordOnDelete;
        private final short schemaId;

        // Reusable GenericRow for aggregation accumulator
        private final GenericRow accumulatorRow;

        // Row encoder for converting GenericRow to target kv format
        private final RowEncoder rowEncoder;

        public PartialAggregateRowMerger(
                RowType schema,
                FieldAggregator[] aggregators,
                int[] targetColumns,
                boolean removeRecordOnDelete,
                DeleteBehavior deleteBehavior,
                RowEncoder rowEncoder,
                short schemaId) {
            this.fieldCount = schema.getFieldCount();
            this.aggregators = aggregators;
            this.removeRecordOnDelete = removeRecordOnDelete;
            this.deleteBehavior = deleteBehavior;
            this.accumulatorRow = new GenericRow(fieldCount);
            this.rowEncoder = rowEncoder;
            this.schemaId = schemaId;

            // Create field getters for accessing field values
            this.fieldGetters = new InternalRow.FieldGetter[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                this.fieldGetters[i] =
                        InternalRow.createFieldGetter(schema.getFields().get(i).getType(), i);
            }

            // Build a BitSet for fast lookup of target columns
            this.targetColumnsBitSet = new java.util.BitSet(fieldCount);
            for (int col : targetColumns) {
                targetColumnsBitSet.set(col);
            }
        }

        @Override
        public BinaryValue merge(BinaryValue oldValue, BinaryValue newValue) {
            // First write: no existing row
            if (oldValue == null || oldValue.row == null) {
                return newValue;
            }

            BinaryRow oldRow = oldValue.row;
            BinaryRow newRow = newValue.row;

            // Copy oldRow field values to GenericRow as accumulator
            for (int i = 0; i < fieldCount; i++) {
                Object fieldValue = fieldGetters[i].getFieldOrNull(oldRow);
                accumulatorRow.setField(i, fieldValue);
            }

            // Apply aggregation for each field
            for (int i = 0; i < fieldCount; i++) {
                if (targetColumnsBitSet.get(i)) {
                    // For target columns: apply aggregation
                    FieldAggregator aggregator = aggregators[i];
                    Object accumulator = fieldGetters[i].getFieldOrNull(accumulatorRow);
                    Object inputField = fieldGetters[i].getFieldOrNull(newRow);
                    Object mergedField = aggregator.agg(accumulator, inputField);
                    accumulatorRow.setField(i, mergedField);
                } else {
                    // For non-target columns: keep old value unchanged
                    // (already copied from oldRow, no action needed)
                }
            }

            // Convert GenericRow to BinaryRow using the target kv format encoder
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldCount; i++) {
                rowEncoder.encodeField(i, accumulatorRow.getField(i));
            }
            BinaryRow mergedRow = rowEncoder.finishRow();

            // Return with schema id
            return new BinaryValue(schemaId, mergedRow);
        }

        @Override
        public BinaryValue delete(BinaryValue oldValue) {
            if (removeRecordOnDelete) {
                return null; // Remove the entire row
            }

            BinaryRow oldRow = oldValue.row;

            // Partial delete: set target columns to null (except primary keys)
            // Check if all non-primary key columns will be null after deletion
            boolean allNonPkNull = true;
            for (int i = 0; i < fieldCount; i++) {
                // Skip primary key columns
                // Note: Primary key fields use FieldPrimaryKeyAgg, we can identify them
                if (aggregators[i] instanceof FieldPrimaryKeyAgg) {
                    continue;
                }

                // Check if non-target, non-primary columns have non-null values
                if (!targetColumnsBitSet.get(i) && fieldGetters[i].getFieldOrNull(oldRow) != null) {
                    allNonPkNull = false;
                    break;
                }
            }

            if (allNonPkNull) {
                // All non-primary key columns are null, delete the entire row
                return null;
            }

            // Perform partial deletion: set target non-primary key columns to null
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldCount; i++) {
                boolean isPrimaryKey = aggregators[i] instanceof FieldPrimaryKeyAgg;
                if (targetColumnsBitSet.get(i) && !isPrimaryKey) {
                    // Set target non-primary key columns to null
                    rowEncoder.encodeField(i, null);
                } else {
                    // Keep other columns unchanged
                    rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(oldRow));
                }
            }
            BinaryRow resultRow = rowEncoder.finishRow();
            return new BinaryValue(schemaId, resultRow);
        }

        @Override
        public DeleteBehavior deleteBehavior() {
            return deleteBehavior;
        }

        @Override
        public RowMerger configureTargetColumns(
                @Nullable int[] targetColumns, short schemaId, Schema schema) {
            throw new IllegalStateException(
                    "PartialAggregateRowMerger does not support reconfigure target merge columns.");
        }
    }
}
