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

import org.apache.fluss.exception.InvalidTargetColumnException;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.aggregate.factory.FieldAggregatorFactory;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldAggregator;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Context for aggregation operations, containing field getters, aggregators, and encoder for a
 * specific schema.
 *
 * <p>This class supports schema evolution by using column IDs to match fields across different
 * schema versions, rather than relying on field positions.
 *
 * <p>This class is thread-safe as it is guaranteed to be accessed by a single thread at a time
 * (protected by KvTablet's write lock).
 */
public class AggregationContext {
    final Schema schema;
    final RowType rowType;
    final InternalRow.FieldGetter[] fieldGetters;
    final FieldAggregator[] aggregators;
    final RowEncoder rowEncoder;
    final int fieldCount;

    /**
     * Mapping from column ID to field index in this schema. This is used for schema evolution to
     * correctly match fields between old and new schemas.
     */
    private final Map<Integer, Integer> columnIdToIndex;

    /**
     * BitSet marking primary key column positions for fast O(1) lookup. This is useful for partial
     * update and delete operations.
     */
    private final BitSet primaryKeyColsBitSet;

    private AggregationContext(
            Schema schema,
            RowType rowType,
            InternalRow.FieldGetter[] fieldGetters,
            FieldAggregator[] aggregators,
            RowEncoder rowEncoder) {
        this.schema = schema;
        this.rowType = rowType;
        this.fieldGetters = fieldGetters;
        this.aggregators = aggregators;
        this.rowEncoder = rowEncoder;
        this.fieldCount = rowType.getFieldCount();

        // Build columnId to index mapping for schema evolution support
        this.columnIdToIndex = new HashMap<>();
        List<Schema.Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            columnIdToIndex.put(columns.get(i).getColumnId(), i);
        }

        // Initialize primary key columns BitSet for fast lookup
        this.primaryKeyColsBitSet = new BitSet();
        for (int pkIndex : schema.getPrimaryKeyIndexes()) {
            primaryKeyColsBitSet.set(pkIndex);
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public RowType getRowType() {
        return rowType;
    }

    public InternalRow.FieldGetter[] getFieldGetters() {
        return fieldGetters;
    }

    public FieldAggregator[] getAggregators() {
        return aggregators;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    /**
     * Get the field index for a given column ID.
     *
     * @param columnId the column ID
     * @return the field index, or null if the column doesn't exist in this schema
     */
    public Integer getFieldIndex(int columnId) {
        return columnIdToIndex.get(columnId);
    }

    /**
     * Get the BitSet marking primary key column positions.
     *
     * @return BitSet with primary key positions set to true
     */
    public BitSet getPrimaryKeyColsBitSet() {
        return primaryKeyColsBitSet;
    }

    /**
     * Sanity check for target columns used in partial aggregate operations.
     *
     * <p>Validates that: 1. Target columns must contain all primary key columns 2. All non-primary
     * key columns must be nullable (for partial update semantics)
     *
     * @param targetColumnIdBitSet BitSet of target column IDs
     * @throws InvalidTargetColumnException if validation fails
     */
    public void sanityCheckTargetColumns(BitSet targetColumnIdBitSet) {
        // Build target column position set from targetColumnIds
        BitSet targetColumnPosBitSet = new BitSet();
        DataType[] fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);

        // Map column IDs to positions
        for (int pos = 0; pos < schema.getColumns().size(); pos++) {
            int columnId = schema.getColumns().get(pos).getColumnId();
            if (targetColumnIdBitSet.get(columnId)) {
                targetColumnPosBitSet.set(pos);
            }
        }

        // Check 1: target columns must contain all primary key columns
        for (int pkIndex : schema.getPrimaryKeyIndexes()) {
            if (!targetColumnPosBitSet.get(pkIndex)) {
                throw new InvalidTargetColumnException(
                        String.format(
                                "The target write columns must contain the primary key columns %s.",
                                schema.getColumnNames(schema.getPrimaryKeyIndexes())));
            }
        }

        // Check 2: all non-primary key columns must be nullable
        for (int i = 0; i < fieldDataTypes.length; i++) {
            if (!primaryKeyColsBitSet.get(i)) {
                if (!fieldDataTypes[i].isNullable()) {
                    throw new InvalidTargetColumnException(
                            String.format(
                                    "Partial aggregate requires all columns except primary key to be nullable, "
                                            + "but column %s is NOT NULL.",
                                    schema.getRowType().getFieldNames().get(i)));
                }
            }
        }
    }

    /**
     * Get the row encoder for encoding rows.
     *
     * <p>This method returns the RowEncoder instance for encoding rows. Callers should use the
     * standard RowEncoder pattern:
     *
     * <pre>{@code
     * RowEncoder encoder = context.getRowEncoder();
     * encoder.startNewRow();
     * for (int i = 0; i < fieldCount; i++) {
     *     encoder.encodeField(i, value);
     * }
     * BinaryRow result = encoder.finishRow();
     * }</pre>
     *
     * <p>This is safe because KvTablet ensures single-threaded access via write lock.
     *
     * @return the row encoder for this schema
     */
    public RowEncoder getRowEncoder() {
        return rowEncoder;
    }

    /**
     * Create an aggregation context for a given schema.
     *
     * @param schema the schema
     * @param kvFormat the KV format
     * @return the aggregation context
     */
    public static AggregationContext create(Schema schema, KvFormat kvFormat) {
        RowType rowType = schema.getRowType();
        int fieldCount = rowType.getFieldCount();

        // Create field getters
        InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] =
                    InternalRow.createFieldGetter(rowType.getFields().get(i).getType(), i);
        }

        // Create aggregators
        FieldAggregator[] aggregators = createAggregators(schema);

        // Create row encoder
        RowEncoder rowEncoder = RowEncoder.create(kvFormat, rowType);

        return new AggregationContext(schema, rowType, fieldGetters, aggregators, rowEncoder);
    }

    /**
     * Creates an array of field aggregators for all fields in the schema.
     *
     * <p>This method reads aggregation functions from the Schema object and creates the appropriate
     * aggregators with their parameters.
     *
     * @param schema the Schema object containing column definitions and aggregation functions
     * @return an array of field aggregators, one for each field
     */
    private static FieldAggregator[] createAggregators(Schema schema) {
        RowType rowType = schema.getRowType();
        List<String> primaryKeys = schema.getPrimaryKeyColumnNames();
        List<String> fieldNames = rowType.getFieldNames();
        int fieldCount = rowType.getFieldCount();

        FieldAggregator[] aggregators = new FieldAggregator[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = fieldNames.get(i);
            DataType fieldType = rowType.getTypeAt(i);

            // Get the aggregate function for this field
            AggFunction aggFunc = getAggFunction(fieldName, primaryKeys, schema);

            // Get the factory for this aggregation function type and create the aggregator
            AggFunctionType type = aggFunc.getType();
            FieldAggregatorFactory factory = FieldAggregatorFactory.getFactory(type);
            if (factory == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported aggregation function: %s or spell aggregate function incorrectly!",
                                type));
            }
            aggregators[i] = factory.create(fieldType, aggFunc);
        }

        return aggregators;
    }

    /**
     * Determines the aggregate function for a field.
     *
     * <p>The priority is:
     *
     * <ol>
     *   <li>Primary key fields use "last_value" (no aggregation)
     *   <li>Schema.getAggFunction() - aggregation function defined in Schema (from Column)
     *   <li>Final fallback: "last_value_ignore_nulls"
     * </ol>
     *
     * @param fieldName the field name
     * @param primaryKeys the list of primary key field names
     * @param schema the Schema object
     * @return the aggregate function to use
     */
    private static AggFunction getAggFunction(
            String fieldName, List<String> primaryKeys, Schema schema) {

        // 1. Primary key fields don't aggregate
        if (primaryKeys.contains(fieldName)) {
            return AggFunctions.of(AggFunctionType.LAST_VALUE);
        }

        // 2. Check Schema for aggregation function, or use default fallback
        return schema.getAggFunction(fieldName).orElseGet(AggFunctions::LAST_VALUE_IGNORE_NULLS);
    }
}
