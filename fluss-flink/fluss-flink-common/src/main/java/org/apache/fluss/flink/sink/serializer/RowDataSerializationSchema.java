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

package org.apache.fluss.flink.sink.serializer;

import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.PaddingRow;
import org.apache.fluss.row.ProjectedRow;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Default implementation of RowDataConverter for RowData.
 *
 * <p>Schema Evolution Handling:
 *
 * <p>There are three schema in sink case.
 *
 * <ul>
 *   <li>(1) Consumed Row Schema (RowData): the schema of RowData consumed, currently, Flink doesn't
 *       provide API to fetch it, but we can get the row arity from RowData.getArity().
 *   <li>(2) Plan Row Schema ({@link InitializationContext#getInputRowSchema()}): the compiled input
 *       row schema of the sink, but the schema is fetched from catalog, so it is not the consumed
 *       row schema. This is updated when job is re-compiled.
 *   <li>(3) Table Latest Row Schema ({@link InitializationContext#getRowSchema()}): the latest
 *       schema of the sink table, which is fetched at open() during runtime, so it will be updated
 *       when Flink job restarts.
 * </ul>
 *
 * <p>We always want to use latest schema to write data, so we need to add conversions from consumed
 * Flink RowData.
 *
 * <ul>
 *   <li>The {@link #converter} is used to wrap Flink {@link RowData} into Fluss {@link InternalRow}
 *       without any schema transformation.
 *   <li>The {@link #outputPadding} is used to pad nulls for new columns when new columns are added.
 *       This may happen when table is added new columns after Flink SQL job restored from
 *       CompiledPlan that the Consumed Row Schema is old but the Plan Row Schema is new, so the
 *       Consumed Row Schema has less fields than Plan Row Schema.
 *   <li>The {@link #outputProjection} is used to re-arrange the fields according to latest schema
 *       if Plan Row Schema is not match Table Latest Row Schema. This may happen when table is
 *       added new columns after the Flink job compiled and before job restarts.
 * </ul>
 */
public class RowDataSerializationSchema implements FlussSerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /**
     * Whether the schema operates in append-only mode. If true, only INSERT operations are
     * supported.
     */
    private final boolean isAppendOnly;

    /** Whether to ignore DELETE and UPDATE_BEFORE operations. */
    private final boolean ignoreDelete;

    /**
     * The converter used to transform Flink {@link RowData} to Fluss {@link InternalRow}.
     * Initialized in {@link #open(InitializationContext)}.
     */
    private transient FlinkAsFlussRow converter;

    /**
     * The padding row for output, used when the input row has fewer fields than the target schema.
     * This may happen when table is added new columns between Flink job restarts.
     */
    private transient PaddingRow outputPadding;

    /**
     * The projected row for output, used when schema evolution occurs, because we want to write
     * rows using new schema (e.g., fill null for new columns). This may happen when table is *
     * added new columns after the Flink job compiled and before job restarts.
     */
    @Nullable private transient ProjectedRow outputProjection;

    /**
     * Constructs a new {@code RowSerializationSchema}.
     *
     * @param isAppendOnly whether the schema is append-only (only INSERTs allowed)
     * @param ignoreDelete whether to ignore DELETE and UPDATE_BEFORE operations
     */
    public RowDataSerializationSchema(boolean isAppendOnly, boolean ignoreDelete) {
        this.isAppendOnly = isAppendOnly;
        this.ignoreDelete = ignoreDelete;
    }

    /**
     * Initializes the schema and its internal converter.
     *
     * @param context the initialization context
     * @throws Exception if initialization fails
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        this.converter = new FlinkAsFlussRow();
        List<String> targetFieldNames = context.getRowSchema().getFieldNames();
        List<String> inputFieldNames = context.getInputRowSchema().getFieldNames();
        this.outputPadding = new PaddingRow(inputFieldNames.size());
        if (targetFieldNames.size() != inputFieldNames.size()) {
            // there is a schema evolution happens (e.g., ADD COLUMN), need to build index mapping
            int[] indexMapping = new int[targetFieldNames.size()];
            for (int i = 0; i < targetFieldNames.size(); i++) {
                String fieldName = targetFieldNames.get(i);
                int fieldIndex = inputFieldNames.indexOf(fieldName);
                indexMapping[i] = fieldIndex;
            }
            outputProjection = ProjectedRow.from(indexMapping);
        }
    }

    /**
     * Serializes a Flink {@link RowData} into a Fluss {@link RowWithOp} containing an {@link
     * InternalRow} and the corresponding {@link OperationType}.
     *
     * @param value the Flink row to serialize
     * @return the serialized row with operation type, or {@code null} if the operation should be
     *     ignored
     * @throws Exception if serialization fails
     */
    @Override
    public RowWithOp serialize(RowData value) throws Exception {
        if (converter == null) {
            throw new IllegalStateException(
                    "Converter not initialized. The open() method must be called before serializing records.");
        }
        InternalRow row = converter.replace(value);
        // handling schema evolution for changes before job compilation
        if (row.getFieldCount() < outputPadding.getFieldCount()) {
            row = outputPadding.replaceRow(row);
        }
        // handling schema evolution for changes after job compilation
        if (outputProjection != null) {
            row = outputProjection.replaceRow(row);
        }
        OperationType opType = toOperationType(value.getRowKind());

        return new RowWithOp(row, opType);
    }

    /**
     * Maps a Flink {@link RowKind} to a Fluss {@link OperationType}, considering the schema's
     * configuration.
     *
     * @param rowKind the Flink row kind
     * @return the corresponding operation type, or {@code null} if the operation should be ignored
     * @throws UnsupportedOperationException if the row kind is not supported in the current mode
     */
    private OperationType toOperationType(RowKind rowKind) {
        if (ignoreDelete) {
            if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
                return OperationType.IGNORE;
            }
        }

        if (isAppendOnly) {
            return OperationType.APPEND;
        } else {
            switch (rowKind) {
                case INSERT:
                case UPDATE_AFTER:
                    return OperationType.UPSERT;
                case DELETE:
                case UPDATE_BEFORE:
                    return OperationType.DELETE;
                default:
                    throw new UnsupportedOperationException("Unsupported row kind: " + rowKind);
            }
        }
    }
}
