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

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/** Default implementation of RowDataConverter for RowData. */
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
        this.converter = FlinkAsFlussRow.from(context.getTableRowType(), context.getRowSchema());
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
