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

import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A merger that keeps last-row upsert semantics but suppresses value-identical writes.
 *
 * <p>When the complete candidate row is logically equal to the currently stored row, the merger
 * returns the {@code oldValue} instance so that the KV write path treats the write as a no-op (no
 * state update, no changelog). When at least one field differs, it returns the candidate row so
 * that a normal update changelog is emitted.
 *
 * <p>Equality is based on logical field values rather than raw serialized bytes: nulls compare with
 * SQL semantics, binary values compare by content, and rows written with an older schema are
 * aligned to the latest schema by stable column IDs (missing fields are treated as null) before
 * comparison.
 *
 * <p>The default merge engine is used as a delegate to preserve its full-row, partial-update, and
 * partial-delete semantics. This class intentionally does not extend {@link DefaultRowMerger}, so
 * writes still look up the stored value before merging and can perform the equality check.
 *
 * <p>This class is not thread-safe: it caches the latest-schema equalizer between {@link
 * #configureTargetColumns} and {@link #merge} calls, and is guaranteed to be accessed by a single
 * thread at a time (protected by KvTablet's write lock).
 *
 * @see MergeEngineType#UPDATE_IF_CHANGED
 */
public class UpdateIfChangedRowMerger implements RowMerger {

    private final RowMerger delegate;
    private final SchemaGetter schemaGetter;

    private RowEqualizer rowEqualizer;

    public UpdateIfChangedRowMerger(
            KvFormat kvFormat, SchemaGetter schemaGetter, @Nullable DeleteBehavior deleteBehavior) {
        this(new DefaultRowMerger(kvFormat, deleteBehavior), schemaGetter, null);
    }

    private UpdateIfChangedRowMerger(
            RowMerger delegate, SchemaGetter schemaGetter, @Nullable RowEqualizer rowEqualizer) {
        this.delegate = delegate;
        this.schemaGetter = schemaGetter;
        this.rowEqualizer = rowEqualizer;
    }

    @Nullable
    @Override
    public BinaryValue merge(@Nullable BinaryValue oldValue, BinaryValue newValue) {
        return suppressUnchanged(oldValue, delegate.merge(oldValue, newValue));
    }

    @Nullable
    @Override
    public BinaryValue delete(BinaryValue oldRow) {
        return suppressUnchanged(oldRow, delegate.delete(oldRow));
    }

    @Override
    public DeleteBehavior deleteBehavior() {
        return delegate.deleteBehavior();
    }

    @Override
    public RowMerger configureTargetColumns(
            @Nullable int[] targetColumns, short latestSchemaId, Schema latestSchema) {
        RowMerger configuredMerger =
                delegate.configureTargetColumns(targetColumns, latestSchemaId, latestSchema);
        if (rowEqualizer == null || latestSchemaId != rowEqualizer.latestSchemaId) {
            this.rowEqualizer = new RowEqualizer(schemaGetter, latestSchemaId, latestSchema);
        }
        return new UpdateIfChangedRowMerger(configuredMerger, schemaGetter, rowEqualizer);
    }

    @Nullable
    private BinaryValue suppressUnchanged(
            @Nullable BinaryValue oldValue, @Nullable BinaryValue candidate) {
        if (oldValue != null && candidate != null && rowEqualizer.equals(oldValue, candidate)) {
            // return the old value (same instance) so the write path treats this as a no-op
            return oldValue;
        }
        return candidate;
    }

    /** Compares two rows by logical field values aligned to the latest schema. */
    static final class RowEqualizer {

        private final SchemaGetter schemaGetter;
        private final short latestSchemaId;
        private final Schema latestSchema;
        private final List<Schema.Column> latestColumns;
        private final Map<Short, InternalRow.FieldGetter[]> alignedFieldGetters;

        RowEqualizer(SchemaGetter schemaGetter, short latestSchemaId, Schema latestSchema) {
            this.schemaGetter = schemaGetter;
            this.latestSchemaId = latestSchemaId;
            this.latestSchema = latestSchema;
            this.latestColumns = latestSchema.getColumns();
            this.alignedFieldGetters = new HashMap<>();
        }

        boolean equals(BinaryValue left, BinaryValue right) {
            InternalRow.FieldGetter[] leftGetters = getOrCreateAlignedFieldGetters(left.schemaId);
            InternalRow.FieldGetter[] rightGetters = getOrCreateAlignedFieldGetters(right.schemaId);
            for (int i = 0; i < latestColumns.size(); i++) {
                Object leftField = getFieldOrNull(leftGetters[i], left.row);
                Object rightField = getFieldOrNull(rightGetters[i], right.row);
                if (!fieldEquals(leftField, rightField)) {
                    return false;
                }
            }
            return true;
        }

        private InternalRow.FieldGetter[] getOrCreateAlignedFieldGetters(short schemaId) {
            InternalRow.FieldGetter[] fieldGetters = alignedFieldGetters.get(schemaId);
            if (fieldGetters != null) {
                return fieldGetters;
            }

            Schema schema =
                    schemaId == latestSchemaId ? latestSchema : schemaGetter.getSchema(schemaId);
            Map<Integer, Integer> columnIdToIndex = new HashMap<>();
            List<Schema.Column> columns = schema.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                columnIdToIndex.put(columns.get(i).getColumnId(), i);
            }

            RowType rowType = schema.getRowType();
            fieldGetters = new InternalRow.FieldGetter[latestColumns.size()];
            for (int i = 0; i < latestColumns.size(); i++) {
                Integer sourceIndex = columnIdToIndex.get(latestColumns.get(i).getColumnId());
                if (sourceIndex != null) {
                    fieldGetters[i] =
                            InternalRow.createFieldGetter(
                                    rowType.getTypeAt(sourceIndex), sourceIndex);
                }
            }
            alignedFieldGetters.put(schemaId, fieldGetters);
            return fieldGetters;
        }

        @Nullable
        private static Object getFieldOrNull(
                @Nullable InternalRow.FieldGetter fieldGetter, BinaryRow row) {
            return fieldGetter == null ? null : fieldGetter.getFieldOrNull(row);
        }

        private static boolean fieldEquals(@Nullable Object left, @Nullable Object right) {
            if (left == right) {
                return true;
            }
            if (left == null || right == null) {
                return false;
            }
            if (left instanceof byte[] && right instanceof byte[]) {
                return Arrays.equals((byte[]) left, (byte[]) right);
            }
            return left.equals(right);
        }
    }
}
