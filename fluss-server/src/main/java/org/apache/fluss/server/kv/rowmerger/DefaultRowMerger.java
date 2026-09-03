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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.TargetColumns;
import org.apache.fluss.server.kv.partialupdate.PartialUpdater;
import org.apache.fluss.server.kv.partialupdate.PartialUpdaterCache;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

/**
 * The default row merger of primary key table that always retains the latest row and supports
 * configuring target merge columns for partial update.
 *
 * <p>If {@link RowMerger#configureTargetColumns(int[], short, Schema)} receives target indexes that
 * cover every field of the latest schema (same semantic as a full-row write), this merger keeps
 * using plain merge semantics instead of wrapping a partial updater.
 */
public class DefaultRowMerger implements RowMerger {

    private final PartialUpdaterCache partialUpdaterCache;
    private final KvFormat kvFormat;
    private final DeleteBehavior deleteBehavior;
    private final boolean arbitrateSequenceGroups;

    // the full-row merger of the schema resolved last, kept so that a sequence group table doesn't
    // rebuild its encoder on every batch. sequence groups only change along with the schema.
    private short resolvedSchemaId = -1;
    private @Nullable RowMerger sequenceGroupRowMerger;

    public DefaultRowMerger(KvFormat kvFormat, @Nullable DeleteBehavior deleteBehavior) {
        this(kvFormat, deleteBehavior, true);
    }

    private DefaultRowMerger(
            KvFormat kvFormat,
            @Nullable DeleteBehavior deleteBehavior,
            boolean arbitrateSequenceGroups) {
        this.kvFormat = kvFormat;
        this.arbitrateSequenceGroups = arbitrateSequenceGroups;
        // for compatibility, default to ALLOW if not specified
        this.deleteBehavior = deleteBehavior != null ? deleteBehavior : DeleteBehavior.ALLOW;
        // TODO: share cache in server level when PartialUpdater is thread-safe
        this.partialUpdaterCache = new PartialUpdaterCache();
    }

    /**
     * Creates a merger that replaces values blindly, bypassing the sequence groups declared on the
     * schema. Used to recover by overwriting an already decided value: such a write restores an
     * earlier state, so arbitrating it would reject it as stale and leave the row inconsistent.
     */
    public static DefaultRowMerger forBlindOverwrite(KvFormat kvFormat) {
        return new DefaultRowMerger(kvFormat, DeleteBehavior.ALLOW, false);
    }

    @Nullable
    @Override
    public BinaryValue merge(@Nullable BinaryValue oldValue, BinaryValue newValue) {
        // always retain the new row (latest row)
        return newValue;
    }

    @Nullable
    @Override
    public BinaryValue delete(BinaryValue oldRow) {
        // returns null to indicate the row is deleted
        return null;
    }

    @Override
    public DeleteBehavior deleteBehavior() {
        return deleteBehavior;
    }

    @Override
    public RowMerger configureTargetColumns(
            @Nullable int[] targetColumns, short latestShemaId, Schema latestSchema) {
        if (targetColumns == null
                || TargetColumns.specifiesAllSchemaFieldIndexes(latestSchema, targetColumns)) {
            return fullRowMerger(latestShemaId, latestSchema);
        } else {
            TargetColumns.checkSequenceGroupsAreFullyTargeted(latestSchema, targetColumns);
            // this also sanity checks the validity of the partial update
            PartialUpdater partialUpdater =
                    arbitrateSequenceGroups
                            ? partialUpdaterCache.getOrCreatePartialUpdater(
                                    kvFormat, latestShemaId, latestSchema, targetColumns)
                            : new PartialUpdater(
                                    kvFormat, latestShemaId, latestSchema, targetColumns, null);
            return new PartialUpdateRowMerger(partialUpdater, deleteBehavior);
        }
    }

    /**
     * Returns the merger handling a full-row write. Without sequence groups the new row always
     * wins, so this merger is used as it is; with sequence groups the stored row has to be
     * consulted to arbitrate every group, which needs a merger of its own.
     */
    private RowMerger fullRowMerger(short latestSchemaId, Schema latestSchema) {
        if (!arbitrateSequenceGroups) {
            return this;
        }
        if (latestSchemaId != resolvedSchemaId) {
            SequenceGroups sequenceGroups = SequenceGroups.create(latestSchema);
            sequenceGroupRowMerger =
                    sequenceGroups == null
                            ? null
                            : new SequenceGroupRowMerger(
                                    kvFormat,
                                    latestSchemaId,
                                    latestSchema,
                                    sequenceGroups,
                                    deleteBehavior);
            resolvedSchemaId = latestSchemaId;
        }
        return sequenceGroupRowMerger == null ? this : sequenceGroupRowMerger;
    }

    /** A merger that partially updates specified columns with the new row. */
    private static class PartialUpdateRowMerger implements RowMerger {

        private final PartialUpdater partialUpdater;
        private final DeleteBehavior deleteBehavior;

        public PartialUpdateRowMerger(
                PartialUpdater partialUpdater, DeleteBehavior deleteBehavior) {
            this.partialUpdater = partialUpdater;
            this.deleteBehavior = deleteBehavior;
        }

        @Override
        public RowMerger configureTargetColumns(
                int[] targetColumns, short schemaId, Schema schema) {
            throw new IllegalStateException(
                    "PartialUpdateRowMerger does not support reconfigure target merge columns.");
        }

        @Nullable
        @Override
        public BinaryValue merge(@Nullable BinaryValue oldValue, BinaryValue newValue) {
            return partialUpdater.updateRow(oldValue, newValue);
        }

        @Nullable
        @Override
        public BinaryValue delete(BinaryValue oldRow) {
            return partialUpdater.deleteRow(oldRow);
        }

        @Override
        public DeleteBehavior deleteBehavior() {
            return deleteBehavior;
        }
    }

    /**
     * A merger that arbitrates a full-row write with sequence groups: a column only takes the
     * incoming value if the group protecting it advances, otherwise the stored value survives.
     * Since this engine has no aggregate functions, a group that doesn't advance simply drops the
     * incoming values, so the outcome depends only on the largest sequence seen per group rather
     * than on the order the records arrive in.
     *
     * <p>Sequence groups arbitrate writes only: a delete carries no sequence values to compare
     * against the stored row, so it keeps removing the whole row as it did before sequence groups
     * existed.
     */
    private static class SequenceGroupRowMerger implements RowMerger {

        private final SequenceGroups sequenceGroups;
        private final InternalRow.FieldGetter[] fieldGetters;
        private final RowEncoder rowEncoder;
        private final short targetSchemaId;
        private final DeleteBehavior deleteBehavior;

        SequenceGroupRowMerger(
                KvFormat kvFormat,
                short targetSchemaId,
                Schema schema,
                SequenceGroups sequenceGroups,
                DeleteBehavior deleteBehavior) {
            this.sequenceGroups = sequenceGroups;
            this.targetSchemaId = targetSchemaId;
            this.deleteBehavior = deleteBehavior;
            DataType[] fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);
            this.fieldGetters = new InternalRow.FieldGetter[fieldDataTypes.length];
            for (int i = 0; i < fieldDataTypes.length; i++) {
                fieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
            }
            this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
        }

        @Nullable
        @Override
        public BinaryValue merge(@Nullable BinaryValue oldValue, BinaryValue newValue) {
            sequenceGroups.arbitrate(oldValue == null ? null : oldValue.row, newValue.row);
            if (sequenceGroups.acceptsEveryArbitratedGroup()) {
                // Every group advances, so the whole incoming row wins
                return newValue;
            }

            rowEncoder.startNewRow();
            for (int i = 0; i < fieldGetters.length; i++) {
                InternalRow source =
                        sequenceGroups.accepts(i)
                                ? newValue.row
                                : oldValue == null ? null : oldValue.row;
                // the stored row may be absent or follow an older schema with fewer fields, in
                // which case the missing fields are null
                if (source == null || source.getFieldCount() < i + 1) {
                    rowEncoder.encodeField(i, null);
                } else {
                    rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(source));
                }
            }
            return new BinaryValue(targetSchemaId, rowEncoder.finishRow());
        }

        @Nullable
        @Override
        public BinaryValue delete(BinaryValue oldRow) {
            // TODO: arbitrate the delete with the sequence groups when a delete record carries the
            //  sequence columns, so that a stale delete no longer drops a newer row
            return null;
        }

        @Override
        public DeleteBehavior deleteBehavior() {
            return deleteBehavior;
        }

        @Override
        public RowMerger configureTargetColumns(
                @Nullable int[] targetColumns, short schemaId, Schema schema) {
            throw new IllegalStateException(
                    "SequenceGroupRowMerger does not support reconfigure target merge columns.");
        }
    }
}
