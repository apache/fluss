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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The sequence groups declared on a schema, resolved into field positions so that a merger can
 * arbitrate each group on its own.
 *
 * <p>A column is put under the order of one or more sequence columns (see {@link
 * Schema.Column#getSequenceColumns()}) and then only takes an incoming value when those sequence
 * columns are not older than the stored ones. Columns ordered by the very same sequence columns
 * form one group advancing together, while different groups advance independently: within a single
 * write one group may advance and another may not. That is what distinguishes sequence groups from
 * the versioned merge engine, which arbitrates the whole row with a single version.
 *
 * <p>Instances are immutable and hold no per-record state, so one instance serves all keys of a
 * table.
 */
@Internal
public class SequenceGroups implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * What a group makes of an incoming row, once its sequence columns have been compared with the
     * stored ones.
     *
     * <p>A merger without aggregate functions treats {@link #SKIP} and {@link #STALE} alike, since
     * both keep the stored values. One with aggregate functions has to tell them apart: a skipped
     * group contributes nothing at all, while a stale one still aggregates, only as a record that
     * happened earlier.
     */
    public enum Decision {
        /**
         * The incoming row carries no sequence value for the group at all, so the group has no way
         * to order it and leaves its fields untouched.
         */
        SKIP,

        /**
         * The incoming sequence is not older than the stored one, so the group moves forward: its
         * fields take the incoming values and its sequence columns advance along with them.
         */
        FORWARD,

        /**
         * The incoming sequence is older than the stored one. The group doesn't move forward, so
         * its sequence columns keep the stored values, and an aggregate function sees the incoming
         * row as one that happened earlier.
         */
        STALE
    }

    /** A field taking part in no group, so it is never held back. */
    private static final int NO_GROUP = -1;

    /**
     * For each field, the group arbitrating it, or {@link #NO_GROUP} if the field takes part in no
     * group. A sequence column is arbitrated by the very group it orders, so that the whole group
     * advances at once.
     */
    private final int[] groupOfField;

    /** For each group, the readers of its sequence columns, in the declared comparison order. */
    private final SequenceReader[][] readersOfGroup;

    private SequenceGroups(int[] groupOfField, SequenceReader[][] readersOfGroup) {
        this.groupOfField = groupOfField;
        this.readersOfGroup = readersOfGroup;
    }

    /**
     * Resolves the sequence groups of the given schema, or returns null if the schema declares
     * none. Returning null lets callers keep their original merge path untouched.
     */
    @Nullable
    public static SequenceGroups create(Schema schema) {
        if (!schema.hasSequenceGroup()) {
            return null;
        }

        RowType rowType = schema.getRowType();
        List<Schema.Column> columns = schema.getColumns();
        int fieldCount = columns.size();

        // columns naming the very same sequence columns belong to one group, keyed by those names
        // so that the group ids stay stable across equal schemas
        Map<List<String>, Integer> groupIds = new LinkedHashMap<>();
        List<List<String>> sequenceColumnsOfGroup = new ArrayList<>();

        int[] groupOfField = new int[fieldCount];
        Arrays.fill(groupOfField, NO_GROUP);

        for (int i = 0; i < fieldCount; i++) {
            List<String> sequenceColumns = columns.get(i).getSequenceColumns().orElse(null);
            if (sequenceColumns == null) {
                continue;
            }
            Integer groupId = groupIds.get(sequenceColumns);
            if (groupId == null) {
                groupId = sequenceColumnsOfGroup.size();
                groupIds.put(sequenceColumns, groupId);
                sequenceColumnsOfGroup.add(sequenceColumns);
            }
            groupOfField[i] = groupId;
        }

        SequenceReader[][] readersOfGroup = new SequenceReader[sequenceColumnsOfGroup.size()][];
        for (int groupId = 0; groupId < sequenceColumnsOfGroup.size(); groupId++) {
            List<String> sequenceColumns = sequenceColumnsOfGroup.get(groupId);
            SequenceReader[] readers = new SequenceReader[sequenceColumns.size()];
            for (int i = 0; i < sequenceColumns.size(); i++) {
                String sequenceColumn = sequenceColumns.get(i);
                int sequenceField = rowType.getFieldIndex(sequenceColumn);
                checkArgument(
                        sequenceField >= 0,
                        "The sequence column '%s' doesn't exist in schema.",
                        sequenceColumn);
                readers[i] =
                        createReader(
                                sequenceColumn, rowType.getTypeAt(sequenceField), sequenceField);
                // a sequence column takes part in the very group it orders, otherwise it would
                // always accept incoming values and report a sequence no longer matching them
                groupOfField[sequenceField] = groupId;
            }
            readersOfGroup[groupId] = readers;
        }

        return new SequenceGroups(groupOfField, readersOfGroup);
    }

    /**
     * Resolves, for every field, whether it may take the value carried by the incoming row.
     *
     * <p>A field is held back only when the group arbitrating it doesn't advance. Fields taking
     * part in no group keep their original behavior and always accept the incoming value.
     *
     * @param oldRow the stored row, or null when there is no stored row yet
     * @param newRow the incoming row
     */
    public boolean[] resolveAcceptance(@Nullable InternalRow oldRow, InternalRow newRow) {
        Decision[] decisions = decideGroups(oldRow, newRow);

        boolean[] acceptance = new boolean[groupOfField.length];
        for (int i = 0; i < groupOfField.length; i++) {
            // without aggregate functions a skipped group and a stale one both keep the stored
            // values, so the two need no telling apart here
            acceptance[i] =
                    groupOfField[i] == NO_GROUP || decisions[groupOfField[i]] == Decision.FORWARD;
        }
        return acceptance;
    }

    /**
     * Resolves, for every field, what the group arbitrating it makes of the incoming row. Fields
     * taking part in no group always report {@link Decision#FORWARD}, keeping their original
     * behavior.
     *
     * <p>Callers that aggregate need this rather than {@link #resolveAcceptance}, so that they can
     * aggregate a stale row in reverse instead of dropping it.
     *
     * @param oldRow the stored row, or null when there is no stored row yet
     * @param newRow the incoming row
     */
    public Decision[] resolveDecisions(@Nullable InternalRow oldRow, InternalRow newRow) {
        Decision[] decisions = decideGroups(oldRow, newRow);

        Decision[] ofField = new Decision[groupOfField.length];
        for (int i = 0; i < groupOfField.length; i++) {
            ofField[i] =
                    groupOfField[i] == NO_GROUP ? Decision.FORWARD : decisions[groupOfField[i]];
        }
        return ofField;
    }

    /** Decides every group of the schema, indexed by group id. */
    private Decision[] decideGroups(@Nullable InternalRow oldRow, InternalRow newRow) {
        Decision[] decisions = new Decision[readersOfGroup.length];
        for (int groupId = 0; groupId < readersOfGroup.length; groupId++) {
            decisions[groupId] = decide(readersOfGroup[groupId], oldRow, newRow);
        }
        return decisions;
    }

    /**
     * Decides one group, by comparing its sequence columns in the declared order until one of them
     * differs.
     */
    private static Decision decide(
            SequenceReader[] readers, @Nullable InternalRow oldRow, InternalRow newRow) {
        Comparable<?>[] newSequence = new Comparable<?>[readers.length];
        boolean allNull = true;
        for (int i = 0; i < readers.length; i++) {
            newSequence[i] = readers[i].read(newRow);
            if (newSequence[i] != null) {
                allNull = false;
            }
        }
        if (allNull) {
            // the group carries no order information at all
            return Decision.SKIP;
        }
        if (oldRow == null) {
            return Decision.FORWARD;
        }

        for (int i = 0; i < readers.length; i++) {
            int comparison = compare(newSequence[i], readers[i].read(oldRow));
            if (comparison != 0) {
                return comparison > 0 ? Decision.FORWARD : Decision.STALE;
            }
        }
        // equal sequences advance, so that a replayed record still refreshes the group
        return Decision.FORWARD;
    }

    /** Null is treated as the smallest value, consistently with the versioned merge engine. */
    @SuppressWarnings("unchecked")
    private static int compare(@Nullable Comparable<?> left, @Nullable Comparable<?> right) {
        if (left == null) {
            return right == null ? 0 : -1;
        }
        if (right == null) {
            return 1;
        }
        return ((Comparable<Object>) left).compareTo(right);
    }

    /**
     * Returns a reader of the given sequence column, and validates that its type can order a group.
     * The accepted types are the same as the version column of the versioned merge engine, so that
     * both order arbitration mechanisms stay consistent.
     */
    private static SequenceReader createReader(
            String columnName, DataType dataType, int fieldIndex) {
        switch (dataType.getTypeRoot()) {
            case INTEGER:
                return row -> absent(row, fieldIndex) ? null : row.getInt(fieldIndex);
            case BIGINT:
                return row -> absent(row, fieldIndex) ? null : row.getLong(fieldIndex);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int ntzPrecision = ((TimestampType) dataType).getPrecision();
                return row ->
                        absent(row, fieldIndex)
                                ? null
                                : row.getTimestampNtz(fieldIndex, ntzPrecision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int ltzPrecision = ((LocalZonedTimestampType) dataType).getPrecision();
                return row ->
                        absent(row, fieldIndex)
                                ? null
                                : row.getTimestampLtz(fieldIndex, ltzPrecision);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "The sequence column '%s' must be one type of "
                                        + "[INT, BIGINT, TIMESTAMP, TIMESTAMP_LTZ], but is %s.",
                                columnName, dataType));
        }
    }

    /**
     * A row written under an older schema may carry fewer fields than the latest schema, in which
     * case the sequence column is absent and read as null, i.e. the oldest sequence.
     */
    private static boolean absent(InternalRow row, int fieldIndex) {
        return row.getFieldCount() < fieldIndex + 1 || row.isNullAt(fieldIndex);
    }

    /** Reads the sequence value of a sequence column out of a row. */
    @FunctionalInterface
    private interface SequenceReader extends Serializable {

        /** Returns the sequence value, or null if the column is absent or SQL NULL. */
        @Nullable
        Comparable<?> read(InternalRow row);
    }
}
