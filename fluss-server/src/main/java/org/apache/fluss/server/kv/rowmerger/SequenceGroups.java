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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The sequence groups declared on a schema, resolved into field positions so that a merger can
 * arbitrate each group on its own.
 *
 * <p>A column is put under the order of one or more sequence columns (see {@link
 * Schema.SequenceGroup#getSequenceColumns()}) and then only takes an incoming value when those
 * sequence columns are not older than the stored ones. Columns ordered by the very same sequence
 * columns form one group advancing together, while different groups advance independently: within a
 * single write one group may advance and another may not. That is what distinguishes sequence
 * groups from the versioned merge engine, which arbitrates the whole row with a single version.
 *
 * <p>One instance serves all keys of a table. The group decisions are reused per record, safe
 * because the write path is single threaded under KvTablet's write lock.
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

    /**
     * For each group, the field indexes of its sequence columns, in the declared comparison order.
     */
    private final int[][] sequenceFieldsOfGroup;

    /**
     * For each group, the comparators of its sequence columns, matching {@link
     * #sequenceFieldsOfGroup}. They compare non-null values only, so that null ordering is decided
     * once in {@link #decide} instead of per column type.
     */
    private final SequenceComparator[][] comparatorsOfGroup;

    /** Stands for a group left out of the arbitration by {@link #restrictTo}. */
    private static final int[] NO_FIELDS = {};

    /** The decision of every group for the record under arbitration, indexed by group id. */
    private final Decision[] groupDecisions;

    /** The primary key field indexes, which hold the same value in both rows being merged. */
    private final BitSet primaryKeyFields;

    private SequenceGroups(
            int[] groupOfField,
            int[][] sequenceFieldsOfGroup,
            SequenceComparator[][] comparatorsOfGroup,
            BitSet primaryKeyFields) {
        this.groupOfField = groupOfField;
        this.sequenceFieldsOfGroup = sequenceFieldsOfGroup;
        this.comparatorsOfGroup = comparatorsOfGroup;
        this.groupDecisions = new Decision[comparatorsOfGroup.length];
        Arrays.fill(groupDecisions, Decision.FORWARD);
        this.primaryKeyFields = primaryKeyFields;
    }

    /**
     * Resolves the sequence groups of the given schema, or returns null if the schema declares
     * none. Returning null lets callers keep their original merge path untouched.
     */
    @Nullable
    public static SequenceGroups create(Schema schema) {
        List<Schema.SequenceGroup> declared = schema.getSequenceGroups();
        if (declared.isEmpty()) {
            return null;
        }

        RowType rowType = schema.getRowType();
        int fieldCount = rowType.getFieldCount();

        int[] groupOfField = new int[fieldCount];
        Arrays.fill(groupOfField, NO_GROUP);

        int[][] sequenceFieldsOfGroup = new int[declared.size()][];
        SequenceComparator[][] comparatorsOfGroup = new SequenceComparator[declared.size()][];
        for (int groupId = 0; groupId < declared.size(); groupId++) {
            Schema.SequenceGroup group = declared.get(groupId);
            List<String> sequenceColumns = group.getSequenceColumns();
            int[] sequenceFields = new int[sequenceColumns.size()];
            SequenceComparator[] comparators = new SequenceComparator[sequenceColumns.size()];
            for (int i = 0; i < sequenceColumns.size(); i++) {
                String sequenceColumn = sequenceColumns.get(i);
                int sequenceField = rowType.getFieldIndex(sequenceColumn);
                checkArgument(
                        sequenceField >= 0,
                        "The sequence column '%s' doesn't exist in schema.",
                        sequenceColumn);
                sequenceFields[i] = sequenceField;
                comparators[i] =
                        createComparator(
                                sequenceColumn, rowType.getTypeAt(sequenceField), sequenceField);
                // a sequence column takes part in the very group it orders, otherwise it would
                // always accept incoming values and report a sequence no longer matching them
                groupOfField[sequenceField] = groupId;
            }
            sequenceFieldsOfGroup[groupId] = sequenceFields;
            comparatorsOfGroup[groupId] = comparators;

            for (String protectedColumn : group.getProtectedColumns()) {
                int fieldIndex = rowType.getFieldIndex(protectedColumn);
                checkArgument(
                        fieldIndex >= 0,
                        "The protected column '%s' doesn't exist in schema.",
                        protectedColumn);
                groupOfField[fieldIndex] = groupId;
            }
        }

        BitSet primaryKeyFields = new BitSet();
        for (int pkIndex : schema.getPrimaryKeyIndexes()) {
            primaryKeyFields.set(pkIndex);
        }
        return new SequenceGroups(
                groupOfField, sequenceFieldsOfGroup, comparatorsOfGroup, primaryKeyFields);
    }

    /**
     * Returns the groups arbitrating only the fields the target set covers, since a group whose
     * sequence is never stored must not decide on values the row keeps.
     *
     * @param targetFields the row field indexes the write targets
     */
    public SequenceGroups restrictTo(BitSet targetFields) {
        int[] restricted = groupOfField.clone();
        for (int i = 0; i < restricted.length; i++) {
            if (!targetFields.get(i)) {
                restricted[i] = NO_GROUP;
            }
        }

        int[][] restrictedFields = sequenceFieldsOfGroup.clone();
        for (int groupId = 0; groupId < restrictedFields.length; groupId++) {
            if (!coversGroup(restricted, groupId)) {
                restrictedFields[groupId] = NO_FIELDS;
            }
        }
        return new SequenceGroups(
                restricted, restrictedFields, comparatorsOfGroup, primaryKeyFields);
    }

    /** Returns whether any field still belongs to the given group. */
    private static boolean coversGroup(int[] groupOfField, int groupId) {
        for (int owner : groupOfField) {
            if (owner == groupId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Decides every covered group for the incoming row, into the reused decision buffer. The
     * decisions live until the next arbitration.
     *
     * @param oldRow the stored row, or null when there is no stored row yet
     * @param newRow the incoming row
     */
    public void arbitrate(@Nullable InternalRow oldRow, InternalRow newRow) {
        for (int groupId = 0; groupId < comparatorsOfGroup.length; groupId++) {
            groupDecisions[groupId] =
                    decide(
                            sequenceFieldsOfGroup[groupId],
                            comparatorsOfGroup[groupId],
                            oldRow,
                            newRow);
        }
    }

    /**
     * Returns whether the field may take the value carried by the last arbitrated row. A field is
     * held back only when the group arbitrating it doesn't advance; without aggregate functions a
     * skipped group and a stale one both keep the stored values, so the two need no telling apart
     * here.
     */
    public boolean accepts(int fieldIndex) {
        int groupId = groupOfField[fieldIndex];
        return groupId == NO_GROUP || groupDecisions[groupId] == Decision.FORWARD;
    }

    /** Returns the field count of the schema these groups were resolved from. */
    public int fieldCount() {
        return groupOfField.length;
    }

    /**
     * Returns what the group arbitrating the field makes of the last arbitrated row. A field taking
     * part in no group always reports {@link Decision#FORWARD}, keeping its original behavior;
     * callers that aggregate use this rather than {@link #accepts}, so that they can aggregate a
     * stale row in reverse instead of dropping it.
     */
    public Decision decisionOf(int fieldIndex) {
        int groupId = groupOfField[fieldIndex];
        return groupId == NO_GROUP ? Decision.FORWARD : groupDecisions[groupId];
    }

    /** Returns whether every arbitrated group advances. */
    public boolean acceptsEveryArbitratedGroup() {
        for (Decision decision : groupDecisions) {
            if (decision != Decision.FORWARD) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether every target field of the write is rejected, so the write changes nothing. A
     * target field is either a primary key, holding the same value in both rows, or arbitrated by a
     * group that rejects the incoming value; a field outside the groups takes the incoming value
     * unconditionally, so it counts as a contribution.
     *
     * <p>Without aggregate functions SKIP and STALE alike keep the stored values, while an
     * aggregating engine still folds a stale record in through aggReversed, so there only SKIP
     * rejects.
     *
     * @param aggregating whether the merging engine aggregates, i.e. whether a stale record still
     *     contributes
     */
    public boolean rejectsEveryTargetField(BitSet targetFields, boolean aggregating) {
        for (int i = targetFields.nextSetBit(0); i >= 0; i = targetFields.nextSetBit(i + 1)) {
            if (primaryKeyFields.get(i)) {
                continue;
            }
            if (groupOfField[i] == NO_GROUP) {
                return false;
            }
            Decision decision = groupDecisions[groupOfField[i]];
            if (decision == Decision.FORWARD || (aggregating && decision == Decision.STALE)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Decides one group, by comparing its sequence columns in the declared order until one of them
     * differs. The values are compared column by column and never stored, so deciding allocates
     * nothing and never boxes a sequence value.
     */
    private static Decision decide(
            int[] sequenceFields,
            SequenceComparator[] comparators,
            @Nullable InternalRow oldRow,
            InternalRow newRow) {
        boolean carriesValue = false;
        for (int field : sequenceFields) {
            if (!absent(newRow, field)) {
                carriesValue = true;
                break;
            }
        }
        if (!carriesValue) {
            // the group carries no order information at all
            return Decision.SKIP;
        }
        if (oldRow == null) {
            return Decision.FORWARD;
        }

        for (int i = 0; i < sequenceFields.length; i++) {
            int field = sequenceFields[i];
            // SQL NULL orders before every value, and a column absent from an older schema is null
            int comparison;
            if (absent(newRow, field)) {
                comparison = absent(oldRow, field) ? 0 : -1;
            } else if (absent(oldRow, field)) {
                comparison = 1;
            } else {
                comparison = comparators[i].compareNonNull(oldRow, newRow);
            }
            if (comparison != 0) {
                return comparison > 0 ? Decision.FORWARD : Decision.STALE;
            }
        }
        // equal sequences advance, so that a replayed record still refreshes the group
        return Decision.FORWARD;
    }

    /**
     * Returns a comparator of the given sequence column, and validates that its type can order a
     * group. The accepted types are the same as the version column of the versioned merge engine,
     * so that both order arbitration mechanisms stay consistent.
     */
    private static SequenceComparator createComparator(
            String columnName, DataType dataType, int fieldIndex) {
        switch (dataType.getTypeRoot()) {
            case INTEGER:
                return (oldRow, newRow) ->
                        Integer.compare(newRow.getInt(fieldIndex), oldRow.getInt(fieldIndex));
            case BIGINT:
                return (oldRow, newRow) ->
                        Long.compare(newRow.getLong(fieldIndex), oldRow.getLong(fieldIndex));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int ntzPrecision = ((TimestampType) dataType).getPrecision();
                return (oldRow, newRow) ->
                        newRow.getTimestampNtz(fieldIndex, ntzPrecision)
                                .compareTo(oldRow.getTimestampNtz(fieldIndex, ntzPrecision));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int ltzPrecision = ((LocalZonedTimestampType) dataType).getPrecision();
                return (oldRow, newRow) ->
                        newRow.getTimestampLtz(fieldIndex, ltzPrecision)
                                .compareTo(oldRow.getTimestampLtz(fieldIndex, ltzPrecision));
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

    /**
     * Compares the sequence value of one column between the stored and the incoming row. Both
     * values are known to be non-null; the null ordering lives in {@link #decide} so it is decided
     * once instead of per column type.
     */
    @FunctionalInterface
    private interface SequenceComparator extends Serializable {

        /** Returns a negative number when the incoming value is older than the stored one. */
        int compareNonNull(InternalRow oldRow, InternalRow newRow);
    }
}
