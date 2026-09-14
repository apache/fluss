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

package org.apache.fluss.server.kv.partialupdate;

import org.apache.fluss.exception.InvalidTargetColumnException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.BitSet;

/** A updater to partial update/delete a row. */
@NotThreadSafe
public class PartialUpdater {

    private final short targetSchemaId;
    private final InternalRow.FieldGetter[] flussFieldGetters;

    private final RowEncoder rowEncoder;

    private final BitSet partialUpdateCols = new BitSet();
    private final BitSet primaryKeyCols = new BitSet();
    private final boolean updatePrimaryKeyOnly;
    private final DataType[] fieldDataTypes;

    /** The target columns that are NOT NULL, checked by {@link #updateRow}. */
    private final BitSet notNullTargetCols = new BitSet();

    /** The same columns without the primary key, checked by {@link #deleteRow}. */
    private final BitSet notNullNonPkTargetCols = new BitSet();

    private final String[] fieldNames;

    public PartialUpdater(KvFormat kvFormat, short schemaId, Schema schema, int[] targetColumns) {
        this.targetSchemaId = schemaId;
        for (int targetColumn : targetColumns) {
            partialUpdateCols.set(targetColumn);
        }
        for (int pkIndex : schema.getPrimaryKeyIndexes()) {
            primaryKeyCols.set(pkIndex);
        }
        this.fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);
        this.fieldNames = schema.getRowType().getFieldNames().toArray(new String[0]);
        for (int i = 0; i < fieldDataTypes.length; i++) {
            if (partialUpdateCols.get(i) && !fieldDataTypes[i].isNullable()) {
                notNullTargetCols.set(i);
                if (!primaryKeyCols.get(i)) {
                    notNullNonPkTargetCols.set(i);
                }
            }
        }
        sanityCheck(schema, targetColumns);

        // getter for the fields in row
        flussFieldGetters = new InternalRow.FieldGetter[fieldDataTypes.length];
        for (int i = 0; i < fieldDataTypes.length; i++) {
            flussFieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
        }
        this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
        this.updatePrimaryKeyOnly = partialUpdateCols.equals(primaryKeyCols);
    }

    private void sanityCheck(Schema schema, int[] targetColumns) {
        // check the target columns contains the primary key
        for (int pkIndex : schema.getPrimaryKeyIndexes()) {
            if (!partialUpdateCols.get(pkIndex)) {
                throw new InvalidTargetColumnException(
                        String.format(
                                "The target write columns %s must contain the primary key columns %s.",
                                schema.getColumnNames(targetColumns),
                                schema.getColumnNames(schema.getPrimaryKeyIndexes())));
            }
        }

        BitSet autoIncrementCols = new BitSet();
        for (String name : schema.getAutoIncrementColumnNames()) {
            autoIncrementCols.set(schema.getRowType().getFieldIndex(name));
        }

        // an omitted column is written as null, so it must be nullable. auto increment columns
        // are always omitted and only get their value after the merge.
        for (int i = 0; i < fieldDataTypes.length; i++) {
            if (!partialUpdateCols.get(i) && !fieldDataTypes[i].isNullable()) {
                String columnName = schema.getRowType().getFieldNames().get(i);
                if (autoIncrementCols.get(i)) {
                    throw new InvalidTargetColumnException(
                            String.format(
                                    "Partial Update requires the auto increment column %s to be nullable, since it is always omitted from the target columns and assigned by the server.",
                                    columnName));
                }
                throw new InvalidTargetColumnException(
                        String.format(
                                "Partial Update requires all columns omitted from the target columns to be nullable, but omitted column %s is NOT NULL.",
                                columnName));
            }
        }

        // the auto increment column is always set, so a partial delete could never collapse
        // the row and would always fail on a NOT NULL non-primary-key target column.
        if (!autoIncrementCols.isEmpty() && !notNullNonPkTargetCols.isEmpty()) {
            throw new InvalidTargetColumnException(
                    String.format(
                            "Partial Update on a table with an auto increment column requires all target columns except the primary key to be nullable, but target column %s is NOT NULL, since the auto increment column is always set and a partial delete could therefore never succeed.",
                            fieldNames[notNullNonPkTargetCols.nextSetBit(0)]));
        }
    }

    /**
     * Partial update the {@code oldValue} with the given new row {@code partialValue}. The {@code
     * oldValue} may be null, in this case, the field don't exist in the {@code partialRow} will be
     * set to null.
     *
     * @param oldValue the old value to be updated
     * @param partialValue the new value to be updated.
     * @return the updated value (schema id + row bytes)
     */
    public BinaryValue updateRow(@Nullable BinaryValue oldValue, BinaryValue partialValue) {
        if (updatePrimaryKeyOnly && oldValue != null) {
            // only primary key columns are updated, return the old value directly
            return oldValue;
        }

        checkNotNullTargetCols(partialValue);

        rowEncoder.startNewRow();
        // write each field
        for (int i = 0; i < fieldDataTypes.length; i++) {
            // use the partial row value
            if (partialUpdateCols.get(i)) {
                rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(partialValue.row));
            } else {
                // use the old row value, the old row may be old schema with fewer fields,
                // in this case, the missing fields will be set to null
                if (oldValue == null || oldValue.row.getFieldCount() < i + 1) {
                    rowEncoder.encodeField(i, null);
                } else {
                    rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(oldValue.row));
                }
            }
        }
        return new BinaryValue(targetSchemaId, rowEncoder.finishRow());
    }

    /**
     * Rejects a null in a non-nullable slot, which would corrupt the encoded row. Runs before any
     * field getter, since a getter deserializes the whole row and would fail first.
     */
    private void checkNotNullTargetCols(BinaryValue partialValue) {
        for (int i = notNullTargetCols.nextSetBit(0);
                i >= 0;
                i = notNullTargetCols.nextSetBit(i + 1)) {
            if (partialValue.row.isNullAt(i)) {
                throw new InvalidTargetColumnException(
                        String.format(
                                "Target column %s is NOT NULL but the written row has no value for it.",
                                fieldNames[i]));
            }
        }
    }

    /**
     * Partial delete the given {@code value}. If all the fields except for {@link
     * #partialUpdateCols} in {@code value.row} are null, return null. Otherwise, update all the
     * {@link #partialUpdateCols} in the {@code value.row} except for the primary key columns to
     * null values, return the updated value.
     *
     * @param value the value to be deleted
     * @return the value after partial deleted
     * @throws InvalidTargetColumnException if a non-primary-key target column is NOT NULL, since it
     *     would have to be set to null
     */
    public @Nullable BinaryValue deleteRow(BinaryValue value) {
        if (isFieldsNull(value.row, partialUpdateCols)) {
            // the whole row is removed, so no column is set to null
            return null;
        } else {
            if (!notNullNonPkTargetCols.isEmpty()) {
                throw new InvalidTargetColumnException(
                        String.format(
                                "Partial Delete sets the target columns to null, so it requires all target columns except primary key to be nullable, but target column %s is NOT NULL.",
                                fieldNames[notNullNonPkTargetCols.nextSetBit(0)]));
            }
            rowEncoder.startNewRow();
            // write each field
            for (int i = 0; i < fieldDataTypes.length; i++) {
                // neither in target columns not primary key columns,
                // write null value,
                if (!primaryKeyCols.get(i) && partialUpdateCols.get(i)) {
                    rowEncoder.encodeField(i, null);
                } else {
                    // use the old row value, the old row may be old schema with fewer fields,
                    // in this case, the missing fields will be set to null
                    if (value.row.getFieldCount() < i + 1) {
                        rowEncoder.encodeField(i, null);
                    } else {
                        rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(value.row));
                    }
                }
            }
            return new BinaryValue(targetSchemaId, rowEncoder.finishRow());
        }
    }

    private boolean isFieldsNull(InternalRow internalRow, BitSet excludeColumns) {
        for (int i = 0; i < internalRow.getFieldCount(); i++) {
            // not in exclude columns and is not null
            if (!excludeColumns.get(i) && !internalRow.isNullAt(i)) {
                return false;
            }
        }
        return true;
    }
}
