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
    private final DataType[] fieldDataTypes;

    public PartialUpdater(KvFormat kvFormat, short schemaId, Schema schema, int[] targetColumns) {
        this.targetSchemaId = schemaId;
        for (int targetColumn : targetColumns) {
            partialUpdateCols.set(targetColumn);
        }
        for (int pkIndex : schema.getPrimaryKeyIndexes()) {
            primaryKeyCols.set(pkIndex);
        }
        this.fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);
        sanityCheck(schema, targetColumns);

        // getter for the fields in row
        flussFieldGetters = new InternalRow.FieldGetter[fieldDataTypes.length];
        for (int i = 0; i < fieldDataTypes.length; i++) {
            flussFieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
        }
        this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
    }

    private void sanityCheck(Schema schema, int[] targetColumns) {
        BitSet pkColumnSet = new BitSet();
        // check the target columns contains the primary key
        for (int pkIndex : schema.getPrimaryKeyIndexes()) {
            if (!partialUpdateCols.get(pkIndex)) {
                throw new InvalidTargetColumnException(
                        String.format(
                                "The target write columns %s must contain the primary key columns %s.",
                                schema.getColumnNames(targetColumns),
                                schema.getColumnNames(schema.getPrimaryKeyIndexes())));
            }
            pkColumnSet.set(pkIndex);
        }

        // check the columns not in targetColumns should be nullable
        for (int i = 0; i < fieldDataTypes.length; i++) {
            // the columns not in primary key should be nullable
            if (!pkColumnSet.get(i)) {
                if (!fieldDataTypes[i].isNullable()) {
                    throw new InvalidTargetColumnException(
                            String.format(
                                    "Partial Update requires all columns except primary key to be nullable, but column %s is NOT NULL.",
                                    schema.getRowType().getFieldNames().get(i)));
                }
            }
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
        rowEncoder.startNewRow();
        // write each field
        for (int i = 0; i < fieldDataTypes.length; i++) {
            // use the partial row value
            if (partialUpdateCols.get(i)) {
                rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(partialValue.row));
            } else {
                // use the old row value
                if (oldValue == null) {
                    rowEncoder.encodeField(i, null);
                } else {
                    rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(oldValue.row));
                }
            }
        }
        return new BinaryValue(targetSchemaId, rowEncoder.finishRow());
    }

    /**
     * Partial delete the given {@code value}. If all the fields except for {@link
     * #partialUpdateCols} in {@code value.row} are null, return null. Otherwise, update all the
     * {@link #partialUpdateCols} in the {@code value.row} except for the primary key columns to
     * null values, return the updated value.
     *
     * @param value the value to be deleted
     * @return the value after partial deleted
     */
    public @Nullable BinaryValue deleteRow(BinaryValue value) {
        if (isFieldsNull(value.row, partialUpdateCols)) {
            return null;
        } else {
            rowEncoder.startNewRow();
            // write each field
            for (int i = 0; i < fieldDataTypes.length; i++) {
                // neither in target columns not primary key columns,
                // write null value,
                if (!primaryKeyCols.get(i) && partialUpdateCols.get(i)) {
                    rowEncoder.encodeField(i, null);
                } else {
                    // use the old row value
                    rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(value.row));
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
