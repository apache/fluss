/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv.partialupdate;

import com.alibaba.fluss.exception.InvalidTargetColumnException;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.RowEncoder;
import com.alibaba.fluss.types.DataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.BitSet;

/** A updater to partial update/delete a row. */
@NotThreadSafe
public class PartialUpdater {

    private final InternalRow.FieldGetter[] flussFieldGetters;

    private final RowEncoder rowEncoder;

    private final BitSet partialUpdateCols = new BitSet();
    private final BitSet primaryKeyCols = new BitSet();
    private final DataType[] fieldDataTypes;

    public PartialUpdater(KvFormat kvFormat, Schema schema, int[] targetColumns) {
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
     * Partial update the {@code oldRow} with the given new row {@code partialRow}. The {@code
     * oldRow} may be null, in this case, the field don't exist in the {@code partialRow} will be
     * set to null.
     *
     * @param oldRow the old row to be updated
     * @param partialRow the new row to be updated.
     * @return the updated row
     */
    public BinaryRow updateRow(@Nullable InternalRow oldRow, InternalRow partialRow) {
        rowEncoder.startNewRow();
        // write each field
        for (int i = 0; i < fieldDataTypes.length; i++) {
            // use the partial row value
            if (partialUpdateCols.get(i)) {
                rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(partialRow));
            } else {
                // use the old row value
                if (oldRow == null) {
                    rowEncoder.encodeField(i, null);
                } else {
                    rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(oldRow));
                }
            }
        }
        return rowEncoder.finishRow();
    }

    /**
     * Partial delete the given {@code row}. If all the fields except for {@link #partialUpdateCols}
     * in {@code row} are null, return null. Otherwise, update all the {@link #partialUpdateCols} in
     * the {@code row} except for the primary key columns to null values, return the updated row.
     *
     * @param row the row to be deleted
     * @return the row after partial deleted
     */
    public @Nullable BinaryRow deleteRow(InternalRow row) {
        if (isFieldsNull(row, partialUpdateCols)) {
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
                    rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(row));
                }
            }
            return rowEncoder.finishRow();
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
