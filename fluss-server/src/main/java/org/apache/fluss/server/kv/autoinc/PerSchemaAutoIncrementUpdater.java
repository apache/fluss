/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.exception.InvalidTargetColumnException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link AutoIncrementUpdater} implementation that assigns auto-increment values to a specific
 * column based on a fixed schema. It is bound to a particular schema version and assumes the
 * auto-increment column position remains constant within that schema.
 *
 * <p>This class is not thread-safe and is intended to be used within a single-threaded execution
 * context.
 */
@NotThreadSafe
public class PerSchemaAutoIncrementUpdater implements AutoIncrementUpdater {
    private final InternalRow.FieldGetter[] flussFieldGetters;
    private final RowEncoder rowEncoder;
    private final int fieldLength;
    private final int targetColumnIdx;
    private final String autoIncrementColumnName;
    private final SequenceGenerator sequenceGenerator;
    private final short schemaId;
    private final boolean requireInteger;

    public PerSchemaAutoIncrementUpdater(
            KvFormat kvFormat,
            short schemaId,
            Schema schema,
            int autoIncrementColumnId,
            SequenceGenerator sequenceGenerator) {
        DataType[] fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);

        fieldLength = fieldDataTypes.length;
        // getter for the fields in row
        InternalRow.FieldGetter[] flussFieldGetters = new InternalRow.FieldGetter[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            flussFieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
        }
        this.sequenceGenerator = sequenceGenerator;
        this.schemaId = schemaId;
        this.targetColumnIdx = schema.getColumnIds().indexOf(autoIncrementColumnId);
        if (targetColumnIdx == -1) {
            throw new IllegalStateException(
                    String.format(
                            "Auto-increment column ID %d not found in schema columns: %s",
                            autoIncrementColumnId, schema.getColumnIds()));
        }
        this.autoIncrementColumnName = schema.getAutoIncrementColumnNames().get(0);
        this.requireInteger = fieldDataTypes[targetColumnIdx].is(DataTypeRoot.INTEGER);
        this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
        this.flussFieldGetters = flussFieldGetters;
    }

    @Override
    public BinaryValue updateAutoIncrementColumns(BinaryValue rowValue) {
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldLength; i++) {
            if (targetColumnIdx == i) {
                long seq = sequenceGenerator.nextVal();
                // cast to integer if needed
                if (requireInteger) {
                    rowEncoder.encodeField(i, (int) seq);
                } else {
                    rowEncoder.encodeField(i, seq);
                }
            } else {
                // use the row value
                rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(rowValue.row));
            }
        }
        return new BinaryValue(schemaId, rowEncoder.finishRow());
    }

    @Override
    public void validateTargetColumns(@Nullable int[] targetColumnIndexes) {
        if (targetColumnIndexes != null) {
            boolean found = false;
            for (int idx : targetColumnIndexes) {
                if (idx == targetColumnIdx) {
                    found = true;
                    break;
                }
            }
            if (found) {
                throw new InvalidTargetColumnException(
                        String.format(
                                "Auto-increment column [%s] at index %d must not be included in target columns.",
                                autoIncrementColumnName, targetColumnIdx));
            }
        } else {
            // The current table contains an auto-increment column, but no target columns have been
            // specified, which implies all columns (including the auto-increment one) would be
            // updated. This is not allowed.
            throw new InvalidTargetColumnException(
                    String.format(
                            "The table contains an auto-increment column [%s], but update target columns are not explicitly specified. "
                                    + "Please specify the update target columns and exclude the auto-increment column from them.",
                            autoIncrementColumnName));
        }
    }

    @Override
    public boolean hasAutoIncrement() {
        return true;
    }
}
