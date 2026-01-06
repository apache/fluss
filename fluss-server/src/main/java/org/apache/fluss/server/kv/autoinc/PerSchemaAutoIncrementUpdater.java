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

import org.apache.fluss.exception.SequenceOverflowException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.function.LongSupplier;

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
    private final LongSupplier idSupplier;
    private final short schemaId;
    private final String targetColumnName;

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
        this.schemaId = schemaId;
        this.targetColumnIdx = schema.getColumnIds().indexOf(autoIncrementColumnId);
        this.targetColumnName = schema.getColumnName(targetColumnIdx);
        if (targetColumnIdx == -1) {
            throw new IllegalStateException(
                    String.format(
                            "Auto-increment column ID %d not found in schema columns: %s",
                            autoIncrementColumnId, schema.getColumnIds()));
        }

        if (fieldDataTypes[targetColumnIdx].is(DataTypeRoot.INTEGER)) {
            this.idSupplier = () -> checkedNextInt(sequenceGenerator.nextVal());
        } else {
            this.idSupplier = sequenceGenerator::nextVal;
        }
        this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
        this.flussFieldGetters = flussFieldGetters;
    }

    private long checkedNextInt(long value) {
        if (value > Integer.MAX_VALUE) {
            throw new SequenceOverflowException(
                    String.format(
                            "Reached maximum value of sequence \"<%s>\" (2147483647).",
                            targetColumnName));
        }
        return value;
    }

    public BinaryValue updateAutoIncrementColumns(BinaryValue rowValue) {
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldLength; i++) {
            if (targetColumnIdx == i) {
                rowEncoder.encodeField(i, idSupplier.getAsLong());
            } else {
                // use the row value
                rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(rowValue.row));
            }
        }
        return new BinaryValue(schemaId, rowEncoder.finishRow());
    }

    @Override
    public boolean hasAutoIncrement() {
        return true;
    }
}
