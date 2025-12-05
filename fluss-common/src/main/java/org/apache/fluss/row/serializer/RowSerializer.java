/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.types.DataType;

import java.io.Serializable;

/** Serializer for {@link InternalRow} to {@link BinaryRow}. */
public class RowSerializer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final DataType[] fieldTypes;

    private transient AlignedRow reuseRow;
    private transient AlignedRowWriter reuseWriter;
    private transient BinaryWriter.ValueWriter[] valueWriters;

    public RowSerializer(DataType[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public BinaryRow toBinaryRow(InternalRow from) {
        if (from instanceof BinaryRow) {
            return (BinaryRow) from;
        }

        int numFields = from.getFieldCount();
        if (reuseRow == null || reuseRow.getFieldCount() != numFields) {
            reuseRow = new AlignedRow(numFields);
            reuseWriter = new AlignedRowWriter(reuseRow);
        } else {
            reuseWriter.reset();
        }
        if (valueWriters == null || valueWriters.length != numFields) {
            valueWriters = new BinaryWriter.ValueWriter[numFields];
            for (int i = 0; i < numFields; i++) {
                valueWriters[i] = BinaryWriter.createValueWriter(fieldTypes[i]);
            }
        }

        for (int i = 0; i < numFields; i++) {
            if (from.isNullAt(i)) {
                reuseWriter.setNullAt(i);
            } else {
                Object field = InternalRow.createFieldGetter(fieldTypes[i], i).getFieldOrNull(from);
                valueWriters[i].writeValue(reuseWriter, i, field);
            }
        }
        reuseWriter.complete();

        return reuseRow;
    }
}
