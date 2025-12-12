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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/** A updater to auto increment column . */
@NotThreadSafe
public class AutoIncColumnProcessor implements AutoIncProcessor {

    private final InternalRow.FieldGetter[] flussFieldGetters;

    private final RowEncoder rowEncoder;

    private final DataType[] fieldDataTypes;
    private final int targetColumnIdx;
    private final IncIDGenerator idGenerator;
    private final short schemaId;

    public AutoIncColumnProcessor(
            KvFormat kvFormat,
            short schemaId,
            Schema schema,
            int targetColumnIdx,
            IncIDGenerator incIDGenerator) {
        this.targetColumnIdx = targetColumnIdx;
        this.fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);

        // getter for the fields in row
        flussFieldGetters = new InternalRow.FieldGetter[fieldDataTypes.length];
        for (int i = 0; i < fieldDataTypes.length; i++) {
            flussFieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
        }
        this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
        this.idGenerator = incIDGenerator;
        this.schemaId = schemaId;
    }

    @Nullable
    @Override
    public BinaryValue processAutoInc(BinaryValue oldValue) {
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldDataTypes.length; i++) {
            if (i == targetColumnIdx) {
                if (oldValue != null && oldValue.row.isNullAt(i)) {
                    rowEncoder.encodeField(i, idGenerator.nextVal());
                }
            } else {
                // use the old row value
                if (oldValue == null) {
                    rowEncoder.encodeField(i, null);
                } else {
                    rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(oldValue.row));
                }
            }
        }
        return new BinaryValue(schemaId, rowEncoder.finishRow());
    }
}
