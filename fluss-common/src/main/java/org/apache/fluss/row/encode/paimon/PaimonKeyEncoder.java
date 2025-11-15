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

package org.apache.fluss.row.encode.paimon;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;

/** An implementation of {@link KeyEncoder} to follow Paimon's encoding strategy. */
public class PaimonKeyEncoder implements KeyEncoder {

    private final InternalRow.FieldGetter[] fieldGetters;

    private final PaimonBinaryRowWriter.FieldWriter[] fieldEncoders;

    private final PaimonBinaryRowWriter paimonBinaryRowWriter;

    private final List<DataType> keyDataTypes = new ArrayList<>();

    public PaimonKeyEncoder(RowType rowType, List<String> keys) {
        // for get fields from fluss internal row
        fieldGetters = new InternalRow.FieldGetter[keys.size()];
        // for encode fields into paimon
        fieldEncoders = new PaimonBinaryRowWriter.FieldWriter[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            int keyIndex = rowType.getFieldIndex(keys.get(i));
            DataType keyDataType = rowType.getTypeAt(keyIndex);
            keyDataTypes.add(keyDataType);
            fieldGetters[i] = InternalRow.createFieldGetter(keyDataType, keyIndex);
            fieldEncoders[i] = PaimonBinaryRowWriter.createFieldWriter(keyDataType);
        }

        paimonBinaryRowWriter = new PaimonBinaryRowWriter(keys.size());
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        paimonBinaryRowWriter.reset();
        // always be ChangeType.INSERT for bucketed row
        paimonBinaryRowWriter.writeChangeType(ChangeType.INSERT);
        // iterate all the fields of the row, and encode each field
        for (int i = 0; i < fieldGetters.length; i++) {
            DataType dataType = keyDataTypes.get(i);
            Object value = fieldGetters[i].getFieldOrNull(row);
            if (dataType.getTypeRoot() == DataTypeRoot.ROW) {
                handleNestedRow((InternalRow) value, paimonBinaryRowWriter, i, dataType);
            } else {
                fieldEncoders[i].writeField(
                        paimonBinaryRowWriter, i, fieldGetters[i].getFieldOrNull(row));
            }
        }
        return paimonBinaryRowWriter.toBytes();
    }

    private void handleNestedRow(
            InternalRow row,
            PaimonBinaryRowWriter targetWriter,
            int targetFieldIndex,
            DataType rowType) {
        DataType[] subTypes = rowType.getChildren().toArray(new DataType[0]);
        PaimonBinaryRowWriter subWriter = new PaimonBinaryRowWriter(subTypes.length);

        for (int i = 0; i < subTypes.length; i++) {
            DataType subType = subTypes[i];
            InternalRow.FieldGetter subFieldGetter = InternalRow.createFieldGetter(subType, i);
            Object value = subFieldGetter.getFieldOrNull(row);

            if (subType.getTypeRoot() == DataTypeRoot.ROW) {
                // Recursively handle deeper nesting.
                handleNestedRow((InternalRow) value, subWriter, i, subType);
            } else {
                // Direct encoding of basic types.
                PaimonBinaryRowWriter.FieldWriter fieldEncoder =
                        PaimonBinaryRowWriter.createFieldWriter(subType);
                fieldEncoder.writeField(subWriter, i, value);
            }
        }

        // Write child layer data to the variable length area of the specified field in the parent
        // layer.
        targetWriter.writeSegmentsToVarLenPart(
                targetFieldIndex,
                new MemorySegment[] {subWriter.getSegment()},
                0,
                subWriter.getSegment().size());
    }
}
