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

package org.apache.fluss.row.encode.hudi;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** An implementation of {@link KeyEncoder} to follow Hudi's encoding strategy. */
public class HudiKeyEncoder implements KeyEncoder {

    private final InternalRow.FieldGetter[] fieldGetters;

    private final HudiBinaryRowWriter.FieldWriter[] fieldEncoders;

    public HudiKeyEncoder(RowType rowType, List<String> keys) {
        // for get fields from fluss internal row
        fieldGetters = new InternalRow.FieldGetter[keys.size()];
        // for encode fields into hudi
        fieldEncoders = new HudiBinaryRowWriter.FieldWriter[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            int keyIndex = rowType.getFieldIndex(keys.get(i));
            DataType keyDataType = rowType.getTypeAt(keyIndex);
            fieldGetters[i] = InternalRow.createFieldGetter(keyDataType, keyIndex);
            fieldEncoders[i] = HudiBinaryRowWriter.createFieldWriter(keyDataType);
        }
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        List<String> values = new ArrayList<>();
        // iterate all the fields of the row, and encode each field
        for (int i = 0; i < fieldGetters.length; i++) {
            Object value = fieldGetters[i].getFieldOrNull(row);
            if (value instanceof BinaryString) {
                values.add(((BinaryString) value).toString());
            } else {
                values.add(String.valueOf(value));
            }
        }
        int hashCode = Arrays.asList(values.toArray(new String[0])).hashCode();

        return ByteBuffer.allocate(4).putInt(hashCode).array();
    }
}
