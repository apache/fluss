/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.encode.paimon;

import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.util.List;

/** An implementation of {@link KeyEncoder} to follow Paimon's encoding strategy. */
public class PaimonKeyEncoder implements KeyEncoder {

    private final InternalRow.FieldGetter[] fieldGetters;

    private final PaimonBinaryRowWriter.FieldWriter[] fieldEncoders;

    private final PaimonBinaryRowWriter paimonBinaryRowWriter;

    public PaimonKeyEncoder(RowType rowType, List<String> keys) {
        // for get fields from fluss internal row
        fieldGetters = new InternalRow.FieldGetter[keys.size()];
        // for encode fields into paimon
        fieldEncoders = new PaimonBinaryRowWriter.FieldWriter[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            int keyIndex = rowType.getFieldIndex(keys.get(i));
            DataType keyDataType = rowType.getTypeAt(keyIndex);
            fieldGetters[i] = InternalRow.createFieldGetter(keyDataType, keyIndex);
            fieldEncoders[i] = PaimonBinaryRowWriter.createFieldWriter(keyDataType);
        }

        paimonBinaryRowWriter = new PaimonBinaryRowWriter(keys.size());
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        paimonBinaryRowWriter.reset();
        // always be RowKind.INSERT for bucketed row
        paimonBinaryRowWriter.writeChangeType(ChangeType.INSERT);
        // iterate all the fields of the row, and encode each field
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldEncoders[i].writeField(
                    paimonBinaryRowWriter, i, fieldGetters[i].getFieldOrNull(row));
        }
        return paimonBinaryRowWriter.toBytes();
    }
}
