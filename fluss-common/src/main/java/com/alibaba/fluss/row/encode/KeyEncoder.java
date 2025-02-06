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

package com.alibaba.fluss.row.encode;

import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.compacted.CompactedKeyWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/** An encoder to encode {@link InternalRow} using {@link CompactedKeyWriter}. */
public class KeyEncoder {

    private final InternalRow.FieldGetter[] fieldGetters;

    private final CompactedKeyWriter.FieldWriter[] fieldEncoders;

    private final CompactedKeyWriter compactedEncoder;

    /**
     * Create a key encoder to encode the key of the row with given {@param rowType} in
     * given @{@param primaryKeyIndexes} and {@param partitionKeys}.
     */
    public static KeyEncoder createKeyEncoder(
            RowType rowType, List<String> primaryKeys, List<String> partitionKeys) {

        // to get the col used to encode the key, for partitioned table,
        // we will remove the partition keys from primary keys
        List<String> encodeCols = new ArrayList<>(primaryKeys);
        encodeCols.removeAll(partitionKeys);

        int[] encodeColIndexes = new int[encodeCols.size()];
        for (int i = 0; i < encodeCols.size(); i++) {
            encodeColIndexes[i] = rowType.getFieldIndex(encodeCols.get(i));
        }

        return new KeyEncoder(rowType, encodeColIndexes);
    }

    public KeyEncoder(RowType rowType) {
        this(rowType, IntStream.range(0, rowType.getFieldCount()).toArray());
    }

    public KeyEncoder(RowType rowType, int[] encodeFieldPos) {
        DataType[] encodeDataTypes = new DataType[encodeFieldPos.length];
        for (int i = 0; i < encodeFieldPos.length; i++) {
            encodeDataTypes[i] = rowType.getTypeAt(encodeFieldPos[i]);
        }

        // for get fields from internal row
        fieldGetters = new InternalRow.FieldGetter[encodeFieldPos.length];
        // for encode fields
        fieldEncoders = new CompactedKeyWriter.FieldWriter[encodeFieldPos.length];
        for (int i = 0; i < encodeFieldPos.length; i++) {
            DataType fieldDataType = encodeDataTypes[i];
            fieldGetters[i] = InternalRow.createFieldGetter(fieldDataType, encodeFieldPos[i]);
            fieldEncoders[i] = CompactedKeyWriter.createFieldWriter(fieldDataType);
        }
        compactedEncoder = new CompactedKeyWriter();
    }

    public byte[] encode(InternalRow row) {
        compactedEncoder.reset();
        // iterate all the fields of the row, and encode each field
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldEncoders[i].writeField(compactedEncoder, i, fieldGetters[i].getFieldOrNull(row));
        }
        return compactedEncoder.toBytes();
    }
}
