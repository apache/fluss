/*
<<<<<<<< HEAD:fluss-common/src/main/java/org/apache/fluss/row/encode/CompactedKeyEncoder.java
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
========
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
>>>>>>>> be8528e4 ([connector] Support spark catalog and introduce some basic classes to support spark read and write):fluss-common/src/main/java/com/alibaba/fluss/row/encode/KeyEncoder.java
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.encode;

<<<<<<<< HEAD:fluss-common/src/main/java/org/apache/fluss/row/encode/CompactedKeyEncoder.java
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedKeyWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

========
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.paimon.PaimonKeyEncoder;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

>>>>>>>> be8528e4 ([connector] Support spark catalog and introduce some basic classes to support spark read and write):fluss-common/src/main/java/com/alibaba/fluss/row/encode/KeyEncoder.java
import java.util.List;

<<<<<<<< HEAD:fluss-common/src/main/java/org/apache/fluss/row/encode/CompactedKeyEncoder.java
/** An encoder to encode {@link InternalRow} using {@link CompactedKeyWriter}. */
public class CompactedKeyEncoder implements KeyEncoder {
========
/** An interface for encoding key of row into bytes. */
public interface KeyEncoder {
>>>>>>>> be8528e4 ([connector] Support spark catalog and introduce some basic classes to support spark read and write):fluss-common/src/main/java/com/alibaba/fluss/row/encode/KeyEncoder.java

    /** Encode the key of given row to byte array. */
    byte[] encodeKey(InternalRow row);

    /**
<<<<<<<< HEAD:fluss-common/src/main/java/org/apache/fluss/row/encode/CompactedKeyEncoder.java
     * Create a key encoder to encode the key of the input row.
     *
     * @param rowType the row type of the input row
     * @param keys the key fields to encode
     */
    public static CompactedKeyEncoder createKeyEncoder(RowType rowType, List<String> keys) {
        int[] encodeColIndexes = new int[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            encodeColIndexes[i] = rowType.getFieldIndex(keys.get(i));
            if (encodeColIndexes[i] == -1) {
                throw new IllegalArgumentException(
                        "Field " + keys.get(i) + " not found in input row type " + rowType);
            }
        }
        return new CompactedKeyEncoder(rowType, encodeColIndexes);
    }

    public CompactedKeyEncoder(RowType rowType) {
        this(rowType, IntStream.range(0, rowType.getFieldCount()).toArray());
    }

    public CompactedKeyEncoder(RowType rowType, int[] encodeFieldPos) {
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

    @Override
    public byte[] encodeKey(InternalRow row) {
        compactedEncoder.reset();
        // iterate all the fields of the row, and encode each field
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldEncoders[i].writeField(compactedEncoder, i, fieldGetters[i].getFieldOrNull(row));
        }
        return compactedEncoder.toBytes();
========
     * Create a key encoder to encode the key array bytes of the input row.
     *
     * @param rowType the row type of the input row
     * @param keyFields the key fields to encode
     * @param lakeFormat the datalake format
     */
    static KeyEncoder of(
            RowType rowType, List<String> keyFields, @Nullable DataLakeFormat lakeFormat) {
        if (lakeFormat == null) {
            // use default compacted key encoder
            return CompactedKeyEncoder.createKeyEncoder(rowType, keyFields);
        } else if (lakeFormat == DataLakeFormat.PAIMON) {
            return new PaimonKeyEncoder(rowType, keyFields);
        } else {
            throw new UnsupportedOperationException("Unsupported datalake format: " + lakeFormat);
        }
>>>>>>>> be8528e4 ([connector] Support spark catalog and introduce some basic classes to support spark read and write):fluss-common/src/main/java/com/alibaba/fluss/row/encode/KeyEncoder.java
    }
}
