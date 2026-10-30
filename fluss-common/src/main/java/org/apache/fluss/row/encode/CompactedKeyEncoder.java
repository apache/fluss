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

package org.apache.fluss.row.encode;

import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.compacted.CompactedKeyWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** An encoder to encode {@link InternalRow} using {@link CompactedKeyWriter}. */
public class CompactedKeyEncoder implements KeyEncoder {

    private final DataType[] encodeDataTypes;

    private final InternalRow.FieldGetter[] fieldGetters;

    private final BinaryWriter.ValueWriter[] fieldEncoders;

    private final CompactedKeyWriter compactedEncoder;

    /**
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
        encodeDataTypes = new DataType[encodeFieldPos.length];
        for (int i = 0; i < encodeFieldPos.length; i++) {
            encodeDataTypes[i] = rowType.getTypeAt(encodeFieldPos[i]);
        }

        // for get fields from internal row
        fieldGetters = new InternalRow.FieldGetter[encodeFieldPos.length];
        // for encode fields
        fieldEncoders = new BinaryWriter.ValueWriter[encodeFieldPos.length];
        for (int i = 0; i < encodeFieldPos.length; i++) {
            DataType fieldDataType = encodeDataTypes[i];
            fieldGetters[i] = InternalRow.createFieldGetter(fieldDataType, encodeFieldPos[i]);
            fieldEncoders[i] = CompactedKeyWriter.createValueWriter(fieldDataType);
        }
        compactedEncoder = new CompactedKeyWriter();
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        compactedEncoder.reset();
        // iterate all the fields of the row, and encode each field
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldEncoders[i].writeValue(compactedEncoder, i, fieldGetters[i].getFieldOrNull(row));
        }
        return compactedEncoder.toBytes();
    }

    @Override
    public Optional<KeyPrefixLayout> getPrefixLayout(int fieldCount) {
        checkArgument(
                fieldCount > 0 && fieldCount <= encodeDataTypes.length,
                "Prefix field count must be between 1 and %s.",
                encodeDataTypes.length);
        List<KeyPrefixLayout.Component> components = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            if (!appendFieldLayout(components, encodeDataTypes[i])) {
                return Optional.empty();
            }
        }
        return Optional.of(new KeyPrefixLayout(components));
    }

    private static boolean appendFieldLayout(List<KeyPrefixLayout.Component> steps, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                steps.add(KeyPrefixLayout.Component.FIXED1);
                break;
            case SMALLINT:
                steps.add(KeyPrefixLayout.Component.FIXED2);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                steps.add(KeyPrefixLayout.Component.VARINT32);
                break;
            case BIGINT:
                steps.add(KeyPrefixLayout.Component.VARINT64);
                break;
            case FLOAT:
                steps.add(KeyPrefixLayout.Component.FIXED4);
                break;
            case DOUBLE:
                steps.add(KeyPrefixLayout.Component.FIXED8);
                break;
            case CHAR:
            case STRING:
            case BINARY:
            case BYTES:
                steps.add(KeyPrefixLayout.Component.BYTES_VARINT32);
                break;
            case DECIMAL:
                steps.add(
                        Decimal.isCompact(getPrecision(type))
                                ? KeyPrefixLayout.Component.VARINT64
                                : KeyPrefixLayout.Component.BYTES_VARINT32);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                steps.add(KeyPrefixLayout.Component.VARINT64);
                if (!TimestampNtz.isCompact(getPrecision(type))) {
                    steps.add(KeyPrefixLayout.Component.VARINT32);
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                steps.add(KeyPrefixLayout.Component.VARINT64);
                if (!TimestampLtz.isCompact(getPrecision(type))) {
                    steps.add(KeyPrefixLayout.Component.VARINT32);
                }
                break;
            default:
                return false;
        }
        return true;
    }
}
