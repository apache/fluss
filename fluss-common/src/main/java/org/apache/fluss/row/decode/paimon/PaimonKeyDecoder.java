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

package org.apache.fluss.row.decode.paimon;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.decode.KeyDecoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.List;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An implementation of {@link KeyDecoder} to decode keys following Paimon's encoding strategy.
 *
 * <p>This decoder reverses the encoding performed by {@link
 * org.apache.fluss.row.encode.paimon.PaimonKeyEncoder}.
 *
 * <p>The binary format follows Paimon's BinaryRow layout:
 *
 * <pre>
 * Fixed-length part:
 * - 1 byte header (RowKind)
 * - Null bit set (aligned to 8-byte boundaries)
 * - Field values (8 bytes each):
 *   - For fixed-length types: actual values
 *   - For variable-length types: offset (4 bytes) + length (4 bytes)
 *
 * Variable-length part:
 * - Actual data for variable-length fields (strings, bytes, etc.)
 * </pre>
 */
public class PaimonKeyDecoder implements KeyDecoder {

    private static final int HEADER_SIZE_IN_BITS = 8;

    private final DataType[] keyDataTypes;
    private final FieldReader[] fieldReaders;
    private final int nullBitsSizeInBytes;

    public PaimonKeyDecoder(RowType rowType, List<String> keys) {
        this.keyDataTypes = new DataType[keys.size()];
        this.fieldReaders = new FieldReader[keys.size()];
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            int keyIndex = rowType.getFieldIndex(keys.get(i));
            this.keyDataTypes[i] = rowType.getTypeAt(keyIndex);
            this.fieldReaders[i] = createFieldReader(keyDataTypes[i], i);
        }
    }

    @Override
    public InternalRow decodeKey(byte[] keyBytes) {
        MemorySegment segment = MemorySegment.wrap(keyBytes);
        GenericRow row = new GenericRow(keyDataTypes.length);

        for (int i = 0; i < keyDataTypes.length; i++) {
            if (isNullAt(segment, i)) {
                row.setField(i, null);
            } else {
                Object value = fieldReaders[i].readField(segment, 0);
                row.setField(i, value);
            }
        }

        return row;
    }

    private boolean isNullAt(MemorySegment segment, int pos) {
        return BinarySegmentUtils.bitGet(segment, 0, pos + HEADER_SIZE_IN_BITS);
    }

    private int getFieldOffset(int pos) {
        return nullBitsSizeInBytes + pos * 8;
    }

    private static int calculateBitSetWidthInBytes(int arity) {
        return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8;
    }

    private FieldReader createFieldReader(DataType fieldType, int pos) {
        final int fieldOffset = getFieldOffset(pos);

        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return (segment, baseOffset) -> segment.getBoolean(baseOffset + fieldOffset);
            case TINYINT:
                return (segment, baseOffset) -> segment.get(baseOffset + fieldOffset);
            case SMALLINT:
                return (segment, baseOffset) -> segment.getShort(baseOffset + fieldOffset);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (segment, baseOffset) -> segment.getInt(baseOffset + fieldOffset);
            case BIGINT:
                return (segment, baseOffset) -> segment.getLong(baseOffset + fieldOffset);
            case FLOAT:
                return (segment, baseOffset) -> segment.getFloat(baseOffset + fieldOffset);
            case DOUBLE:
                return (segment, baseOffset) -> segment.getDouble(baseOffset + fieldOffset);
            case CHAR:
            case STRING:
                return (segment, baseOffset) -> readString(segment, baseOffset, fieldOffset);
            case BINARY:
            case BYTES:
                return (segment, baseOffset) -> readBinary(segment, baseOffset, fieldOffset);
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                return (segment, baseOffset) ->
                        readDecimal(
                                segment, baseOffset, fieldOffset, decimalPrecision, decimalScale);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                return (segment, baseOffset) ->
                        readTimestampNtz(segment, baseOffset, fieldOffset, timestampNtzPrecision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                return (segment, baseOffset) ->
                        readTimestampLtz(segment, baseOffset, fieldOffset, timestampLtzPrecision);
            default:
                throw new IllegalArgumentException(
                        "Unsupported type for Paimon key decoder: " + fieldType);
        }
    }

    private BinaryString readString(MemorySegment segment, int baseOffset, int fieldOffset) {
        final long offsetAndLen = segment.getLong(baseOffset + fieldOffset);
        return BinarySegmentUtils.readBinaryString(
                new MemorySegment[] {segment}, 0, fieldOffset, offsetAndLen);
    }

    private byte[] readBinary(MemorySegment segment, int baseOffset, int fieldOffset) {
        final long offsetAndLen = segment.getLong(baseOffset + fieldOffset);
        return BinarySegmentUtils.readBinary(
                new MemorySegment[] {segment}, 0, fieldOffset, offsetAndLen);
    }

    private Decimal readDecimal(
            MemorySegment segment, int baseOffset, int fieldOffset, int precision, int scale) {
        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(
                    segment.getLong(baseOffset + fieldOffset), precision, scale);
        }

        final long offsetAndSize = segment.getLong(baseOffset + fieldOffset);
        return BinarySegmentUtils.readDecimal(
                new MemorySegment[] {segment}, 0, offsetAndSize, precision, scale);
    }

    private TimestampNtz readTimestampNtz(
            MemorySegment segment, int baseOffset, int fieldOffset, int precision) {
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(segment.getLong(baseOffset + fieldOffset));
        }

        final long offsetAndNanoOfMilli = segment.getLong(baseOffset + fieldOffset);
        final int nanoOfMillisecond = (int) offsetAndNanoOfMilli;
        final int subOffset = (int) (offsetAndNanoOfMilli >> 32);
        final long millisecond = segment.getLong(subOffset);
        return TimestampNtz.fromMillis(millisecond, nanoOfMillisecond);
    }

    private TimestampLtz readTimestampLtz(
            MemorySegment segment, int baseOffset, int fieldOffset, int precision) {
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(segment.getLong(baseOffset + fieldOffset));
        }

        final long offsetAndNanoOfMilli = segment.getLong(baseOffset + fieldOffset);
        final int nanoOfMillisecond = (int) offsetAndNanoOfMilli;
        final int subOffset = (int) (offsetAndNanoOfMilli >> 32);
        final long millisecond = segment.getLong(subOffset);
        return TimestampLtz.fromEpochMillis(millisecond, nanoOfMillisecond);
    }

    /** Accessor for reading fields from Paimon binary format. */
    interface FieldReader {
        Object readField(MemorySegment segment, int baseOffset);
    }
}
