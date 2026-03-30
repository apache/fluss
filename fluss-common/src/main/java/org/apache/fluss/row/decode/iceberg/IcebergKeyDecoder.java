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

package org.apache.fluss.row.decode.iceberg;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.decode.KeyDecoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.List;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Decoder for Iceberg-encoded key bytes.
 *
 * <p>This decoder reverses the encoding performed by {@link
 * org.apache.fluss.row.encode.iceberg.IcebergKeyEncoder}.
 *
 * <p>The binary format follows Iceberg's encoding strategy:
 *
 * <ul>
 *   <li>Only single key field is supported
 *   <li>Integer types are encoded as long (8 bytes) in LITTLE_ENDIAN
 *   <li>Time is encoded as microseconds (long, 8 bytes)
 *   <li>Timestamp is encoded as microseconds (long, 8 bytes)
 *   <li>Strings/bytes/decimals have NO length prefix (direct binary data)
 *   <li>Decimals use BIG_ENDIAN byte order
 * </ul>
 */
public class IcebergKeyDecoder implements KeyDecoder {

    private final FieldReader fieldReader;

    public IcebergKeyDecoder(RowType rowType, List<String> keys) {
        checkArgument(
                keys.size() == 1,
                "Key fields must have exactly one field for iceberg format, but got: %s",
                keys);

        int keyIndex = rowType.getFieldIndex(keys.get(0));
        DataType keyDataType = rowType.getTypeAt(keyIndex);
        this.fieldReader = createFieldReader(keyDataType);
    }

    @Override
    public InternalRow decodeKey(byte[] keyBytes) {
        MemorySegment segment = MemorySegment.wrap(keyBytes);
        GenericRow row = new GenericRow(1);
        Object value = fieldReader.readField(segment, 0);
        row.setField(0, value);
        return row;
    }

    private FieldReader createFieldReader(DataType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case INTEGER:
            case DATE:
                // Integer is encoded as long (8 bytes)
                return (segment, offset) -> (int) segment.getLong(offset);

            case TIME_WITHOUT_TIME_ZONE:
                // Time encoded as microseconds long
                return (segment, offset) -> {
                    long micros = segment.getLong(offset);
                    return (int) (micros / 1000L);
                };

            case BIGINT:
                return MemorySegment::getLong;

            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (segment, offset) -> {
                    long micros = segment.getLong(offset);
                    long millis = micros / 1000L;
                    int nanoOfMillis = (int) ((micros % 1000L) * 1000L);
                    return TimestampNtz.fromMillis(millis, nanoOfMillis);
                };

            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                return (segment, offset) -> {
                    // Decimal bytes are written directly without length prefix
                    int length = segment.size() - offset;
                    byte[] bytes = new byte[length];
                    segment.get(offset, bytes, 0, length);
                    return Decimal.fromUnscaledBytes(bytes, decimalPrecision, decimalScale);
                };

            case STRING:
            case CHAR:
                return (segment, offset) -> {
                    // String bytes are written directly without length prefix
                    int length = segment.size() - offset;
                    byte[] bytes = new byte[length];
                    segment.get(offset, bytes, 0, length);
                    return BinaryString.fromBytes(bytes);
                };

            case BINARY:
            case BYTES:
                return (segment, offset) -> {
                    // Binary bytes are written directly without length prefix
                    int length = segment.size() - offset;
                    byte[] bytes = new byte[length];
                    segment.get(offset, bytes, 0, length);
                    return bytes;
                };

            default:
                throw new IllegalArgumentException(
                        "Unsupported type for Iceberg key decoder: " + fieldType);
        }
    }

    /** Accessor for reading fields from Iceberg binary format. */
    interface FieldReader {
        Object readField(MemorySegment segment, int offset);
    }
}
