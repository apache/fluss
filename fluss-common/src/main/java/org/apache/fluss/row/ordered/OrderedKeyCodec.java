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

package org.apache.fluss.row.ordered;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Schema-bound ascending key format shared by the ordered key encoder and decoder.
 *
 * <p>Fields are concatenated without a row header or type tags. Signed numbers use big-endian
 * sign-flipped representations. Variable-length fields use eight-byte groups followed by a padding
 * marker. Every field is self-delimiting for its schema, so encoding leading fields produces a byte
 * prefix of matching complete keys. Ordering is unsigned bytewise, matching RocksDB's default
 * comparator. Null keys, floating-point types and constructed types are not supported.
 *
 * <p>Binary fields preserve the signed-byte ordering used by Fluss predicates; strings preserve
 * BinaryString's unsigned UTF-8 ordering. The field schema and encoding version must be supplied
 * out of band; these bytes are not interchangeable with compacted keys.
 *
 * <p>Signed integer widths are 1/2/4/8 bytes. Decimal width is the minimum signed width that holds
 * every unscaled value of the declared precision. Timestamps contain an ordered eight-byte
 * millisecond followed, for precision above three, by a four-byte nanosecond remainder. A byte
 * group ends in {@code 0xff - paddingCount}; a complete final group is followed by an empty
 * terminating group. All padding bytes are zero, after any binary-field sign transformation.
 */
@Internal
@NotThreadSafe
public final class OrderedKeyCodec {

    private static final int GROUP_SIZE = 8;

    private final DataType[] types;
    private final InternalRow.FieldGetter[] getters;
    private final int[] positions;
    private final int[] decimalWidths;
    private final int rowArity;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final DataOutputStream output = new DataOutputStream(buffer);

    /** Creates a codec for fields selected in their logical key order. */
    public OrderedKeyCodec(RowType rowType, List<String> keyFields) {
        checkNotNull(rowType, "rowType");
        checkNotNull(keyFields, "keyFields");
        checkArgument(!keyFields.isEmpty(), "Key fields must not be empty.");
        checkArgument(new HashSet<>(keyFields).size() == keyFields.size(), "Duplicate key fields.");
        rowArity = rowType.getFieldCount();
        types = new DataType[keyFields.size()];
        getters = new InternalRow.FieldGetter[keyFields.size()];
        positions = new int[keyFields.size()];
        decimalWidths = new int[keyFields.size()];
        for (int i = 0; i < types.length; i++) {
            int position = rowType.getFieldIndex(keyFields.get(i));
            checkArgument(position >= 0, "Unknown key field: %s", keyFields.get(i));
            types[i] = rowType.getTypeAt(position);
            positions[i] = position;
            validateType(types[i]);
            if (types[i].getTypeRoot() == DataTypeRoot.DECIMAL) {
                decimalWidths[i] = decimalWidth(types[i]);
            }
            getters[i] = InternalRow.createFieldGetter(types[i], position);
        }
    }

    /** Encodes the selected non-null fields; the returned bytes are independent of later calls. */
    public byte[] encodeKey(InternalRow row) {
        checkNotNull(row, "row");
        checkArgument(row.getFieldCount() == rowArity, "Row arity does not match the schema.");
        buffer.reset();
        try {
            for (int i = 0; i < types.length; i++) {
                checkArgument(!row.isNullAt(positions[i]), "Null key field at position %s.", i);
                Object value = getters[i].getFieldOrNull(row);
                checkArgument(value != null, "Null key field at position %s.", i);
                writeField(types[i], value, decimalWidths[i]);
            }
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Could not encode an in-memory ordered key.", e);
        }
    }

    /** Decodes exactly the selected fields, rejecting truncated, noncanonical or trailing bytes. */
    public InternalRow decodeKey(byte[] bytes) {
        checkNotNull(bytes, "bytes");
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
        GenericRow row = new GenericRow(types.length);
        try {
            for (int i = 0; i < types.length; i++) {
                row.setField(i, readField(input, types[i], decimalWidths[i]));
            }
            checkArgument(input.available() == 0, "Trailing bytes in ordered key.");
            return row;
        } catch (IOException e) {
            throw new IllegalArgumentException("Truncated ordered key.", e);
        }
    }

    /**
     * Returns the exclusive upper bound for all byte strings beginning with {@code prefix}. Returns
     * null if no finite upper bound exists, including for the empty prefix. The bound is a seek
     * target, not necessarily a decodable key.
     */
    @Nullable
    public static byte[] prefixUpperBound(byte[] prefix) {
        checkNotNull(prefix, "prefix");
        for (int i = prefix.length - 1; i >= 0; i--) {
            if ((prefix[i] & 0xff) != 0xff) {
                byte[] upper = Arrays.copyOf(prefix, i + 1);
                upper[i]++;
                return upper;
            }
        }
        return null;
    }

    private static void validateType(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case CHAR:
            case STRING:
            case BINARY:
            case BYTES:
            case DECIMAL:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return;
            default:
                throw new IllegalArgumentException("Unsupported ordered key type: " + type);
        }
    }

    private void writeField(DataType type, Object value, int decimalWidth) throws IOException {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                output.writeByte((boolean) value ? 1 : 0);
                return;
            case TINYINT:
                output.writeByte(((byte) value) ^ 0x80);
                return;
            case SMALLINT:
                output.writeShort(((short) value) ^ 0x8000);
                return;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                output.writeInt(((int) value) ^ Integer.MIN_VALUE);
                return;
            case BIGINT:
                output.writeLong(((long) value) ^ Long.MIN_VALUE);
                return;
            case CHAR:
            case STRING:
                writeBytes(((BinaryString) value).toBytes(), false);
                return;
            case BINARY:
            case BYTES:
                writeBytes((byte[]) value, true);
                return;
            case DECIMAL:
                writeDecimal(type, (Decimal) value, decimalWidth);
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz ntz = (TimestampNtz) value;
                writeTimestamp(type, ntz.getMillisecond(), ntz.getNanoOfMillisecond());
                return;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampLtz ltz = (TimestampLtz) value;
                writeTimestamp(type, ltz.getEpochMillisecond(), ltz.getNanoOfMillisecond());
                return;
            default:
                throw new IllegalArgumentException("Unsupported ordered key type: " + type);
        }
    }

    private static Object readField(DataInputStream input, DataType type, int decimalWidth)
            throws IOException {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                int bool = input.readUnsignedByte();
                checkArgument(bool <= 1, "Invalid ordered boolean.");
                return bool != 0;
            case TINYINT:
                return (byte) (input.readUnsignedByte() ^ 0x80);
            case SMALLINT:
                return (short) (input.readUnsignedShort() ^ 0x8000);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return input.readInt() ^ Integer.MIN_VALUE;
            case BIGINT:
                return input.readLong() ^ Long.MIN_VALUE;
            case CHAR:
            case STRING:
                return BinaryString.fromBytes(readBytes(input, false));
            case BINARY:
            case BYTES:
                return readBytes(input, true);
            case DECIMAL:
                byte[] decimal = new byte[decimalWidth];
                input.readFully(decimal);
                decimal[0] ^= (byte) 0x80;
                BigDecimal number = new BigDecimal(new BigInteger(decimal), getScale(type));
                checkArgument(
                        number.precision() <= getPrecision(type), "Decimal precision overflow.");
                return Decimal.fromBigDecimal(number, getPrecision(type), getScale(type));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                long millis = input.readLong() ^ Long.MIN_VALUE;
                int nanos = getPrecision(type) > 3 ? input.readInt() : 0;
                validateTimestamp(type, millis, nanos);
                return type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                        ? TimestampNtz.fromMillis(millis, nanos)
                        : TimestampLtz.fromEpochMillis(millis, nanos);
            default:
                throw new IllegalArgumentException("Unsupported ordered key type: " + type);
        }
    }

    private void writeBytes(byte[] bytes, boolean signed) throws IOException {
        int position = 0;
        while (true) {
            int size = Math.min(GROUP_SIZE, bytes.length - position);
            for (int i = 0; i < GROUP_SIZE; i++) {
                output.writeByte(i < size ? bytes[position + i] ^ (signed ? 0x80 : 0) : 0);
            }
            output.writeByte(0xff - (GROUP_SIZE - size));
            position += size;
            if (size < GROUP_SIZE) {
                return;
            }
        }
    }

    private static byte[] readBytes(DataInputStream input, boolean signed) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] group = new byte[GROUP_SIZE];
        while (true) {
            input.readFully(group);
            int padding = 0xff - input.readUnsignedByte();
            checkArgument(padding <= GROUP_SIZE, "Invalid ordered byte-group marker.");
            for (int i = 0; i < GROUP_SIZE - padding; i++) {
                bytes.write(group[i] ^ (signed ? 0x80 : 0));
            }
            for (int i = GROUP_SIZE - padding; i < GROUP_SIZE; i++) {
                checkArgument(group[i] == 0, "Nonzero ordered byte-group padding.");
            }
            if (padding > 0) {
                return bytes.toByteArray();
            }
        }
    }

    private void writeDecimal(DataType type, Decimal value, int width) throws IOException {
        checkArgument(value.scale() == getScale(type), "Decimal scale does not match the schema.");
        BigDecimal number = value.toBigDecimal();
        checkArgument(number.precision() <= getPrecision(type), "Decimal precision overflow.");
        byte[] bytes = number.unscaledValue().toByteArray();
        byte[] fixed = new byte[width];
        Arrays.fill(fixed, (byte) (number.signum() < 0 ? 0xff : 0));
        System.arraycopy(bytes, 0, fixed, width - bytes.length, bytes.length);
        fixed[0] ^= (byte) 0x80;
        output.write(fixed);
    }

    private static int decimalWidth(DataType type) {
        return (BigInteger.TEN.pow(getPrecision(type)).subtract(BigInteger.ONE).bitLength() + 8)
                / 8;
    }

    private void writeTimestamp(DataType type, long millis, int nanos) throws IOException {
        validateTimestamp(type, millis, nanos);
        output.writeLong(millis ^ Long.MIN_VALUE);
        if (getPrecision(type) > 3) {
            output.writeInt(nanos);
        }
    }

    private static void validateTimestamp(DataType type, long millis, int nanos) {
        int precision = getPrecision(type);
        checkArgument(nanos >= 0 && nanos < 1_000_000, "Invalid timestamp nanosecond remainder.");
        int factor = 1;
        for (int i = precision; i < (precision < 3 ? 3 : 9); i++) {
            factor *= 10;
        }
        checkArgument(
                precision < 3 ? millis % factor == 0 && nanos == 0 : nanos % factor == 0,
                "Timestamp precision does not match the schema.");
    }
}
