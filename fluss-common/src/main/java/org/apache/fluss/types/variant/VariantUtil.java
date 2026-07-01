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

package org.apache.fluss.types.variant;

import org.apache.fluss.annotation.Internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Low-level utility methods for Variant binary encoding/decoding following the Parquet Variant
 * Binary Encoding specification.
 *
 * <h3>Why this class exists (mirror of Arrow shaded VariantUtil)</h3>
 *
 * <p>The Arrow shaded {@code parquet-variant} library bundles {@code
 * org.apache.fluss.shaded.arrow.org.apache.parquet.variant.VariantUtil}, which implements the same
 * Parquet Variant Binary Encoding spec covered here. Unfortunately that class — and all of its
 * constants ({@code BASIC_TYPE_*}, {@code PRIMITIVE_TYPE_*}, {@code findFieldId}, {@code
 * encodeObject}, …) — is package-private in upstream parquet-variant, so Fluss code outside that
 * package cannot call into it. This class is therefore a Fluss-side mirror of the same spec,
 * preserving the exact byte layout so values written via Arrow {@code VariantBuilder} stay
 * interoperable with values read here. High-level accessors on {@link Variant} delegate to the
 * Arrow shaded {@code Variant} class wherever possible — see the read-path notes on {@link Variant}
 * — so this {@code VariantUtil} is intentionally limited to byte/header inspection, field-id
 * manipulation, and primitive encoders that the Arrow delegate cannot satisfy.
 *
 * <h3>Metadata format</h3>
 *
 * <pre>
 * [header byte] [dictionary_size (offset_size bytes LE)] [offsets (offset_size bytes each LE)] [string bytes]
 *
 * header: bits 0-3 = version (must be 1), bit 4 = sorted_strings flag, bits 6-7 = offset_size_minus_1
 * offset_size: (header bits 6-7) + 1, giving 1-4 bytes per offset
 * dictionary_size: number of strings in the dictionary
 * offsets: (dictionary_size + 1) offsets into the string bytes area
 * </pre>
 *
 * <h3>Value format</h3>
 *
 * <pre>
 * [value_header byte] [value_data...]
 *
 * value_header bits 0-1 = basic_type:
 *   0 = Primitive (bits 2-6 encode primitive type ID)
 *   1 = ShortString (bits 2-6 encode string length)
 *   2 = Object
 *   3 = Array
 * </pre>
 */
@Internal
public final class VariantUtil {

    private VariantUtil() {}

    // --------------------------------------------------------------------------------------------
    // Constants: Basic Types (2-bit, stored in value_header bits 0-1)
    // --------------------------------------------------------------------------------------------

    public static final int BASIC_TYPE_PRIMITIVE = 0;
    public static final int BASIC_TYPE_SHORT_STRING = 1;
    public static final int BASIC_TYPE_OBJECT = 2;
    public static final int BASIC_TYPE_ARRAY = 3;
    public static final int BASIC_TYPE_MASK = 0x03;

    // --------------------------------------------------------------------------------------------
    // Constants: Primitive Type IDs (5-bit, stored in value_header bits 2-6)
    // --------------------------------------------------------------------------------------------

    public static final int PRIMITIVE_TYPE_NULL = 0;
    public static final int PRIMITIVE_TYPE_TRUE = 1;
    public static final int PRIMITIVE_TYPE_FALSE = 2;
    public static final int PRIMITIVE_TYPE_INT8 = 3;
    public static final int PRIMITIVE_TYPE_INT16 = 4;
    public static final int PRIMITIVE_TYPE_INT32 = 5;
    public static final int PRIMITIVE_TYPE_INT64 = 6;
    public static final int PRIMITIVE_TYPE_DOUBLE = 7;
    public static final int PRIMITIVE_TYPE_DECIMAL4 = 8;
    public static final int PRIMITIVE_TYPE_DECIMAL8 = 9;
    public static final int PRIMITIVE_TYPE_DECIMAL16 = 10;
    public static final int PRIMITIVE_TYPE_DATE = 11;
    public static final int PRIMITIVE_TYPE_TIMESTAMP = 12;
    public static final int PRIMITIVE_TYPE_TIMESTAMP_NTZ = 13;
    public static final int PRIMITIVE_TYPE_FLOAT = 14;
    public static final int PRIMITIVE_TYPE_BINARY = 15;
    public static final int PRIMITIVE_TYPE_STRING = 16;

    // --------------------------------------------------------------------------------------------
    // Constants: Metadata
    // --------------------------------------------------------------------------------------------

    public static final int METADATA_VERSION = 1;
    public static final int METADATA_VERSION_MASK = 0x0F;
    public static final int METADATA_SORTED_STRINGS_BIT = 0x10;

    /**
     * Bits 6-7 of the metadata header encode {@code offset_size_minus_1}. Fluss always writes
     * 4-byte offsets (offset_size=4, so offset_size_minus_1=3). Shifting 3 left by 6 positions
     * yields {@code 0xC0}. Setting these bits makes the metadata header
     * Parquet-Variant-spec-compliant so that external systems (e.g. Flink's {@code BinaryVariant})
     * can correctly interpret the variable-width offset encoding.
     */
    public static final int METADATA_OFFSET_SIZE_BITS = 3 << 6; // 0xC0

    /** Empty metadata: version 1, 0 dictionary entries. */
    public static final byte[] EMPTY_METADATA;

    static {
        ByteBuffer buf = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(
                (byte)
                        (METADATA_VERSION
                                | METADATA_SORTED_STRINGS_BIT
                                | METADATA_OFFSET_SIZE_BITS));
        buf.putInt(0);
        EMPTY_METADATA = buf.array();
    }

    // --------------------------------------------------------------------------------------------
    // Value header decoding
    // --------------------------------------------------------------------------------------------

    /** Returns the basic type (0-3) from the value bytes at the given offset. */
    public static int basicType(byte[] value, int offset) {
        return (value[offset] & 0xFF) & BASIC_TYPE_MASK;
    }

    /** Returns the primitive type ID from the value header (6-bit type_info field). */
    public static int primitiveTypeId(byte[] value, int offset) {
        return ((value[offset] & 0xFF) >> 2) & 0x3F;
    }

    /** Returns the short string length from the value header (6-bit type_info field, max 63). */
    public static int shortStringLength(byte[] value, int offset) {
        return ((value[offset] & 0xFF) >> 2) & 0x3F;
    }

    /** Returns true if the value at the given offset represents null. */
    public static boolean isNull(byte[] value, int offset) {
        return basicType(value, offset) == BASIC_TYPE_PRIMITIVE
                && primitiveTypeId(value, offset) == PRIMITIVE_TYPE_NULL;
    }

    // --------------------------------------------------------------------------------------------
    // Primitive value reading
    // --------------------------------------------------------------------------------------------

    /** Reads a boolean from the value bytes. */
    public static boolean getBoolean(byte[] value, int offset) {
        int typeId = primitiveTypeId(value, offset);
        if (typeId == PRIMITIVE_TYPE_TRUE) {
            return true;
        } else if (typeId == PRIMITIVE_TYPE_FALSE) {
            return false;
        }
        throw new IllegalStateException("Not a boolean variant value");
    }

    /** Reads an int8 from the value bytes. */
    public static byte getByte(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_INT8);
        return value[offset + 1];
    }

    /** Reads an int16 from the value bytes. */
    public static short getShort(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_INT16);
        return readShortLE(value, offset + 1);
    }

    /** Reads an int32 from the value bytes. */
    public static int getInt(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_INT32);
        return readIntLE(value, offset + 1);
    }

    /** Reads an int64 from the value bytes. */
    public static long getLong(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_INT64);
        return readLongLE(value, offset + 1);
    }

    /** Reads a float from the value bytes. */
    public static float getFloat(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_FLOAT);
        return Float.intBitsToFloat(readIntLE(value, offset + 1));
    }

    /** Reads a double from the value bytes. */
    public static double getDouble(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_DOUBLE);
        return Double.longBitsToDouble(readLongLE(value, offset + 1));
    }

    /** Reads a string from the value bytes (handles both short string and primitive string). */
    public static String getString(byte[] value, int offset) {
        int basic = basicType(value, offset);
        if (basic == BASIC_TYPE_SHORT_STRING) {
            int len = shortStringLength(value, offset);
            return new String(value, offset + 1, len, StandardCharsets.UTF_8);
        } else if (basic == BASIC_TYPE_PRIMITIVE) {
            checkPrimitiveType(value, offset, PRIMITIVE_TYPE_STRING);
            int len = readIntLE(value, offset + 1);
            return new String(value, offset + 5, len, StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Not a string variant value");
    }

    /** Reads binary data from the value bytes. */
    public static byte[] getBinary(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_BINARY);
        int len = readIntLE(value, offset + 1);
        return Arrays.copyOfRange(value, offset + 5, offset + 5 + len);
    }

    // --------------------------------------------------------------------------------------------
    // Decimal value reading/writing
    // --------------------------------------------------------------------------------------------

    /**
     * Reads a decimal value from the value bytes.
     *
     * <p>Decimal encoding: [header(1)][scale(1)][mantissa(4/8/16 bytes little-endian)]
     *
     * @return the BigDecimal value
     */
    public static BigDecimal getDecimal(byte[] value, int offset) {
        int typeId = primitiveTypeId(value, offset);
        int scale = value[offset + 1] & 0xFF;
        int mantissaOffset = offset + 2;
        BigInteger unscaled;
        switch (typeId) {
            case PRIMITIVE_TYPE_DECIMAL4:
                unscaled = BigInteger.valueOf(readIntLE(value, mantissaOffset));
                break;
            case PRIMITIVE_TYPE_DECIMAL8:
                unscaled = BigInteger.valueOf(readLongLE(value, mantissaOffset));
                break;
            case PRIMITIVE_TYPE_DECIMAL16:
                // 16-byte little-endian to big-endian for BigInteger
                byte[] be = new byte[16];
                for (int i = 0; i < 16; i++) {
                    be[15 - i] = value[mantissaOffset + i];
                }
                unscaled = new BigInteger(be);
                break;
            default:
                throw new IllegalStateException("Not a decimal variant value, type: " + typeId);
        }
        return new BigDecimal(unscaled, scale);
    }

    /**
     * Encodes a decimal value. Automatically selects DECIMAL4, DECIMAL8, or DECIMAL16 based on the
     * unscaled value magnitude.
     */
    public static byte[] encodeDecimal(BigDecimal value) {
        int scale = value.scale();
        BigInteger unscaled = value.unscaledValue();
        int bitLen = unscaled.bitLength(); // sign bit not counted

        if (bitLen < 32) {
            // DECIMAL4: header(1) + scale(1) + 4 bytes
            byte[] result = new byte[6];
            result[0] = primitiveHeader(PRIMITIVE_TYPE_DECIMAL4);
            result[1] = (byte) scale;
            writeIntLE(result, 2, unscaled.intValue());
            return result;
        } else if (bitLen < 64) {
            // DECIMAL8: header(1) + scale(1) + 8 bytes
            byte[] result = new byte[10];
            result[0] = primitiveHeader(PRIMITIVE_TYPE_DECIMAL8);
            result[1] = (byte) scale;
            writeLongLE(result, 2, unscaled.longValue());
            return result;
        } else {
            // DECIMAL16: header(1) + scale(1) + 16 bytes
            byte[] result = new byte[18];
            result[0] = primitiveHeader(PRIMITIVE_TYPE_DECIMAL16);
            result[1] = (byte) scale;
            // Big-endian BigInteger to 16-byte little-endian
            byte[] be = unscaled.toByteArray();
            // Sign-extend to 16 bytes and reverse to little-endian
            byte fill = (byte) (unscaled.signum() < 0 ? 0xFF : 0x00);
            Arrays.fill(result, 2, 18, fill);
            for (int i = 0; i < be.length; i++) {
                result[2 + (be.length - 1 - i)] = be[i];
            }
            return result;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Timestamp value reading/writing
    // --------------------------------------------------------------------------------------------

    /**
     * Reads a timestamp value (microseconds since epoch) from the value bytes.
     *
     * @return microseconds since epoch
     */
    public static long getTimestamp(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_TIMESTAMP);
        return readLongLE(value, offset + 1);
    }

    /**
     * Reads a timestamp without timezone value (microseconds since epoch) from the value bytes.
     *
     * @return microseconds since epoch
     */
    public static long getTimestampNtz(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_TIMESTAMP_NTZ);
        return readLongLE(value, offset + 1);
    }

    /** Encodes a timestamp value (microseconds since epoch). */
    public static byte[] encodeTimestamp(long microsSinceEpoch) {
        byte[] result = new byte[9];
        result[0] = primitiveHeader(PRIMITIVE_TYPE_TIMESTAMP);
        writeLongLE(result, 1, microsSinceEpoch);
        return result;
    }

    /** Encodes a timestamp without timezone value (microseconds since epoch). */
    public static byte[] encodeTimestampNtz(long microsSinceEpoch) {
        byte[] result = new byte[9];
        result[0] = primitiveHeader(PRIMITIVE_TYPE_TIMESTAMP_NTZ);
        writeLongLE(result, 1, microsSinceEpoch);
        return result;
    }

    // --------------------------------------------------------------------------------------------
    // Object field access
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the number of fields in an object value.
     *
     * <p>Object encoding: [header(1)] [num_fields(4)] [field_id_list] [field_offset_list]
     * [field_values]
     */
    public static int objectSize(byte[] value, int offset) {
        if (basicType(value, offset) != BASIC_TYPE_OBJECT) {
            throw new IllegalStateException("Not an object variant value");
        }
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        return readUnsignedLE(value, offset + 1, sizeBytes);
    }

    /**
     * Finds the field ID for a given field name in the metadata dictionary.
     *
     * @return the field ID, or -1 if not found
     */
    public static int findFieldId(byte[] metadata, String fieldName) {
        int dictSize = metadataDictSize(metadata);
        boolean sorted = isMetadataSorted(metadata);
        byte[] nameBytes = fieldName.getBytes(StandardCharsets.UTF_8);

        if (sorted) {
            return binarySearchFieldName(metadata, dictSize, nameBytes);
        } else {
            return linearSearchFieldName(metadata, dictSize, nameBytes);
        }
    }

    /**
     * Finds the value offset for a given field ID in an object value. Uses binary search since
     * field IDs are stored in sorted order by {@link #encodeObject}.
     *
     * @return the value offset within the value byte array, or -1 if field ID not found
     */
    public static int findFieldValueOffset(byte[] value, int offset, int fieldId) {
        if (basicType(value, offset) != BASIC_TYPE_OBJECT) {
            return -1;
        }
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        int numFields = readUnsignedLE(value, offset + 1, sizeBytes);
        int idSize = ((typeInfo >> 2) & 0x3) + 1;
        int offsetSize = (typeInfo & 0x3) + 1;
        int idStart = offset + 1 + sizeBytes;
        int offsetStart = idStart + numFields * idSize;
        int dataStart = offsetStart + (numFields + 1) * offsetSize;

        // Binary search on sorted field IDs
        int lo = 0, hi = numFields - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int midId = readUnsignedLE(value, idStart + mid * idSize, idSize);
            if (midId < fieldId) {
                lo = mid + 1;
            } else if (midId > fieldId) {
                hi = mid - 1;
            } else {
                int fieldOffset = readUnsignedLE(value, offsetStart + mid * offsetSize, offsetSize);
                return dataStart + fieldOffset;
            }
        }
        return -1;
    }

    /**
     * Returns the field ID (metadata dictionary index) of the i-th field in an object value.
     *
     * @param value the value bytes
     * @param offset the offset to the object value
     * @param fieldIndex the field index (0-based)
     * @return the field ID (metadata dictionary index)
     */
    public static int objectFieldId(byte[] value, int offset, int fieldIndex) {
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        int idSize = ((typeInfo >> 2) & 0x3) + 1;
        int idStart = offset + 1 + sizeBytes;
        return readUnsignedLE(value, idStart + fieldIndex * idSize, idSize);
    }

    /**
     * Returns the value offset of the i-th field in an object value.
     *
     * @param value the value bytes
     * @param offset the offset to the object value
     * @param fieldIndex the field index (0-based)
     * @return the absolute offset of the field value within the value byte array
     */
    public static int objectFieldValueOffset(byte[] value, int offset, int fieldIndex) {
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        int numFields = readUnsignedLE(value, offset + 1, sizeBytes);
        int idSize = ((typeInfo >> 2) & 0x3) + 1;
        int offsetSize = (typeInfo & 0x3) + 1;
        int idStart = offset + 1 + sizeBytes;
        int offsetStart = idStart + numFields * idSize;
        int dataStart = offsetStart + (numFields + 1) * offsetSize;
        int fieldOffset = readUnsignedLE(value, offsetStart + fieldIndex * offsetSize, offsetSize);
        return dataStart + fieldOffset;
    }

    // --------------------------------------------------------------------------------------------
    // Array element access
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the number of elements in an array value.
     *
     * <p>Array encoding: [header(1)] [num_elements(4)] [offset_list] [element_values]
     */
    public static int arraySize(byte[] value, int offset) {
        if (basicType(value, offset) != BASIC_TYPE_ARRAY) {
            throw new IllegalStateException("Not an array variant value");
        }
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        return readUnsignedLE(value, offset + 1, sizeBytes);
    }

    public static int arrayElementOffset(byte[] value, int offset, int index) {
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        int numElements = readUnsignedLE(value, offset + 1, sizeBytes);
        if (index < 0 || index >= numElements) {
            throw new IndexOutOfBoundsException(
                    "Array index " + index + " out of bounds [0, " + numElements + ")");
        }
        int offsetSize = (typeInfo & 0x3) + 1;
        int offsetListStart = offset + 1 + sizeBytes;
        int elementValuesStart = offsetListStart + (numElements + 1) * offsetSize;
        int elementOffset = readUnsignedLE(value, offsetListStart + index * offsetSize, offsetSize);
        return elementValuesStart + elementOffset;
    }

    // --------------------------------------------------------------------------------------------
    // Value size calculation
    // --------------------------------------------------------------------------------------------

    /** Computes the total byte size of a value starting at the given offset. */
    public static int valueSize(byte[] value, int offset) {
        int basic = basicType(value, offset);
        switch (basic) {
            case BASIC_TYPE_PRIMITIVE:
                return primitiveValueSize(value, offset);
            case BASIC_TYPE_SHORT_STRING:
                return 1 + shortStringLength(value, offset);
            case BASIC_TYPE_OBJECT:
                return objectValueSize(value, offset);
            case BASIC_TYPE_ARRAY:
                return arrayValueSize(value, offset);
            default:
                throw new IllegalStateException("Unknown basic type: " + basic);
        }
    }

    private static int primitiveValueSize(byte[] value, int offset) {
        int typeId = primitiveTypeId(value, offset);
        switch (typeId) {
            case PRIMITIVE_TYPE_NULL:
            case PRIMITIVE_TYPE_TRUE:
            case PRIMITIVE_TYPE_FALSE:
                return 1; // header only
            case PRIMITIVE_TYPE_INT8:
                return 2; // header + 1 byte
            case PRIMITIVE_TYPE_INT16:
                return 3; // header + 2 bytes
            case PRIMITIVE_TYPE_INT32:
            case PRIMITIVE_TYPE_DATE:
            case PRIMITIVE_TYPE_FLOAT:
                return 5; // header + 4 bytes
            case PRIMITIVE_TYPE_INT64:
            case PRIMITIVE_TYPE_DOUBLE:
            case PRIMITIVE_TYPE_TIMESTAMP:
            case PRIMITIVE_TYPE_TIMESTAMP_NTZ:
                return 9; // header + 8 bytes
            case PRIMITIVE_TYPE_DECIMAL4:
                return 6; // header(1) + scale(1) + 4 bytes mantissa
            case PRIMITIVE_TYPE_DECIMAL8:
                return 10; // header(1) + scale(1) + 8 bytes mantissa
            case PRIMITIVE_TYPE_DECIMAL16:
                return 18; // header(1) + scale(1) + 16 bytes mantissa
            case PRIMITIVE_TYPE_BINARY:
            case PRIMITIVE_TYPE_STRING:
                {
                    int len = readIntLE(value, offset + 1);
                    return 5 + len; // header + 4-byte length + data
                }
            default:
                throw new IllegalStateException("Unknown primitive type: " + typeId);
        }
    }

    private static int objectValueSize(byte[] value, int offset) {
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        int numFields = readUnsignedLE(value, offset + 1, sizeBytes);
        int idSize = ((typeInfo >> 2) & 0x3) + 1;
        int offsetSize = (typeInfo & 0x3) + 1;
        int idStart = offset + 1 + sizeBytes;
        int offsetStart = idStart + numFields * idSize;
        int totalValueSize =
                readUnsignedLE(value, offsetStart + numFields * offsetSize, offsetSize);
        int dataStart = offsetStart + (numFields + 1) * offsetSize;
        return (dataStart - offset) + totalValueSize;
    }

    private static int arrayValueSize(byte[] value, int offset) {
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        int numElements = readUnsignedLE(value, offset + 1, sizeBytes);
        int offsetSize = (typeInfo & 0x3) + 1;
        int offsetListStart = offset + 1 + sizeBytes;
        int totalValueSize =
                readUnsignedLE(value, offsetListStart + numElements * offsetSize, offsetSize);
        int dataStart = offsetListStart + (numElements + 1) * offsetSize;
        return (dataStart - offset) + totalValueSize;
    }

    // --------------------------------------------------------------------------------------------
    // Metadata helpers
    // --------------------------------------------------------------------------------------------

    /** Returns the number of strings in the metadata dictionary. */
    public static int metadataDictSize(byte[] metadata) {
        int offsetSize = metadataOffsetSize(metadata);
        return readUnsignedLE(metadata, 1, offsetSize);
    }

    /** Returns true if the metadata dictionary strings are sorted. */
    public static boolean isMetadataSorted(byte[] metadata) {
        return (metadata[0] & METADATA_SORTED_STRINGS_BIT) != 0;
    }

    /** Returns the field name at the given dictionary index. */
    public static String metadataFieldName(byte[] metadata, int index) {
        int offsetSize = metadataOffsetSize(metadata);
        int dictSize = readUnsignedLE(metadata, 1, offsetSize);
        if (index < 0 || index >= dictSize) {
            throw new IndexOutOfBoundsException(
                    "Dict index " + index + " out of bounds [0, " + dictSize + ")");
        }
        int offsetsStart = 1 + offsetSize;
        int strStart = readUnsignedLE(metadata, offsetsStart + index * offsetSize, offsetSize);
        int strEnd = readUnsignedLE(metadata, offsetsStart + (index + 1) * offsetSize, offsetSize);
        int strBytesStart = offsetsStart + (dictSize + 1) * offsetSize;
        return new String(
                metadata, strBytesStart + strStart, strEnd - strStart, StandardCharsets.UTF_8);
    }

    // --------------------------------------------------------------------------------------------
    // Object value encoding
    // --------------------------------------------------------------------------------------------

    /**
     * Encodes an object value from field IDs and corresponding field value byte arrays. Fields are
     * sorted by field ID in the output.
     *
     * <p>This implementation uses the "large" (4-byte) format for field ID size, offset size, and
     * element count uniformly, for simplicity. All reader methods ({@link #objectSize}, {@link
     * #objectFieldId}, {@link #objectFieldValueOffset}) are consistent with this choice. A future
     * optimization could use variable-width encoding for smaller objects.
     *
     * <p>Object header typeInfo (6 bits): bits[1:0]=field_id_size-1, bits[3:2]=offset_size-1,
     * bit[4]=is_large. With 4-byte widths: field_id_size=4 (bits[1:0]=3), offset_size=4
     * (bits[3:2]=3), is_large=1 (bit[4]=1) → typeInfo = 0b011111 = 0x1F. Header byte = (typeInfo
     * &lt;&lt; 2) | BASIC_TYPE_OBJECT = 0x7E.
     *
     * @param fieldIds the field IDs (metadata dictionary indices) for each field
     * @param fieldValues the encoded Variant value bytes for each field
     * @return the encoded object value bytes
     */
    public static byte[] encodeObject(List<Integer> fieldIds, List<byte[]> fieldValues) {
        int n = fieldIds.size();
        // Object header: field_id_size=4 (3<<2), offset_size=4 (3<<4), is_large=1 (1<<6)
        byte objectHeader = (byte) (BASIC_TYPE_OBJECT | (3 << 2) | (3 << 4) | (1 << 6));
        if (n == 0) {
            // Empty object: header(1) + numFields(4) + lastOffset(4)
            byte[] result = new byte[9];
            result[0] = objectHeader;
            writeIntLE(result, 1, 0);
            writeIntLE(result, 5, 0);
            return result;
        }

        // Sort fields by field ID
        Integer[] indices = new Integer[n];
        for (int i = 0; i < n; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, (a, b) -> Integer.compare(fieldIds.get(a), fieldIds.get(b)));

        // Calculate total field values size
        int totalDataSize = 0;
        for (byte[] v : fieldValues) {
            totalDataSize += v.length;
        }

        // Total: header(1) + numFields(4) + fieldIds(4*n) + offsets(4*(n+1)) + data
        int totalSize = 1 + 4 + 4 * n + 4 * (n + 1) + totalDataSize;
        byte[] result = new byte[totalSize];

        result[0] = objectHeader;
        writeIntLE(result, 1, n);

        int pos = 5;

        // Field IDs in sorted order
        for (int i = 0; i < n; i++) {
            writeIntLE(result, pos, fieldIds.get(indices[i]));
            pos += 4;
        }

        // Offsets (n+1 entries, last = totalDataSize)
        int dataOffset = 0;
        for (int i = 0; i < n; i++) {
            writeIntLE(result, pos, dataOffset);
            pos += 4;
            dataOffset += fieldValues.get(indices[i]).length;
        }
        writeIntLE(result, pos, totalDataSize);
        pos += 4;

        // Field value data in sorted field ID order
        for (int i = 0; i < n; i++) {
            byte[] v = fieldValues.get(indices[i]);
            System.arraycopy(v, 0, result, pos, v.length);
            pos += v.length;
        }

        return result;
    }

    // --------------------------------------------------------------------------------------------
    // Primitive value encoding
    // --------------------------------------------------------------------------------------------

    /** Encodes a null value. */
    public static byte[] encodeNull() {
        return new byte[] {primitiveHeader(PRIMITIVE_TYPE_NULL)};
    }

    /** Encodes a boolean value. */
    public static byte[] encodeBoolean(boolean value) {
        return new byte[] {primitiveHeader(value ? PRIMITIVE_TYPE_TRUE : PRIMITIVE_TYPE_FALSE)};
    }

    /** Encodes an int8 value. */
    public static byte[] encodeByte(byte value) {
        return new byte[] {primitiveHeader(PRIMITIVE_TYPE_INT8), value};
    }

    /** Encodes an int16 value. */
    public static byte[] encodeShort(short value) {
        byte[] result = new byte[3];
        result[0] = primitiveHeader(PRIMITIVE_TYPE_INT16);
        writeShortLE(result, 1, value);
        return result;
    }

    /** Encodes an int32 value. */
    public static byte[] encodeInt(int value) {
        byte[] result = new byte[5];
        result[0] = primitiveHeader(PRIMITIVE_TYPE_INT32);
        writeIntLE(result, 1, value);
        return result;
    }

    /** Encodes an int64 value. */
    public static byte[] encodeLong(long value) {
        byte[] result = new byte[9];
        result[0] = primitiveHeader(PRIMITIVE_TYPE_INT64);
        writeLongLE(result, 1, value);
        return result;
    }

    /** Encodes a float value. */
    public static byte[] encodeFloat(float value) {
        byte[] result = new byte[5];
        result[0] = primitiveHeader(PRIMITIVE_TYPE_FLOAT);
        writeIntLE(result, 1, Float.floatToIntBits(value));
        return result;
    }

    /** Encodes a double value. */
    public static byte[] encodeDouble(double value) {
        byte[] result = new byte[9];
        result[0] = primitiveHeader(PRIMITIVE_TYPE_DOUBLE);
        writeLongLE(result, 1, Double.doubleToLongBits(value));
        return result;
    }

    /** Encodes a date value (days since epoch). */
    public static byte[] encodeDate(int daysSinceEpoch) {
        byte[] result = new byte[5];
        result[0] = primitiveHeader(PRIMITIVE_TYPE_DATE);
        writeIntLE(result, 1, daysSinceEpoch);
        return result;
    }

    /** Reads a date value (days since epoch) from the value bytes. */
    public static int getDate(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_DATE);
        return readIntLE(value, offset + 1);
    }

    /** Encodes a string value (uses short string encoding if length <= 63). */
    public static byte[] encodeString(byte[] utf8Bytes) {
        if (utf8Bytes.length <= 63) {
            // Short string encoding
            byte[] result = new byte[1 + utf8Bytes.length];
            result[0] = (byte) ((utf8Bytes.length << 2) | BASIC_TYPE_SHORT_STRING);
            System.arraycopy(utf8Bytes, 0, result, 1, utf8Bytes.length);
            return result;
        } else {
            // Long string encoding (primitive type STRING)
            byte[] result = new byte[5 + utf8Bytes.length];
            result[0] = primitiveHeader(PRIMITIVE_TYPE_STRING);
            writeIntLE(result, 1, utf8Bytes.length);
            System.arraycopy(utf8Bytes, 0, result, 5, utf8Bytes.length);
            return result;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Internal helpers
    // --------------------------------------------------------------------------------------------

    static byte primitiveHeader(int primitiveTypeId) {
        return (byte) ((primitiveTypeId << 2) | BASIC_TYPE_PRIMITIVE);
    }

    private static void checkPrimitiveType(byte[] value, int offset, int expectedTypeId) {
        if (basicType(value, offset) != BASIC_TYPE_PRIMITIVE) {
            throw new IllegalStateException("Not a primitive variant value");
        }
        int typeId = primitiveTypeId(value, offset);
        if (typeId != expectedTypeId) {
            throw new IllegalStateException(
                    "Expected primitive type " + expectedTypeId + " but got " + typeId);
        }
    }

    private static int binarySearchFieldName(byte[] metadata, int dictSize, byte[] nameBytes) {
        int offsetSize = metadataOffsetSize(metadata);
        int offsetsStart = 1 + offsetSize;
        int strBytesStart = offsetsStart + (dictSize + 1) * offsetSize;
        int lo = 0, hi = dictSize - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int strStart = readUnsignedLE(metadata, offsetsStart + mid * offsetSize, offsetSize);
            int strEnd =
                    readUnsignedLE(metadata, offsetsStart + (mid + 1) * offsetSize, offsetSize);
            int strLen = strEnd - strStart;

            int cmp = compareBytes(metadata, strBytesStart + strStart, strLen, nameBytes);
            if (cmp < 0) {
                lo = mid + 1;
            } else if (cmp > 0) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    private static int linearSearchFieldName(byte[] metadata, int dictSize, byte[] nameBytes) {
        int offsetSize = metadataOffsetSize(metadata);
        int offsetsStart = 1 + offsetSize;
        int strBytesStart = offsetsStart + (dictSize + 1) * offsetSize;
        for (int i = 0; i < dictSize; i++) {
            int strStart = readUnsignedLE(metadata, offsetsStart + i * offsetSize, offsetSize);
            int strEnd = readUnsignedLE(metadata, offsetsStart + (i + 1) * offsetSize, offsetSize);
            int strLen = strEnd - strStart;
            if (strLen == nameBytes.length
                    && compareBytes(metadata, strBytesStart + strStart, strLen, nameBytes) == 0) {
                return i;
            }
        }
        return -1;
    }

    private static int compareBytes(byte[] a, int aOffset, int aLen, byte[] b) {
        int minLen = Math.min(aLen, b.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = (a[aOffset + i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return aLen - b.length;
    }

    // --------------------------------------------------------------------------------------------
    // Little-endian read/write helpers
    // --------------------------------------------------------------------------------------------

    static short readShortLE(byte[] bytes, int offset) {
        return (short) ((bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8));
    }

    static int readIntLE(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF)
                | ((bytes[offset + 1] & 0xFF) << 8)
                | ((bytes[offset + 2] & 0xFF) << 16)
                | ((bytes[offset + 3] & 0xFF) << 24);
    }

    static long readLongLE(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFFL)
                | ((bytes[offset + 1] & 0xFFL) << 8)
                | ((bytes[offset + 2] & 0xFFL) << 16)
                | ((bytes[offset + 3] & 0xFFL) << 24)
                | ((bytes[offset + 4] & 0xFFL) << 32)
                | ((bytes[offset + 5] & 0xFFL) << 40)
                | ((bytes[offset + 6] & 0xFFL) << 48)
                | ((bytes[offset + 7] & 0xFFL) << 56);
    }

    static void writeShortLE(byte[] bytes, int offset, short value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
    }

    public static void writeIntLE(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 3] = (byte) (value >>> 24);
    }

    static void writeLongLE(byte[] bytes, int offset, long value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 3] = (byte) (value >>> 24);
        bytes[offset + 4] = (byte) (value >>> 32);
        bytes[offset + 5] = (byte) (value >>> 40);
        bytes[offset + 6] = (byte) (value >>> 48);
        bytes[offset + 7] = (byte) (value >>> 56);
    }

    /**
     * Reads a variable-width unsigned little-endian integer (1-4 bytes) from the byte array at the
     * given position. This is required for reading values encoded with variable-width Parquet
     * Variant encoding, where metadata offsets, object field IDs, and object/array offsets may be
     * 1, 2, 3, or 4 bytes wide.
     */
    static int readUnsignedLE(byte[] bytes, int offset, int numBytes) {
        int result = 0;
        for (int i = 0; i < numBytes; i++) {
            result |= (bytes[offset + i] & 0xFF) << (8 * i);
        }
        return result;
    }

    /**
     * Returns the metadata offset size (1-4 bytes) from the metadata header byte. Bits 6-7 of the
     * header encode {@code offset_size_minus_1}.
     */
    static int metadataOffsetSize(byte[] metadata) {
        return ((metadata[0] >> 6) & 0x3) + 1;
    }
}
