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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Low-level utility methods for Variant binary encoding/decoding following the Parquet Variant
 * Binary Encoding specification.
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

    /** Empty metadata: version 1, sorted dictionary, one-byte offsets, no field names. */
    public static final byte[] EMPTY_METADATA = {
        METADATA_VERSION | METADATA_SORTED_STRINGS_BIT, 0, 0
    };

    // --------------------------------------------------------------------------------------------
    // Value header decoding
    // --------------------------------------------------------------------------------------------

    /** Returns the basic type (0-3) from the value bytes at the given offset. */
    public static int basicType(byte[] value, int offset) {
        checkedOffset(value, offset, 1);
        return (value[offset] & 0xFF) & BASIC_TYPE_MASK;
    }

    /** Returns the primitive type ID from the value header (6-bit type_info field). */
    public static int primitiveTypeId(byte[] value, int offset) {
        checkedOffset(value, offset, 1);
        return ((value[offset] & 0xFF) >> 2) & 0x3F;
    }

    /** Returns the short string length from the value header (6-bit type_info field, max 63). */
    public static int shortStringLength(byte[] value, int offset) {
        checkedOffset(value, offset, 1);
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
        if (basicType(value, offset) != BASIC_TYPE_PRIMITIVE) {
            throw new IllegalStateException("Not a boolean variant value");
        }
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
        checkedOffset(value, (long) offset + 1, 1);
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
            checkedOffset(value, (long) offset + 1, len);
            return new String(value, offset + 1, len, StandardCharsets.UTF_8);
        } else if (basic == BASIC_TYPE_PRIMITIVE) {
            checkPrimitiveType(value, offset, PRIMITIVE_TYPE_STRING);
            int len = variableLength(value, offset);
            return new String(value, offset + 5, len, StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Not a string variant value");
    }

    /** Reads binary data from the value bytes. */
    public static byte[] getBinary(byte[] value, int offset) {
        checkPrimitiveType(value, offset, PRIMITIVE_TYPE_BINARY);
        int len = variableLength(value, offset);
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
        if (basicType(value, offset) != BASIC_TYPE_PRIMITIVE) {
            throw new IllegalStateException("Not a decimal variant value");
        }
        int typeId = primitiveTypeId(value, offset);
        if (typeId < PRIMITIVE_TYPE_DECIMAL4 || typeId > PRIMITIVE_TYPE_DECIMAL16) {
            throw new IllegalStateException("Not a decimal variant value, type: " + typeId);
        }
        valueSize(value, offset);
        int scale = value[offset + 1] & 0xFF;
        checkArgument(scale <= 38, "Variant decimal scale exceeds 38.");
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
     * precision and scale. The scale thresholds also accommodate Flink 2.3 readers, which reject
     * scales above 9 for DECIMAL4 and above 18 for DECIMAL8. Readers still accept all Parquet-valid
     * scales (0-38) independently of the physical width.
     */
    public static byte[] encodeDecimal(BigDecimal value) {
        checkArgument(value.scale() <= 38, "Variant decimal scale exceeds 38.");
        if (value.scale() < 0) {
            checkArgument(
                    (long) value.precision() - value.scale() <= 38,
                    "Variant decimal precision exceeds 38.");
            value = value.setScale(0);
        }
        checkArgument(value.precision() <= 38, "Variant decimal precision exceeds 38.");
        int scale = value.scale();
        BigInteger unscaled = value.unscaledValue();
        int precision = Math.max(value.precision(), scale);

        if (precision <= 9) {
            // DECIMAL4: header(1) + scale(1) + 4 bytes
            byte[] result = new byte[6];
            result[0] = primitiveHeader(PRIMITIVE_TYPE_DECIMAL4);
            result[1] = (byte) scale;
            writeIntLE(result, 2, unscaled.intValue());
            return result;
        } else if (precision <= 18) {
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
     * <p>Object encoding: [header(1)] [num_fields(1 or 4)] [field_id_list] [field_offset_list]
     * [field_values]
     */
    public static int objectSize(byte[] value, int offset) {
        if (basicType(value, offset) != BASIC_TYPE_OBJECT) {
            throw new IllegalStateException("Not an object variant value");
        }
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        containerValueSize(value, offset, true);
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
        byte[] nameBytes = encodeUtf8(fieldName);

        if (sorted) {
            return binarySearchFieldName(metadata, dictSize, nameBytes);
        } else {
            return linearSearchFieldName(metadata, dictSize, nameBytes);
        }
    }

    /**
     * Finds the value offset by binary searching the field names. Dictionary IDs themselves need
     * not be ordered when the metadata dictionary is unsorted.
     *
     * @return the value offset within the value byte array, or -1 if field ID not found
     */
    public static int findFieldValueOffset(byte[] metadata, byte[] value, int offset, int fieldId) {
        if (basicType(value, offset) != BASIC_TYPE_OBJECT) {
            return -1;
        }
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        int numFields = objectSize(value, offset);
        int idSize = ((typeInfo >> 2) & 0x3) + 1;
        int offsetSize = (typeInfo & 0x3) + 1;
        int idStart = offset + 1 + sizeBytes;
        int offsetStart = idStart + numFields * idSize;
        int dataStart = containerDataStart(value, offset, true);

        // Objects are ordered by UTF-8 field names, independently of dictionary ID order.
        int lo = 0, hi = numFields - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int midId = readUnsignedLE(value, idStart + mid * idSize, idSize);
            int comparison = compareFieldIds(metadata, midId, fieldId);
            if (comparison < 0) {
                lo = mid + 1;
            } else if (comparison > 0) {
                hi = mid - 1;
            } else {
                int fieldOffset = readUnsignedLE(value, offsetStart + mid * offsetSize, offsetSize);
                int dataSize = readUnsignedLE(value, dataStart - offsetSize, offsetSize);
                return childOffset(value, dataStart, dataSize, fieldOffset);
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
        int count = objectSize(value, offset);
        checkArgument(fieldIndex >= 0 && fieldIndex < count, "Invalid Variant field index.");
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
        int numFields = objectSize(value, offset);
        checkArgument(fieldIndex >= 0 && fieldIndex < numFields, "Invalid Variant field index.");
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        int idSize = ((typeInfo >> 2) & 0x3) + 1;
        int offsetSize = (typeInfo & 0x3) + 1;
        int idStart = offset + 1 + sizeBytes;
        int offsetStart = idStart + numFields * idSize;
        int dataStart = containerDataStart(value, offset, true);
        int fieldOffset = readUnsignedLE(value, offsetStart + fieldIndex * offsetSize, offsetSize);
        int dataSize = readUnsignedLE(value, dataStart - offsetSize, offsetSize);
        return childOffset(value, dataStart, dataSize, fieldOffset);
    }

    // --------------------------------------------------------------------------------------------
    // Array element access
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the number of elements in an array value.
     *
     * <p>Array encoding: [header(1)] [num_elements(1 or 4)] [offset_list] [element_values]
     */
    public static int arraySize(byte[] value, int offset) {
        if (basicType(value, offset) != BASIC_TYPE_ARRAY) {
            throw new IllegalStateException("Not an array variant value");
        }
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 2) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        containerValueSize(value, offset, false);
        return readUnsignedLE(value, offset + 1, sizeBytes);
    }

    /** Returns the absolute offset of an element after checking its containing array's bounds. */
    public static int arrayElementOffset(byte[] value, int offset, int index) {
        int numElements = arraySize(value, offset);
        int typeInfo = ((value[offset] & 0xFF) >> 2) & 0x3F;
        boolean largeSize = ((typeInfo >> 2) & 0x1) != 0;
        int sizeBytes = largeSize ? 4 : 1;
        if (index < 0 || index >= numElements) {
            throw new IndexOutOfBoundsException(
                    "Array index " + index + " out of bounds [0, " + numElements + ")");
        }
        int offsetSize = (typeInfo & 0x3) + 1;
        int offsetListStart = offset + 1 + sizeBytes;
        int elementValuesStart = containerDataStart(value, offset, false);
        int elementOffset = readUnsignedLE(value, offsetListStart + index * offsetSize, offsetSize);
        int dataSize = readUnsignedLE(value, elementValuesStart - offsetSize, offsetSize);
        return childOffset(value, elementValuesStart, dataSize, elementOffset);
    }

    // --------------------------------------------------------------------------------------------
    // Value size calculation
    // --------------------------------------------------------------------------------------------

    /** Computes the total byte size of a value starting at the given offset. */
    public static int valueSize(byte[] value, int offset) {
        int size;
        switch (basicType(value, offset)) {
            case BASIC_TYPE_PRIMITIVE:
                size = primitiveValueSize(value, offset);
                break;
            case BASIC_TYPE_SHORT_STRING:
                size = 1 + shortStringLength(value, offset);
                break;
            case BASIC_TYPE_OBJECT:
                size = objectValueSize(value, offset);
                break;
            case BASIC_TYPE_ARRAY:
                size = arrayValueSize(value, offset);
                break;
            default:
                throw new IllegalStateException("Unknown Variant basic type");
        }
        checkComponentSize(size);
        checkedOffset(value, offset, size);
        return size;
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
                    int len = variableLength(value, offset);
                    return 5 + len; // header + 4-byte length + data
                }
            default:
                throw new IllegalStateException("Unknown primitive type: " + typeId);
        }
    }

    private static int objectValueSize(byte[] value, int offset) {
        return containerValueSize(value, offset, true);
    }

    private static int arrayValueSize(byte[] value, int offset) {
        return containerValueSize(value, offset, false);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata helpers
    // --------------------------------------------------------------------------------------------

    /** Returns the number of strings in the metadata dictionary. */
    public static int metadataDictSize(byte[] metadata) {
        int width = metadataOffsetSize(metadata);
        int count = readUnsignedLE(metadata, 1, width);
        int strings = metadataStringStart(metadata, count, width);
        int length = readUnsignedLE(metadata, strings - width, width);
        checkedOffset(metadata, strings, length);
        checkArgument(
                readUnsignedLE(metadata, 1 + width, width) == 0,
                "Invalid Variant metadata initial offset.");
        return count;
    }

    /** Returns true if the metadata dictionary strings are sorted. */
    public static boolean isMetadataSorted(byte[] metadata) {
        checkedOffset(metadata, 0, 1);
        return (metadata[0] & METADATA_SORTED_STRINGS_BIT) != 0;
    }

    /** Returns the field name at the given dictionary index. */
    public static String metadataFieldName(byte[] metadata, int index) {
        int offsetSize = metadataOffsetSize(metadata);
        int dictSize = metadataDictSize(metadata);
        if (index < 0 || index >= dictSize) {
            throw new IndexOutOfBoundsException(
                    "Dict index " + index + " out of bounds [0, " + dictSize + ")");
        }
        int offsetsStart = 1 + offsetSize;
        int strStart = readUnsignedLE(metadata, offsetsStart + index * offsetSize, offsetSize);
        int strEnd = readUnsignedLE(metadata, offsetsStart + (index + 1) * offsetSize, offsetSize);
        int strBytesStart = metadataStringStart(metadata, dictSize, offsetSize);
        checkMetadataStringRange(metadata, strBytesStart, offsetSize, strStart, strEnd);
        return new String(
                metadata, strBytesStart + strStart, strEnd - strStart, StandardCharsets.UTF_8);
    }

    // --------------------------------------------------------------------------------------------
    // Object value encoding
    // --------------------------------------------------------------------------------------------

    /**
     * Encodes an object value from field IDs and corresponding field value byte arrays. Fields are
     * sorted by UTF-8 field name in the output, retaining their original dictionary IDs.
     *
     * <p>Uses the smallest widths that can hold the largest field ID and the final data offset. The
     * element count occupies one byte through 255 fields, and four bytes thereafter.
     *
     * @param metadata the shared field-name dictionary
     * @param fieldIds the field IDs (metadata dictionary indices) for each field
     * @param fieldValues the encoded Variant value bytes for each field
     * @return the encoded object value bytes
     */
    public static byte[] encodeObject(
            byte[] metadata, List<Integer> fieldIds, List<byte[]> fieldValues) {
        int n = fieldIds.size();
        checkArgument(n == fieldValues.size(), "Field IDs and values must have the same size.");
        checkComponentSize(3L + 2L * n);
        int dataSize = 0;
        int maxId = 0;
        for (int i = 0; i < n; i++) {
            int id = fieldIds.get(i);
            checkArgument(id >= 0 && id < metadataDictSize(metadata), "Invalid field ID: %s", id);
            maxId = Math.max(maxId, id);
            dataSize = checkComponentSize((long) dataSize + fieldValues.get(i).length);
        }
        int idWidth = unsignedWidth(maxId);
        int offsetWidth = unsignedWidth(dataSize);
        int countWidth = n <= 255 ? 1 : 4;
        int dataStart =
                checkComponentSize(
                        1L + countWidth + (long) n * idWidth + ((long) n + 1) * offsetWidth);
        byte[] result = new byte[checkComponentSize((long) dataStart + dataSize)];
        result[0] =
                (byte)
                        (BASIC_TYPE_OBJECT
                                | ((offsetWidth - 1) << 2)
                                | ((idWidth - 1) << 4)
                                | (n > 255 ? 1 << 6 : 0));
        writeUnsignedLE(result, 1, n, countWidth);

        // Sort by field name without renumbering IDs used by nested values.
        Integer[] indices = new Integer[n];
        for (int i = 0; i < n; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, (a, b) -> compareFieldIds(metadata, fieldIds.get(a), fieldIds.get(b)));
        int idsStart = 1 + countWidth;
        int offsetsStart = idsStart + n * idWidth;
        int dataOffset = 0;
        for (int i = 0; i < n; i++) {
            int index = indices[i];
            writeUnsignedLE(result, idsStart + i * idWidth, fieldIds.get(index), idWidth);
            writeUnsignedLE(result, offsetsStart + i * offsetWidth, dataOffset, offsetWidth);
            byte[] field = fieldValues.get(index);
            System.arraycopy(field, 0, result, dataStart + dataOffset, field.length);
            dataOffset += field.length;
        }
        writeUnsignedLE(result, offsetsStart + n * offsetWidth, dataOffset, offsetWidth);
        return result;
    }

    // --------------------------------------------------------------------------------------------
    // Primitive value encoding
    // --------------------------------------------------------------------------------------------

    /** Encodes an array while preserving element order. */
    public static byte[] encodeArray(List<byte[]> elements) {
        int count = elements.size();
        checkComponentSize(3L + count);
        int dataSize = 0;
        for (byte[] element : elements) {
            dataSize = checkComponentSize((long) dataSize + element.length);
        }
        int offsetWidth = unsignedWidth(dataSize);
        int countWidth = count <= 255 ? 1 : 4;
        int offsetsStart = 1 + countWidth;
        int dataStart = checkComponentSize(offsetsStart + ((long) count + 1) * offsetWidth);
        byte[] result = new byte[checkComponentSize((long) dataStart + dataSize)];
        result[0] =
                (byte) (BASIC_TYPE_ARRAY | ((offsetWidth - 1) << 2) | (count > 255 ? 1 << 4 : 0));
        writeUnsignedLE(result, 1, count, countWidth);
        int offset = 0;
        for (int i = 0; i < count; i++) {
            byte[] element = elements.get(i);
            writeUnsignedLE(result, offsetsStart + offsetWidth * i, offset, offsetWidth);
            System.arraycopy(element, 0, result, dataStart + offset, element.length);
            offset += element.length;
        }
        writeUnsignedLE(result, offsetsStart + offsetWidth * count, offset, offsetWidth);
        return result;
    }

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
        checkComponentSize((long) utf8Bytes.length + (utf8Bytes.length <= 63 ? 1 : 5));
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

    private static int compareFieldIds(byte[] metadata, int left, int right) {
        int width = metadataOffsetSize(metadata);
        int offsets = 1 + width;
        int count = metadataDictSize(metadata);
        checkArgument(
                left >= 0 && left < count && right >= 0 && right < count,
                "Invalid Variant dictionary ID.");
        int strings = metadataStringStart(metadata, count, width);
        int leftStart = readUnsignedLE(metadata, offsets + left * width, width);
        int rightStart = readUnsignedLE(metadata, offsets + right * width, width);
        int leftLength = readUnsignedLE(metadata, offsets + (left + 1) * width, width) - leftStart;
        int rightLength =
                readUnsignedLE(metadata, offsets + (right + 1) * width, width) - rightStart;
        checkMetadataStringRange(metadata, strings, width, leftStart, leftStart + leftLength);
        checkMetadataStringRange(metadata, strings, width, rightStart, rightStart + rightLength);
        for (int i = 0; i < Math.min(leftLength, rightLength); i++) {
            int comparison =
                    (metadata[strings + leftStart + i] & 0xFF)
                            - (metadata[strings + rightStart + i] & 0xFF);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    /** Builds a dictionary in the supplied ID order, preserving IDs referenced by nested values. */
    public static byte[] buildMetadata(List<String> names) {
        int count = names.size();
        checkComponentSize(3L + count);
        byte[][] encoded = new byte[count][];
        int stringsSize = 0;
        boolean sorted = true;
        for (int i = 0; i < count; i++) {
            encoded[i] = encodeUtf8(names.get(i));
            stringsSize = checkComponentSize((long) stringsSize + encoded[i].length);
            if (i > 0 && compareBytes(encoded[i - 1], 0, encoded[i - 1].length, encoded[i]) >= 0) {
                sorted = false;
            }
        }
        int width = unsignedWidth(Math.max(count, stringsSize));
        int stringsStart = checkComponentSize(1L + width + ((long) count + 1) * width);
        byte[] metadata = new byte[checkComponentSize((long) stringsStart + stringsSize)];
        metadata[0] =
                (byte)
                        (METADATA_VERSION
                                | ((width - 1) << 6)
                                | (sorted ? METADATA_SORTED_STRINGS_BIT : 0));
        writeUnsignedLE(metadata, 1, count, width);
        int offset = 0;
        int offsetsStart = 1 + width;
        for (int i = 0; i < count; i++) {
            writeUnsignedLE(metadata, offsetsStart + i * width, offset, width);
            System.arraycopy(encoded[i], 0, metadata, stringsStart + offset, encoded[i].length);
            offset += encoded[i].length;
        }
        writeUnsignedLE(metadata, offsetsStart + count * width, offset, width);
        return metadata;
    }

    static byte[] encodeUtf8(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isSurrogate(c)) {
                checkArgument(
                        Character.isHighSurrogate(c)
                                && i + 1 < value.length()
                                && Character.isLowSurrogate(value.charAt(i + 1)),
                        "Unpaired surrogate in Variant string at index %s.",
                        i);
                i++;
            }
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static int checkedOffset(byte[] bytes, long offset, long length) {
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IllegalArgumentException(
                    "Invalid Variant byte range: offset=" + offset + ", length=" + length);
        }
        return (int) offset;
    }

    private static int variableLength(byte[] value, int offset) {
        int length = readIntLE(value, offset + 1);
        checkArgument(length >= 0, "Negative Variant value length.");
        checkComponentSize(5L + length);
        checkedOffset(value, (long) offset + 5, length);
        return length;
    }

    private static int containerDataStart(byte[] value, int offset, boolean object) {
        int info = primitiveTypeId(value, offset);
        int countWidth = (info & (object ? 16 : 4)) != 0 ? 4 : 1;
        int count = readUnsignedLE(value, offset + 1, countWidth);
        int offsetWidth = (info & 3) + 1;
        int idWidth = object ? ((info >> 2) & 3) + 1 : 0;
        long dataStart =
                offset
                        + 1L
                        + countWidth
                        + (long) count * idWidth
                        + ((long) count + 1) * offsetWidth;
        return checkedOffset(value, dataStart, 0);
    }

    private static int containerValueSize(byte[] value, int offset, boolean object) {
        int dataStart = containerDataStart(value, offset, object);
        int offsetWidth = (primitiveTypeId(value, offset) & 3) + 1;
        int dataSize = readUnsignedLE(value, dataStart - offsetWidth, offsetWidth);
        int size = checkComponentSize((long) dataStart - offset + dataSize);
        checkedOffset(value, offset, size);
        return size;
    }

    private static int childOffset(byte[] value, int dataStart, int dataSize, int offset) {
        checkArgument(offset >= 0 && offset < dataSize, "Invalid Variant child offset.");
        int start = dataStart + offset;
        checkArgument(
                valueSize(value, start) <= dataSize - offset,
                "Variant child exceeds its container.");
        return start;
    }

    private static int metadataStringStart(byte[] metadata, int count, int width) {
        return checkedOffset(metadata, 1L + width + ((long) count + 1) * width, 0);
    }

    private static void checkMetadataStringRange(
            byte[] metadata, int strings, int width, int start, int end) {
        int size = readUnsignedLE(metadata, strings - width, width);
        checkArgument(
                start >= 0 && start <= end && end <= size,
                "Invalid Variant metadata string range.");
        checkedOffset(metadata, (long) strings + start, end - start);
    }

    static int checkComponentSize(long size) {
        checkArgument(
                size >= 0 && size <= Variant.MAX_COMPONENT_SIZE,
                "Variant metadata and value must each be at most 16 MiB: %s bytes.",
                size);
        return (int) size;
    }

    private static int unsignedWidth(int value) {
        return value <= 0xFF ? 1 : value <= 0xFFFF ? 2 : value <= 0xFFFFFF ? 3 : 4;
    }

    private static void writeUnsignedLE(byte[] bytes, int offset, int value, int width) {
        for (int i = 0; i < width; i++) {
            bytes[offset + i] = (byte) (value >>> (8 * i));
        }
    }

    /**
     * Compares field names in the unsigned UTF-8 order required by Parquet Variant. Readers and
     * writers using UTF-16 order (including Flink 2.3) are not interchangeable for all Unicode
     * keys.
     */
    public static int compareFieldNames(String left, String right) {
        byte[] leftBytes = encodeUtf8(left);
        return compareBytes(leftBytes, 0, leftBytes.length, encodeUtf8(right));
    }

    private static int binarySearchFieldName(byte[] metadata, int dictSize, byte[] nameBytes) {
        int offsetSize = metadataOffsetSize(metadata);
        int offsetsStart = 1 + offsetSize;
        int strBytesStart = metadataStringStart(metadata, dictSize, offsetSize);
        int lo = 0, hi = dictSize - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int strStart = readUnsignedLE(metadata, offsetsStart + mid * offsetSize, offsetSize);
            int strEnd =
                    readUnsignedLE(metadata, offsetsStart + (mid + 1) * offsetSize, offsetSize);
            checkMetadataStringRange(metadata, strBytesStart, offsetSize, strStart, strEnd);
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
        int strBytesStart = metadataStringStart(metadata, dictSize, offsetSize);
        for (int i = 0; i < dictSize; i++) {
            int strStart = readUnsignedLE(metadata, offsetsStart + i * offsetSize, offsetSize);
            int strEnd = readUnsignedLE(metadata, offsetsStart + (i + 1) * offsetSize, offsetSize);
            checkMetadataStringRange(metadata, strBytesStart, offsetSize, strStart, strEnd);
            int strLen = strEnd - strStart;
            if (strLen == nameBytes.length
                    && compareBytes(metadata, strBytesStart + strStart, strLen, nameBytes) == 0) {
                return i;
            }
        }
        return -1;
    }

    private static int compareBytes(byte[] a, int aOffset, int aLen, byte[] b) {
        checkedOffset(a, aOffset, aLen);
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
        checkedOffset(bytes, offset, 2);
        return (short) ((bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8));
    }

    static int readIntLE(byte[] bytes, int offset) {
        checkedOffset(bytes, offset, 4);
        return (bytes[offset] & 0xFF)
                | ((bytes[offset + 1] & 0xFF) << 8)
                | ((bytes[offset + 2] & 0xFF) << 16)
                | ((bytes[offset + 3] & 0xFF) << 24);
    }

    static long readLongLE(byte[] bytes, int offset) {
        checkedOffset(bytes, offset, 8);
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
        checkedOffset(bytes, offset, numBytes);
        int result = 0;
        for (int i = 0; i < numBytes; i++) {
            result |= (bytes[offset + i] & 0xFF) << (8 * i);
        }
        checkArgument(result >= 0, "Variant unsigned integer exceeds the supported range.");
        return result;
    }

    /**
     * Returns the metadata offset size (1-4 bytes) from the metadata header byte. Bits 6-7 of the
     * header encode {@code offset_size_minus_1}.
     */
    static int metadataOffsetSize(byte[] metadata) {
        checkedOffset(metadata, 0, 1);
        return ((metadata[0] >> 6) & 0x3) + 1;
    }
}
