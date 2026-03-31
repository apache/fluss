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

import org.apache.fluss.annotation.PublicStable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents a semi-structured variant value, stored using the Parquet Variant Binary Encoding
 * format. A Variant consists of two byte arrays:
 *
 * <ul>
 *   <li><b>metadata</b>: A dictionary of field names used by object values. Contains a header byte,
 *       dictionary size, offsets, and UTF-8 encoded string bytes.
 *   <li><b>value</b>: A self-describing binary-encoded value. The first byte encodes the basic type
 *       (Primitive, ShortString, Object, Array) and type-specific header information.
 * </ul>
 *
 * <p>For storage in Arrow {@code VarBinaryVector}, the two arrays are concatenated as: {@code
 * [4-byte metadata length (little-endian)][metadata bytes][value bytes]}.
 *
 * @since 0.7
 */
@PublicStable
public final class Variant implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] metadata;
    private final byte[] value;

    public Variant(byte[] metadata, byte[] value) {
        this.metadata = checkNotNull(metadata, "metadata must not be null");
        this.value = checkNotNull(value, "value must not be null");
    }

    /** Returns the metadata bytes (field name dictionary). */
    public byte[] metadata() {
        return metadata;
    }

    /** Returns the value bytes (self-describing encoded value). */
    public byte[] value() {
        return value;
    }

    // --------------------------------------------------------------------------------------------
    // Binary serialization (for Arrow VarBinaryVector storage)
    // --------------------------------------------------------------------------------------------

    /**
     * Serializes this variant to a single byte array for storage.
     *
     * <p>Format: {@code [4-byte metadata length (little-endian)][metadata bytes][value bytes]}
     */
    public byte[] toBytes() {
        byte[] result = new byte[4 + metadata.length + value.length];
        ByteBuffer buf = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(metadata.length);
        buf.put(metadata);
        buf.put(value);
        return result;
    }

    /**
     * Deserializes a variant from a byte array.
     *
     * @param bytes the serialized bytes
     * @return the deserialized Variant
     */
    public static Variant fromBytes(byte[] bytes) {
        return fromBytes(bytes, 0, bytes.length);
    }

    /**
     * Deserializes a variant from a byte array with offset and length.
     *
     * @param bytes the byte array containing the serialized variant
     * @param offset the starting offset
     * @param length the total length
     * @return the deserialized Variant
     */
    public static Variant fromBytes(byte[] bytes, int offset, int length) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        int metadataLen = buf.getInt();
        byte[] meta = new byte[metadataLen];
        buf.get(meta);
        byte[] val = new byte[length - 4 - metadataLen];
        buf.get(val);
        return new Variant(meta, val);
    }

    // --------------------------------------------------------------------------------------------
    // Value type inspection
    // --------------------------------------------------------------------------------------------

    /** Returns the basic type of this variant value. */
    public int basicType() {
        return VariantUtil.basicType(value, 0);
    }

    /** Returns true if this variant value is null. */
    public boolean isNull() {
        return VariantUtil.isNull(value, 0);
    }

    /** Returns true if this variant is a primitive type. */
    public boolean isPrimitive() {
        return basicType() == VariantUtil.BASIC_TYPE_PRIMITIVE;
    }

    /** Returns true if this variant is a short string. */
    public boolean isShortString() {
        return basicType() == VariantUtil.BASIC_TYPE_SHORT_STRING;
    }

    /** Returns true if this variant is an object. */
    public boolean isObject() {
        return basicType() == VariantUtil.BASIC_TYPE_OBJECT;
    }

    /** Returns true if this variant is an array. */
    public boolean isArray() {
        return basicType() == VariantUtil.BASIC_TYPE_ARRAY;
    }

    // --------------------------------------------------------------------------------------------
    // Primitive value accessors
    // --------------------------------------------------------------------------------------------

    /** Returns the boolean value of this variant. */
    public boolean getBoolean() {
        return VariantUtil.getBoolean(value, 0);
    }

    /** Returns the byte value of this variant. */
    public byte getByte() {
        return VariantUtil.getByte(value, 0);
    }

    /** Returns the short value of this variant. */
    public short getShort() {
        return VariantUtil.getShort(value, 0);
    }

    /** Returns the int value of this variant. */
    public int getInt() {
        return VariantUtil.getInt(value, 0);
    }

    /** Returns the long value of this variant. */
    public long getLong() {
        return VariantUtil.getLong(value, 0);
    }

    /** Returns the float value of this variant. */
    public float getFloat() {
        return VariantUtil.getFloat(value, 0);
    }

    /** Returns the double value of this variant. */
    public double getDouble() {
        return VariantUtil.getDouble(value, 0);
    }

    /** Returns the string value of this variant. */
    public String getString() {
        return VariantUtil.getString(value, 0);
    }

    /** Returns the binary value of this variant. */
    public byte[] getBinary() {
        return VariantUtil.getBinary(value, 0);
    }

    /** Returns the decimal value of this variant. */
    public BigDecimal getDecimal() {
        return VariantUtil.getDecimal(value, 0);
    }

    /** Returns the date value (days since epoch) of this variant. */
    public int getDate() {
        return VariantUtil.getDate(value, 0);
    }

    /** Returns the timestamp value (microseconds since epoch) of this variant. */
    public long getTimestamp() {
        return VariantUtil.getTimestamp(value, 0);
    }

    /** Returns the timestamp without timezone value (microseconds since epoch) of this variant. */
    public long getTimestampNtz() {
        return VariantUtil.getTimestampNtz(value, 0);
    }

    /**
     * Returns the total size of this variant in bytes (metadata + value).
     *
     * @return total byte size
     */
    public int sizeInBytes() {
        return metadata.length + value.length;
    }

    // --------------------------------------------------------------------------------------------
    // Object field access
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the number of fields in this object variant.
     *
     * @throws IllegalStateException if this variant is not an object
     */
    public int objectSize() {
        return VariantUtil.objectSize(value, 0);
    }

    /**
     * Gets a field value from this object variant by field name.
     *
     * @param fieldName the field name to look up
     * @return a new Variant for the field value, or null if the field does not exist
     */
    public Variant getFieldByName(String fieldName) {
        int fieldId = VariantUtil.findFieldId(metadata, fieldName);
        if (fieldId < 0) {
            return null;
        }
        int valueOffset = VariantUtil.findFieldValueOffset(value, 0, fieldId);
        if (valueOffset < 0) {
            return null;
        }
        int valueSize = VariantUtil.valueSize(value, valueOffset);
        byte[] fieldValue = Arrays.copyOfRange(value, valueOffset, valueOffset + valueSize);
        return new Variant(metadata, fieldValue);
    }

    // --------------------------------------------------------------------------------------------
    // Array element access
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the number of elements in this array variant.
     *
     * @throws IllegalStateException if this variant is not an array
     */
    public int arraySize() {
        return VariantUtil.arraySize(value, 0);
    }

    /**
     * Gets an element from this array variant by index.
     *
     * @param index the element index
     * @return a new Variant for the element value
     */
    public Variant getElementAt(int index) {
        int elementOffset = VariantUtil.arrayElementOffset(value, 0, index);
        int elementSize = VariantUtil.valueSize(value, elementOffset);
        byte[] elementValue = Arrays.copyOfRange(value, elementOffset, elementOffset + elementSize);
        return new Variant(metadata, elementValue);
    }

    // --------------------------------------------------------------------------------------------
    // JSON conversion
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a Variant from a JSON string.
     *
     * @param json the JSON string to parse
     * @return a new Variant representing the parsed JSON
     */
    public static Variant fromJson(String json) {
        return VariantBuilder.parseJson(json);
    }

    /**
     * Converts this variant to its JSON string representation.
     *
     * @return a JSON string
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        VariantUtil.toJson(metadata, value, 0, sb);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Variant)) {
            return false;
        }
        Variant variant = (Variant) o;
        return Arrays.equals(metadata, variant.metadata) && Arrays.equals(value, variant.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(metadata);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return toJson();
    }

    // --------------------------------------------------------------------------------------------
    // Utility: empty metadata for primitive-only variants
    // --------------------------------------------------------------------------------------------

    /** Creates an empty metadata (no field names, for primitive/array-only variants). */
    public static byte[] emptyMetadata() {
        return VariantUtil.EMPTY_METADATA;
    }

    /** Creates a Variant representing a JSON null value. */
    public static Variant ofNull() {
        return new Variant(emptyMetadata(), VariantUtil.encodeNull());
    }

    /** Creates a Variant representing a boolean value. */
    public static Variant ofBoolean(boolean value) {
        return new Variant(emptyMetadata(), VariantUtil.encodeBoolean(value));
    }

    /** Creates a Variant representing an int value. */
    public static Variant ofInt(int value) {
        return new Variant(emptyMetadata(), VariantUtil.encodeInt(value));
    }

    /** Creates a Variant representing a long value. */
    public static Variant ofLong(long value) {
        return new Variant(emptyMetadata(), VariantUtil.encodeLong(value));
    }

    /** Creates a Variant representing a float value. */
    public static Variant ofFloat(float value) {
        return new Variant(emptyMetadata(), VariantUtil.encodeFloat(value));
    }

    /** Creates a Variant representing a double value. */
    public static Variant ofDouble(double value) {
        return new Variant(emptyMetadata(), VariantUtil.encodeDouble(value));
    }

    /** Creates a Variant representing a string value. */
    public static Variant ofString(String value) {
        byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
        return new Variant(emptyMetadata(), VariantUtil.encodeString(stringBytes));
    }
}
