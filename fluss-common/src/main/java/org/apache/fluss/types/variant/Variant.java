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

import org.apache.fluss.annotation.PublicEvolving;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkArgument;
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
 * <p>For row-oriented storage, the two arrays are concatenated as: {@code [4-byte metadata length
 * (little-endian)][metadata bytes][value bytes]}. The enclosing row must supply the total length of
 * this blob; it is not a self-delimiting stream format.
 *
 * <p>Metadata and value are each limited to {@link #MAX_COMPONENT_SIZE} bytes, including their
 * headers. This is a Fluss limit compatible with Flink 2.3, not a Parquet format limit. Record and
 * request limits apply independently and may restrict the usable size further.
 *
 * <p>The constructor wraps the supplied arrays without copying. Callers must not modify them after
 * construction. Nested fields and elements are views retaining the parent's arrays. All exposed
 * arrays are read-only by contract. Use {@link #copy()} to detach a value from the parent, or
 * {@link #toBytes()} for an independent serialized copy.
 *
 * <p>Equality and hashing compare the encoded metadata and value bytes, not logical values.
 * Equivalent values using different integer widths or dictionary layouts may compare unequal.
 *
 * @since 1.1
 */
@PublicEvolving
public class Variant implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Maximum encoded size of each of the metadata and value components: 16 MiB. */
    public static final int MAX_COMPONENT_SIZE = 16 << 20;

    private final byte[] metadata;
    private final byte[] value;
    private final int valueOffset;

    /** Wraps the encoded dictionary and value without copying either array. */
    public Variant(byte[] metadata, byte[] value) {
        this.metadata = checkNotNull(metadata, "metadata must not be null");
        this.value = checkNotNull(value, "value must not be null");
        VariantUtil.checkComponentSize(metadata.length);
        VariantUtil.checkComponentSize(value.length);
        checkArgument(metadata.length >= 3, "Variant metadata is truncated.");
        checkArgument(
                (metadata[0] & VariantUtil.METADATA_VERSION_MASK) == VariantUtil.METADATA_VERSION,
                "Unsupported Variant metadata version.");
        checkArgument(value.length > 0, "Variant value must not be empty.");
        VariantUtil.metadataDictSize(metadata);
        checkArgument(
                VariantUtil.valueSize(value, 0) == value.length,
                "Variant value length does not match its encoding.");
        this.valueOffset = 0;
    }

    private Variant(byte[] metadata, byte[] value, int valueOffset) {
        this.metadata = metadata;
        this.value = value;
        this.valueOffset = valueOffset;
    }

    /** Returns the metadata bytes (field name dictionary). */
    public byte[] metadata() {
        return metadata;
    }

    /**
     * Returns the complete encoding of this value, starting at index zero. For a nested view this
     * copies only the selected value, without exposing the parent's surrounding values.
     */
    public byte[] value() {
        return valueOffset == 0
                ? value
                : Arrays.copyOfRange(value, valueOffset, valueOffset + valueLength());
    }

    /** Copies this value and its dictionary into independently owned arrays. */
    public Variant copy() {
        byte[] bytes = valueBuffer();
        return new Variant(
                metadata().clone(),
                Arrays.copyOfRange(bytes, valueOffset, valueOffset + valueLength()));
    }

    private byte[] valueBuffer() {
        // Root values use the accessor so subclasses can supply their encoding lazily.
        return valueOffset == 0 ? value() : value;
    }

    private int valueLength() {
        return valueOffset == 0 ? value().length : VariantUtil.valueSize(value, valueOffset);
    }

    // --------------------------------------------------------------------------------------------
    // Binary serialization (for row-oriented storage)
    // --------------------------------------------------------------------------------------------

    /**
     * Serializes this variant to a single byte array for storage.
     *
     * <p>Format: {@code [4-byte metadata length (little-endian)][metadata bytes][value bytes]}. The
     * enclosing row stores the total length, including the four-byte prefix.
     */
    public byte[] toBytes() {
        byte[] m = metadata();
        byte[] v = valueBuffer();
        int valueLength = valueLength();
        VariantUtil.checkComponentSize(m.length);
        VariantUtil.checkComponentSize(valueLength);
        byte[] result = new byte[4 + m.length + valueLength];
        ByteBuffer buf = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(m.length);
        buf.put(m);
        buf.put(v, valueOffset, valueLength);
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
     * @param length the exact blob length supplied by the enclosing format, including the prefix;
     *     the remaining bytes after metadata are the value
     * @return the deserialized Variant
     */
    public static Variant fromBytes(byte[] bytes, int offset, int length) {
        checkNotNull(bytes, "bytes must not be null");
        checkArgument(
                offset >= 0 && length >= 4 && offset <= bytes.length - length,
                "Invalid Variant byte range.");
        ByteBuffer buf = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        int metadataLen = buf.getInt();
        checkArgument(
                metadataLen >= 3 && metadataLen < length - 4,
                "Invalid Variant metadata length: %s",
                metadataLen);
        VariantUtil.checkComponentSize(metadataLen);
        VariantUtil.checkComponentSize(length - 4 - metadataLen);
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
        return VariantUtil.basicType(valueBuffer(), valueOffset);
    }

    /** Returns true if this variant value is null. */
    public boolean isNull() {
        return VariantUtil.isNull(valueBuffer(), valueOffset);
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

    private int primitiveType() {
        return isPrimitive() ? VariantUtil.primitiveTypeId(valueBuffer(), valueOffset) : -1;
    }

    /** Returns the boolean value of this variant. */
    public boolean getBoolean() {
        return VariantUtil.getBoolean(valueBuffer(), valueOffset);
    }

    /** Returns the byte value of this variant. */
    public byte getByte() {
        return VariantUtil.getByte(valueBuffer(), valueOffset);
    }

    /** Returns the short value of this variant. */
    public short getShort() {
        return primitiveType() == VariantUtil.PRIMITIVE_TYPE_INT8
                ? getByte()
                : VariantUtil.getShort(valueBuffer(), valueOffset);
    }

    /** Returns the int value of this variant. */
    public int getInt() {
        return primitiveType() == VariantUtil.PRIMITIVE_TYPE_INT8
                        || primitiveType() == VariantUtil.PRIMITIVE_TYPE_INT16
                ? getShort()
                : VariantUtil.getInt(valueBuffer(), valueOffset);
    }

    /** Returns the long value of this variant. */
    public long getLong() {
        return primitiveType() >= VariantUtil.PRIMITIVE_TYPE_INT8
                        && primitiveType() <= VariantUtil.PRIMITIVE_TYPE_INT32
                ? getInt()
                : VariantUtil.getLong(valueBuffer(), valueOffset);
    }

    /** Returns the float value of this variant. */
    public float getFloat() {
        return VariantUtil.getFloat(valueBuffer(), valueOffset);
    }

    /** Returns the double value of this variant. */
    public double getDouble() {
        return VariantUtil.getDouble(valueBuffer(), valueOffset);
    }

    /** Returns the string value of this variant. */
    public String getString() {
        return VariantUtil.getString(valueBuffer(), valueOffset);
    }

    /** Returns the binary value of this variant. */
    public byte[] getBinary() {
        return VariantUtil.getBinary(valueBuffer(), valueOffset);
    }

    /** Returns the decimal value of this variant. */
    public BigDecimal getDecimal() {
        return VariantUtil.getDecimal(valueBuffer(), valueOffset);
    }

    /** Returns the date value (days since epoch). */
    public int getDate() {
        return VariantUtil.getDate(valueBuffer(), valueOffset);
    }

    /** Returns the timestamp value (microseconds since epoch). */
    public long getTimestamp() {
        return VariantUtil.getTimestamp(valueBuffer(), valueOffset);
    }

    /** Returns the timestamp without timezone value (microseconds since epoch). */
    public long getTimestampNtz() {
        return VariantUtil.getTimestampNtz(valueBuffer(), valueOffset);
    }

    /**
     * Returns the total size of this variant in bytes (metadata + value).
     *
     * @return total byte size
     */
    public int sizeInBytes() {
        return metadata().length + valueLength();
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
        return VariantUtil.objectSize(valueBuffer(), valueOffset);
    }

    /**
     * Gets a field value from this object variant by field name.
     *
     * @param fieldName the field name to look up
     * @return a shared view of the field value, or null if the field does not exist
     */
    public Variant getFieldByName(String fieldName) {
        if (!isObject()) {
            return null;
        }
        byte[] metadata = metadata();
        byte[] value = valueBuffer();
        int fieldId = VariantUtil.findFieldId(metadata, fieldName);
        if (fieldId < 0) {
            return null;
        }
        int offset = VariantUtil.findFieldValueOffset(metadata, value, valueOffset, fieldId);
        return offset < 0 ? null : slice(metadata, value, offset);
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
        return VariantUtil.arraySize(valueBuffer(), valueOffset);
    }

    /**
     * Gets an element from this array variant by index.
     *
     * @param index the element index
     * @return a shared view of the element, or null if this is not an array or the index is invalid
     */
    public Variant getElementAt(int index) {
        if (!isArray() || index < 0 || index >= arraySize()) {
            return null;
        }
        byte[] bytes = valueBuffer();
        return slice(metadata(), bytes, VariantUtil.arrayElementOffset(bytes, valueOffset, index));
    }

    private static Variant slice(byte[] metadata, byte[] value, int offset) {
        int length = VariantUtil.valueSize(value, offset);
        checkArgument(
                offset >= 0 && length > 0 && offset <= value.length - length,
                "Invalid Variant sub-value range.");
        return new Variant(metadata, value, offset);
    }

    // --------------------------------------------------------------------------------------------
    // JSON conversion
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a Variant from a single, strictly validated JSON value.
     *
     * @param json the JSON string to parse
     * @return a new Variant representing the parsed JSON
     */
    public static Variant fromJson(String json) {
        return JsonToVariant.parse(json);
    }

    /**
     * Converts this variant to its JSON string representation.
     *
     * <p>Dates use ISO local dates. Timestamps use ISO date-times in UTC with an explicit {@code
     * +00:00} offset; timestamps without time zone omit the offset. Fractions preserve microsecond
     * precision. JSON has no temporal types, so parsing these strings does not recover the original
     * Variant type. Binary values use Base64 strings. Non-finite floats and doubles use the JSON
     * strings {@code "NaN"}, {@code "Infinity"}, and {@code "-Infinity"}; parsing these strings
     * likewise does not recover the numeric type.
     *
     * @return a JSON string
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        appendJson(metadata(), valueBuffer(), valueOffset, sb);
        return sb.toString();
    }

    private static void appendJson(byte[] metadata, byte[] value, int offset, StringBuilder sb) {
        int basic = VariantUtil.basicType(value, offset);
        if (basic == VariantUtil.BASIC_TYPE_OBJECT) {
            sb.append('{');
            for (int i = 0; i < VariantUtil.objectSize(value, offset); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                appendQuoted(
                        VariantUtil.metadataFieldName(
                                metadata, VariantUtil.objectFieldId(value, offset, i)),
                        sb);
                sb.append(':');
                appendJson(
                        metadata, value, VariantUtil.objectFieldValueOffset(value, offset, i), sb);
            }
            sb.append('}');
        } else if (basic == VariantUtil.BASIC_TYPE_ARRAY) {
            sb.append('[');
            for (int i = 0; i < VariantUtil.arraySize(value, offset); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                appendJson(metadata, value, VariantUtil.arrayElementOffset(value, offset, i), sb);
            }
            sb.append(']');
        } else if (basic == VariantUtil.BASIC_TYPE_SHORT_STRING) {
            appendQuoted(VariantUtil.getString(value, offset), sb);
        } else {
            switch (VariantUtil.primitiveTypeId(value, offset)) {
                case VariantUtil.PRIMITIVE_TYPE_NULL:
                    sb.append("null");
                    break;
                case VariantUtil.PRIMITIVE_TYPE_TRUE:
                case VariantUtil.PRIMITIVE_TYPE_FALSE:
                    sb.append(VariantUtil.getBoolean(value, offset));
                    break;
                case VariantUtil.PRIMITIVE_TYPE_INT8:
                    sb.append(VariantUtil.getByte(value, offset));
                    break;
                case VariantUtil.PRIMITIVE_TYPE_INT16:
                    sb.append(VariantUtil.getShort(value, offset));
                    break;
                case VariantUtil.PRIMITIVE_TYPE_INT32:
                    sb.append(VariantUtil.getInt(value, offset));
                    break;
                case VariantUtil.PRIMITIVE_TYPE_INT64:
                    sb.append(VariantUtil.getLong(value, offset));
                    break;
                case VariantUtil.PRIMITIVE_TYPE_FLOAT:
                    float floatValue = VariantUtil.getFloat(value, offset);
                    if (Float.isFinite(floatValue)) {
                        sb.append(floatValue);
                    } else {
                        appendQuoted(Float.toString(floatValue), sb);
                    }
                    break;
                case VariantUtil.PRIMITIVE_TYPE_DOUBLE:
                    double doubleValue = VariantUtil.getDouble(value, offset);
                    if (Double.isFinite(doubleValue)) {
                        sb.append(doubleValue);
                    } else {
                        appendQuoted(Double.toString(doubleValue), sb);
                    }
                    break;
                case VariantUtil.PRIMITIVE_TYPE_DECIMAL4:
                case VariantUtil.PRIMITIVE_TYPE_DECIMAL8:
                case VariantUtil.PRIMITIVE_TYPE_DECIMAL16:
                    sb.append(VariantUtil.getDecimal(value, offset).toPlainString());
                    break;
                case VariantUtil.PRIMITIVE_TYPE_DATE:
                    appendQuoted(
                            LocalDate.ofEpochDay(VariantUtil.getDate(value, offset)).toString(),
                            sb);
                    break;
                case VariantUtil.PRIMITIVE_TYPE_TIMESTAMP:
                    appendQuoted(
                            formatTimestamp(VariantUtil.getTimestamp(value, offset)) + "+00:00",
                            sb);
                    break;
                case VariantUtil.PRIMITIVE_TYPE_TIMESTAMP_NTZ:
                    appendQuoted(formatTimestamp(VariantUtil.getTimestampNtz(value, offset)), sb);
                    break;
                case VariantUtil.PRIMITIVE_TYPE_STRING:
                    appendQuoted(VariantUtil.getString(value, offset), sb);
                    break;
                case VariantUtil.PRIMITIVE_TYPE_BINARY:
                    appendQuoted(
                            java.util.Base64.getEncoder()
                                    .encodeToString(VariantUtil.getBinary(value, offset)),
                            sb);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported Variant primitive type: "
                                    + VariantUtil.primitiveTypeId(value, offset));
            }
        }
    }

    private static String formatTimestamp(long micros) {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.ofEpochSecond(
                        Math.floorDiv(micros, 1_000_000),
                        (int) Math.floorMod(micros, 1_000_000) * 1_000,
                        ZoneOffset.UTC));
    }

    private static void appendQuoted(String value, StringBuilder sb) {
        sb.append('"');
        escapeJsonString(value, sb);
        sb.append('"');
    }

    private static void escapeJsonString(String s, StringBuilder sb) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
    }

    /** Compares the encoded metadata and selected value bytes, independently of backing arrays. */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Variant)) {
            return false;
        }
        Variant variant = (Variant) o;
        if (!Arrays.equals(metadata(), variant.metadata())) {
            return false;
        }
        int length = valueLength();
        if (length != variant.valueLength()) {
            return false;
        }
        byte[] left = valueBuffer();
        byte[] right = variant.valueBuffer();
        for (int i = 0; i < length; i++) {
            if (left[valueOffset + i] != right[variant.valueOffset + i]) {
                return false;
            }
        }
        return true;
    }

    /** Returns a hash of the encoded metadata and selected value bytes. */
    @Override
    public int hashCode() {
        byte[] bytes = valueBuffer();
        int length = valueLength();
        int valueHash = 1;
        for (int i = 0; i < length; i++) {
            valueHash = 31 * valueHash + bytes[valueOffset + i];
        }
        return 31 * Arrays.hashCode(metadata()) + valueHash;
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
        byte[] stringBytes = VariantUtil.encodeUtf8(value);
        return new Variant(emptyMetadata(), VariantUtil.encodeString(stringBytes));
    }
}
