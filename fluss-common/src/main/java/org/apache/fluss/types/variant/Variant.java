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
import org.apache.fluss.shaded.arrow.org.apache.parquet.variant.VariantBuilder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant.Type;
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
 * <h3>Read path: hybrid Arrow delegate + Fluss {@link VariantUtil}</h3>
 *
 * <p>This class deliberately uses two read paths:
 *
 * <ul>
 *   <li><b>High-level accessors</b> (typed getters such as {@link #getBoolean()}, {@link
 *       #getInt()}, {@link #getString()}, {@link #getFieldByName(String)}, {@link
 *       #getElementAt(int)}, {@link #toJson()}) are delegated to the Arrow shaded {@code
 *       parquet-variant} {@code Variant} class via {@link #getDelegate()}, so we share its
 *       battle-tested type dispatch.
 *   <li><b>Low-level byte/header inspection</b> (such as {@link #basicType()}, {@link #getDate()},
 *       {@link #getTimestamp()}, {@code encodeXxx} factories, and {@link #emptyMetadata()}) goes
 *       through Fluss {@link VariantUtil}. This is forced by the shaded Arrow library exposing its
 *       own {@code VariantUtil} as package-private (see the javadoc on {@link VariantUtil}). For
 *       these helpers we cannot call into Arrow from outside its package, so {@link VariantUtil} is
 *       a Fluss-side mirror of the Parquet Variant Binary Encoding spec.
 * </ul>
 *
 * @since 0.7
 */
@PublicEvolving
public class Variant implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] metadata;
    private final byte[] value;

    public Variant(byte[] metadata, byte[] value) {
        this.metadata = checkNotNull(metadata, "metadata must not be null");
        this.value = checkNotNull(value, "value must not be null");
    }

    /**
     * Protected constructor for subclasses that override {@link #metadata()} and {@link #value()}
     * to provide lazy initialization. All public methods in this class access data through the
     * accessor methods, so subclasses can safely override them.
     */
    protected Variant() {
        this.metadata = null;
        this.value = null;
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
        byte[] m = metadata();
        byte[] v = value();
        byte[] result = new byte[4 + m.length + v.length];
        ByteBuffer buf = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(m.length);
        buf.put(m);
        buf.put(v);
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
        return VariantUtil.basicType(value(), 0);
    }

    /** Returns true if this variant value is null. */
    public boolean isNull() {
        // Delegate to the Arrow shaded Variant for the typed null check (Type.NULL).
        return getDelegate().getType() == Type.NULL;
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
    // Parquet-variant delegate (lazy)
    // --------------------------------------------------------------------------------------------

    /**
     * Lazily created delegate to parquet-variant's Variant for read operations. This avoids
     * duplicating the Parquet Variant Binary Encoding read logic.
     */
    private transient org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant delegate;

    /**
     * Returns the parquet-variant delegate, creating it lazily from metadata()/value() bytes. This
     * is used by accessor methods to delegate read operations.
     */
    protected org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant getDelegate() {
        if (delegate == null) {
            delegate =
                    new org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant(
                            value(), metadata());
        }
        return delegate;
    }

    // --------------------------------------------------------------------------------------------
    // Primitive value accessors (delegated to parquet-variant)
    // --------------------------------------------------------------------------------------------

    /** Returns the boolean value of this variant. */
    public boolean getBoolean() {
        return getDelegate().getBoolean();
    }

    /** Returns the byte value of this variant. */
    public byte getByte() {
        return getDelegate().getByte();
    }

    /** Returns the short value of this variant. */
    public short getShort() {
        return getDelegate().getShort();
    }

    /** Returns the int value of this variant. */
    public int getInt() {
        return getDelegate().getInt();
    }

    /** Returns the long value of this variant. */
    public long getLong() {
        return getDelegate().getLong();
    }

    /** Returns the float value of this variant. */
    public float getFloat() {
        return getDelegate().getFloat();
    }

    /** Returns the double value of this variant. */
    public double getDouble() {
        return getDelegate().getDouble();
    }

    /** Returns the string value of this variant. */
    public String getString() {
        return getDelegate().getString();
    }

    /** Returns the binary value of this variant. */
    public byte[] getBinary() {
        ByteBuffer buf = getDelegate().getBinary();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    /** Returns the decimal value of this variant. */
    public BigDecimal getDecimal() {
        return getDelegate().getDecimal();
    }

    /**
     * Returns the date value (days since epoch) of this variant.
     *
     * <p>Note: the Arrow shaded {@code Variant} only exposes generic {@code getInt()} / {@code
     * getLong()} accessors for DATE / TIMESTAMP types — they read raw bytes without verifying the
     * variant type. Going through {@link VariantUtil#getDate(byte[], int)} keeps the type assertion
     * explicit and avoids the indirect ByteBuffer hop.
     */
    public int getDate() {
        return VariantUtil.getDate(value(), 0);
    }

    /**
     * Returns the timestamp value (microseconds since epoch) of this variant.
     *
     * <p>See {@link #getDate()} for why this stays on {@link VariantUtil} rather than the Arrow
     * delegate.
     */
    public long getTimestamp() {
        return VariantUtil.getTimestamp(value(), 0);
    }

    /**
     * Returns the timestamp without timezone value (microseconds since epoch) of this variant.
     *
     * <p>See {@link #getDate()} for why this stays on {@link VariantUtil} rather than the Arrow
     * delegate.
     */
    public long getTimestampNtz() {
        return VariantUtil.getTimestampNtz(value(), 0);
    }

    /**
     * Returns the total size of this variant in bytes (metadata + value).
     *
     * @return total byte size
     */
    public int sizeInBytes() {
        return metadata().length + value().length;
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
        return getDelegate().numObjectElements();
    }

    /**
     * Gets a field value from this object variant by field name.
     *
     * @param fieldName the field name to look up
     * @return a new Variant for the field value, or null if the field does not exist
     */
    public Variant getFieldByName(String fieldName) {
        org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant field =
                getDelegate().getFieldByKey(fieldName);
        if (field == null) {
            return null;
        }
        return fromParquetVariant(field);
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
        return getDelegate().numArrayElements();
    }

    /**
     * Gets an element from this array variant by index.
     *
     * @param index the element index
     * @return a new Variant for the element value
     */
    public Variant getElementAt(int index) {
        return fromParquetVariant(getDelegate().getElementAtIndex(index));
    }

    /**
     * Converts a parquet-variant Variant back to a Fluss Variant by extracting the metadata and
     * value bytes.
     */
    private static Variant fromParquetVariant(
            org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant pv) {
        ByteBuffer metaBuf = pv.getMetadataBuffer().duplicate();
        ByteBuffer valBuf = pv.getValueBuffer().duplicate();
        byte[] m = new byte[metaBuf.remaining()];
        metaBuf.get(m);
        byte[] v = new byte[valBuf.remaining()];
        valBuf.get(v);
        return new Variant(m, v);
    }

    // --------------------------------------------------------------------------------------------
    // JSON conversion
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a Variant from a JSON string using parquet-variant's VariantBuilder.
     *
     * @param json the JSON string to parse
     * @return a new Variant representing the parsed JSON
     */
    public static Variant fromJson(String json) {
        VariantBuilder builder = new VariantBuilder();
        JsonToVariant.parse(json, builder);
        org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant built = builder.build();
        ByteBuffer metaBuf = built.getMetadataBuffer();
        ByteBuffer valBuf = built.getValueBuffer();
        byte[] meta = new byte[metaBuf.remaining()];
        metaBuf.get(meta);
        byte[] val = new byte[valBuf.remaining()];
        valBuf.get(val);
        return new Variant(meta, val);
    }

    /**
     * Converts this variant to its JSON string representation. Uses parquet-variant's type system
     * and accessor methods to produce standard JSON output.
     *
     * @return a JSON string
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        appendJson(getDelegate(), sb);
        return sb.toString();
    }

    private static void appendJson(
            org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant v, StringBuilder sb) {
        org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant.Type type = v.getType();
        switch (type) {
            case NULL:
                sb.append("null");
                break;
            case BOOLEAN:
                sb.append(v.getBoolean());
                break;
            case BYTE:
                sb.append(v.getByte());
                break;
            case SHORT:
                sb.append(v.getShort());
                break;
            case INT:
                sb.append(v.getInt());
                break;
            case LONG:
                sb.append(v.getLong());
                break;
            case FLOAT:
                sb.append(v.getFloat());
                break;
            case DOUBLE:
                sb.append(v.getDouble());
                break;
            case DATE:
                sb.append(v.getInt());
                break;
            case TIMESTAMP_TZ:
            case TIMESTAMP_NTZ:
                sb.append(v.getLong());
                break;
            case DECIMAL4:
            case DECIMAL8:
            case DECIMAL16:
                sb.append(v.getDecimal().toPlainString());
                break;
            case STRING:
                sb.append('"');
                escapeJsonString(v.getString(), sb);
                sb.append('"');
                break;
            case BINARY:
                sb.append('"');
                ByteBuffer binBuf = v.getBinary();
                byte[] binBytes = new byte[binBuf.remaining()];
                binBuf.get(binBytes);
                sb.append(java.util.Base64.getEncoder().encodeToString(binBytes));
                sb.append('"');
                break;
            case OBJECT:
                sb.append('{');
                int numFields = v.numObjectElements();
                for (int i = 0; i < numFields; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    org.apache.fluss.shaded.arrow.org.apache.parquet.variant.Variant.ObjectField
                            field = v.getFieldAtIndex(i);
                    sb.append('"');
                    escapeJsonString(field.key, sb);
                    sb.append("\":");
                    appendJson(field.value, sb);
                }
                sb.append('}');
                break;
            case ARRAY:
                sb.append('[');
                int numElements = v.numArrayElements();
                for (int i = 0; i < numElements; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    appendJson(v.getElementAtIndex(i), sb);
                }
                sb.append(']');
                break;
            default:
                sb.append("null");
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Variant)) {
            return false;
        }
        Variant variant = (Variant) o;
        return Arrays.equals(metadata(), variant.metadata())
                && Arrays.equals(value(), variant.value());
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(metadata());
        result = 31 * result + Arrays.hashCode(value());
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
