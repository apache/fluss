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

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.StringUtils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * An internal data structure representing data of {@link RowType}.
 *
 * <p>{@link GenericRow} is a generic implementation of {@link InternalRow} which is backed by an
 * array of Java {@link Object}. A {@link GenericRow} can have an arbitrary number of fields of
 * different types. The fields in a row can be accessed by position (0-based) using either the
 * generic {@link #getField(int)} or type-specific getters (such as {@link #getInt(int)}). A field
 * can be updated by the generic {@link #setField(int, Object)}.
 *
 * <p>Note: All fields of this data structure must be internal data structures. See {@link
 * InternalRow} for more information about internal data structures.
 *
 * <p>The fields in {@link GenericRow} can be null for representing nullability.
 *
 * @since 0.2
 */
@PublicEvolving
public final class GenericRow implements InternalRow, Serializable {

    /** The array to store the actual internal format values. */
    private final Object[] fields;

    /**
     * Creates an instance of {@link GenericRow} with number of fields.
     *
     * <p>Initially, all fields are set to null.
     *
     * <p>Note: All fields of the row must be internal data structures.
     *
     * @param numFields number of fields
     */
    public GenericRow(int numFields) {
        this.fields = new Object[numFields];
    }

    /**
     * Sets the field value at the given position.
     *
     * <p>Note: The given field value must be an internal data structures. Otherwise, the {@link
     * GenericRow} is corrupted and may throw exception when processing. See {@link InternalRow} for
     * more information about internal data structures.
     *
     * <p>The field value can be null for representing nullability.
     */
    public void setField(int pos, Object value) {
        this.fields[pos] = value;
    }

    /**
     * Returns the field value at the given position.
     *
     * <p>Note: The returned value is in internal data structure. See {@link InternalRow} for more
     * information about internal data structures.
     *
     * <p>The returned field value can be null for representing nullability.
     */
    public Object getField(int pos) {
        return this.fields[pos];
    }

    @Override
    public int getFieldCount() {
        return fields.length;
    }

    @Override
    public boolean isNullAt(int pos) {
        return this.fields[pos] == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) this.fields[pos];
    }

    @Override
    public byte getByte(int pos) {
        return (byte) this.fields[pos];
    }

    @Override
    public short getShort(int pos) {
        return (short) this.fields[pos];
    }

    @Override
    public int getInt(int pos) {
        return (int) this.fields[pos];
    }

    @Override
    public long getLong(int pos) {
        return (long) this.fields[pos];
    }

    @Override
    public float getFloat(int pos) {
        return (float) this.fields[pos];
    }

    @Override
    public double getDouble(int pos) {
        return (double) this.fields[pos];
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return (BinaryString) this.fields[pos];
    }

    @Override
    public BinaryString getString(int pos) {
        return (BinaryString) this.fields[pos];
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return (Decimal) this.fields[pos];
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return (TimestampNtz) this.fields[pos];
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return (TimestampLtz) this.fields[pos];
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return (byte[]) this.fields[pos];
    }

    @Override
    public byte[] getBytes(int pos) {
        return (byte[]) this.fields[pos];
    }

    @Override
    public InternalArray getArray(int pos) {
        return (InternalArray) this.fields[pos];
    }

    // TODO: getMap() will be added in Issue #1973
    // TODO: getRow() will be added in Issue #1974

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GenericRow)) {
            return false;
        }
        GenericRow that = (GenericRow) o;
        return Arrays.deepEquals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(fields);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < fields.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(StringUtils.arrayAwareToString(fields[i]));
        }
        sb.append(")");
        return sb.toString();
    }

    // ----------------------------------------------------------------------------------------
    // Utilities
    // ----------------------------------------------------------------------------------------

    /**
     * Creates an instance of {@link GenericRow} with given field values.
     *
     * <p>Note: All fields of the row must be internal data structures.
     */
    public static GenericRow of(Object... values) {
        GenericRow row = new GenericRow(values.length);

        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }

        return row;
    }
}
