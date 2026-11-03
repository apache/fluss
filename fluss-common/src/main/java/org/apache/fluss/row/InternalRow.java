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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.columnar.VectorizedColumnBatch;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;

/**
 * Base interface for an internal data structure representing data of {@link RowType}.
 *
 * <p>The mappings from SQL data types to the internal data structures are listed in the following
 * table:
 *
 * <pre>
 * +--------------------------------+-----------------------------------------+
 * | SQL Data Types                 | Internal Data Structures                |
 * +--------------------------------+-----------------------------------------+
 * | BOOLEAN                        | boolean                                 |
 * +--------------------------------+-----------------------------------------+
 * | CHAR / STRING                  | {@link BinaryString}                    |
 * +--------------------------------+-----------------------------------------+
 * | BINARY / BYTES                 | byte[]                                  |
 * +--------------------------------+-----------------------------------------+
 * | DECIMAL                        | {@link Decimal}                         |
 * +--------------------------------+-----------------------------------------+
 * | TINYINT                        | byte                                    |
 * +--------------------------------+-----------------------------------------+
 * | SMALLINT                       | short                                   |
 * +--------------------------------+-----------------------------------------+
 * | INT                            | int                                     |
 * +--------------------------------+-----------------------------------------+
 * | BIGINT                         | long                                    |
 * +--------------------------------+-----------------------------------------+
 * | FLOAT                          | float                                   |
 * +--------------------------------+-----------------------------------------+
 * | DOUBLE                         | double                                  |
 * +--------------------------------+-----------------------------------------+
 * | DATE                           | int (number of days since epoch)        |
 * +--------------------------------+-----------------------------------------+
 * | TIME                           | int (number of milliseconds of the day) |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITHOUT TIME ZONE    | {@link TimestampNtz}                    |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH LOCAL TIME ZONE | {@link TimestampLtz}                    |
 * +--------------------------------+-----------------------------------------+
 * | ARRAY                          | {@link InternalArray}                   |
 * +--------------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>Nullability is always handled by the container data structure.
 *
 * @since 0.1
 */
@PublicEvolving
public interface InternalRow extends DataGetters {

    /**
     * Returns the number of fields in this row.
     *
     * <p>The number does not include {@link ChangeType}. It is kept separately.
     */
    int getFieldCount();

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /** Returns the data class for the given {@link DataType}. */
    static Class<?> getDataClass(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                return BinaryString.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case BYTES:
                return byte[].class;
            case DECIMAL:
                return Decimal.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampNtz.class;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampLtz.class;
            case ARRAY:
                return InternalArray.class;
            case MAP:
                return InternalMap.class;
            case ROW:
                return InternalRow.class;
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /**
     * Creates an array of accessors for getting fields in an internal row data structure.
     *
     * @param rowType the row type of the internal row
     */
    static FieldGetter[] createFieldGetters(RowType rowType) {
        final FieldGetter[] fieldGetters = new FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldGetters[i] = createFieldGetter(rowType.getTypeAt(i), i);
        }
        return fieldGetters;
    }

    /**
     * Creates an accessor for getting elements in an internal row data structure at the given
     * position.
     *
     * @param fieldType the element type of the row
     * @param fieldPos the element position of the row
     */
    static FieldGetter createFieldGetter(DataType fieldType, int fieldPos) {
        final FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
                final int bytesLength = getLength(fieldType);
                fieldGetter = row -> row.getChar(fieldPos, bytesLength);
                break;
            case STRING:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
                final int binaryLength = getLength(fieldType);
                fieldGetter = row -> row.getBinary(fieldPos, binaryLength);
                break;
            case BYTES:
                fieldGetter = row -> row.getBytes(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestampNtz(fieldPos, timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestampLtz(fieldPos, timestampLtzPrecision);
                break;
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
                final int numFields = ((RowType) fieldType).getFieldCount();
                fieldGetter = row -> row.getRow(fieldPos, numFields);
                break;
            default:
                throw new IllegalArgumentException("Illegal type: " + fieldType);
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /**
     * Creates a deep accessor for getting elements in an internal row data structure at the given
     * position. Returns new objects (GenericArray/GenericMap/GenericRow) for nested array/map/row
     * types to prevent use-after-free when the underlying buffer is released.
     *
     * <p>For ARROW, pass copyStrings=false since {@link VectorizedColumnBatch} already copies
     * strings. For INDEXED and COMPACTED, pass copyStrings=true to copy STRING/CHAR out of the
     * pooled buffer they reference.
     */
    static FieldGetter createDeepFieldGetter(
            DataType fieldType, int fieldPos, boolean copyStrings) {
        final FieldGetter fieldGetter;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
                final int charLen = getLength(fieldType);
                fieldGetter =
                        copyStrings
                                ? row -> row.getChar(fieldPos, charLen).copy()
                                : row -> row.getChar(fieldPos, charLen);
                break;
            case STRING:
                fieldGetter =
                        copyStrings
                                ? row -> row.getString(fieldPos).copy()
                                : row -> row.getString(fieldPos);
                break;
            case ARRAY:
                DataType elementType = ((ArrayType) fieldType).getElementType();
                InternalArray.ElementGetter nestedGetter =
                        InternalArray.createDeepElementGetter(elementType, copyStrings);
                fieldGetter =
                        row -> {
                            InternalArray array = row.getArray(fieldPos);
                            Object[] objs = new Object[array.size()];
                            for (int i = 0; i < array.size(); i++) {
                                objs[i] = nestedGetter.getElementOrNull(array, i);
                            }
                            return new GenericArray(objs);
                        };
                break;
            case MAP:
                MapType mapType = (MapType) fieldType;
                InternalArray.ElementGetter keyGetter =
                        InternalArray.createDeepElementGetter(mapType.getKeyType(), copyStrings);
                InternalArray.ElementGetter valueGetter =
                        InternalArray.createDeepElementGetter(mapType.getValueType(), copyStrings);
                fieldGetter =
                        row -> {
                            InternalMap map = row.getMap(fieldPos);
                            Map<Object, Object> javaMap = new HashMap<>();
                            for (int i = 0; i < map.size(); i++) {
                                Object key = keyGetter.getElementOrNull(map.keyArray(), i);
                                Object value = valueGetter.getElementOrNull(map.valueArray(), i);
                                javaMap.put(key, value);
                            }
                            return new GenericMap(javaMap);
                        };
                break;
            case ROW:
                RowType rowType = (RowType) fieldType;
                int numFields = rowType.getFieldCount();
                FieldGetter[] nestedFieldGetters = new FieldGetter[numFields];
                for (int i = 0; i < numFields; i++) {
                    nestedFieldGetters[i] =
                            createDeepFieldGetter(rowType.getTypeAt(i), i, copyStrings);
                }
                fieldGetter =
                        row -> {
                            InternalRow nestedRow = row.getRow(fieldPos, numFields);
                            GenericRow genericRow = new GenericRow(numFields);
                            for (int i = 0; i < numFields; i++) {
                                genericRow.setField(
                                        i, nestedFieldGetters[i].getFieldOrNull(nestedRow));
                            }
                            return genericRow;
                        };
                break;
            default:
                // for primitive types, use the normal field getter
                fieldGetter = createFieldGetter(fieldType, fieldPos);
                break;
        }

        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /** Accessor for getting the field of a row during runtime. */
    interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(InternalRow row);
    }

    /**
     * Creates an array of converters for getting all fields of an internal row data structure as
     * human-readable strings.
     *
     * @param rowType the row type of the internal row
     */
    static FieldToString[] createFieldToStrings(RowType rowType) {
        final FieldToString[] fieldToStrings = new FieldToString[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldToStrings[i] = createFieldToString(rowType.getTypeAt(i), i);
        }
        return fieldToStrings;
    }

    /**
     * Creates a converter that reads the field at the given position of an internal row and renders
     * it as a human-readable string.
     *
     * <p>Rendering conventions:
     *
     * <ul>
     *   <li>A null field is rendered as Java {@code null}, regardless of the declared nullability
     *       of the field.
     *   <li>CHAR, STRING, BOOLEAN, numeric types and DECIMAL are rendered as their Java string
     *       form, e.g. {@code hello}, {@code true}, {@code 3.14}, {@code 1.50}. FLOAT and DOUBLE
     *       follow {@link Float#toString} and {@link Double#toString} respectively and may use
     *       scientific notation for extreme magnitudes (e.g. {@code 1.0E-4}); DECIMAL never does.
     *   <li>DATE is rendered as {@code yyyy-MM-dd}; TIME as {@code HH:mm:ss[.fff]} with at most the
     *       declared precision fractional digits (the internal representation holds milliseconds of
     *       the day, so at most 3 fractional digits are ever printed); TIMESTAMP and TIMESTAMP_LTZ
     *       as SQL-style {@code yyyy-MM-dd HH:mm:ss[.fff]} with at most the declared precision
     *       fractional digits, trailing zeros trimmed. TIMESTAMP_LTZ is rendered in UTC with a
     *       {@code 'Z'} zone suffix.
     *   <li>BINARY and BYTES values are rendered as Base64 of the bytes returned by the row's
     *       getter; the bytes themselves are used as-is, neither padded nor trimmed. Note that
     *       IndexedRow, including rows nested inside an indexed row, strips trailing 0x00 bytes
     *       when reading a BINARY(n) field, because its fixed-length encoding does not record the
     *       actual length. Array elements, map keys and values, and other row implementations
     *       return the bytes as stored. A BINARY(n) value whose last byte is 0x00 may therefore
     *       render with fewer bytes when read from an indexed row. BYTES is not affected.
     *   <li>ARRAY, MAP and ROW are rendered as JSON: arrays as {@code [1,"a"]}, maps as {@code
     *       {"one":1}} with keys converted to their string representation (map key types are
     *       declared non-nullable, but writers do not enforce it, so a null key is rendered as the
     *       string {@code "null"}), rows as {@code {"id":1}} with the row's field names. Distinct
     *       keys whose string representations collide — e.g. timestamps differing beyond the
     *       declared precision, or fixed-length BINARY/CHAR values differing only in trailing pad
     *       bytes that some row implementations trim on read — are merged into a single JSON entry,
     *       because keys are rendered with the same conventions as values. Null elements inside
     *       nested structures are rendered as JSON {@code null}. Note that non-finite DOUBLE values
     *       (NaN, Infinity) are not representable in JSON and follow Jackson's behavior.
     * </ul>
     *
     * <p>A field whose runtime representation does not match the declared type fails with an {@link
     * IllegalArgumentException} that reports where the rendering failed with the original exception
     * as the cause.
     *
     * @param fieldType the element type of the row
     * @param fieldPos the element position of the row
     */
    static FieldToString createFieldToString(DataType fieldType, int fieldPos) {
        return FieldToStringConverters.createFieldToString(fieldType, fieldPos);
    }

    /** Accessor for getting the field of a row as a human-readable string. */
    interface FieldToString extends Serializable {
        @Nullable
        String getFieldAsStringOrNull(InternalRow row);
    }
}
