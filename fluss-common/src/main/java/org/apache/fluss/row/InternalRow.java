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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;

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
                // TODO: MAP support will be added in Issue #1973
                // TODO: ROW support will be added in Issue #1974
            case MAP:
            case ROW:
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

    /** Accessor for getting the field of a row during runtime. */
    interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(InternalRow row);
    }
}
