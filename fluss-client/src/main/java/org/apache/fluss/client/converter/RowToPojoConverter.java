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

package org.apache.fluss.client.converter;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Converter for scanner path: converts InternalRow (possibly projected) to POJO, leaving
 * non-projected fields as null on the POJO. Validation is done against the full table schema.
 */
public final class RowToPojoConverter<T> {

    private final PojoType<T> pojoType;
    private final RowType tableSchema;
    private final RowType projection;
    private final List<String> projectionFieldNames;
    private final RowToField[] rowReaders;

    private RowToPojoConverter(PojoType<T> pojoType, RowType tableSchema, RowType projection) {
        this.pojoType = pojoType;
        this.tableSchema = tableSchema;
        this.projection = projection;
        this.projectionFieldNames = projection.getFieldNames();
        ConverterCommons.validatePojoMatchesTable(pojoType, tableSchema);
        ConverterCommons.validateProjectionSubset(projection, tableSchema);
        this.rowReaders = createRowReaders();
    }

    public static <T> RowToPojoConverter<T> of(
            Class<T> pojoClass, RowType tableSchema, RowType projection) {
        return new RowToPojoConverter<>(PojoType.of(pojoClass), tableSchema, projection);
    }

    public T fromRow(@Nullable InternalRow row) {
        if (row == null) {
            return null;
        }
        try {
            T pojo = pojoType.getDefaultConstructor().newInstance();
            for (int i = 0; i < rowReaders.length; i++) {
                if (!row.isNullAt(i)) {
                    Object v = rowReaders[i].convert(row, i);
                    PojoType.Property prop = pojoType.getProperty(projectionFieldNames.get(i));
                    if (v != null) {
                        prop.write(pojo, v);
                    }
                }
            }
            return pojo;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            String message =
                    String.format(
                            "Failed to instantiate POJO class %s using the public default constructor. Cause: %s",
                            pojoType.getPojoClass().getName(), e.getMessage());
            throw new IllegalStateException(message, e);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to set field on POJO class " + pojoType.getPojoClass().getName(), e);
        }
    }

    private RowToField[] createRowReaders() {
        RowToField[] arr = new RowToField[projection.getFieldCount()];
        for (int i = 0; i < projection.getFieldCount(); i++) {
            String name = projectionFieldNames.get(i);
            DataType type = projection.getTypeAt(i);
            PojoType.Property prop = requireProperty(name);
            ConverterCommons.validateCompatibility(type, prop);
            arr[i] = createRowReader(type, prop);
        }
        return arr;
    }

    private PojoType.Property requireProperty(String fieldName) {
        PojoType.Property p = pojoType.getProperty(fieldName);
        if (p == null) {
            throw new IllegalArgumentException(
                    "Field '"
                            + fieldName
                            + "' not found in POJO class "
                            + pojoType.getPojoClass().getName()
                            + ".");
        }
        return p;
    }

    private static RowToField createRowReader(DataType fieldType, PojoType.Property prop) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return InternalRow::getBoolean;
            case TINYINT:
                return InternalRow::getByte;
            case SMALLINT:
                return InternalRow::getShort;
            case INTEGER:
                return InternalRow::getInt;
            case BIGINT:
                return InternalRow::getLong;
            case FLOAT:
                return InternalRow::getFloat;
            case DOUBLE:
                return InternalRow::getDouble;
            case CHAR:
            case STRING:
                return (row, pos) -> convertTextValue(fieldType, prop, row.getString(pos));
            case BINARY:
            case BYTES:
                return InternalRow::getBytes;
            case DECIMAL:
                return (row, pos) -> convertDecimalValue((DecimalType) fieldType, row, pos);
            case DATE:
                return RowToPojoConverter::convertDateValue;
            case TIME_WITHOUT_TIME_ZONE:
                return RowToPojoConverter::convertTimeValue;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return (row, pos) -> convertTimestampNtzValue(precision, row, pos);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return (row, pos) -> convertTimestampLtzValue(precision, prop, row, pos);
                }
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported field type %s for field %s.",
                                fieldType.getTypeRoot(), prop.name));
        }
    }

    /**
     * Converts a text value (CHAR/STRING) read from an InternalRow into the target Java type
     * declared by the POJO property.
     *
     * <p>Supported target types are String and Character. For CHAR columns, this enforces that the
     * value has exactly one character. For Character targets, empty strings are rejected.
     *
     * @param fieldType Fluss column DataType (must be CHAR or STRING)
     * @param prop The target POJO property (used for type and error messages)
     * @param s The BinaryString read from the row
     * @return Converted Java value (String or Character)
     * @throws IllegalArgumentException if the target type is unsupported or constraints are
     *     violated
     */
    private static Object convertTextValue(
            DataType fieldType, PojoType.Property prop, BinaryString s) {
        String v = s.toString();
        if (prop.type == String.class) {
            if (fieldType.getTypeRoot() == DataTypeRoot.CHAR && v.length() != 1) {
                throw new IllegalArgumentException(
                        charLengthExceptionMessage(prop.name, v.length()));
            }
            return v;
        } else if (prop.type == Character.class) {
            if (v.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Field %s expects Character, but the string value is empty.",
                                prop.name));
            }
            if (fieldType.getTypeRoot() == DataTypeRoot.CHAR && v.length() != 1) {
                throw new IllegalArgumentException(
                        charLengthExceptionMessage(prop.name, v.length()));
            }
            return v.charAt(0);
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s is not a String or Character. Cannot convert from string.",
                        prop.name));
    }

    public static String charLengthExceptionMessage(String fieldName, int length) {
        return String.format(
                "Field %s expects exactly one character for CHAR type, got length %d.",
                fieldName, length);
    }

    /**
     * Converts a DECIMAL value from an InternalRow into a BigDecimal using the column's precision
     * and scale. The row position is assumed non-null (caller checks), so this never returns null.
     */
    private static BigDecimal convertDecimalValue(
            DecimalType decimalType, InternalRow row, int pos) {
        Decimal d = row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
        return d.toBigDecimal();
    }

    /** Converts a DATE value stored as int days since epoch to a LocalDate. */
    private static LocalDate convertDateValue(InternalRow row, int pos) {
        return LocalDate.ofEpochDay(row.getInt(pos));
    }

    /** Converts a TIME_WITHOUT_TIME_ZONE value stored as int millis of day to a LocalTime. */
    private static LocalTime convertTimeValue(InternalRow row, int pos) {
        return LocalTime.ofNanoOfDay(row.getInt(pos) * 1_000_000L);
    }

    /** Converts a TIMESTAMP_WITHOUT_TIME_ZONE value to a LocalDateTime honoring precision. */
    private static Object convertTimestampNtzValue(int precision, InternalRow row, int pos) {
        TimestampNtz t = row.getTimestampNtz(pos, precision);
        return t.toLocalDateTime();
    }

    /**
     * Converts a TIMESTAMP_WITH_LOCAL_TIME_ZONE value to either Instant or OffsetDateTime in UTC,
     * depending on the target POJO property type.
     */
    private static Object convertTimestampLtzValue(
            int precision, PojoType.Property prop, InternalRow row, int pos) {
        TimestampLtz t = row.getTimestampLtz(pos, precision);
        if (prop.type == Instant.class) {
            return t.toInstant();
        } else if (prop.type == OffsetDateTime.class) {
            return OffsetDateTime.ofInstant(t.toInstant(), ZoneOffset.UTC);
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s is not an Instant or OffsetDateTime. Cannot convert from TimestampData.",
                        prop.name));
    }

    private interface RowToField {
        Object convert(InternalRow row, int pos) throws Exception;
    }
}
