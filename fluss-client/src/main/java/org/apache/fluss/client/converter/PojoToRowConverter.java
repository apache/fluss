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
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * Converter for writer path: converts POJO instances to Fluss InternalRow according to a (possibly
 * projected) RowType. Validation is done against the full table schema.
 */
public final class PojoToRowConverter<T> {

    private final PojoType<T> pojoType;
    private final RowType tableSchema;
    private final RowType projection;
    private final List<String> projectionFieldNames;
    private final FieldToRow[] fieldConverters; // index corresponds to projection position

    private PojoToRowConverter(PojoType<T> pojoType, RowType tableSchema, RowType projection) {
        this.pojoType = pojoType;
        this.tableSchema = tableSchema;
        this.projection = projection;
        this.projectionFieldNames = projection.getFieldNames();
        ConverterCommons.validatePojoMatchesTable(pojoType, tableSchema);
        ConverterCommons.validateProjectionSubset(projection, tableSchema);
        this.fieldConverters = createFieldConverters();
    }

    public static <T> PojoToRowConverter<T> of(
            Class<T> pojoClass, RowType tableSchema, RowType projection) {
        return new PojoToRowConverter<>(PojoType.of(pojoClass), tableSchema, projection);
    }

    public GenericRow toRow(@Nullable T pojo) {
        if (pojo == null) {
            return null;
        }
        GenericRow row = new GenericRow(projection.getFieldCount());
        for (int i = 0; i < fieldConverters.length; i++) {
            Object v;
            try {
                v = fieldConverters[i].readAndConvert(pojo);
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to access field '"
                                + projectionFieldNames.get(i)
                                + "' from POJO "
                                + pojoType.getPojoClass().getName(),
                        e);
            }
            row.setField(i, v);
        }
        return row;
    }

    private FieldToRow[] createFieldConverters() {
        FieldToRow[] arr = new FieldToRow[projection.getFieldCount()];
        for (int i = 0; i < projection.getFieldCount(); i++) {
            String fieldName = projectionFieldNames.get(i);
            DataType fieldType = projection.getTypeAt(i);
            PojoType.Property prop = requireProperty(fieldName);
            ConverterCommons.validateCompatibility(fieldType, prop);
            arr[i] = createFieldConverter(prop, fieldType);
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

    private static FieldToRow createFieldConverter(PojoType.Property prop, DataType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
            case BINARY:
            case BYTES:
                return prop::read; // No casting needed

            case TINYINT:
                return (obj) -> convertToTinyInt(prop, prop.read(obj));
            case SMALLINT:
                return (obj) -> convertToSmallInt(prop, prop.read(obj));
            case INTEGER:
                return (obj) -> convertToInteger(prop, prop.read(obj));
            case BIGINT:
                return (obj) -> convertToBigInt(prop, prop.read(obj));
            case FLOAT:
                return (obj) -> convertToFloat(prop, prop.read(obj));
            case DOUBLE:
                return (obj) -> convertToDouble(prop, prop.read(obj));

            case CHAR:
            case STRING:
                return (obj) -> convertTextValue(fieldType, prop, prop.read(obj));
            case DECIMAL:
                return (obj) -> convertDecimalValue((DecimalType) fieldType, prop, prop.read(obj));
            case DATE:
                return (obj) -> convertDateValue(prop, prop.read(obj));
            case TIME_WITHOUT_TIME_ZONE:
                return (obj) -> convertTimeValue(prop, prop.read(obj));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (obj) -> convertTimestampNtzValue(prop, prop.read(obj));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (obj) -> convertTimestampLtzValue(prop, prop.read(obj));
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported field type %s for field %s.",
                                fieldType.getTypeRoot(), prop.name));
        }
    }

    /**
     * Converts a text value (String or Character) from a POJO property to Fluss BinaryString.
     *
     * <p>For CHAR columns, enforces that the text has exactly one character. Nulls are passed
     * through.
     */
    private static @Nullable BinaryString convertTextValue(
            DataType fieldType, PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        return ConverterCommons.toBinaryStringForText(v, prop.name, fieldType.getTypeRoot());
    }

    /** Converts a BigDecimal POJO property to Fluss Decimal respecting precision and scale. */
    private static @Nullable Decimal convertDecimalValue(
            DecimalType decimalType, PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof BigDecimal)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not a BigDecimal. Cannot convert to Decimal.", prop.name));
        }
        return Decimal.fromBigDecimal(
                (BigDecimal) v, decimalType.getPrecision(), decimalType.getScale());
    }

    /** Converts a LocalDate POJO property to number of days since epoch. */
    private static @Nullable Integer convertDateValue(PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof LocalDate)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not a LocalDate. Cannot convert to int days.", prop.name));
        }
        return (int) ((LocalDate) v).toEpochDay();
    }

    /** Converts a LocalTime POJO property to milliseconds of day. */
    private static @Nullable Integer convertTimeValue(PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof LocalTime)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not a LocalTime. Cannot convert to int millis.",
                            prop.name));
        }
        LocalTime t = (LocalTime) v;
        return (int) (t.toNanoOfDay() / 1_000_000);
    }

    /** Converts a LocalDateTime POJO property to Fluss TimestampNtz. */
    private static @Nullable TimestampNtz convertTimestampNtzValue(
            PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof LocalDateTime)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not a LocalDateTime. Cannot convert to TimestampNtz.",
                            prop.name));
        }
        return TimestampNtz.fromLocalDateTime((LocalDateTime) v);
    }

    /** Converts an Instant or OffsetDateTime POJO property to Fluss TimestampLtz (UTC based). */
    private static @Nullable TimestampLtz convertTimestampLtzValue(
            PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Instant) {
            return TimestampLtz.fromInstant((Instant) v);
        } else if (v instanceof OffsetDateTime) {
            return TimestampLtz.fromInstant(((OffsetDateTime) v).toInstant());
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s is not an Instant or OffsetDateTime. Cannot convert to TimestampLtz.",
                        prop.name));
    }

    /** Converts a numeric POJO property to TINYINT (Byte). */
    private static @Nullable Byte convertToTinyInt(PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Byte) {
            return (Byte) v;
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s cannot be converted to TINYINT. Type: %s",
                        prop.name, v.getClass().getName()));
    }

    /** Converts a numeric POJO property to SMALLINT (Short) with widening support. */
    private static @Nullable Short convertToSmallInt(PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Short) {
            return (Short) v;
        }
        if (v instanceof Byte) {
            return ((Byte) v).shortValue(); // byte -> short widening
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s cannot be converted to SMALLINT. Type: %s",
                        prop.name, v.getClass().getName()));
    }

    /** Converts a numeric POJO property to INTEGER (Integer) with widening support. */
    private static @Nullable Integer convertToInteger(PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Integer) {
            return (Integer) v;
        }
        if (v instanceof Short) {
            return ((Short) v).intValue(); // short -> int widening
        }
        if (v instanceof Byte) {
            return ((Byte) v).intValue(); // byte -> int widening
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s cannot be converted to INTEGER. Type: %s",
                        prop.name, v.getClass().getName()));
    }

    /** Converts a numeric POJO property to BIGINT (Long) with widening support. */
    private static @Nullable Long convertToBigInt(PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Long) {
            return (Long) v;
        }
        if (v instanceof Integer) {
            return ((Integer) v).longValue(); // int -> long widening
        }
        if (v instanceof Short) {
            return ((Short) v).longValue(); // short -> long widening
        }
        if (v instanceof Byte) {
            return ((Byte) v).longValue(); // byte -> long widening
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s cannot be converted to BIGINT. Type: %s",
                        prop.name, v.getClass().getName()));
    }

    /** Converts a numeric POJO property to FLOAT (Float) with widening support. */
    private static @Nullable Float convertToFloat(PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Float) {
            return (Float) v;
        }
        if (v instanceof Long) {
            return ((Long) v).floatValue(); // long -> float widening
        }
        if (v instanceof Integer) {
            return ((Integer) v).floatValue(); // int -> float widening
        }
        if (v instanceof Short) {
            return ((Short) v).floatValue(); // short -> float widening
        }
        if (v instanceof Byte) {
            return ((Byte) v).floatValue(); // byte -> float widening
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s cannot be converted to FLOAT. Type: %s",
                        prop.name, v.getClass().getName()));
    }

    /** Converts a numeric POJO property to DOUBLE (Double) with widening support. */
    private static @Nullable Double convertToDouble(PojoType.Property prop, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Double) {
            return (Double) v;
        }
        if (v instanceof Float) {
            return ((Float) v).doubleValue(); // float -> double widening
        }
        if (v instanceof Long) {
            return ((Long) v).doubleValue(); // long -> double widening
        }
        if (v instanceof Integer) {
            return ((Integer) v).doubleValue(); // int -> double widening
        }
        if (v instanceof Short) {
            return ((Short) v).doubleValue(); // short -> double widening
        }
        if (v instanceof Byte) {
            return ((Byte) v).doubleValue(); // byte -> double widening
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s cannot be converted to DOUBLE. Type: %s",
                        prop.name, v.getClass().getName()));
    }

    private interface FieldToRow {
        Object readAndConvert(Object pojo) throws Exception;
    }
}
