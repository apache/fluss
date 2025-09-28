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
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case BYTES:
                return prop::read;
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

    private interface FieldToRow {
        Object readAndConvert(Object pojo) throws Exception;
    }
}
