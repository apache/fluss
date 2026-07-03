/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;

/** Shared utilities for POJO type and Fluss type. */
public class PojoTypeToFlussTypeConverter {
    /**
     * Converts a text value (String or Character) from a POJO property to Fluss BinaryString.
     *
     * <p>For CHAR columns, enforces that the text has exactly one character. Nulls are passed
     * through.
     */
    static @Nullable BinaryString convertTextValue(
            DataType fieldType, String fieldName, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        return ConverterCommons.toBinaryStringForText(v, fieldName, fieldType.getTypeRoot());
    }

    /** Converts a BigDecimal POJO property to Fluss Decimal respecting precision and scale. */
    static @Nullable Decimal convertDecimalValue(
            DecimalType decimalType, String fieldName, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof BigDecimal)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not a BigDecimal. Cannot convert to Decimal.", fieldName));
        }
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();

        // Scale with a deterministic rounding mode to avoid ArithmeticException when rounding is
        // needed.
        BigDecimal bd = (BigDecimal) v;
        BigDecimal scaled = bd.setScale(scale, RoundingMode.HALF_UP);

        if (scaled.precision() > precision) {
            throw new IllegalArgumentException(
                    String.format(
                            "Decimal value for field %s exceeds precision %d after scaling to %d: %s",
                            fieldName, precision, scale, scaled));
        }

        return Decimal.fromBigDecimal(scaled, precision, scale);
    }

    /** Converts a LocalDate POJO property to number of days since epoch. */
    static @Nullable Integer convertDateValue(String fieldName, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof LocalDate)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not a LocalDate. Cannot convert to int days.", fieldName));
        }
        return (int) ((LocalDate) v).toEpochDay();
    }

    /** Converts a LocalTime POJO property to milliseconds of day. */
    static @Nullable Integer convertTimeValue(String fieldName, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof LocalTime)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not a LocalTime. Cannot convert to int millis.",
                            fieldName));
        }
        LocalTime t = (LocalTime) v;
        return (int) (t.toNanoOfDay() / 1_000_000);
    }

    /**
     * Converts a LocalDateTime POJO property to Fluss TimestampNtz, respecting the specified
     * precision.
     *
     * @param precision the timestamp precision (0-9)
     * @param fieldName the field name
     * @param v the value to convert
     * @return TimestampNtz with precision applied, or null if v is null
     */
    static @Nullable TimestampNtz convertTimestampNtzValue(
            int precision, String fieldName, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof LocalDateTime)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not a LocalDateTime. Cannot convert to TimestampNtz.",
                            fieldName));
        }
        LocalDateTime ldt = (LocalDateTime) v;
        LocalDateTime truncated = truncateToTimestampPrecision(ldt, precision);
        return TimestampNtz.fromLocalDateTime(truncated);
    }

    /**
     * Converts an Instant or OffsetDateTime POJO property to Fluss TimestampLtz (UTC based),
     * respecting the specified precision.
     *
     * @param precision the timestamp precision (0-9)
     * @param fieldName the field name
     * @param v the value to convert
     * @return TimestampLtz with precision applied, or null if v is null
     */
    static @Nullable TimestampLtz convertTimestampLtzValue(
            int precision, String fieldName, @Nullable Object v) {
        if (v == null) {
            return null;
        }
        Instant instant;
        if (v instanceof Instant) {
            instant = (Instant) v;
        } else if (v instanceof OffsetDateTime) {
            instant = ((OffsetDateTime) v).toInstant();
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s is not an Instant or OffsetDateTime. Cannot convert to TimestampLtz.",
                            fieldName));
        }
        Instant truncated = truncateToTimestampPrecision(instant, precision);
        return TimestampLtz.fromInstant(truncated);
    }

    /**
     * Truncates a LocalDateTime to the specified timestamp precision.
     *
     * @param ldt the LocalDateTime to truncate
     * @param precision the precision (0-9)
     * @return truncated LocalDateTime
     */
    private static LocalDateTime truncateToTimestampPrecision(LocalDateTime ldt, int precision) {
        if (precision >= 9) {
            return ldt;
        }
        int nanos = ldt.getNano();
        int truncatedNanos = truncateNanos(nanos, precision);
        return ldt.withNano(truncatedNanos);
    }

    /**
     * Truncates an Instant to the specified timestamp precision.
     *
     * @param instant the Instant to truncate
     * @param precision the precision (0-9)
     * @return truncated Instant
     */
    private static Instant truncateToTimestampPrecision(Instant instant, int precision) {
        if (precision >= 9) {
            return instant;
        }
        int nanos = instant.getNano();
        int truncatedNanos = truncateNanos(nanos, precision);
        return Instant.ofEpochSecond(instant.getEpochSecond(), truncatedNanos);
    }

    /**
     * Truncates nanoseconds to the specified precision.
     *
     * @param nanos the nanoseconds value (0-999,999,999)
     * @param precision the precision (0-9)
     * @return truncated nanoseconds
     */
    private static int truncateNanos(int nanos, int precision) {
        int divisor = (int) Math.pow(10, 9 - precision);
        return (nanos / divisor) * divisor;
    }

    /**
     * Converts a Pojo element to Fluss internal type.
     *
     * @param obj the pojo
     * @param elementType the data type of Fluss
     * @param fieldName the field name
     */
    static @Nullable Object convertElementValue(
            @Nullable Object obj, DataType elementType, String fieldName) {
        if (obj == null) {
            return null;
        }

        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case BYTES:
                return obj;
            case CHAR:
            case STRING:
                return PojoTypeToFlussTypeConverter.convertTextValue(elementType, fieldName, obj);
            case DECIMAL:
                return PojoTypeToFlussTypeConverter.convertDecimalValue(
                        (DecimalType) elementType, fieldName, obj);
            case DATE:
                return PojoTypeToFlussTypeConverter.convertDateValue(fieldName, obj);
            case TIME_WITHOUT_TIME_ZONE:
                return PojoTypeToFlussTypeConverter.convertTimeValue(fieldName, obj);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(elementType);
                    return PojoTypeToFlussTypeConverter.convertTimestampNtzValue(
                            precision, fieldName, obj);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(elementType);
                    return PojoTypeToFlussTypeConverter.convertTimestampLtzValue(
                            precision, fieldName, obj);
                }
            case ARRAY:
                return new PojoArrayToFlussArray(obj, elementType, fieldName).convertArray();
            case MAP:
                return new PojoMapToFlussMap((Map<?, ?>) obj, (MapType) elementType, fieldName)
                        .convertMap();
            case ROW:
                {
                    RowType nestedRowType = (RowType) elementType;
                    @SuppressWarnings("unchecked")
                    PojoToRowConverter<Object> nestedConverter =
                            PojoToRowConverter.of(
                                    (Class<Object>) obj.getClass(), nestedRowType, nestedRowType);
                    return nestedConverter.toRow(obj);
                }
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported field type %s for field %s.",
                                elementType.getTypeRoot(), fieldName));
        }
    }
}
