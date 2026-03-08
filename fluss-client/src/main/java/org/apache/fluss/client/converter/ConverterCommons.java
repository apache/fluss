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
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.client.converter.RowToPojoConverter.charLengthExceptionMessage;

/**
 * Internal shared utilities for POJO and Fluss InternalRow conversions.
 *
 * <p>Provides validation helpers and common functions used by PojoToRowConverter and
 * RowToPojoConverter (e.g., supported Java types per Fluss DataType, projection/table validation,
 * and text conversion helpers).
 */
final class ConverterCommons {

    static final Map<DataTypeRoot, Set<Class<?>>> SUPPORTED_TYPES = createSupportedTypes();
    static final Set<DataTypeRoot> SUPPORTED_COMPLEX_TYPES =
            EnumSet.of(DataTypeRoot.ARRAY, DataTypeRoot.MAP);

    private static Map<DataTypeRoot, Set<Class<?>>> createSupportedTypes() {
        Map<DataTypeRoot, Set<Class<?>>> map = new EnumMap<>(DataTypeRoot.class);
        map.put(DataTypeRoot.BOOLEAN, setOf(Boolean.class));
        map.put(DataTypeRoot.TINYINT, setOf(Byte.class));
        map.put(DataTypeRoot.SMALLINT, setOf(Short.class));
        map.put(DataTypeRoot.INTEGER, setOf(Integer.class));
        map.put(DataTypeRoot.BIGINT, setOf(Long.class));
        map.put(DataTypeRoot.FLOAT, setOf(Float.class));
        map.put(DataTypeRoot.DOUBLE, setOf(Double.class));
        map.put(DataTypeRoot.CHAR, setOf(String.class, Character.class));
        map.put(DataTypeRoot.STRING, setOf(String.class, Character.class));
        map.put(DataTypeRoot.BINARY, setOf(byte[].class));
        map.put(DataTypeRoot.BYTES, setOf(byte[].class));
        map.put(DataTypeRoot.DECIMAL, setOf(BigDecimal.class));
        map.put(DataTypeRoot.DATE, setOf(java.time.LocalDate.class));
        map.put(DataTypeRoot.TIME_WITHOUT_TIME_ZONE, setOf(java.time.LocalTime.class));
        map.put(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, setOf(java.time.LocalDateTime.class));
        map.put(
                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                setOf(java.time.Instant.class, java.time.OffsetDateTime.class));
        map.put(DataTypeRoot.ARRAY, setOf(java.util.Arrays.class));
        map.put(DataTypeRoot.MAP, setOf(java.util.Map.class));
        return map;
    }

    static void validatePojoMatchesTable(PojoType<?> pojoType, RowType tableSchema) {
        Set<String> pojoNames = pojoType.getProperties().keySet();
        List<String> fieldNames = tableSchema.getFieldNames();
        if (!pojoNames.containsAll(fieldNames)) {
            throw new IllegalArgumentException(
                    String.format(
                            "POJO fields %s must exactly match table schema fields %s.",
                            pojoNames, fieldNames));
        }
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            String name = fieldNames.get(i);
            DataType dt = tableSchema.getTypeAt(i);
            PojoType.Property prop = pojoType.getProperty(name);
            validateCompatibility(dt, prop);
        }
    }

    static void validatePojoMatchesProjection(PojoType<?> pojoType, RowType projection) {
        Set<String> pojoNames = pojoType.getProperties().keySet();
        List<String> fieldNames = projection.getFieldNames();
        if (!pojoNames.containsAll(fieldNames)) {
            throw new IllegalArgumentException(
                    String.format(
                            "POJO fields %s must contain all projection fields %s. "
                                    + "For full-table writes, POJO fields must exactly match table schema fields.",
                            pojoNames, fieldNames));
        }
        for (int i = 0; i < projection.getFieldCount(); i++) {
            String name = fieldNames.get(i);
            DataType dt = projection.getTypeAt(i);
            PojoType.Property prop = pojoType.getProperty(name);
            validateCompatibility(dt, prop);
        }
    }

    static void validateProjectionSubset(RowType projection, RowType tableSchema) {
        Set<String> tableNames = new HashSet<>(tableSchema.getFieldNames());
        for (String n : projection.getFieldNames()) {
            if (!tableNames.contains(n)) {
                throw new IllegalArgumentException(
                        "Projection field '" + n + "' is not part of table schema.");
            }
        }
    }

    static void validateCompatibility(DataType fieldType, PojoType.Property prop) {
        Set<Class<?>> supported = SUPPORTED_TYPES.get(fieldType.getTypeRoot());
        Class<?> actual = prop.type;
        if (supported == null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported field type %s for field %s.",
                            fieldType.getTypeRoot(), prop.name));
        }
        if (!supported.contains(actual)
                & !SUPPORTED_COMPLEX_TYPES.contains(fieldType.getTypeRoot())) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field '%s' in POJO has Java type %s which is incompatible with Fluss type %s. Supported Java types: %s",
                            prop.name, actual.getName(), fieldType.getTypeRoot(), supported));
        }
    }

    static BinaryString toBinaryStringForText(Object v, String fieldName, DataTypeRoot root) {
        final String s = String.valueOf(v);
        if (root == DataTypeRoot.CHAR && s.length() != 1) {
            throw new IllegalArgumentException(charLengthExceptionMessage(fieldName, s.length()));
        }
        return BinaryString.fromString(s);
    }

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
        return toBinaryStringForText(v, fieldName, fieldType.getTypeRoot());
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

    static Set<Class<?>> setOf(Class<?>... javaTypes) {
        return new HashSet<>(Arrays.asList(javaTypes));
    }
}
