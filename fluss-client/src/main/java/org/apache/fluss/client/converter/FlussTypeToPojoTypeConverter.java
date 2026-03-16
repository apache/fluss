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
import org.apache.fluss.types.DataTypeRoot;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** Shared utilities for Fluss type and Pojo type. */
public class FlussTypeToPojoTypeConverter {
    /**
     * Converts a text value (CHAR/STRING) read from an InternalRow into the target Java type
     * declared by the POJO property.
     *
     * <p>Supported target types are String and Character. For CHAR columns, this enforces that the
     * value has exactly one character. For Character targets, empty strings are rejected.
     *
     * @param fieldType Fluss column DataType (must be CHAR or STRING)
     * @param fieldName The field name
     * @param pojoType The pojo type
     * @param s The BinaryString read from the row
     * @return Converted Java value (String or Character)
     * @throws IllegalArgumentException if the target type is unsupported or constraints are
     *     violated
     */
    static Object convertTextValue(
            DataType fieldType, String fieldName, Class<?> pojoType, BinaryString s) {
        if (s == null) {
            return null;
        }

        String v = s.toString();
        if (pojoType == String.class) {
            if (fieldType.getTypeRoot() == DataTypeRoot.CHAR && v.length() != 1) {
                throw new IllegalArgumentException(
                        ConverterCommons.charLengthExceptionMessage(fieldName, v.length()));
            }
            return v;
        } else if (pojoType == Character.class) {
            if (v.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Field %s expects Character, but the string value is empty.",
                                fieldName));
            }
            if (fieldType.getTypeRoot() == DataTypeRoot.CHAR && v.length() != 1) {
                throw new IllegalArgumentException(
                        ConverterCommons.charLengthExceptionMessage(fieldName, v.length()));
            }
            return v.charAt(0);
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s is not a String or Character. Cannot convert from string.",
                        fieldName));
    }

    /**
     * Converts a DECIMAL value from an InternalRow into a BigDecimal using the column's precision
     * and scale. The row position is assumed non-null (caller checks), so this never returns null.
     */
    static BigDecimal convertDecimalValue(Decimal d) {
        return d.toBigDecimal();
    }

    /** Converts a DATE value stored as int days since epoch to a LocalDate. */
    static LocalDate convertDateValue(int daysSinceEpoch) {
        return LocalDate.ofEpochDay(daysSinceEpoch);
    }

    /** Converts a TIME_WITHOUT_TIME_ZONE value stored as int millis of day to a LocalTime. */
    static LocalTime convertTimeValue(int millisOfDay) {
        return LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L);
    }

    /** Converts a TIMESTAMP_WITHOUT_TIME_ZONE value to a LocalDateTime honoring precision. */
    static Object convertTimestampNtzValue(TimestampNtz t) {
        return t.toLocalDateTime();
    }

    /**
     * Converts a TIMESTAMP_WITH_LOCAL_TIME_ZONE value to either Instant or OffsetDateTime in UTC,
     * depending on the target POJO property type.
     */
    static Object convertTimestampLtzValue(TimestampLtz t, String fieldName, Class<?> pojoType) {
        if (pojoType == Instant.class) {
            return t.toInstant();
        } else if (pojoType == OffsetDateTime.class) {
            return OffsetDateTime.ofInstant(t.toInstant(), ZoneOffset.UTC);
        }
        throw new IllegalArgumentException(
                String.format(
                        "Field %s is not an Instant or OffsetDateTime. Cannot convert from TimestampData.",
                        fieldName));
    }
}
