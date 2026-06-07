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
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussTypeToPojoTypeConverter}. */
public class FlussTypeToPojoTypeConverterTest {

    // ==================== convertTextValue Tests ====================

    @Test
    public void testConvertTextValueToString() {
        BinaryString binaryStr = BinaryString.fromString("Hello");
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.STRING(), "field", String.class, binaryStr);
        assertThat(result).isEqualTo("Hello");
    }

    @Test
    public void testConvertTextValueToStringFromChar() {
        BinaryString binaryStr = BinaryString.fromString("A");
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.CHAR(1), "field", String.class, binaryStr);
        assertThat(result).isEqualTo("A");
    }

    @Test
    public void testConvertTextValueToCharacter() {
        BinaryString binaryStr = BinaryString.fromString("X");
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.STRING(), "field", Character.class, binaryStr);
        assertThat(result).isEqualTo('X');
    }

    @Test
    public void testConvertTextValueToCharacterFromChar() {
        BinaryString binaryStr = BinaryString.fromString("Z");
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.CHAR(1), "field", Character.class, binaryStr);
        assertThat(result).isEqualTo('Z');
    }

    @Test
    public void testConvertTextValueNullReturnsNull() {
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.STRING(), "field", String.class, null);
        assertThat(result).isNull();
    }

    @Test
    public void testConvertTextValueCharWithLengthOneIsValid() {
        BinaryString binaryStr = BinaryString.fromString("M");
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.CHAR(1), "field", String.class, binaryStr);
        assertThat(result).isEqualTo("M");
    }

    @Test
    public void testConvertTextValueCharWithWrongLengthThrows() {
        BinaryString binaryStr = BinaryString.fromString("Hello");
        assertThatThrownBy(
                        () ->
                                FlussTypeToPojoTypeConverter.convertTextValue(
                                        DataTypes.CHAR(1), "myField", String.class, binaryStr))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConvertTextValueCharacterWithWrongLengthThrows() {
        BinaryString binaryStr = BinaryString.fromString("AB");
        assertThatThrownBy(
                        () ->
                                FlussTypeToPojoTypeConverter.convertTextValue(
                                        DataTypes.CHAR(1), "charField", Character.class, binaryStr))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConvertTextValueEmptyStringToCharacterThrows() {
        BinaryString binaryStr = BinaryString.fromString("");
        assertThatThrownBy(
                        () ->
                                FlussTypeToPojoTypeConverter.convertTextValue(
                                        DataTypes.STRING(),
                                        "emptyField",
                                        Character.class,
                                        binaryStr))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConvertTextValueToEnumWithMatchingValue() {
        BinaryString binaryStr = BinaryString.fromString("ACTIVE");
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.STRING(), "status", StatusEnum.class, binaryStr);
        assertThat(result).isEqualTo(StatusEnum.ACTIVE);
    }

    @Test
    public void testConvertTextValueToEnumWithLowercaseInput() {
        BinaryString binaryStr = BinaryString.fromString("inactive");
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.STRING(), "status", StatusEnum.class, binaryStr);
        assertThat(result).isEqualTo(StatusEnum.INACTIVE);
    }

    @Test
    public void testConvertTextValueToEnumWithMixedCaseInput() {
        BinaryString binaryStr = BinaryString.fromString("PeNdInG");
        Object result =
                FlussTypeToPojoTypeConverter.convertTextValue(
                        DataTypes.STRING(), "status", StatusEnum.class, binaryStr);
        assertThat(result).isEqualTo(StatusEnum.PENDING);
    }

    @Test
    public void testConvertTextValueToEnumWithInvalidValueThrows() {
        BinaryString binaryStr = BinaryString.fromString("UNKNOWN");
        assertThatThrownBy(
                        () ->
                                FlussTypeToPojoTypeConverter.convertTextValue(
                                        DataTypes.STRING(), "status", StatusEnum.class, binaryStr))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConvertTextValueUnsupportedTypeThrows() {
        BinaryString binaryStr = BinaryString.fromString("value");
        assertThatThrownBy(
                        () ->
                                FlussTypeToPojoTypeConverter.convertTextValue(
                                        DataTypes.STRING(), "wrongField", Integer.class, binaryStr))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ==================== convertDateValue Tests ====================

    @Test
    public void testConvertDateValueEpoch() {
        LocalDate result = FlussTypeToPojoTypeConverter.convertDateValue(0);
        assertThat(result).isEqualTo(LocalDate.of(1970, 1, 1));
    }

    @Test
    public void testConvertDateValuePositiveOffset() {
        LocalDate result = FlussTypeToPojoTypeConverter.convertDateValue(18993);
        assertThat(result).isEqualTo(LocalDate.of(2022, 1, 1));
    }

    // ==================== convertTimeValue Tests ====================

    @Test
    public void testConvertTimeValueMidnight() {
        LocalTime result = FlussTypeToPojoTypeConverter.convertTimeValue(0);
        assertThat(result).isEqualTo(LocalTime.MIDNIGHT);
    }

    @Test
    public void testConvertTimeValueNoon() {
        long millisOfDay = 12 * 60 * 60 * 1000;
        LocalTime result = FlussTypeToPojoTypeConverter.convertTimeValue((int) millisOfDay);
        assertThat(result).isEqualTo(LocalTime.of(12, 0, 0));
    }

    // ==================== convertTimestampNtzValue Tests ====================

    @Test
    public void testConvertTimestampNtzValue() {
        TimestampNtz ts = TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 1, 15, 10, 30));
        Object result = FlussTypeToPojoTypeConverter.convertTimestampNtzValue(ts);
        assertThat(result).isEqualTo(LocalDateTime.of(2023, 1, 15, 10, 30));
    }

    @Test
    public void testConvertTimestampNtzValueWithNanoseconds() {
        TimestampNtz ts = TimestampNtz.fromMillis(1000L, 500000); // 1 second + 500000 nanos
        Object result = FlussTypeToPojoTypeConverter.convertTimestampNtzValue(ts);
        assertThat(result).isInstanceOf(LocalDateTime.class);
        LocalDateTime ldt = (LocalDateTime) result;
        assertThat(ldt.getNano()).isEqualTo(500000);
    }

    // ==================== convertTimestampLtzValue Tests ====================

    @Test
    public void testConvertTimestampLtzValueToInstant() {
        TimestampLtz ts = TimestampLtz.fromEpochMillis(1000L);
        Object result =
                FlussTypeToPojoTypeConverter.convertTimestampLtzValue(ts, "field", Instant.class);
        assertThat(result).isEqualTo(Instant.ofEpochMilli(1000L));
    }

    @Test
    public void testConvertTimestampLtzValueToOffsetDateTime() {
        TimestampLtz ts = TimestampLtz.fromEpochMillis(0L);
        Object result =
                FlussTypeToPojoTypeConverter.convertTimestampLtzValue(
                        ts, "field", OffsetDateTime.class);
        assertThat(result).isEqualTo(OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    // ==================== Helper Enum ====================

    /** Test enum for String-to-Enum conversion tests. */
    public enum StatusEnum {
        ACTIVE,
        INACTIVE,
        PENDING
    }
}
