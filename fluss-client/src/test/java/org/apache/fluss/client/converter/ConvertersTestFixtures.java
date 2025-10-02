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

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** Shared fixtures and helper POJOs for converter tests. */
public final class ConvertersTestFixtures {

    private ConvertersTestFixtures() {}

    public static RowType fullSchema() {
        return RowType.builder()
                .field("booleanField", DataTypes.BOOLEAN())
                .field("byteField", DataTypes.TINYINT())
                .field("shortField", DataTypes.SMALLINT())
                .field("intField", DataTypes.INT())
                .field("longField", DataTypes.BIGINT())
                .field("floatField", DataTypes.FLOAT())
                .field("doubleField", DataTypes.DOUBLE())
                .field("stringField", DataTypes.STRING())
                .field("bytesField", DataTypes.BYTES())
                .field("decimalField", DataTypes.DECIMAL(10, 2))
                .field("dateField", DataTypes.DATE())
                .field("timeField", DataTypes.TIME())
                .field("timestampField", DataTypes.TIMESTAMP())
                .field("timestampLtzField", DataTypes.TIMESTAMP_LTZ())
                .field("offsetDateTimeField", DataTypes.TIMESTAMP_LTZ())
                .build();
    }

    // ----------------------- Helper POJOs -----------------------
    /** Test POJO used for end-to-end converter tests. */
    public static class TestPojo {
        public Boolean booleanField;
        public Byte byteField;
        public Short shortField;
        public Integer intField;
        public Long longField;
        public Float floatField;
        public Double doubleField;
        public String stringField;
        public byte[] bytesField;
        public BigDecimal decimalField;
        public LocalDate dateField;
        public LocalTime timeField;
        public LocalDateTime timestampField;
        public Instant timestampLtzField;
        public OffsetDateTime offsetDateTimeField;

        public TestPojo() {}

        public TestPojo(
                Boolean booleanField,
                Byte byteField,
                Short shortField,
                Integer intField,
                Long longField,
                Float floatField,
                Double doubleField,
                String stringField,
                byte[] bytesField,
                BigDecimal decimalField,
                LocalDate dateField,
                LocalTime timeField,
                LocalDateTime timestampField,
                Instant timestampLtzField,
                OffsetDateTime offsetDateTimeField) {
            this.booleanField = booleanField;
            this.byteField = byteField;
            this.shortField = shortField;
            this.intField = intField;
            this.longField = longField;
            this.floatField = floatField;
            this.doubleField = doubleField;
            this.stringField = stringField;
            this.bytesField = bytesField;
            this.decimalField = decimalField;
            this.dateField = dateField;
            this.timeField = timeField;
            this.timestampField = timestampField;
            this.timestampLtzField = timestampLtzField;
            this.offsetDateTimeField = offsetDateTimeField;
        }

        public static TestPojo sample() {
            return new TestPojo(
                    true,
                    (byte) 42,
                    (short) 1234,
                    123456,
                    9876543210L,
                    3.14f,
                    2.71828,
                    "Hello, World!",
                    new byte[] {1, 2, 3, 4, 5},
                    new BigDecimal("123.45"),
                    LocalDate.of(2025, 7, 23),
                    LocalTime.of(15, 1, 30),
                    LocalDateTime.of(2025, 7, 23, 15, 1, 30),
                    Instant.parse("2025-07-23T15:01:30Z"),
                    OffsetDateTime.of(2025, 7, 23, 15, 1, 30, 0, ZoneOffset.UTC));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestPojo testPojo = (TestPojo) o;
            return Objects.equals(booleanField, testPojo.booleanField)
                    && Objects.equals(byteField, testPojo.byteField)
                    && Objects.equals(shortField, testPojo.shortField)
                    && Objects.equals(intField, testPojo.intField)
                    && Objects.equals(longField, testPojo.longField)
                    && Objects.equals(floatField, testPojo.floatField)
                    && Objects.equals(doubleField, testPojo.doubleField)
                    && Objects.equals(stringField, testPojo.stringField)
                    && Arrays.equals(bytesField, testPojo.bytesField)
                    && Objects.equals(decimalField, testPojo.decimalField)
                    && Objects.equals(dateField, testPojo.dateField)
                    && Objects.equals(timeField, testPojo.timeField)
                    && Objects.equals(timestampField, testPojo.timestampField)
                    && Objects.equals(timestampLtzField, testPojo.timestampLtzField)
                    && Objects.equals(offsetDateTimeField, testPojo.offsetDateTimeField);
        }

        @Override
        public int hashCode() {
            int result =
                    Objects.hash(
                            booleanField,
                            byteField,
                            shortField,
                            intField,
                            longField,
                            floatField,
                            doubleField,
                            stringField,
                            decimalField,
                            dateField,
                            timeField,
                            timestampField,
                            timestampLtzField,
                            offsetDateTimeField);
            result = 31 * result + Arrays.hashCode(bytesField);
            return result;
        }
    }

    /** A minimal POJO with a subset of fields for projection tests. */
    public static class PartialTestPojo {
        public Boolean booleanField;
        public Integer intField;
        public String stringField;

        public PartialTestPojo() {}
    }

    /** POJO without public default constructor, used for validation tests. */
    public static class NoDefaultConstructorPojo {
        public Integer intField;

        public NoDefaultConstructorPojo(int v) {
            this.intField = v;
        }
    }

    /** POJO whose default constructor throws, used to test instantiation error handling. */
    public static class ThrowingCtorPojo {
        public Integer intField;

        public ThrowingCtorPojo() {
            throw new RuntimeException("ctor failure");
        }
    }

    /** POJO with wrong Java type for DECIMAL field, used for negative tests. */
    public static class DecimalWrongTypePojo {
        public String decimalField;

        public DecimalWrongTypePojo() {}
    }

    /** POJO with wrong Java type for DATE field, used for negative tests. */
    public static class DateWrongTypePojo {
        public String dateField;

        public DateWrongTypePojo() {}
    }

    /** POJO with wrong Java type for TIME field, used for negative tests. */
    public static class TimeWrongTypePojo {
        public String timeField;

        public TimeWrongTypePojo() {}
    }

    /** POJO with wrong Java type for TIMESTAMP_NTZ field, used for negative tests. */
    public static class TimestampWrongTypePojo {
        public String timestampField;

        public TimestampWrongTypePojo() {}
    }

    /** POJO with wrong Java type for TIMESTAMP_LTZ field, used for negative tests. */
    public static class TimestampLtzWrongTypePojo {
        public String timestampLtzField;

        public TimestampLtzWrongTypePojo() {}
    }

    /** POJO with unsupported Map field, used for negative tests. */
    public static class MapPojo {
        public Map<String, Integer> mapField;

        public MapPojo() {}
    }

    /** POJO with Character field for CHAR/STRING handling tests. */
    public static class CharacterFieldPojo {
        public Character charField;

        public CharacterFieldPojo() {}

        public CharacterFieldPojo(Character c) {
            this.charField = c;
        }
    }
}
