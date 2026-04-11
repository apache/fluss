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
import java.util.HashMap;
import java.util.List;
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
                .field("arrayField", DataTypes.ARRAY(DataTypes.INT()))
                .field("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
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
        public Integer[] arrayField;
        public Map<String, Integer> mapField;

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
                OffsetDateTime offsetDateTimeField,
                Integer[] arrayField,
                Map<String, Integer> mapField) {
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
            this.arrayField = arrayField;
            this.mapField = mapField;
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
                    OffsetDateTime.of(2025, 7, 23, 15, 1, 30, 0, ZoneOffset.UTC),
                    new Integer[] {1, 2},
                    new HashMap<String, Integer>() {
                        {
                            put("test_1", 1);
                            put("test_2", 2);
                        }
                    });
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
                    && Objects.equals(offsetDateTimeField, testPojo.offsetDateTimeField)
                    && Arrays.equals(arrayField, testPojo.arrayField)
                    && Objects.equals(mapField, testPojo.mapField);
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
                            offsetDateTimeField,
                            mapField);
            result = 31 * result + Arrays.hashCode(bytesField);
            result = 31 * result + Arrays.hashCode(arrayField);
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

    // ----------------------- Nested ROW POJOs -----------------------

    /** Address POJO used as nested ROW type in tests. */
    public static class AddressPojo {
        public String city;
        public Integer zipCode;

        public AddressPojo() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AddressPojo that = (AddressPojo) o;
            return Objects.equals(city, that.city) && Objects.equals(zipCode, that.zipCode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(city, zipCode);
        }
    }

    /** Person POJO with nested Address. */
    public static class PersonPojo {
        public Integer id;
        public AddressPojo address;

        public PersonPojo() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PersonPojo that = (PersonPojo) o;
            return Objects.equals(id, that.id) && Objects.equals(address, that.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, address);
        }
    }

    /** Inner POJO for deeply nested row tests. */
    public static class InnerPojo {
        public Double val;
        public Boolean flag;

        public InnerPojo() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InnerPojo that = (InnerPojo) o;
            return Objects.equals(val, that.val) && Objects.equals(flag, that.flag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(val, flag);
        }
    }

    /** Middle POJO containing inner nested POJO. */
    public static class MiddlePojo {
        public Integer id;
        public InnerPojo inner;

        public MiddlePojo() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MiddlePojo that = (MiddlePojo) o;
            return Objects.equals(id, that.id) && Objects.equals(inner, that.inner);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, inner);
        }
    }

    /** Outer POJO for deeply nested row tests. */
    public static class DeepNestOuterPojo {
        public String name;
        public MiddlePojo nested;

        public DeepNestOuterPojo() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeepNestOuterPojo that = (DeepNestOuterPojo) o;
            return Objects.equals(name, that.name) && Objects.equals(nested, that.nested);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, nested);
        }
    }

    /** Nested POJO with array field. */
    public static class RowWithArrayPojo {
        public String label;
        public Integer[] values;

        public RowWithArrayPojo() {}
    }

    /** Outer POJO with nested row containing array. */
    public static class RowWithArrayOuterPojo {
        public Integer id;
        public RowWithArrayPojo data;

        public RowWithArrayOuterPojo() {}
    }

    /** Nested POJO with map field. */
    public static class RowWithMapPojo {
        public String name;
        public Map<String, Integer> attrs;

        public RowWithMapPojo() {}
    }

    /** Outer POJO with nested row containing map. */
    public static class RowWithMapOuterPojo {
        public Integer id;
        public RowWithMapPojo info;

        public RowWithMapOuterPojo() {}
    }

    /** POJO with array of nested row type. */
    public static class ArrayOfRowPojo {
        public Integer id;
        public AddressPojo[] addresses;

        public ArrayOfRowPojo() {}
    }

    /** POJO with map having nested row values. */
    public static class MapOfRowPojo {
        public Integer id;
        public Map<String, AddressPojo> addressMap;

        public MapOfRowPojo() {}
    }

    /** Negative test: array cannot be used as ROW type POJO field. */
    public static class PrimitiveArrayFieldPojo {
        public Integer[] badField;

        public PrimitiveArrayFieldPojo() {}
    }

    /** Negative test: Map cannot be used as ROW type POJO field. */
    public static class MapFieldPojo {
        public Map<String, Integer> badField;

        public MapFieldPojo() {}
    }

    /** POJO with a List of nested row type (tests {@code Collection<ROW>} deserialization). */
    public static class ListOfRowPojo {
        public Integer id;
        public List<AddressPojo> addresses;

        public ListOfRowPojo() {}
    }
}
