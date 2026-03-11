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

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussArrayToPojoArray}. */
public class FlussArrayToPojoArrayTest {
    @Test
    public void testArrayWithAllTypes() {
        RowType table =
                RowType.builder()
                        .field("booleanArray", DataTypes.ARRAY(DataTypes.BOOLEAN()))
                        .field("byteArray", DataTypes.ARRAY(DataTypes.TINYINT()))
                        .field("shortArray", DataTypes.ARRAY(DataTypes.SMALLINT()))
                        .field("intArray", DataTypes.ARRAY(DataTypes.INT()))
                        .field("longArray", DataTypes.ARRAY(DataTypes.BIGINT()))
                        .field("floatArray", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .field("doubleArray", DataTypes.ARRAY(DataTypes.DOUBLE()))
                        .field("stringArray", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("decimalArray", DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)))
                        .field("dateArray", DataTypes.ARRAY(DataTypes.DATE()))
                        .field("timeArray", DataTypes.ARRAY(DataTypes.TIME()))
                        .field("timestampArray", DataTypes.ARRAY(DataTypes.TIMESTAMP(3)))
                        .field("timestampLtzArray", DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ(3)))
                        .field("nestedIntArray", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                        .field(
                                "mapArray",
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())))
                        .build();

        PojoToRowConverter<ArrayPojo> writer = PojoToRowConverter.of(ArrayPojo.class, table, table);
        RowToPojoConverter<ArrayPojo> reader = RowToPojoConverter.of(ArrayPojo.class, table, table);

        ArrayPojo pojo = ArrayPojo.sample();

        // POJO -> Row -> POJO
        GenericRow row = writer.toRow(pojo);
        ArrayPojo back = reader.fromRow(row);

        // Verify boolean array
        Object[] boolArray = back.booleanArray;
        assertThat(boolArray.length).isEqualTo(2);
        assertThat(boolArray).isEqualTo(new Boolean[] {true, false});

        // Verify byte array
        Object[] byteArray = back.byteArray;
        assertThat(byteArray.length).isEqualTo(2);
        assertThat(byteArray).isEqualTo(new Byte[] {1, 2});

        // Verify short array
        Object[] shortArray = back.shortArray;
        assertThat(shortArray.length).isEqualTo(2);
        assertThat(shortArray).isEqualTo(new Short[] {100, 200});

        // Verify int array
        Object[] intArray = back.intArray;
        assertThat(intArray.length).isEqualTo(2);
        assertThat(intArray).isEqualTo(new Integer[] {1000, 2000});

        // Verify long array
        Object[] longArray = back.longArray;
        assertThat(longArray.length).isEqualTo(2);
        assertThat(longArray).isEqualTo(new Long[] {10000L, 20000L});

        // Verify float array
        Object[] floatArray = back.floatArray;
        assertThat(floatArray.length).isEqualTo(2);
        assertThat(floatArray).isEqualTo(new Float[] {1.1f, 2.2f});

        // Verify double array
        Object[] doubleArray = back.doubleArray;
        assertThat(doubleArray.length).isEqualTo(2);
        assertThat(doubleArray).isEqualTo(new Double[] {1.11, 2.22});

        // Verify string array
        Object[] stringArray = back.stringArray;
        assertThat(stringArray.length).isEqualTo(2);
        assertThat(stringArray).isEqualTo(new String[] {"hello", "world"});

        // Verify decimal array
        Object[] decimalArray = back.decimalArray;
        assertThat(decimalArray.length).isEqualTo(2);
        assertThat(decimalArray)
                .isEqualTo(new BigDecimal[] {new BigDecimal("123.45"), new BigDecimal("678.90")});

        // Verify date array (days since epoch)
        Object[] dateArray = back.dateArray;
        assertThat(dateArray.length).isEqualTo(2);
        assertThat(dateArray)
                .isEqualTo(new LocalDate[] {LocalDate.of(2025, 1, 1), LocalDate.of(2025, 12, 31)});

        // Verify time array (millis of day)
        Object[] timeArray = back.timeArray;
        assertThat(timeArray.length).isEqualTo(2);
        assertThat(timeArray)
                .isEqualTo(new LocalTime[] {LocalTime.MIDNIGHT, LocalTime.of(12, 30, 0)});

        // Verify timestamp array
        Object[] timestampArray = back.timestampArray;
        assertThat(timestampArray.length).isEqualTo(2);
        assertThat(timestampArray)
                .isEqualTo(
                        new LocalDateTime[] {
                            LocalDateTime.of(2025, 7, 23, 15, 0, 0),
                            LocalDateTime.of(2025, 12, 31, 23, 59, 59)
                        });

        // Verify timestampLtz array
        Object[] timestampLtzArray = back.timestampLtzArray;
        assertThat(timestampLtzArray.length).isEqualTo(2);
        assertThat(timestampLtzArray)
                .isEqualTo(
                        new Instant[] {
                            Instant.parse("2025-01-01T00:00:00Z"),
                            Instant.parse("2025-12-31T23:59:59Z")
                        });

        // Verify nested array (array<array<int>>)
        Object[][] nestedIntArray = back.nestedIntArray;
        assertThat(nestedIntArray.length).isEqualTo(2);
        assertThat(back.nestedIntArray)
                .isEqualTo(
                        new Integer[][] {
                            {1, 2},
                            {3, 4, 5}
                        });
        // Verify map array (array<map<string, int>>)
        Object[] mapArray = back.mapArray;
        assertThat(mapArray.length).isEqualTo(2);
        assertThat(back.mapArray)
                .isEqualTo(
                        new HashMap[] {
                            new HashMap<String, Integer>() {
                                {
                                    put("test_1", 1);
                                    put("test_2", 2);
                                }
                            },
                            new HashMap<String, Integer>() {
                                {
                                    put("test_3", 3);
                                    put("test_4", 4);
                                }
                            }
                        });
    }

    @Test
    public void testArrayWithNullElements() {
        RowType table =
                RowType.builder()
                        .field("stringArray", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("intObjectArray", DataTypes.ARRAY(DataTypes.INT()))
                        .build();

        PojoToRowConverter<NullableArrayPojo> writer =
                PojoToRowConverter.of(NullableArrayPojo.class, table, table);
        RowToPojoConverter<NullableArrayPojo> reader =
                RowToPojoConverter.of(NullableArrayPojo.class, table, table);

        NullableArrayPojo pojo = new NullableArrayPojo();
        pojo.stringArray = new String[] {"hello", null, "world"};
        pojo.intObjectArray = new Integer[] {1, null, 3};

        // POJO -> Row -> POJO
        GenericRow row = writer.toRow(pojo);
        NullableArrayPojo back = reader.fromRow(row);

        Object[] stringArray = back.stringArray;
        assertThat(stringArray.length).isEqualTo(3);
        assertThat(stringArray).isEqualTo(new String[] {"hello", null, "world"});

        Object[] intArray = back.intObjectArray;
        assertThat(intArray.length).isEqualTo(3);
        assertThat(intArray).isEqualTo(new Integer[] {1, null, 3});
    }

    @Test
    public void testNullArrayField() {
        RowType table =
                RowType.builder().field("intArray", DataTypes.ARRAY(DataTypes.INT())).build();

        PojoToRowConverter<SimpleArrayPojo> writer =
                PojoToRowConverter.of(SimpleArrayPojo.class, table, table);
        RowToPojoConverter<SimpleArrayPojo> reader =
                RowToPojoConverter.of(SimpleArrayPojo.class, table, table);

        SimpleArrayPojo pojo = new SimpleArrayPojo();
        pojo.intArray = null;

        // POJO -> Row -> POJO
        GenericRow row = writer.toRow(pojo);
        SimpleArrayPojo back = reader.fromRow(row);
        assertThat(back.intArray).isEqualTo(null);
    }

    @Test
    public void testListFieldsOnReadPath() {
        // POJO fields declared as List<T> should deserialize as ArrayList<Object>
        RowType table =
                RowType.builder()
                        .field("intList", DataTypes.ARRAY(DataTypes.INT()))
                        .field("strList", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("nullableList", DataTypes.ARRAY(DataTypes.INT()))
                        .build();

        PojoToRowConverter<ListFieldPojo> writer =
                PojoToRowConverter.of(ListFieldPojo.class, table, table);
        RowToPojoConverter<ListFieldPojo> reader =
                RowToPojoConverter.of(ListFieldPojo.class, table, table);

        ListFieldPojo pojo = new ListFieldPojo();
        pojo.intList = Arrays.asList(1, 2, 3);
        pojo.strList = Arrays.asList("x", "y");
        pojo.nullableList = Arrays.asList(10, null, 30);

        GenericRow row = writer.toRow(pojo);
        ListFieldPojo back = reader.fromRow(row);

        assertThat(back.intList).isInstanceOf(List.class).containsExactly(1, 2, 3);
        assertThat(back.strList).isInstanceOf(List.class).containsExactly("x", "y");
        assertThat(back.nullableList).isInstanceOf(List.class).containsExactly(10, null, 30);
    }

    /** POJO with List fields for read-path List deserialization tests. */
    public static class ListFieldPojo {
        public List<Integer> intList;
        public List<String> strList;
        public List<Integer> nullableList;

        public ListFieldPojo() {}
    }

    @Test
    public void testTypedArrayFieldsOnReadPath() {
        // Verify that typed POJO array fields (String[], Character[], OffsetDateTime[]) are
        // handled correctly on the read path — not just Object[].
        RowType table =
                RowType.builder()
                        .field("stringArray", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("charArray", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("intArray", DataTypes.ARRAY(DataTypes.INT()))
                        .field("timestampLtzArray", DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ(3)))
                        .build();

        PojoToRowConverter<TypedArrayPojo> writer =
                PojoToRowConverter.of(TypedArrayPojo.class, table, table);
        RowToPojoConverter<TypedArrayPojo> reader =
                RowToPojoConverter.of(TypedArrayPojo.class, table, table);

        TypedArrayPojo pojo = new TypedArrayPojo();
        pojo.stringArray = new String[] {"hello", "world"};
        pojo.charArray = new Character[] {'A', 'B'};
        pojo.intArray = new Integer[] {10, 20};
        pojo.timestampLtzArray =
                new OffsetDateTime[] {
                    OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
                    OffsetDateTime.of(2025, 6, 15, 12, 0, 0, 0, ZoneOffset.UTC)
                };

        GenericRow row = writer.toRow(pojo);
        TypedArrayPojo back = reader.fromRow(row);

        assertThat((String[]) back.stringArray).isEqualTo(new String[] {"hello", "world"});
        assertThat((Character[]) back.charArray).isEqualTo(new Character[] {'A', 'B'});
        assertThat((Integer[]) back.intArray).isEqualTo(new Integer[] {10, 20});
        assertThat(((OffsetDateTime[]) back.timestampLtzArray)[0])
                .isEqualTo(OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
        assertThat(((OffsetDateTime[]) back.timestampLtzArray)[1])
                .isEqualTo(OffsetDateTime.of(2025, 6, 15, 12, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    public void testNullElementInPrimitiveArrayThrows() {
        // A Fluss ARRAY<INT> with a null element cannot be read into an int[] POJO field.
        // The converter must throw a clear NullPointerException rather than a cryptic one.
        RowType table =
                RowType.builder()
                        .field("intObjectArray", DataTypes.ARRAY(DataTypes.INT()))
                        .build();

        PojoToRowConverter<NullableArrayPojo> writer =
                PojoToRowConverter.of(NullableArrayPojo.class, table, table);
        RowToPojoConverter<PrimitiveIntArrayPojo> reader =
                RowToPojoConverter.of(PrimitiveIntArrayPojo.class, table, table);

        NullableArrayPojo pojo = new NullableArrayPojo();
        pojo.stringArray = null; // unused — only intObjectArray is in projection
        pojo.intObjectArray = new Integer[] {1, null, 3};

        GenericRow row = writer.toRow(pojo);
        assertThatThrownBy(() -> reader.fromRow(row))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("primitive array")
                .hasMessageContaining("intObjectArray");
    }

    /** POJO with typed (non-Object[]) array fields for read-path type-fidelity tests. */
    public static class TypedArrayPojo {
        public String[] stringArray;
        public Character[] charArray;
        public Integer[] intArray;
        public OffsetDateTime[] timestampLtzArray;

        public TypedArrayPojo() {}
    }

    /** POJO with primitive int[] for null-element guard test. */
    public static class PrimitiveIntArrayPojo {
        public int[] intObjectArray;

        public PrimitiveIntArrayPojo() {}
    }

    /** POJO for testing all array types. */
    @SuppressWarnings("unchecked")
    public static class ArrayPojo {
        public Object[] booleanArray;
        public Object[] byteArray;
        public Object[] shortArray;
        public Object[] intArray;
        public Object[] longArray;
        public Object[] floatArray;
        public Object[] doubleArray;
        public Object[] stringArray;
        public Object[] decimalArray;
        public Object[] dateArray;
        public Object[] timeArray;
        public Object[] timestampArray;
        public Object[] timestampLtzArray;
        public Object[][] nestedIntArray;
        public Map<Object, Object>[] mapArray;

        public ArrayPojo() {}

        public static ArrayPojo sample() {
            ArrayPojo pojo = new ArrayPojo();
            pojo.booleanArray = new Boolean[] {true, false};
            pojo.byteArray = new Byte[] {1, 2};
            pojo.shortArray = new Short[] {100, 200};
            pojo.intArray = new Integer[] {1000, 2000};
            pojo.longArray = new Long[] {10000L, 20000L};
            pojo.floatArray = new Float[] {1.1f, 2.2f};
            pojo.doubleArray = new Double[] {1.11, 2.22};
            pojo.stringArray = new String[] {"hello", "world"};
            pojo.decimalArray =
                    new BigDecimal[] {new BigDecimal("123.45"), new BigDecimal("678.90")};
            pojo.dateArray = new LocalDate[] {LocalDate.of(2025, 1, 1), LocalDate.of(2025, 12, 31)};
            pojo.timeArray = new LocalTime[] {LocalTime.MIDNIGHT, LocalTime.of(12, 30, 0)};
            pojo.timestampArray =
                    new LocalDateTime[] {
                        LocalDateTime.of(2025, 7, 23, 15, 0, 0),
                        LocalDateTime.of(2025, 12, 31, 23, 59, 59)
                    };
            pojo.timestampLtzArray =
                    new Instant[] {
                        Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2025-12-31T23:59:59Z")
                    };
            pojo.nestedIntArray =
                    new Integer[][] {
                        {1, 2},
                        {3, 4, 5}
                    };
            pojo.mapArray =
                    new HashMap[] {
                        new HashMap<Object, Object>() {
                            {
                                put("test_1", 1);
                                put("test_2", 2);
                            }
                        },
                        new HashMap<Object, Object>() {
                            {
                                put("test_3", 3);
                                put("test_4", 4);
                            }
                        }
                    };
            return pojo;
        }
    }

    /** POJO for testing arrays with null elements. */
    public static class NullableArrayPojo {
        public Object[] stringArray;
        public Object[] intObjectArray;

        public NullableArrayPojo() {}
    }

    /** Simple POJO for testing null array field. */
    public static class SimpleArrayPojo {
        public Object[] intArray;

        public SimpleArrayPojo() {}
    }
}
