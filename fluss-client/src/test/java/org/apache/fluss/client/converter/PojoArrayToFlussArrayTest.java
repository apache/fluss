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
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PojoArrayToFlussArray}. */
public class PojoArrayToFlussArrayTest {
    @Test
    public void testArrayWithAllTypes() {
        // Schema with all array types
        RowType table =
                RowType.builder()
                        .field("primitiveBooleanArray", DataTypes.ARRAY(DataTypes.BOOLEAN()))
                        .field("primitiveByteArray", DataTypes.ARRAY(DataTypes.TINYINT()))
                        .field("primitiveShortArray", DataTypes.ARRAY(DataTypes.SMALLINT()))
                        .field("primitiveIntArray", DataTypes.ARRAY(DataTypes.INT()))
                        .field("primitiveLongArray", DataTypes.ARRAY(DataTypes.BIGINT()))
                        .field("primitiveFloatArray", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .field("primitiveDoubleArray", DataTypes.ARRAY(DataTypes.DOUBLE()))
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

        ArrayPojo pojo = ArrayPojo.sample();
        GenericRow row = writer.toRow(pojo);

        // Verify primitive boolean array
        InternalArray primitiveBoolArray = row.getArray(0);
        assertThat(primitiveBoolArray.size()).isEqualTo(2);
        assertThat(primitiveBoolArray.getBoolean(0)).isTrue();
        assertThat(primitiveBoolArray.getBoolean(1)).isFalse();

        // Verify primitive byte array
        InternalArray primitiveByteArray = row.getArray(1);
        assertThat(primitiveByteArray.size()).isEqualTo(2);
        assertThat(primitiveByteArray.getByte(0)).isEqualTo((byte) 1);
        assertThat(primitiveByteArray.getByte(1)).isEqualTo((byte) 2);

        // Verify primitive short array
        InternalArray primitiveShortArray = row.getArray(2);
        assertThat(primitiveShortArray.size()).isEqualTo(2);
        assertThat(primitiveShortArray.getShort(0)).isEqualTo((short) 100);
        assertThat(primitiveShortArray.getShort(1)).isEqualTo((short) 200);

        // Verify primitive int array
        InternalArray primitiveIntArray = row.getArray(3);
        assertThat(primitiveIntArray.size()).isEqualTo(2);
        assertThat(primitiveIntArray.getInt(0)).isEqualTo(1000);
        assertThat(primitiveIntArray.getInt(1)).isEqualTo(2000);

        // Verify primitive long array
        InternalArray primitiveLongArray = row.getArray(4);
        assertThat(primitiveLongArray.size()).isEqualTo(2);
        assertThat(primitiveLongArray.getLong(0)).isEqualTo(10000L);
        assertThat(primitiveLongArray.getLong(1)).isEqualTo(20000L);

        // Verify primitive float array
        InternalArray primitiveFloatArray = row.getArray(5);
        assertThat(primitiveFloatArray.size()).isEqualTo(2);
        assertThat(primitiveFloatArray.getFloat(0)).isEqualTo(1.1f);
        assertThat(primitiveFloatArray.getFloat(1)).isEqualTo(2.2f);

        // Verify primitive double array
        InternalArray primitiveDoubleArray = row.getArray(6);
        assertThat(primitiveDoubleArray.size()).isEqualTo(2);
        assertThat(primitiveDoubleArray.getDouble(0)).isEqualTo(1.11);
        assertThat(primitiveDoubleArray.getDouble(1)).isEqualTo(2.22);

        // Verify boolean array
        InternalArray boolArray = row.getArray(7);
        assertThat(boolArray.size()).isEqualTo(2);
        assertThat(boolArray.getBoolean(0)).isTrue();
        assertThat(boolArray.getBoolean(1)).isFalse();

        // Verify byte array
        InternalArray byteArray = row.getArray(8);
        assertThat(byteArray.size()).isEqualTo(2);
        assertThat(byteArray.getByte(0)).isEqualTo((byte) 1);
        assertThat(byteArray.getByte(1)).isEqualTo((byte) 2);

        // Verify short array
        InternalArray shortArray = row.getArray(9);
        assertThat(shortArray.size()).isEqualTo(2);
        assertThat(shortArray.getShort(0)).isEqualTo((short) 100);
        assertThat(shortArray.getShort(1)).isEqualTo((short) 200);

        // Verify int array
        InternalArray intArray = row.getArray(10);
        assertThat(intArray.size()).isEqualTo(2);
        assertThat(intArray.getInt(0)).isEqualTo(1000);
        assertThat(intArray.getInt(1)).isEqualTo(2000);

        // Verify long array
        InternalArray longArray = row.getArray(11);
        assertThat(longArray.size()).isEqualTo(2);
        assertThat(longArray.getLong(0)).isEqualTo(10000L);
        assertThat(longArray.getLong(1)).isEqualTo(20000L);

        // Verify float array
        InternalArray floatArray = row.getArray(12);
        assertThat(floatArray.size()).isEqualTo(2);
        assertThat(floatArray.getFloat(0)).isEqualTo(1.1f);
        assertThat(floatArray.getFloat(1)).isEqualTo(2.2f);

        // Verify double array
        InternalArray doubleArray = row.getArray(13);
        assertThat(doubleArray.size()).isEqualTo(2);
        assertThat(doubleArray.getDouble(0)).isEqualTo(1.11);
        assertThat(doubleArray.getDouble(1)).isEqualTo(2.22);

        // Verify string array
        InternalArray stringArray = row.getArray(14);
        assertThat(stringArray.size()).isEqualTo(2);
        assertThat(stringArray.getString(0).toString()).isEqualTo("hello");
        assertThat(stringArray.getString(1).toString()).isEqualTo("world");

        // Verify decimal array
        InternalArray decimalArray = row.getArray(15);
        assertThat(decimalArray.size()).isEqualTo(2);
        assertThat(decimalArray.getDecimal(0, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("123.45"));
        assertThat(decimalArray.getDecimal(1, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("678.90"));

        // Verify date array (days since epoch)
        InternalArray dateArray = row.getArray(16);
        assertThat(dateArray.size()).isEqualTo(2);
        assertThat(dateArray.getInt(0)).isEqualTo((int) LocalDate.of(2025, 1, 1).toEpochDay());
        assertThat(dateArray.getInt(1)).isEqualTo((int) LocalDate.of(2025, 12, 31).toEpochDay());

        // Verify time array (millis of day)
        InternalArray timeArray = row.getArray(17);
        assertThat(timeArray.size()).isEqualTo(2);
        assertThat(timeArray.getInt(0)).isEqualTo(0); // midnight
        assertThat(timeArray.getInt(1))
                .isEqualTo((int) (LocalTime.of(12, 30, 0).toNanoOfDay() / 1_000_000));

        // Verify timestamp array
        InternalArray timestampArray = row.getArray(18);
        assertThat(timestampArray.size()).isEqualTo(2);
        assertThat(timestampArray.getTimestampNtz(0, 3).getMillisecond())
                .isEqualTo(
                        LocalDateTime.of(2025, 7, 23, 15, 0, 0)
                                .atZone(java.time.ZoneOffset.UTC)
                                .toInstant()
                                .toEpochMilli());

        // Verify timestampLtz array
        InternalArray timestampLtzArray = row.getArray(19);
        assertThat(timestampLtzArray.size()).isEqualTo(2);
        assertThat(timestampLtzArray.getTimestampLtz(0, 3).getEpochMillisecond())
                .isEqualTo(Instant.parse("2025-01-01T00:00:00Z").toEpochMilli());

        // Verify nested array (array<array<int>>)
        InternalArray nestedArray = row.getArray(20);
        assertThat(nestedArray.size()).isEqualTo(2);
        InternalArray innerArray1 = nestedArray.getArray(0);
        assertThat(innerArray1.getInt(0)).isEqualTo(1);
        assertThat(innerArray1.getInt(1)).isEqualTo(2);
        InternalArray innerArray2 = nestedArray.getArray(1);
        assertThat(innerArray2.getInt(0)).isEqualTo(3);
        assertThat(innerArray2.getInt(1)).isEqualTo(4);
        assertThat(innerArray2.getInt(2)).isEqualTo(5);

        // Verify map array (array<map<string, int>>)
        InternalArray mapArray = row.getArray(21);
        assertThat(mapArray.size()).isEqualTo(2);
        // Verify inner map 1
        InternalMap innerMap1 = mapArray.getMap(0);
        assertThat(innerMap1.size()).isEqualTo(2);
        InternalArray keyArray1 = innerMap1.keyArray();
        InternalArray valueArray1 = innerMap1.valueArray();

        Map<String, Integer> resultMap1 = new HashMap<>();
        resultMap1.put(keyArray1.getString(0).toString(), valueArray1.getInt(0));
        resultMap1.put(keyArray1.getString(1).toString(), valueArray1.getInt(1));

        assertThat(resultMap1).containsEntry("test_1", 1);
        assertThat(resultMap1).containsEntry("test_2", 2);
        // Verify inner map 2
        InternalMap innerMap2 = mapArray.getMap(1);
        assertThat(innerMap2.size()).isEqualTo(2);
        InternalArray keyArray2 = innerMap2.keyArray();
        InternalArray valueArray2 = innerMap2.valueArray();

        Map<String, Integer> resultMap2 = new HashMap<>();
        resultMap2.put(keyArray2.getString(0).toString(), valueArray2.getInt(0));
        resultMap2.put(keyArray2.getString(1).toString(), valueArray2.getInt(1));

        assertThat(resultMap2).containsEntry("test_3", 3);
        assertThat(resultMap2).containsEntry("test_4", 4);
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

        NullableArrayPojo pojo = new NullableArrayPojo();
        pojo.stringArray = new String[] {"hello", null, "world"};
        pojo.intObjectArray = new Integer[] {1, null, 3};

        GenericRow row = writer.toRow(pojo);

        InternalArray stringArray = row.getArray(0);
        assertThat(stringArray.size()).isEqualTo(3);
        assertThat(stringArray.getString(0).toString()).isEqualTo("hello");
        assertThat(stringArray.isNullAt(1)).isTrue();
        assertThat(stringArray.getString(2).toString()).isEqualTo("world");

        InternalArray intArray = row.getArray(1);
        assertThat(intArray.size()).isEqualTo(3);
        assertThat(intArray.getInt(0)).isEqualTo(1);
        assertThat(intArray.isNullAt(1)).isTrue();
        assertThat(intArray.getInt(2)).isEqualTo(3);
    }

    @Test
    public void testNullArrayField() {
        RowType table =
                RowType.builder().field("intArray", DataTypes.ARRAY(DataTypes.INT())).build();

        PojoToRowConverter<SimpleArrayPojo> writer =
                PojoToRowConverter.of(SimpleArrayPojo.class, table, table);

        SimpleArrayPojo pojo = new SimpleArrayPojo();
        pojo.intArray = null;

        GenericRow row = writer.toRow(pojo);
        assertThat(row.isNullAt(0)).isTrue();
    }

    /** POJO for testing all array types. */
    @SuppressWarnings("unchecked")
    public static class ArrayPojo {
        public boolean[] primitiveBooleanArray;
        public byte[] primitiveByteArray;
        public short[] primitiveShortArray;
        public int[] primitiveIntArray;
        public long[] primitiveLongArray;
        public float[] primitiveFloatArray;
        public double[] primitiveDoubleArray;
        public Boolean[] booleanArray;
        public Byte[] byteArray;
        public Short[] shortArray;
        public Integer[] intArray;
        public Long[] longArray;
        public Float[] floatArray;
        public Double[] doubleArray;
        public String[] stringArray;
        public BigDecimal[] decimalArray;
        public LocalDate[] dateArray;
        public LocalTime[] timeArray;
        public LocalDateTime[] timestampArray;
        public Instant[] timestampLtzArray;
        public Integer[][] nestedIntArray;
        public Map<String, Integer>[] mapArray;

        public ArrayPojo() {}

        public static ArrayPojo sample() {
            ArrayPojo pojo = new ArrayPojo();
            pojo.primitiveBooleanArray = new boolean[] {true, false};
            pojo.primitiveByteArray = new byte[] {1, 2};
            pojo.primitiveShortArray = new short[] {100, 200};
            pojo.primitiveIntArray = new int[] {1000, 2000};
            pojo.primitiveLongArray = new long[] {10000L, 20000L};
            pojo.primitiveFloatArray = new float[] {1.1f, 2.2f};
            pojo.primitiveDoubleArray = new double[] {1.11, 2.22};
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
        public String[] stringArray;
        public Integer[] intObjectArray;

        public NullableArrayPojo() {}
    }

    /** Simple POJO for testing null array field. */
    public static class SimpleArrayPojo {
        public Integer[] intArray;

        public SimpleArrayPojo() {}
    }
}
