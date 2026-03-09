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

import static org.assertj.core.api.Assertions.assertThat;

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

    /** POJO for testing all array types. */
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
