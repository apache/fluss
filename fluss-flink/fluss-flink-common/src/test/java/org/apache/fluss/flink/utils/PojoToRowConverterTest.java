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

package org.apache.fluss.flink.utils;

import org.apache.fluss.flink.source.testutils.Order;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link PojoToRowConverter}. */
public class PojoToRowConverterTest {

    @Test
    public void testBasicConversion() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField(
                                        "address", new StringType(true), "Shipping address")));

        PojoToRowConverter<Order> converter = new PojoToRowConverter<>(Order.class, rowType);

        Order order = new Order(1001L, 5001L, 10, "123 Mumbai");

        GenericRow result = converter.convert(order);

        assertThat(result.getFieldCount()).isEqualTo(4);
        assertThat(result.getLong(0)).isEqualTo(1001L);
        assertThat(result.getLong(1)).isEqualTo(5001L);
        assertThat(result.getInt(2)).isEqualTo(10);
        assertThat(result.getString(3).toString()).isEqualTo("123 Mumbai");
    }

    @Test
    public void testNullHandling() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField(
                                        "address", new StringType(true), "Shipping address")));

        PojoToRowConverter<Order> converter = new PojoToRowConverter<>(Order.class, rowType);

        GenericRow nullResult = converter.convert(null);
        assertThat(nullResult).isNull();

        Order order = new Order(1002L, 5002L, 5, null);
        GenericRow result = converter.convert(order);

        assertThat(result.getLong(0)).isEqualTo(1002L);
        assertThat(result.getLong(1)).isEqualTo(5002L);
        assertThat(result.getInt(2)).isEqualTo(5);
        assertThat(result.isNullAt(3)).isTrue();
    }

    @Test
    public void testMissingFields() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField("address", new StringType(true), "Shipping address"),
                                new DataField(
                                        "nonExistentField",
                                        new StringType(true),
                                        "Non-existent field")));

        PojoToRowConverter<Order> converter = new PojoToRowConverter<>(Order.class, rowType);

        Order order = new Order(1003L, 5003L, 15, "456 Shenzhen");

        GenericRow result = converter.convert(order);

        assertThat(result.getFieldCount()).isEqualTo(5);
        assertThat(result.getLong(0)).isEqualTo(1003L);
        assertThat(result.getLong(1)).isEqualTo(5003L);
        assertThat(result.getInt(2)).isEqualTo(15);
        assertThat(result.getString(3).toString()).isEqualTo("456 Shenzhen");
        assertThat(result.isNullAt(4)).isTrue();
    }

    @Test
    public void testFieldOrderIndependence() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("address", new StringType(true), "Shipping address"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID")));

        PojoToRowConverter<Order> converter = new PojoToRowConverter<>(Order.class, rowType);

        Order order = new Order(1004L, 5004L, 20, "789 Greece");

        GenericRow result = converter.convert(order);

        assertThat(result.getFieldCount()).isEqualTo(4);
        assertThat(result.getString(0).toString()).isEqualTo("789 Greece");
        assertThat(result.getInt(1)).isEqualTo(20);
        assertThat(result.getLong(2)).isEqualTo(1004L);
        assertThat(result.getLong(3)).isEqualTo(5004L);
    }

    /**
     * Test POJO class used for nested converter test scenarios.
     *
     * <p>This class is used for testing purposes:
     *
     * <ul>
     *   <li>Serving as a nested field in other classes to verify that nested POJOs are properly
     *       rejected by the converter
     * </ul>
     */
    public static class ProductWithPrice {
        private long id;
        private BigDecimal price;

        // Must have public no-args constructor
        public ProductWithPrice() {}

        public ProductWithPrice(long id, BigDecimal price) {
            this.id = id;
            this.price = price;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public BigDecimal getPrice() {
            return price;
        }

        public void setPrice(BigDecimal price) {
            this.price = price;
        }
    }

    @Test
    public void testNestedPojoThrowsException() {
        // Define a class with a nested POJO field
        class NestedContainer {
            private String name;
            private ProductWithPrice nestedPojo; // This is a nested POJO

            public NestedContainer(String name, ProductWithPrice nestedPojo) {
                this.name = name;
                this.nestedPojo = nestedPojo;
            }
        }

        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("name", new StringType(false), "Name"),
                                new DataField(
                                        "nestedPojo",
                                        new RowType(true, Collections.emptyList()),
                                        "Nested POJO")));

        // Accept either exception type since Flink's POJO analysis happens first
        assertThatThrownBy(() -> new PojoToRowConverter<>(NestedContainer.class, rowType))
                .isInstanceOfAny(UnsupportedOperationException.class, InvalidTypesException.class);
    }

    @Test
    public void testUnsupportedJavaClass() {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("id", new IntType(false), "Id"),
                                new DataField("price", new DecimalType(38, 18), "Price")));

        assertThatThrownBy(() -> new PojoToRowConverter<>(ProductWithPrice.class, rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Field Java type class java.lang.Long for field id is not supported, the supported Java types are [class java.lang.Integer, int, class java.lang.Short, short, class java.lang.Byte, byte]");
    }

    @Test
    public void testEmptySchema() throws Exception {
        RowType emptyRowType = new RowType(true, Collections.emptyList());

        PojoToRowConverter<Order> converter = new PojoToRowConverter<>(Order.class, emptyRowType);

        Order order = new Order(1005L, 5005L, 25, "Empty schema test");

        GenericRow result = converter.convert(order);

        assertThat(result.getFieldCount()).isEqualTo(0);
    }

    /** Test class with various data types to test type conversion. */
    public static class ComplexTypeOrder extends Order {
        private boolean booleanValue;
        private byte tinyintValue;
        private short smallintValue;
        private float floatValue;
        private double doubleValue;
        private BigDecimal decimalValue;
        private LocalDate dateValue;
        private LocalTime timeValue;
        private LocalDateTime timestampValue;
        private byte[] bytesValue;
        private char charValue;

        public ComplexTypeOrder() {
            super();
        }

        public ComplexTypeOrder(
                long orderId,
                long itemId,
                int amount,
                String address,
                boolean booleanValue,
                byte tinyintValue,
                short smallintValue,
                float floatValue,
                double doubleValue,
                BigDecimal decimalValue,
                LocalDate dateValue,
                LocalTime timeValue,
                LocalDateTime timestampValue,
                byte[] bytesValue,
                char charValue) {
            super(orderId, itemId, amount, address);
            this.booleanValue = booleanValue;
            this.tinyintValue = tinyintValue;
            this.smallintValue = smallintValue;
            this.floatValue = floatValue;
            this.doubleValue = doubleValue;
            this.decimalValue = decimalValue;
            this.dateValue = dateValue;
            this.timeValue = timeValue;
            this.timestampValue = timestampValue;
            this.bytesValue = bytesValue;
            this.charValue = charValue;
        }

        public boolean isBooleanValue() {
            return booleanValue;
        }

        public void setBooleanValue(boolean booleanValue) {
            this.booleanValue = booleanValue;
        }

        public byte getTinyintValue() {
            return tinyintValue;
        }

        public void setTinyintValue(byte tinyintValue) {
            this.tinyintValue = tinyintValue;
        }

        public short getSmallintValue() {
            return smallintValue;
        }

        public void setSmallintValue(short smallintValue) {
            this.smallintValue = smallintValue;
        }

        public float getFloatValue() {
            return floatValue;
        }

        public void setFloatValue(float floatValue) {
            this.floatValue = floatValue;
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public void setDoubleValue(double doubleValue) {
            this.doubleValue = doubleValue;
        }

        public BigDecimal getDecimalValue() {
            return decimalValue;
        }

        public void setDecimalValue(BigDecimal decimalValue) {
            this.decimalValue = decimalValue;
        }

        public LocalDate getDateValue() {
            return dateValue;
        }

        public void setDateValue(LocalDate dateValue) {
            this.dateValue = dateValue;
        }

        public LocalTime getTimeValue() {
            return timeValue;
        }

        public void setTimeValue(LocalTime timeValue) {
            this.timeValue = timeValue;
        }

        public LocalDateTime getTimestampValue() {
            return timestampValue;
        }

        public void setTimestampValue(LocalDateTime timestampValue) {
            this.timestampValue = timestampValue;
        }

        public byte[] getBytesValue() {
            return bytesValue;
        }

        public void setBytesValue(byte[] bytesValue) {
            this.bytesValue = bytesValue;
        }

        public char getCharValue() {
            return charValue;
        }

        public void setCharValue(char charValue) {
            this.charValue = charValue;
        }
    }

    @Test
    void testConvertAllDataTypes() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                // Basic Order fields
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField("address", new StringType(true), "Shipping address"),
                                // Additional data types
                                new DataField(
                                        "booleanValue", new BooleanType(false), "Boolean value"),
                                new DataField(
                                        "tinyintValue", new TinyIntType(false), "TinyInt value"),
                                new DataField(
                                        "smallintValue", new SmallIntType(false), "SmallInt value"),
                                new DataField("floatValue", new FloatType(false), "Float value"),
                                new DataField("doubleValue", new DoubleType(false), "Double value"),
                                new DataField(
                                        "decimalValue",
                                        new DecimalType(false, 10, 2),
                                        "Decimal value"),
                                new DataField("dateValue", new DateType(false), "Date value"),
                                new DataField("timeValue", new TimeType(false, 3), "Time value"),
                                new DataField(
                                        "timestampValue",
                                        new TimestampType(false, 6),
                                        "Timestamp value"),
                                new DataField("bytesValue", new BinaryType(5), "Binary value"),
                                new DataField("charValue", new CharType(false, 1), "Char value")));

        // Create a ComplexTypeOrder with all fields
        ComplexTypeOrder order =
                new ComplexTypeOrder(
                        1001L,
                        5001L,
                        10,
                        "123 Mumbai",
                        true, // boolean
                        (byte) 127, // tinyint
                        (short) 32767, // smallint
                        3.14f, // float
                        2.71828, // double
                        new BigDecimal("123.45"), // decimal
                        LocalDate.of(2023, 7, 15), // date
                        LocalTime.of(14, 30, 45, 123000000), // time
                        LocalDateTime.of(2023, 7, 15, 14, 30, 45, 123456000), // timestamp
                        new byte[] {1, 2, 3, 4, 5}, // binary
                        'A' // char
                        );

        PojoToRowConverter<ComplexTypeOrder> converter =
                new PojoToRowConverter<>(ComplexTypeOrder.class, rowType);
        GenericRow result = converter.convert(order);

        assertThat(result.getFieldCount()).isEqualTo(15);
        assertThat(result.getLong(0)).isEqualTo(1001L);
        assertThat(result.getLong(1)).isEqualTo(5001L);
        assertThat(result.getInt(2)).isEqualTo(10);
        assertThat(result.getString(3).toString()).isEqualTo("123 Mumbai");

        // Additional data types
        assertThat(result.getBoolean(4)).isTrue();
        assertThat(result.getByte(5)).isEqualTo((byte) 127);
        assertThat(result.getShort(6)).isEqualTo((short) 32767);
        assertThat(result.getFloat(7)).isEqualTo(3.14f);
        assertThat(result.getDouble(8)).isEqualTo(2.71828);
        assertThat(result.getDecimal(9, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));

        LocalDate expectedDate = LocalDate.of(2023, 7, 15);
        int expectedEpochDays = (int) expectedDate.toEpochDay();
        assertThat(result.getInt(10)).isEqualTo(expectedEpochDays);

        LocalTime expectedTime = LocalTime.of(14, 30, 45, 123000000);
        int expectedMillisOfDay = (int) (expectedTime.toNanoOfDay() / 1_000_000);
        assertThat(result.getInt(11)).isEqualTo(expectedMillisOfDay);

        LocalDateTime expectedTimestamp = LocalDateTime.of(2023, 7, 15, 14, 30, 45, 123456000);
        long expectedEpochMillis =
                expectedTimestamp.toEpochSecond(java.time.ZoneOffset.UTC) * 1000L + 123L;
        assertThat(result.getTimestampNtz(12, 6).getMillisecond()).isEqualTo(expectedEpochMillis);

        assertThat(result.getBytes(13)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(result.getString(14).toString()).isEqualTo("A");
    }

    // ========== Numeric Type Widening Tests ==========

    @Test
    public void testIntToBigIntWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("orderId", new BigIntType(false), "Order ID")));

        PojoToRowConverter<IntOrderPojo> converter =
                new PojoToRowConverter<>(IntOrderPojo.class, rowType);

        IntOrderPojo pojo = new IntOrderPojo();
        pojo.orderId = 123456;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getLong(0)).isEqualTo(123456L);
    }

    @Test
    public void testShortToBigIntWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("quantity", new BigIntType(false), "Quantity")));

        PojoToRowConverter<ShortQuantityPojo> converter =
                new PojoToRowConverter<>(ShortQuantityPojo.class, rowType);

        ShortQuantityPojo pojo = new ShortQuantityPojo();
        pojo.quantity = (short) 999;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getLong(0)).isEqualTo(999L);
    }

    @Test
    public void testByteToIntWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("value", new IntType(false), "Value")));

        PojoToRowConverter<ByteValuePojo> converter =
                new PojoToRowConverter<>(ByteValuePojo.class, rowType);

        ByteValuePojo pojo = new ByteValuePojo();
        pojo.value = (byte) 42;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getInt(0)).isEqualTo(42);
    }

    @Test
    public void testByteToSmallIntWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("value", new SmallIntType(false), "Value")));

        PojoToRowConverter<ByteValuePojo> converter =
                new PojoToRowConverter<>(ByteValuePojo.class, rowType);

        ByteValuePojo pojo = new ByteValuePojo();
        pojo.value = (byte) 42;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getShort(0)).isEqualTo((short) 42);
    }

    @Test
    public void testShortToIntWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("quantity", new IntType(false), "Quantity")));

        PojoToRowConverter<ShortQuantityPojo> converter =
                new PojoToRowConverter<>(ShortQuantityPojo.class, rowType);

        ShortQuantityPojo pojo = new ShortQuantityPojo();
        pojo.quantity = (short) 999;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getInt(0)).isEqualTo(999);
    }

    @Test
    public void testIntToFloatWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("orderId", new FloatType(false), "Order ID")));

        PojoToRowConverter<IntOrderPojo> converter =
                new PojoToRowConverter<>(IntOrderPojo.class, rowType);

        IntOrderPojo pojo = new IntOrderPojo();
        pojo.orderId = 1000;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getFloat(0)).isEqualTo(1000.0f);
    }

    @Test
    public void testLongToFloatWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("value", new FloatType(false), "Value")));

        PojoToRowConverter<LongValuePojo> converter =
                new PojoToRowConverter<>(LongValuePojo.class, rowType);

        LongValuePojo pojo = new LongValuePojo();
        pojo.value = 123456789L;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getFloat(0)).isEqualTo(1.23456792E8f);
    }

    @Test
    public void testFloatToDoubleWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("price", new DoubleType(false), "Price")));

        PojoToRowConverter<FloatPricePojo> converter =
                new PojoToRowConverter<>(FloatPricePojo.class, rowType);

        FloatPricePojo pojo = new FloatPricePojo();
        pojo.price = 99.99f;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getDouble(0)).isCloseTo(99.99, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    public void testLongToDoubleWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("value", new DoubleType(false), "Value")));

        PojoToRowConverter<LongValuePojo> converter =
                new PojoToRowConverter<>(LongValuePojo.class, rowType);

        LongValuePojo pojo = new LongValuePojo();
        pojo.value = 123456789L;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getDouble(0)).isEqualTo(123456789.0);
    }

    @Test
    public void testIntToDoubleWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("orderId", new DoubleType(false), "Order ID")));

        PojoToRowConverter<IntOrderPojo> converter =
                new PojoToRowConverter<>(IntOrderPojo.class, rowType);

        IntOrderPojo pojo = new IntOrderPojo();
        pojo.orderId = 1000;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getDouble(0)).isEqualTo(1000.0);
    }

    @Test
    public void testPrimitiveIntToBigIntWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("id", new BigIntType(false), "ID")));

        PojoToRowConverter<PrimitiveIntPojo> converter =
                new PojoToRowConverter<>(PrimitiveIntPojo.class, rowType);

        PrimitiveIntPojo pojo = new PrimitiveIntPojo();
        pojo.id = 789;

        GenericRow row = converter.convert(pojo);
        assertThat(row.getLong(0)).isEqualTo(789L);
    }

    @Test
    public void testMultipleWideningInSamePojo() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("byteVal", new BigIntType(false), "Byte Value"),
                                new DataField("shortVal", new BigIntType(false), "Short Value"),
                                new DataField("intVal", new BigIntType(false), "Int Value"),
                                new DataField("floatVal", new DoubleType(false), "Float Value")));

        PojoToRowConverter<MixedNumericPojo> converter =
                new PojoToRowConverter<>(MixedNumericPojo.class, rowType);

        MixedNumericPojo pojo = new MixedNumericPojo();
        pojo.byteVal = (byte) 10;
        pojo.shortVal = (short) 100;
        pojo.intVal = 1000;
        pojo.floatVal = 10.5f;

        GenericRow row = converter.convert(pojo);

        assertThat(row.getLong(0)).isEqualTo(10L);
        assertThat(row.getLong(1)).isEqualTo(100L);
        assertThat(row.getLong(2)).isEqualTo(1000L);
        assertThat(row.getDouble(3)).isCloseTo(10.5, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    public void testNarrowingConversionStillFails() {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("value", new SmallIntType(false), "Value")));

        // Long to Short is narrowing - should still fail
        assertThatThrownBy(() -> new PojoToRowConverter<>(LongValuePojo.class, rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("is not supported");
    }

    @Test
    public void testNullHandlingWithWidening() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Collections.singletonList(
                                new DataField("orderId", new BigIntType(false), "Order ID")));

        PojoToRowConverter<IntOrderPojo> converter =
                new PojoToRowConverter<>(IntOrderPojo.class, rowType);

        IntOrderPojo pojo = new IntOrderPojo();
        pojo.orderId = null;

        GenericRow row = converter.convert(pojo);
        assertThat(row.isNullAt(0)).isTrue();
    }

    // ========== Test POJO classes for widening tests ==========

    /** Test POJO with Integer field. */
    public static class IntOrderPojo {
        public Integer orderId;

        public IntOrderPojo() {}
    }

    /** Test POJO with Short field. */
    public static class ShortQuantityPojo {
        public Short quantity;

        public ShortQuantityPojo() {}
    }

    /** Test POJO with Byte field. */
    public static class ByteValuePojo {
        public Byte value;

        public ByteValuePojo() {}
    }

    /** Test POJO with Float field. */
    public static class FloatPricePojo {
        public Float price;

        public FloatPricePojo() {}
    }

    /** Test POJO with Long field. */
    public static class LongValuePojo {
        public Long value;

        public LongValuePojo() {}
    }

    /** Test POJO with primitive int field. */
    public static class PrimitiveIntPojo {
        public int id;

        public PrimitiveIntPojo() {}
    }

    /** Test POJO with mixed numeric types. */
    public static class MixedNumericPojo {
        public Byte byteVal;
        public Short shortVal;
        public Integer intVal;
        public Float floatVal;

        public MixedNumericPojo() {}
    }
}
