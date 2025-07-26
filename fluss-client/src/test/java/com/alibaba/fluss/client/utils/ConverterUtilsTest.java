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

package com.alibaba.fluss.client.utils;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConverterUtils}. */
public class ConverterUtilsTest extends ClientToServerITCaseBase {

    private TestPojo createTestPojo() {
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

    @Test
    public void testToRow() {
        ConverterUtils<TestPojo> converter =
                ConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        TestPojo pojo = createTestPojo();

        GenericRow row = converter.toRow(pojo);

        // Verify the row
        assertThat(row).isNotNull();
        assertThat(row.getFieldCount()).isEqualTo(15);
        assertThat(row.getBoolean(0)).isEqualTo(true);
        assertThat(row.getByte(1)).isEqualTo((byte) 42);
        assertThat(row.getShort(2)).isEqualTo((short) 1234);
        assertThat(row.getInt(3)).isEqualTo(123456);
        assertThat(row.getLong(4)).isEqualTo(9876543210L);
        assertThat(row.getFloat(5)).isEqualTo(3.14f);
        assertThat(row.getDouble(6)).isEqualTo(2.71828);
        assertThat(row.getString(7).toString()).isEqualTo("Hello, World!");
        assertThat(row.getBytes(8)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(row.getDecimal(9, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));
        assertThat(row.getInt(10)).isEqualTo(LocalDate.of(2025, 7, 23).toEpochDay());
        assertThat(row.getInt(11)).isEqualTo(LocalTime.of(15, 1, 30).toNanoOfDay() / 1_000_000);
        assertThat(row.getTimestampNtz(12, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2025, 7, 23, 15, 1, 30));
        assertThat(row.getTimestampLtz(13, 6).toInstant())
                .isEqualTo(Instant.parse("2025-07-23T15:01:30Z"));
        assertThat(row.getTimestampLtz(14, 6).toInstant())
                .isEqualTo(
                        OffsetDateTime.of(2025, 7, 23, 15, 1, 30, 0, ZoneOffset.UTC).toInstant());
    }

    @Test
    public void testFromRow() {
        ConverterUtils<TestPojo> converter =
                ConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        TestPojo originalPojo = createTestPojo();

        GenericRow row = converter.toRow(originalPojo);

        TestPojo convertedPojo = converter.fromRow(row);

        // Verify individual fields
        assertThat(convertedPojo.booleanField).isEqualTo(originalPojo.booleanField);
        assertThat(convertedPojo.byteField).isEqualTo(originalPojo.byteField);
        assertThat(convertedPojo.shortField).isEqualTo(originalPojo.shortField);
        assertThat(convertedPojo.intField).isEqualTo(originalPojo.intField);
        assertThat(convertedPojo.longField).isEqualTo(originalPojo.longField); // Verify long field
        assertThat(convertedPojo.floatField).isEqualTo(originalPojo.floatField);
        assertThat(convertedPojo.doubleField).isEqualTo(originalPojo.doubleField);
        assertThat(convertedPojo.stringField).isEqualTo(originalPojo.stringField);
        assertThat(convertedPojo.bytesField).isEqualTo(originalPojo.bytesField);
        assertThat(convertedPojo.decimalField).isEqualTo(originalPojo.decimalField);
        assertThat(convertedPojo.dateField).isEqualTo(originalPojo.dateField);
        assertThat(convertedPojo.timeField).isEqualTo(originalPojo.timeField);
        assertThat(convertedPojo.timestampField).isEqualTo(originalPojo.timestampField);
        assertThat(convertedPojo.timestampLtzField).isEqualTo(originalPojo.timestampLtzField);
        assertThat(convertedPojo.offsetDateTimeField).isEqualTo(originalPojo.offsetDateTimeField);

        // Verify the POJO
        assertThat(convertedPojo).isNotNull();
        assertThat(convertedPojo).isEqualTo(originalPojo);
    }

    @Test
    public void testNullValues() {
        ConverterUtils<TestPojo> converter =
                ConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        assertThat(converter.toRow(null)).isNull();

        assertThat(converter.fromRow(null)).isNull();

        GenericRow row = new GenericRow(15);

        TestPojo pojo = converter.fromRow(row);

        // Verify the POJO
        assertThat(pojo).isNotNull();
        assertThat(pojo.booleanField).isEqualTo(false); // Default value for boolean is false
        assertThat(pojo.byteField).isEqualTo((byte) 0); // Default value for byte is 0
        assertThat(pojo.shortField).isEqualTo((short) 0); // Default value for short is 0
        assertThat(pojo.intField).isEqualTo(0); // Default value for int is 0
        assertThat(pojo.longField).isEqualTo(0L); // Default value for long is 0
        assertThat(pojo.floatField).isEqualTo(0.0f); // Default value for float is 0.0
        assertThat(pojo.doubleField).isEqualTo(0.0); // Default value for double is 0.0
        assertThat(pojo.stringField).isNull();
        assertThat(pojo.bytesField).isNull();
        assertThat(pojo.decimalField).isNull();
        assertThat(pojo.dateField).isNull();
        assertThat(pojo.timeField).isNull();
        assertThat(pojo.timestampField).isNull();
        assertThat(pojo.timestampLtzField).isNull();
        assertThat(pojo.offsetDateTimeField).isNull();
    }

    @Test
    public void testCaching() {
        ConverterUtils<TestPojo> converter1 =
                ConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        ConverterUtils<TestPojo> converter2 =
                ConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        assertThat(converter2).isSameAs(converter1);
    }

    @Test
    public void testPartialPojo() {
        // Create a row type with more fields than the POJO
        RowType rowType =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .field("extraField", DataTypes.DOUBLE()) // Not in POJO
                        .build();

        // Expect an IllegalArgumentException when creating a converter with a field not in the POJO
        assertThatThrownBy(() -> ConverterUtils.getConverter(PartialTestPojo.class, rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field 'extraField' not found in POJO class");
    }

    @Test
    public void testRowWithMissingFields() {
        // Create a row type with fewer fields than the POJO
        RowType rowType =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .build();

        ConverterUtils<TestPojo> converter = ConverterUtils.getConverter(TestPojo.class, rowType);

        TestPojo pojo = createTestPojo();

        GenericRow row = converter.toRow(pojo);

        assertThat(row).isNotNull();
        assertThat(row.getFieldCount()).isEqualTo(3);
        assertThat(row.getBoolean(0)).isEqualTo(true);
        assertThat(row.getInt(1)).isEqualTo(123456);
        assertThat(row.getString(2).toString()).isEqualTo("Hello, World!");

        TestPojo convertedPojo = converter.fromRow(row);

        assertThat(convertedPojo).isNotNull();
        assertThat(convertedPojo.booleanField).isEqualTo(true);
        assertThat(convertedPojo.intField).isEqualTo(123456);
        assertThat(convertedPojo.stringField).isEqualTo("Hello, World!");
        // Other fields have default values
        assertThat(convertedPojo.byteField).isEqualTo((byte) 0);
        assertThat(convertedPojo.shortField).isEqualTo((short) 0);
        assertThat(convertedPojo.longField).isEqualTo(0L);
        assertThat(convertedPojo.floatField).isEqualTo(0.0f);
        assertThat(convertedPojo.doubleField).isEqualTo(0.0);
        assertThat(convertedPojo.bytesField).isNull();
        assertThat(convertedPojo.decimalField).isNull();
        assertThat(convertedPojo.dateField).isNull();
        assertThat(convertedPojo.timeField).isNull();
        assertThat(convertedPojo.timestampField).isNull();
        assertThat(convertedPojo.timestampLtzField).isNull();
        assertThat(convertedPojo.offsetDateTimeField).isNull();
    }

    @Test
    public void testWriteAndReadPojos() throws Exception {
        TablePath tablePath = new TablePath("test_db", "pojo_table");

        // Create a schema for the table
        Schema schema =
                Schema.newBuilder()
                        .column("booleanField", DataTypes.BOOLEAN())
                        .column("byteField", DataTypes.TINYINT())
                        .column("shortField", DataTypes.SMALLINT())
                        .column("intField", DataTypes.INT())
                        .column("longField", DataTypes.BIGINT())
                        .column("floatField", DataTypes.FLOAT())
                        .column("doubleField", DataTypes.DOUBLE())
                        .column("stringField", DataTypes.STRING())
                        .column("bytesField", DataTypes.BYTES())
                        .column("decimalField", DataTypes.DECIMAL(10, 2))
                        .column("dateField", DataTypes.DATE())
                        .column("timeField", DataTypes.TIME())
                        .column("timestampField", DataTypes.TIMESTAMP())
                        .column("timestampLtzField", DataTypes.TIMESTAMP_LTZ())
                        .column("offsetDateTimeField", DataTypes.TIMESTAMP_LTZ())
                        .build();

        // Create a table descriptor
        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

        // Create the table
        createTable(tablePath, tableDescriptor, false);

        // Create a converter for TestPojo
        ConverterUtils<TestPojo> converter =
                ConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        List<TestPojo> originalPojos = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            originalPojos.add(createTestPojo());
        }

        // Write the POJOs to the table
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (TestPojo pojo : originalPojos) {
                GenericRow row = converter.toRow(pojo);
                appendWriter.append(row).get();
            }

            // Create a log scanner
            LogScanner logScanner = createLogScanner(table);

            // Subscribe to the log from the beginning
            subscribeFromBeginning(logScanner, table);

            // Read the rows back
            List<TestPojo> readPojos = new ArrayList<>();
            while (readPojos.size() < originalPojos.size()) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    InternalRow row = scanRecord.getRow();
                    // Convert row back to POJO
                    TestPojo pojo = converter.fromRow(row);
                    readPojos.add(pojo);
                }
            }

            assertThat(readPojos.size()).isEqualTo(originalPojos.size());

            // Verify that the read POJOs match the original POJOs
            for (TestPojo originalPojo : originalPojos) {
                assertThat(originalPojo).isNotNull();
                // Find a matching POJO in the read list
                boolean found = false;
                for (TestPojo readPojo : readPojos) {
                    if (originalPojo.equals(readPojo)) {
                        found = true;
                        break;
                    }
                }
                assertThat(found)
                        .withFailMessage(
                                "Could not find matching POJO: " + originalPojo.stringField)
                        .isTrue();
            }
        }
    }

    /** Test POJO class with various field types. */
    public static class TestPojo {
        private boolean booleanField;
        private byte byteField;
        private short shortField;
        private int intField;
        private long longField;
        private float floatField;
        private double doubleField;
        private String stringField;
        private byte[] bytesField;
        private BigDecimal decimalField;
        private LocalDate dateField;
        private LocalTime timeField;
        private LocalDateTime timestampField;
        private Instant timestampLtzField;
        private OffsetDateTime offsetDateTimeField;

        public TestPojo() {}

        public TestPojo(
                boolean booleanField,
                byte byteField,
                short shortField,
                int intField,
                long longField,
                float floatField,
                double doubleField,
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestPojo testPojo = (TestPojo) o;
            return booleanField == testPojo.booleanField
                    && byteField == testPojo.byteField
                    && shortField == testPojo.shortField
                    && intField == testPojo.intField
                    && longField == testPojo.longField
                    && Float.compare(testPojo.floatField, floatField) == 0
                    && Double.compare(testPojo.doubleField, doubleField) == 0
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

    /** Test POJO class with missing fields compared to the row type. */
    public static class PartialTestPojo {
        private boolean booleanField;
        private int intField;
        private String stringField;

        public PartialTestPojo() {}

        public PartialTestPojo(boolean booleanField, int intField, String stringField) {
            this.booleanField = booleanField;
            this.intField = intField;
            this.stringField = stringField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartialTestPojo that = (PartialTestPojo) o;
            return booleanField == that.booleanField
                    && intField == that.intField
                    && Objects.equals(stringField, that.stringField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(booleanField, intField, stringField);
        }
    }

    /** Creates a row type for the TestPojo class. */
    private RowType createTestPojoRowType() {
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
}
