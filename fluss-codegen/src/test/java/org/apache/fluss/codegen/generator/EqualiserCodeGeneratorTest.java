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

package org.apache.fluss.codegen.generator;

import org.apache.fluss.codegen.CodeGenTestUtils;
import org.apache.fluss.codegen.GeneratedClass;
import org.apache.fluss.codegen.types.RecordEqualiser;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link EqualiserCodeGenerator}.
 *
 * <p>Each test verifies both:
 *
 * <ul>
 *   <li>Generated code structure via expected file assertion
 *   <li>Runtime behavior via actual comparison
 * </ul>
 */
public class EqualiserCodeGeneratorTest {

    private static final String EXPECTED_DIR = "equaliser-code-generator/";

    // ==================== Primitive Types ====================

    @Test
    public void testIntField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.INT()})
                        .generateRecordEqualiser("IntFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testIntField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        assertThat(equaliser.equals(GenericRow.of(1), GenericRow.of(1))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(1), GenericRow.of(2))).isFalse();
    }

    @Test
    public void testBooleanField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.BOOLEAN()})
                        .generateRecordEqualiser("BooleanFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testBooleanField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        assertThat(equaliser.equals(GenericRow.of(true), GenericRow.of(true))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(true), GenericRow.of(false))).isFalse();
    }

    @Test
    public void testLongField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.BIGINT()})
                        .generateRecordEqualiser("LongFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testLongField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        assertThat(equaliser.equals(GenericRow.of(100L), GenericRow.of(100L))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(100L), GenericRow.of(200L))).isFalse();
    }

    @Test
    public void testDoubleField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.DOUBLE()})
                        .generateRecordEqualiser("DoubleFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testDoubleField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        assertThat(equaliser.equals(GenericRow.of(1.5), GenericRow.of(1.5))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(1.5), GenericRow.of(2.5))).isFalse();
    }

    // ==================== String and Binary Types ====================

    @Test
    public void testStringField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.STRING()})
                        .generateRecordEqualiser("StringFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testStringField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        assertThat(
                        equaliser.equals(
                                GenericRow.of(BinaryString.fromString("hello")),
                                GenericRow.of(BinaryString.fromString("hello"))))
                .isTrue();
        assertThat(
                        equaliser.equals(
                                GenericRow.of(BinaryString.fromString("hello")),
                                GenericRow.of(BinaryString.fromString("world"))))
                .isFalse();
    }

    @Test
    public void testBinaryField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.BINARY(5)})
                        .generateRecordEqualiser("BinaryFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testBinaryField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        byte[] b1 = new byte[] {1, 2, 3, 4, 5};
        byte[] b2 = new byte[] {1, 2, 3, 4, 5};
        byte[] b3 = new byte[] {1, 2, 3, 4, 6};
        assertThat(equaliser.equals(GenericRow.of((Object) b1), GenericRow.of((Object) b2)))
                .isTrue();
        assertThat(equaliser.equals(GenericRow.of((Object) b1), GenericRow.of((Object) b3)))
                .isFalse();
    }

    @Test
    public void testBytesField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.BYTES()})
                        .generateRecordEqualiser("BytesFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testBytesField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        assertThat(
                        equaliser.equals(
                                GenericRow.of((Object) new byte[] {1, 2}),
                                GenericRow.of((Object) new byte[] {1, 2})))
                .isTrue();
        assertThat(
                        equaliser.equals(
                                GenericRow.of((Object) new byte[] {1, 2}),
                                GenericRow.of((Object) new byte[] {1, 3})))
                .isFalse();
    }

    // ==================== Comparable Types ====================

    @Test
    public void testDecimalField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.DECIMAL(10, 2)})
                        .generateRecordEqualiser("DecimalFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testDecimalField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        Decimal d1 = Decimal.fromUnscaledLong(12345, 10, 2);
        Decimal d2 = Decimal.fromUnscaledLong(12345, 10, 2);
        Decimal d3 = Decimal.fromUnscaledLong(12346, 10, 2);
        assertThat(equaliser.equals(GenericRow.of(d1), GenericRow.of(d2))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(d1), GenericRow.of(d3))).isFalse();
    }

    @Test
    public void testTimestampNtzField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.TIMESTAMP(6)})
                        .generateRecordEqualiser("TimestampNtzFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testTimestampNtzField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        TimestampNtz ts1 = TimestampNtz.fromMillis(1684814400000L);
        TimestampNtz ts2 = TimestampNtz.fromMillis(1684814400000L);
        TimestampNtz ts3 = TimestampNtz.fromMillis(1684814500000L);
        assertThat(equaliser.equals(GenericRow.of(ts1), GenericRow.of(ts2))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(ts1), GenericRow.of(ts3))).isFalse();
    }

    @Test
    public void testTimestampLtzField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.TIMESTAMP_LTZ(3)})
                        .generateRecordEqualiser("TimestampLtzFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testTimestampLtzField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        TimestampLtz ts1 = TimestampLtz.fromEpochMillis(1684814600000L);
        TimestampLtz ts2 = TimestampLtz.fromEpochMillis(1684814600000L);
        TimestampLtz ts3 = TimestampLtz.fromEpochMillis(1684814700000L);
        assertThat(equaliser.equals(GenericRow.of(ts1), GenericRow.of(ts2))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(ts1), GenericRow.of(ts3))).isFalse();
    }

    // ==================== Composite Types ====================

    @Test
    public void testArrayField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.ARRAY(DataTypes.INT())})
                        .generateRecordEqualiser("ArrayFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testArrayField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());
        GenericArray arr1 = new GenericArray(new Integer[] {1, 2, 3});
        GenericArray arr2 = new GenericArray(new Integer[] {1, 2, 3});
        GenericArray arr3 = new GenericArray(new Integer[] {1, 2, 4});
        assertThat(equaliser.equals(GenericRow.of(arr1), GenericRow.of(arr2))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(arr1), GenericRow.of(arr3))).isFalse();
    }

    @Test
    public void testMapField() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(
                                new DataType[] {DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())})
                        .generateRecordEqualiser("MapFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testMapField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());

        Map<BinaryString, Integer> map1 = new HashMap<>();
        map1.put(BinaryString.fromString("a"), 1);
        map1.put(BinaryString.fromString("b"), 2);

        Map<BinaryString, Integer> map2 = new HashMap<>();
        map2.put(BinaryString.fromString("a"), 1);
        map2.put(BinaryString.fromString("b"), 2);

        Map<BinaryString, Integer> map3 = new HashMap<>();
        map3.put(BinaryString.fromString("a"), 1);
        map3.put(BinaryString.fromString("b"), 3);

        assertThat(
                        equaliser.equals(
                                GenericRow.of(new GenericMap(map1)),
                                GenericRow.of(new GenericMap(map2))))
                .isTrue();
        assertThat(
                        equaliser.equals(
                                GenericRow.of(new GenericMap(map1)),
                                GenericRow.of(new GenericMap(map3))))
                .isFalse();
    }

    @Test
    public void testNestedRowField() {
        DataType nestedRowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());

        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {nestedRowType})
                        .generateRecordEqualiser("NestedRowFieldEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testNestedRowField.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());

        GenericRow inner1 = GenericRow.of(1, BinaryString.fromString("test"));
        GenericRow inner2 = GenericRow.of(1, BinaryString.fromString("test"));
        GenericRow inner3 = GenericRow.of(1, BinaryString.fromString("other"));

        assertThat(equaliser.equals(GenericRow.of(inner1), GenericRow.of(inner2))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(inner1), GenericRow.of(inner3))).isFalse();
    }

    // ==================== Multiple Fields ====================

    @Test
    public void testMultipleFields() {
        DataType[] types =
                new DataType[] {
                    DataTypes.INT(),
                    DataTypes.STRING(),
                    DataTypes.BOOLEAN(),
                    DataTypes.BIGINT(),
                    DataTypes.DOUBLE()
                };

        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(types)
                        .generateRecordEqualiser("MultipleFieldsEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testMultipleFields.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());

        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("test"), true, 100L, 1.5);
        GenericRow row2 = GenericRow.of(1, BinaryString.fromString("test"), true, 100L, 1.5);
        GenericRow row3 = GenericRow.of(1, BinaryString.fromString("test"), false, 100L, 1.5);

        assertThat(equaliser.equals(row1, row2)).isTrue();
        assertThat(equaliser.equals(row1, row3)).isFalse();
    }

    // ==================== Projection ====================

    @Test
    public void testProjection() {
        DataType[] types = new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()};
        int[] projection = new int[] {1, 2}; // Only compare STRING and BIGINT

        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(types, projection)
                        .generateRecordEqualiser("ProjectionEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testProjection.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());

        // Different INT (field 0) but same STRING and BIGINT (fields 1, 2)
        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("test"), 100L);
        GenericRow row2 = GenericRow.of(2, BinaryString.fromString("test"), 100L);
        GenericRow row3 = GenericRow.of(1, BinaryString.fromString("other"), 100L);

        // Should be equal because INT is not in projection
        assertThat(equaliser.equals(row1, row2)).isTrue();
        // Should not be equal because STRING differs
        assertThat(equaliser.equals(row1, row3)).isFalse();
    }

    // ==================== Null Handling ====================

    @Test
    public void testNullHandling() {
        GeneratedClass<RecordEqualiser> generated =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.INT()})
                        .generateRecordEqualiser("NullHandlingEqualiser");

        CodeGenTestUtils.assertMatchesExpectedNormalized(
                generated.getCode(), EXPECTED_DIR + "testNullHandling.java.expected");

        RecordEqualiser equaliser =
                generated.newInstance(Thread.currentThread().getContextClassLoader());

        GenericRow nullRow1 = new GenericRow(1);
        nullRow1.setField(0, null);
        GenericRow nullRow2 = new GenericRow(1);
        nullRow2.setField(0, null);
        GenericRow nonNullRow = GenericRow.of(1);

        // Both null - should be equal
        assertThat(equaliser.equals(nullRow1, nullRow2)).isTrue();
        // One null, one not - should not be equal
        assertThat(equaliser.equals(nullRow1, nonNullRow)).isFalse();
        assertThat(equaliser.equals(nonNullRow, nullRow1)).isFalse();
    }
}
