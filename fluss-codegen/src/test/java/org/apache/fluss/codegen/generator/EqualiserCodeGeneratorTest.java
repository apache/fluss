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

import org.apache.fluss.codegen.types.RecordEqualiser;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link EqualiserCodeGenerator}. */
public class EqualiserCodeGeneratorTest {

    private static final Map<DataTypeRoot, GeneratedData> TEST_DATA =
            new HashMap<DataTypeRoot, GeneratedData>();

    static {
        TEST_DATA.put(
                DataTypeRoot.CHAR,
                new GeneratedData(
                        DataTypes.CHAR(1),
                        BinaryString.fromString("1"),
                        BinaryString.fromString("2")));
        TEST_DATA.put(
                DataTypeRoot.STRING,
                new GeneratedData(
                        DataTypes.STRING(),
                        BinaryString.fromString("3"),
                        BinaryString.fromString("4")));
        TEST_DATA.put(DataTypeRoot.BOOLEAN, new GeneratedData(DataTypes.BOOLEAN(), true, false));
        TEST_DATA.put(
                DataTypeRoot.BINARY,
                new GeneratedData(DataTypes.BINARY(1), new byte[] {5}, new byte[] {6}));
        TEST_DATA.put(
                DataTypeRoot.BYTES,
                new GeneratedData(DataTypes.BYTES(), new byte[] {7}, new byte[] {8}));
        TEST_DATA.put(
                DataTypeRoot.DECIMAL,
                new GeneratedData(
                        DataTypes.DECIMAL(10, 0),
                        Decimal.fromUnscaledLong(9, 10, 0),
                        Decimal.fromUnscaledLong(10, 10, 0)));
        TEST_DATA.put(
                DataTypeRoot.TINYINT, new GeneratedData(DataTypes.TINYINT(), (byte) 11, (byte) 12));
        TEST_DATA.put(
                DataTypeRoot.SMALLINT,
                new GeneratedData(DataTypes.SMALLINT(), (short) 13, (short) 14));
        TEST_DATA.put(DataTypeRoot.INTEGER, new GeneratedData(DataTypes.INT(), 15, 16));
        TEST_DATA.put(DataTypeRoot.BIGINT, new GeneratedData(DataTypes.BIGINT(), 17L, 18L));
        TEST_DATA.put(DataTypeRoot.FLOAT, new GeneratedData(DataTypes.FLOAT(), 19.0f, 20.0f));
        TEST_DATA.put(DataTypeRoot.DOUBLE, new GeneratedData(DataTypes.DOUBLE(), 21.0d, 22.0d));
        TEST_DATA.put(
                DataTypeRoot.DATE,
                new GeneratedData(DataTypes.DATE(), 19500, 19501)); // days since epoch
        TEST_DATA.put(
                DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                new GeneratedData(DataTypes.TIME(), 34200000, 37800000)); // milliseconds of day
        TEST_DATA.put(
                DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                new GeneratedData(
                        DataTypes.TIMESTAMP(3),
                        TimestampNtz.fromMillis(1684814400000L),
                        TimestampNtz.fromMillis(1684814500000L)));
        TEST_DATA.put(
                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                new GeneratedData(
                        DataTypes.TIMESTAMP_LTZ(3),
                        TimestampLtz.fromEpochMillis(1684814600000L),
                        TimestampLtz.fromEpochMillis(1684814700000L)));
        TEST_DATA.put(
                DataTypeRoot.ARRAY,
                new GeneratedData(
                        DataTypes.ARRAY(DataTypes.INT()),
                        new GenericArray(new Integer[] {1, 2, 3}),
                        new GenericArray(new Integer[] {4, 5, 6})));
        TEST_DATA.put(
                DataTypeRoot.MAP,
                new GeneratedData(
                        DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                        GenericMap.of(26, 27, 28, 29),
                        GenericMap.of(26, 27, 28, 30)));
        TEST_DATA.put(
                DataTypeRoot.ROW,
                new GeneratedData(
                        DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()),
                        GenericRow.of(31, BinaryString.fromString("32")),
                        GenericRow.of(31, BinaryString.fromString("33"))));
    }

    @ParameterizedTest
    @EnumSource(
            value = DataTypeRoot.class,
            names = {
                "CHAR",
                "STRING",
                "BOOLEAN",
                "BINARY",
                "BYTES",
                "DECIMAL",
                "TINYINT",
                "SMALLINT",
                "INTEGER",
                "BIGINT",
                "FLOAT",
                "DOUBLE",
                "DATE",
                "TIME_WITHOUT_TIME_ZONE",
                "TIMESTAMP_WITHOUT_TIME_ZONE",
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
                "ARRAY",
                "MAP",
                "ROW"
            })
    public void testSingleField(DataTypeRoot dataTypeRoot) {
        GeneratedData testData = TEST_DATA.get(dataTypeRoot);
        if (testData == null) {
            throw new UnsupportedOperationException("Unsupported type: " + dataTypeRoot);
        }

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {testData.dataType})
                        .generateRecordEqualiser("singleFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        // Test equal values
        assertThat(equaliser.equals(GenericRow.of(testData.left), GenericRow.of(testData.left)))
                .isTrue();

        // Test different values
        assertThat(equaliser.equals(GenericRow.of(testData.left), GenericRow.of(testData.right)))
                .isFalse();
    }

    @Test
    public void testNullValues() {
        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.INT()})
                        .generateRecordEqualiser("nullTest")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        // Both null - should be equal
        GenericRow nullRow1 = new GenericRow(1);
        nullRow1.setField(0, null);
        GenericRow nullRow2 = new GenericRow(1);
        nullRow2.setField(0, null);
        assertThat(equaliser.equals(nullRow1, nullRow2)).isTrue();

        // One null, one not - should not be equal
        assertThat(equaliser.equals(nullRow1, GenericRow.of(1))).isFalse();
        assertThat(equaliser.equals(GenericRow.of(1), nullRow1)).isFalse();
    }

    @RepeatedTest(100)
    public void testProjection() {
        GeneratedData field0 = TEST_DATA.get(DataTypeRoot.INTEGER);
        GeneratedData field1 = TEST_DATA.get(DataTypeRoot.STRING);
        GeneratedData field2 = TEST_DATA.get(DataTypeRoot.BIGINT);

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(
                                new DataType[] {field0.dataType, field1.dataType, field2.dataType},
                                new int[] {1, 2})
                        .generateRecordEqualiser("projectionFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        boolean result =
                equaliser.equals(
                        GenericRow.of(field0.left, field1.left, field2.left),
                        GenericRow.of(field0.right, field1.right, field2.right));
        boolean expected =
                Objects.equals(
                        GenericRow.of(field1.left, field2.left),
                        GenericRow.of(field1.right, field2.right));
        assertThat(result).isEqualTo(expected);
    }

    @RepeatedTest(50)
    public void testManyFields() {
        int size = 50;
        GeneratedData[] generatedData = new GeneratedData[size];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        DataTypeRoot[] supportedTypes =
                new DataTypeRoot[] {
                    DataTypeRoot.CHAR,
                    DataTypeRoot.STRING,
                    DataTypeRoot.BOOLEAN,
                    DataTypeRoot.BINARY,
                    DataTypeRoot.BYTES,
                    DataTypeRoot.DECIMAL,
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.FLOAT,
                    DataTypeRoot.DOUBLE,
                    DataTypeRoot.DATE,
                    DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                    DataTypeRoot.ARRAY,
                    DataTypeRoot.MAP,
                    DataTypeRoot.ROW
                };

        for (int i = 0; i < size; i++) {
            int index = random.nextInt(0, supportedTypes.length);
            GeneratedData testData = TEST_DATA.get(supportedTypes[index]);
            if (testData == null) {
                throw new UnsupportedOperationException(
                        "Unsupported type: " + supportedTypes[index]);
            }
            generatedData[i] = testData;
        }

        DataType[] dataTypes = new DataType[size];
        for (int i = 0; i < size; i++) {
            dataTypes[i] = generatedData[i].dataType;
        }

        final RecordEqualiser equaliser =
                new EqualiserCodeGenerator(dataTypes)
                        .generateRecordEqualiser("ManyFields")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        Object[] fields1 = new Object[size];
        Object[] fields2 = new Object[size];
        boolean equal = true;
        for (int i = 0; i < size; i++) {
            boolean randomEqual = random.nextBoolean();
            fields1[i] = generatedData[i].left;
            fields2[i] = randomEqual ? generatedData[i].left : generatedData[i].right;
            equal = equal && randomEqual;
        }
        assertThat(equaliser.equals(GenericRow.of(fields1), GenericRow.of(fields1))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(fields1), GenericRow.of(fields2)))
                .isEqualTo(equal);
    }

    @Test
    public void testNestedRow() {
        DataType nestedRowType =
                DataTypes.ROW(
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.ROW(DataTypes.BIGINT(), DataTypes.DOUBLE()));

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {nestedRowType})
                        .generateRecordEqualiser("nestedRowEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        GenericRow innerRow1 = GenericRow.of(100L, 1.5);
        GenericRow innerRow2 = GenericRow.of(100L, 1.5);
        GenericRow innerRow3 = GenericRow.of(100L, 2.5);

        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("test"), innerRow1);
        GenericRow row2 = GenericRow.of(1, BinaryString.fromString("test"), innerRow2);
        GenericRow row3 = GenericRow.of(1, BinaryString.fromString("test"), innerRow3);

        assertThat(equaliser.equals(GenericRow.of(row1), GenericRow.of(row2))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(row1), GenericRow.of(row3))).isFalse();
    }

    @Test
    public void testArrayWithDifferentSizes() {
        DataType arrayType = DataTypes.ARRAY(DataTypes.INT());

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {arrayType})
                        .generateRecordEqualiser("arraySizeEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        GenericArray arr1 = new GenericArray(new Integer[] {1, 2, 3});
        GenericArray arr2 = new GenericArray(new Integer[] {1, 2});

        assertThat(equaliser.equals(GenericRow.of(arr1), GenericRow.of(arr2))).isFalse();
    }

    @Test
    public void testMapWithDifferentValues() {
        DataType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {mapType})
                        .generateRecordEqualiser("mapValueEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        Map<BinaryString, Integer> map1 = new HashMap<BinaryString, Integer>();
        map1.put(BinaryString.fromString("a"), 1);
        map1.put(BinaryString.fromString("b"), 2);

        Map<BinaryString, Integer> map2 = new HashMap<BinaryString, Integer>();
        map2.put(BinaryString.fromString("a"), 1);
        map2.put(BinaryString.fromString("b"), 3); // different value

        assertThat(
                        equaliser.equals(
                                GenericRow.of(new GenericMap(map1)),
                                GenericRow.of(new GenericMap(map2))))
                .isFalse();
    }

    @Test
    public void testMapWithSameValues() {
        DataType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {mapType})
                        .generateRecordEqualiser("mapSameEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        Map<BinaryString, Integer> map1 = new HashMap<BinaryString, Integer>();
        map1.put(BinaryString.fromString("a"), 1);
        map1.put(BinaryString.fromString("b"), 2);

        Map<BinaryString, Integer> map2 = new HashMap<BinaryString, Integer>();
        map2.put(BinaryString.fromString("a"), 1);
        map2.put(BinaryString.fromString("b"), 2);

        assertThat(
                        equaliser.equals(
                                GenericRow.of(new GenericMap(map1)),
                                GenericRow.of(new GenericMap(map2))))
                .isTrue();
    }

    @Test
    public void testDecimalComparison() {
        DataType decimalType = DataTypes.DECIMAL(10, 2);

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {decimalType})
                        .generateRecordEqualiser("decimalEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        Decimal d1 = Decimal.fromUnscaledLong(12345, 10, 2);
        Decimal d2 = Decimal.fromUnscaledLong(12345, 10, 2);
        Decimal d3 = Decimal.fromUnscaledLong(12346, 10, 2);

        assertThat(equaliser.equals(GenericRow.of(d1), GenericRow.of(d2))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(d1), GenericRow.of(d3))).isFalse();
    }

    @Test
    public void testTimestampComparison() {
        DataType timestampType = DataTypes.TIMESTAMP(6);

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {timestampType})
                        .generateRecordEqualiser("timestampEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        TimestampNtz ts1 = TimestampNtz.fromMillis(1684814400000L);
        TimestampNtz ts2 = TimestampNtz.fromMillis(1684814400000L);
        TimestampNtz ts3 = TimestampNtz.fromMillis(1684814500000L);

        assertThat(equaliser.equals(GenericRow.of(ts1), GenericRow.of(ts2))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(ts1), GenericRow.of(ts3))).isFalse();
    }

    @Test
    public void testBinaryComparison() {
        DataType binaryType = DataTypes.BINARY(5);

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {binaryType})
                        .generateRecordEqualiser("binaryEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        byte[] b1 = new byte[] {1, 2, 3, 4, 5};
        byte[] b2 = new byte[] {1, 2, 3, 4, 5};
        byte[] b3 = new byte[] {1, 2, 3, 4, 6};

        assertThat(equaliser.equals(GenericRow.of((Object) b1), GenericRow.of((Object) b2)))
                .isTrue();
        assertThat(equaliser.equals(GenericRow.of((Object) b1), GenericRow.of((Object) b3)))
                .isFalse();
    }

    @Test
    public void testMultipleFieldTypes() {
        DataType[] types =
                new DataType[] {
                    DataTypes.INT(),
                    DataTypes.STRING(),
                    DataTypes.BOOLEAN(),
                    DataTypes.BIGINT(),
                    DataTypes.DOUBLE()
                };

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(types)
                        .generateRecordEqualiser("multiFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        InternalRow row1 = GenericRow.of(1, BinaryString.fromString("test"), true, 100L, 1.5);
        InternalRow row2 = GenericRow.of(1, BinaryString.fromString("test"), true, 100L, 1.5);
        InternalRow row3 = GenericRow.of(1, BinaryString.fromString("test"), false, 100L, 1.5);

        assertThat(equaliser.equals(row1, row2)).isTrue();
        assertThat(equaliser.equals(row1, row3)).isFalse();
    }

    /** Helper class to hold test data for each type. */
    private static class GeneratedData {
        public final DataType dataType;
        public final Object left;
        public final Object right;

        public GeneratedData(DataType dataType, Object left, Object right) {
            this.dataType = dataType;
            this.left = left;
            this.right = right;
        }
    }
}
