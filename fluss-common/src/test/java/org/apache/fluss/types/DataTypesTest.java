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

package org.apache.fluss.types;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@link org.apache.fluss.types.DataType} and the {@link
 * org.apache.fluss.types.DataTypes} class.
 */
public class DataTypesTest {

    @Test
    void testTypeRoot() {
        CharType charType = new CharType();
        assertThat(charType.is(DataTypeRoot.CHAR)).isTrue();
        assertThat(charType.is(DataTypeRoot.BOOLEAN)).isFalse();
        assertThat(charType.isAnyOf(DataTypeRoot.CHAR)).isTrue();
        assertThat(charType.isAnyOf(DataTypeRoot.BOOLEAN)).isFalse();

        assertThat(charType.is(DataTypeFamily.PREDEFINED)).isTrue();
        assertThat(charType.is(DataTypeFamily.EXACT_NUMERIC)).isFalse();
        assertThat(charType.isAnyOf(DataTypeFamily.PREDEFINED)).isTrue();
        assertThat(charType.isAnyOf(DataTypeFamily.EXACT_NUMERIC)).isFalse();
    }

    @Test
    void testCharType() {
        CharType charType = new CharType();
        assertThat(charType.getLength()).isEqualTo(1);
        dataTypeBaseAssert(charType, true, "CHAR(1)", new CharType(55));

        charType = new CharType(44);
        assertThat(charType.getLength()).isEqualTo(44);
        dataTypeBaseAssert(charType, true, "CHAR(44)", new CharType(55));

        charType = new CharType(false, 55);
        assertThat(charType.getLength()).isEqualTo(55);
        dataTypeBaseAssert(charType, false, "CHAR(55) NOT NULL", new CharType(1));

        charType = DataTypes.CHAR(66);
        assertThat(charType.getLength()).isEqualTo(66);
        dataTypeBaseAssert(charType, true, "CHAR(66)", new CharType(1));

        assertThatThrownBy(() -> new CharType(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Character string length must be between 1 and 2147483647 (both inclusive)");

        assertThatThrownBy(() -> new CharType(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Character string length must be between 1 and 2147483647 (both inclusive)");

        // test get children.
        assertThat(charType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(charType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED, DataTypeFamily.CHARACTER_STRING);
    }

    @Test
    void testStringType() {
        StringType stringType = new StringType();
        dataTypeBaseAssert(stringType, true, "STRING", new StringType(false));

        stringType = new StringType(false);
        dataTypeBaseAssert(stringType, false, "STRING NOT NULL", new StringType(true));

        stringType = DataTypes.STRING();
        dataTypeBaseAssert(stringType, true, "STRING", new StringType(false));

        // test get children.
        assertThat(stringType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(stringType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED, DataTypeFamily.CHARACTER_STRING);
    }

    @Test
    void testBooleanType() {
        BooleanType booleanType = new BooleanType();
        dataTypeBaseAssert(booleanType, true, "BOOLEAN", new BooleanType(false));

        booleanType = new BooleanType(false);
        dataTypeBaseAssert(booleanType, false, "BOOLEAN NOT NULL", new BooleanType(true));

        booleanType = DataTypes.BOOLEAN();
        dataTypeBaseAssert(booleanType, true, "BOOLEAN", new BooleanType(false));

        // test get children.
        assertThat(booleanType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(booleanType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(DataTypeFamily.PREDEFINED);
    }

    @Test
    void testBinaryType() {
        BinaryType binaryType = new BinaryType();
        assertThat(binaryType.getLength()).isEqualTo(1);
        dataTypeBaseAssert(binaryType, true, "BINARY(1)", new BinaryType(55));

        binaryType = new BinaryType(33);
        assertThat(binaryType.getLength()).isEqualTo(33);
        dataTypeBaseAssert(binaryType, true, "BINARY(33)", new BinaryType(55));

        binaryType = new BinaryType(false, 44);
        assertThat(binaryType.getLength()).isEqualTo(44);
        dataTypeBaseAssert(binaryType, false, "BINARY(44) NOT NULL", new BinaryType(55));

        binaryType = DataTypes.BINARY(55);
        assertThat(binaryType.getLength()).isEqualTo(55);
        dataTypeBaseAssert(binaryType, true, "BINARY(55)", new BinaryType(1));

        assertThatThrownBy(() -> new BinaryType(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Binary string length must be between 1 and 2147483647 (both inclusive)");

        assertThatThrownBy(() -> new BinaryType(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Binary string length must be between 1 and 2147483647 (both inclusive)");

        // test get children.
        assertThat(binaryType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(binaryType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(DataTypeFamily.PREDEFINED, DataTypeFamily.BINARY_STRING);
    }

    @Test
    void testBytesType() {
        BytesType bytesType = new BytesType();
        dataTypeBaseAssert(bytesType, true, "BYTES", new BytesType(false));

        bytesType = new BytesType(false);
        dataTypeBaseAssert(bytesType, false, "BYTES NOT NULL", new BytesType(true));

        bytesType = DataTypes.BYTES();
        dataTypeBaseAssert(bytesType, true, "BYTES", new BytesType(false));

        // test get children.
        assertThat(bytesType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(bytesType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(DataTypeFamily.PREDEFINED, DataTypeFamily.BINARY_STRING);
    }

    @Test
    void testDecimalType() {
        DecimalType decimalType = new DecimalType();
        assertThat(decimalType.getPrecision()).isEqualTo(10);
        assertThat(decimalType.getScale()).isEqualTo(0);
        dataTypeBaseAssert(decimalType, true, "DECIMAL(10, 0)", new DecimalType(10, 10));

        decimalType = new DecimalType(5);
        assertThat(decimalType.getPrecision()).isEqualTo(5);
        assertThat(decimalType.getScale()).isEqualTo(0);
        dataTypeBaseAssert(decimalType, true, "DECIMAL(5, 0)", new DecimalType(10, 10));

        decimalType = new DecimalType(5, 5);
        assertThat(decimalType.getPrecision()).isEqualTo(5);
        assertThat(decimalType.getScale()).isEqualTo(5);
        dataTypeBaseAssert(decimalType, true, "DECIMAL(5, 5)", new DecimalType(10, 10));

        decimalType = new DecimalType(false, 5, 5);
        assertThat(decimalType.getPrecision()).isEqualTo(5);
        assertThat(decimalType.getScale()).isEqualTo(5);
        dataTypeBaseAssert(decimalType, false, "DECIMAL(5, 5) NOT NULL", new DecimalType(10, 10));

        decimalType = DataTypes.DECIMAL(5, 5);
        assertThat(decimalType.getPrecision()).isEqualTo(5);
        assertThat(decimalType.getScale()).isEqualTo(5);
        dataTypeBaseAssert(decimalType, true, "DECIMAL(5, 5)", new DecimalType(10, 10));

        assertThatThrownBy(() -> new DecimalType(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Decimal precision must be between 1 and 38 (both inclusive)");

        assertThatThrownBy(() -> new DecimalType(40))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Decimal precision must be between 1 and 38 (both inclusive)");

        assertThatThrownBy(() -> new DecimalType(1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Decimal scale must be between 0 and the precision 1 (both inclusive)");

        // test get children.
        assertThat(decimalType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(decimalType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.NUMERIC,
                        DataTypeFamily.EXACT_NUMERIC);
    }

    @Test
    void testTinyIntType() {
        TinyIntType tinyIntType = new TinyIntType();
        dataTypeBaseAssert(tinyIntType, true, "TINYINT", new TinyIntType(false));

        tinyIntType = new TinyIntType(false);
        dataTypeBaseAssert(tinyIntType, false, "TINYINT NOT NULL", new TinyIntType());

        tinyIntType = DataTypes.TINYINT();
        dataTypeBaseAssert(tinyIntType, true, "TINYINT", new TinyIntType(false));

        // test get children.
        assertThat(tinyIntType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(tinyIntType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.NUMERIC,
                        DataTypeFamily.INTEGER_NUMERIC,
                        DataTypeFamily.EXACT_NUMERIC);
    }

    @Test
    void testSmallIntType() {
        SmallIntType smallIntType = new SmallIntType();
        dataTypeBaseAssert(smallIntType, true, "SMALLINT", new SmallIntType(false));

        smallIntType = new SmallIntType(false);
        dataTypeBaseAssert(smallIntType, false, "SMALLINT NOT NULL", new SmallIntType());

        smallIntType = DataTypes.SMALLINT();
        dataTypeBaseAssert(smallIntType, true, "SMALLINT", new SmallIntType(false));

        // test get children.
        assertThat(smallIntType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(smallIntType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.NUMERIC,
                        DataTypeFamily.INTEGER_NUMERIC,
                        DataTypeFamily.EXACT_NUMERIC);
    }

    @Test
    void testIntType() {
        IntType intType = new IntType();
        dataTypeBaseAssert(intType, true, "INT", new IntType(false));

        intType = new IntType(false);
        dataTypeBaseAssert(intType, false, "INT NOT NULL", new IntType());

        intType = DataTypes.INT();
        dataTypeBaseAssert(intType, true, "INT", new IntType(false));

        // test get children.
        assertThat(intType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(intType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.NUMERIC,
                        DataTypeFamily.INTEGER_NUMERIC,
                        DataTypeFamily.EXACT_NUMERIC);
    }

    @Test
    void testBigIntType() {
        BigIntType bigIntType = new BigIntType();
        dataTypeBaseAssert(bigIntType, true, "BIGINT", new IntType(false));

        bigIntType = new BigIntType(false);
        dataTypeBaseAssert(bigIntType, false, "BIGINT NOT NULL", new IntType());

        bigIntType = DataTypes.BIGINT();
        dataTypeBaseAssert(bigIntType, true, "BIGINT", new IntType(false));

        // test get children.
        assertThat(bigIntType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(bigIntType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.NUMERIC,
                        DataTypeFamily.INTEGER_NUMERIC,
                        DataTypeFamily.EXACT_NUMERIC);
    }

    @Test
    void testFloatType() {
        FloatType floatType = new FloatType();
        dataTypeBaseAssert(floatType, true, "FLOAT", new FloatType(false));

        floatType = new FloatType(false);
        dataTypeBaseAssert(floatType, false, "FLOAT NOT NULL", new FloatType());

        floatType = DataTypes.FLOAT();
        dataTypeBaseAssert(floatType, true, "FLOAT", new FloatType(false));

        // test get children.
        assertThat(floatType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(floatType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.NUMERIC,
                        DataTypeFamily.APPROXIMATE_NUMERIC);
    }

    @Test
    void testDoubleType() {
        DoubleType doubleType = new DoubleType();
        dataTypeBaseAssert(doubleType, true, "DOUBLE", new DoubleType(false));

        doubleType = new DoubleType(false);
        dataTypeBaseAssert(doubleType, false, "DOUBLE NOT NULL", new DoubleType());

        doubleType = DataTypes.DOUBLE();
        dataTypeBaseAssert(doubleType, true, "DOUBLE", new DoubleType(false));

        // test get children.
        assertThat(doubleType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(doubleType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.NUMERIC,
                        DataTypeFamily.APPROXIMATE_NUMERIC);
    }

    @Test
    void testDateType() {
        DateType dateType = new DateType();
        dataTypeBaseAssert(dateType, true, "DATE", new DateType(false));

        dateType = new DateType(false);
        dataTypeBaseAssert(dateType, false, "DATE NOT NULL", new DateType());

        dateType = DataTypes.DATE();
        dataTypeBaseAssert(dateType, true, "DATE", new DateType(false));

        // test get children.
        assertThat(dateType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(dateType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME);
    }

    @Test
    void testTimeType() {
        TimeType timeType = new TimeType();
        assertThat(timeType.getPrecision()).isEqualTo(0);
        dataTypeBaseAssert(timeType, true, "TIME(0)", new TimeType(5));

        timeType = new TimeType(1);
        assertThat(timeType.getPrecision()).isEqualTo(1);
        dataTypeBaseAssert(timeType, true, "TIME(1)", new TimeType(5));

        timeType = new TimeType(false, 2);
        assertThat(timeType.getPrecision()).isEqualTo(2);
        dataTypeBaseAssert(timeType, false, "TIME(2) NOT NULL", new TimeType(5));

        timeType = DataTypes.TIME();
        assertThat(timeType.getPrecision()).isEqualTo(0);
        dataTypeBaseAssert(timeType, true, "TIME(0)", new TimeType(5));

        timeType = DataTypes.TIME(1);
        assertThat(timeType.getPrecision()).isEqualTo(1);
        dataTypeBaseAssert(timeType, true, "TIME(1)", new TimeType(5));

        assertThatThrownBy(() -> new TimeType(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Time precision must be between 0 and 9 (both inclusive)");

        assertThatThrownBy(() -> new TimeType(15))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Time precision must be between 0 and 9 (both inclusive)");

        // test get children.
        assertThat(timeType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(timeType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME, DataTypeFamily.TIME);
    }

    @Test
    void testTimestampType() {
        TimestampType timestampType = new TimestampType();
        assertThat(timestampType.getPrecision()).isEqualTo(6);
        dataTypeBaseAssert(timestampType, true, "TIMESTAMP(6)", new TimestampType(1));

        timestampType = new TimestampType(3);
        assertThat(timestampType.getPrecision()).isEqualTo(3);
        dataTypeBaseAssert(timestampType, true, "TIMESTAMP(3)", new TimestampType(1));

        timestampType = new TimestampType(false, 4);
        assertThat(timestampType.getPrecision()).isEqualTo(4);
        dataTypeBaseAssert(timestampType, false, "TIMESTAMP(4) NOT NULL", new TimestampType(1));

        timestampType = DataTypes.TIMESTAMP();
        assertThat(timestampType.getPrecision()).isEqualTo(6);
        dataTypeBaseAssert(timestampType, true, "TIMESTAMP(6)", new TimestampType(1));

        timestampType = DataTypes.TIMESTAMP(3);
        assertThat(timestampType.getPrecision()).isEqualTo(3);
        dataTypeBaseAssert(timestampType, true, "TIMESTAMP(3)", new TimestampType(1));

        assertThatThrownBy(() -> new TimestampType(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Timestamp precision must be between 0 and 9 (both inclusive)");

        assertThatThrownBy(() -> new TimestampType(15))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Timestamp precision must be between 0 and 9 (both inclusive)");

        // test get children.
        assertThat(timestampType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(timestampType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.DATETIME,
                        DataTypeFamily.TIMESTAMP);
    }

    @Test
    void testLocalZonedTimestampType() {
        LocalZonedTimestampType localZonedTimestampType = new LocalZonedTimestampType();
        assertThat(localZonedTimestampType.getPrecision()).isEqualTo(6);
        dataTypeBaseAssert(
                localZonedTimestampType,
                true,
                "TIMESTAMP(6) WITH LOCAL TIME ZONE",
                new LocalZonedTimestampType(1));

        localZonedTimestampType = new LocalZonedTimestampType(3);
        assertThat(localZonedTimestampType.getPrecision()).isEqualTo(3);
        dataTypeBaseAssert(
                localZonedTimestampType,
                true,
                "TIMESTAMP(3) WITH LOCAL TIME ZONE",
                new LocalZonedTimestampType(1));

        localZonedTimestampType = new LocalZonedTimestampType(false, 4);
        assertThat(localZonedTimestampType.getPrecision()).isEqualTo(4);
        dataTypeBaseAssert(
                localZonedTimestampType,
                false,
                "TIMESTAMP(4) WITH LOCAL TIME ZONE NOT NULL",
                new LocalZonedTimestampType(1));

        localZonedTimestampType = DataTypes.TIMESTAMP_LTZ();
        assertThat(localZonedTimestampType.getPrecision()).isEqualTo(6);
        dataTypeBaseAssert(
                localZonedTimestampType,
                true,
                "TIMESTAMP(6) WITH LOCAL TIME ZONE",
                new LocalZonedTimestampType(1));

        localZonedTimestampType = DataTypes.TIMESTAMP_LTZ(3);
        assertThat(localZonedTimestampType.getPrecision()).isEqualTo(3);
        dataTypeBaseAssert(
                localZonedTimestampType,
                true,
                "TIMESTAMP(3) WITH LOCAL TIME ZONE",
                new LocalZonedTimestampType(1));

        assertThatThrownBy(() -> new LocalZonedTimestampType(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Timestamp with local time zone precision must be between 0 and 9 (both inclusive)");

        assertThatThrownBy(() -> new LocalZonedTimestampType(15))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Timestamp with local time zone precision must be between 0 and 9 (both inclusive)");

        // test get children.
        assertThat(localZonedTimestampType.getChildren().size()).isEqualTo(0);
        // test getTypeRoot.
        assertThat(localZonedTimestampType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(
                        DataTypeFamily.PREDEFINED,
                        DataTypeFamily.DATETIME,
                        DataTypeFamily.TIMESTAMP,
                        DataTypeFamily.EXTENSION);
    }

    @Test
    void testArrayType() {
        ArrayType arrayType = new ArrayType(new CharType(5));
        dataTypeBaseAssert(arrayType.getElementType(), true, "CHAR(5)", new CharType(1));
        dataTypeBaseAssert(arrayType, true, "ARRAY<CHAR(5)>", new ArrayType(new CharType(1)));

        arrayType = new ArrayType(false, new CharType(5));
        dataTypeBaseAssert(arrayType.getElementType(), true, "CHAR(5)", new CharType(1));
        dataTypeBaseAssert(
                arrayType, false, "ARRAY<CHAR(5)> NOT NULL", new ArrayType(new CharType(1)));

        arrayType = new ArrayType(new ArrayType(new CharType(5)));
        dataTypeBaseAssert(
                arrayType,
                true,
                "ARRAY<ARRAY<CHAR(5)>>",
                new ArrayType(new ArrayType(new CharType(1))));

        arrayType = DataTypes.ARRAY(new CharType(5));
        dataTypeBaseAssert(arrayType, true, "ARRAY<CHAR(5)>", new ArrayType(new CharType(1)));

        assertThatThrownBy(() -> new ArrayType(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Element type must not be null");

        // test get children.
        assertThat(arrayType.getChildren().size()).isEqualTo(1);
        assertThat(arrayType.getChildren().get(0)).isEqualTo(new CharType(5));
        // test getTypeRoot.
        assertThat(arrayType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(DataTypeFamily.CONSTRUCTED, DataTypeFamily.COLLECTION);
    }

    @Test
    void testMapType() {
        MapType mapType = new MapType(new IntType(), new CharType(5));
        dataTypeBaseAssert(mapType.getKeyType(), true, "INT", new IntType(false));
        dataTypeBaseAssert(mapType.getValueType(), true, "CHAR(5)", new CharType(1));
        dataTypeBaseAssert(
                mapType, true, "MAP<INT, CHAR(5)>", new MapType(new IntType(), new CharType(1)));

        mapType = DataTypes.MAP(new IntType(), new CharType(5));
        dataTypeBaseAssert(
                mapType, true, "MAP<INT, CHAR(5)>", new MapType(new IntType(), new CharType(1)));

        assertThatThrownBy(() -> new MapType(null, new CharType(5)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Key type must not be null");

        assertThatThrownBy(() -> new MapType(new IntType(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Value type must not be null");

        // test get children.
        assertThat(mapType.getChildren().size()).isEqualTo(2);
        assertThat(mapType.getChildren()).containsExactly(new IntType(), new CharType(5));
        // test getTypeRoot.
        assertThat(mapType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(DataTypeFamily.CONSTRUCTED, DataTypeFamily.EXTENSION);
    }

    @Test
    void testRowType() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField("a", new IntType(), "column a"),
                                new DataField("b", new CharType(5), "column b")));
        assertThat(rowType.getFieldCount()).isEqualTo(2);
        assertThat(rowType.getFields().size()).isEqualTo(2);
        assertThat(rowType.getFields())
                .containsExactlyInAnyOrder(
                        new DataField("a", new IntType(), "column a"),
                        new DataField("b", new CharType(5), "column b"));
        assertThat(rowType.getFieldNames()).containsExactlyInAnyOrder("a", "b");
        assertThat(rowType.getTypeAt(0)).isInstanceOf(IntType.class);
        assertThat(rowType.getTypeAt(1)).isInstanceOf(CharType.class);
        assertThat(rowType.getFieldIndex("a")).isEqualTo(0);
        dataTypeBaseAssert(
                rowType,
                true,
                "ROW<`a` INT 'column a', `b` CHAR(5) 'column b'>",
                new RowType(
                        Arrays.asList(
                                new DataField("a1", new IntType(), "column a1"),
                                new DataField("b", new CharType(5), "column b"))));

        rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new DataField("a", new IntType(), "column a"),
                                new DataField("b", new CharType(5), "column b")));
        dataTypeBaseAssert(
                rowType,
                false,
                "ROW<`a` INT 'column a', `b` CHAR(5) 'column b'> NOT NULL",
                new RowType(
                        Arrays.asList(
                                new DataField("a", new IntType(), "column a"),
                                new DataField("b", new CharType(5), "column b"))));

        rowType =
                DataTypes.ROW(
                        new DataField("a", new IntType(), "column a"),
                        new DataField("b", new CharType(5), "column b"));
        dataTypeBaseAssert(
                rowType,
                true,
                "ROW<`a` INT 'column a', `b` CHAR(5) 'column b'>",
                new RowType(
                        Arrays.asList(
                                new DataField("a1", new IntType(), "column a1"),
                                new DataField("b", new CharType(5), "column b"))));

        rowType = DataTypes.ROW(new IntType(), new CharType(5));
        dataTypeBaseAssert(
                rowType,
                true,
                "ROW<`f0` INT, `f1` CHAR(5)>",
                DataTypes.ROW(new IntType(false), new CharType(5)));

        rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("a", new IntType(), "column a"),
                        DataTypes.FIELD("b", new CharType(5)));
        dataTypeBaseAssert(
                rowType,
                true,
                "ROW<`a` INT 'column a', `b` CHAR(5)>",
                new RowType(
                        Arrays.asList(
                                new DataField("a1", new IntType(), "column a1"),
                                new DataField("b", new CharType(5)))));

        rowType = RowType.of(DataTypes.CHAR(1), DataTypes.CHAR(2));
        dataTypeBaseAssert(
                rowType,
                true,
                "ROW<`f0` CHAR(1), `f1` CHAR(2)>",
                RowType.of(DataTypes.CHAR(2), DataTypes.CHAR(2)));

        rowType = RowType.of(false, DataTypes.CHAR(1), DataTypes.CHAR(2));
        dataTypeBaseAssert(
                rowType,
                false,
                "ROW<`f0` CHAR(1), `f1` CHAR(2)> NOT NULL",
                RowType.of(DataTypes.CHAR(2), DataTypes.CHAR(2)));

        rowType =
                RowType.builder()
                        .fields(Arrays.asList(DataTypes.CHAR(1), DataTypes.CHAR(2)))
                        .fields(
                                new DataType[] {DataTypes.CHAR(3), DataTypes.CHAR(4)},
                                new String[] {"c", "d"})
                        .field("e", DataTypes.CHAR(5))
                        .build();
        dataTypeBaseAssert(
                rowType,
                true,
                "ROW<`f0` CHAR(1), `f1` CHAR(2), `c` CHAR(3), `d` CHAR(4), `e` CHAR(5)>",
                RowType.of(DataTypes.CHAR(2)));

        rowType = DataTypes.ROW(new IntType(), new CharType(5));
        assertThat(rowType.getChildren().size()).isEqualTo(2);
        assertThat(rowType.getChildren()).containsExactly(new IntType(), new CharType(5));
        // test getTypeRoot.
        assertThat(rowType.getTypeRoot().getFamilies())
                .containsExactlyInAnyOrder(DataTypeFamily.CONSTRUCTED);
    }

    private static void dataTypeBaseAssert(
            DataType dataType,
            boolean isNullable,
            String serializableString,
            DataType otherDataType) {
        assertThat(dataType.isNullable()).isEqualTo(isNullable);
        assertThat(dataType.asSerializableString()).isEqualTo(serializableString);
        assertThat(dataType).isNotEqualTo(otherDataType);
        assertThat(dataType.copy()).isEqualTo(dataType);
        assertThat(dataType.copy(true).isNullable()).isTrue();
    }
}
