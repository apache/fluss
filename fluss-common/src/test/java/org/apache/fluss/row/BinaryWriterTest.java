/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.row.array.PrimitiveBinaryArray;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinaryWriter}. */
public class BinaryWriterTest {

    @Test
    public void testCreateValueSetterForAllTypes() {
        BinaryWriter.ValueWriter booleanSetter = createPrimitiveValueWriter(DataTypes.BOOLEAN());
        BinaryWriter.ValueWriter tinyintSetter = createPrimitiveValueWriter(DataTypes.TINYINT());
        BinaryWriter.ValueWriter smallintSetter = createPrimitiveValueWriter(DataTypes.SMALLINT());
        BinaryWriter.ValueWriter intSetter = createPrimitiveValueWriter(DataTypes.INT());
        BinaryWriter.ValueWriter bigintSetter = createPrimitiveValueWriter(DataTypes.BIGINT());
        BinaryWriter.ValueWriter floatSetter = createPrimitiveValueWriter(DataTypes.FLOAT());
        BinaryWriter.ValueWriter doubleSetter = createPrimitiveValueWriter(DataTypes.DOUBLE());
        BinaryWriter.ValueWriter stringSetter = createPrimitiveValueWriter(DataTypes.STRING());
        BinaryWriter.ValueWriter charSetter = createPrimitiveValueWriter(DataTypes.CHAR(10));
        BinaryWriter.ValueWriter binarySetter = createPrimitiveValueWriter(DataTypes.BINARY(10));
        BinaryWriter.ValueWriter decimalSetter =
                createPrimitiveValueWriter(DataTypes.DECIMAL(5, 2));
        BinaryWriter.ValueWriter timestampNtzSetter =
                createPrimitiveValueWriter(DataTypes.TIMESTAMP(3));
        BinaryWriter.ValueWriter timestampLtzSetter =
                createPrimitiveValueWriter(DataTypes.TIMESTAMP_LTZ(3));
        BinaryWriter.ValueWriter dateSetter = createPrimitiveValueWriter(DataTypes.DATE());
        BinaryWriter.ValueWriter timeSetter = createPrimitiveValueWriter(DataTypes.TIME());

        assertThat(booleanSetter).isNotNull();
        assertThat(tinyintSetter).isNotNull();
        assertThat(smallintSetter).isNotNull();
        assertThat(intSetter).isNotNull();
        assertThat(bigintSetter).isNotNull();
        assertThat(floatSetter).isNotNull();
        assertThat(doubleSetter).isNotNull();
        assertThat(stringSetter).isNotNull();
        assertThat(charSetter).isNotNull();
        assertThat(binarySetter).isNotNull();
        assertThat(decimalSetter).isNotNull();
        assertThat(timestampNtzSetter).isNotNull();
        assertThat(timestampLtzSetter).isNotNull();
        assertThat(dateSetter).isNotNull();
        assertThat(timeSetter).isNotNull();
    }

    @Test
    public void testValueSetterWithBooleanType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.BOOLEAN());
        setter.writeValue(writer, 0, true);
        setter.writeValue(writer, 1, false);
        writer.complete();

        assertThat(array.getBoolean(0)).isTrue();
        assertThat(array.getBoolean(1)).isFalse();
    }

    @Test
    public void testValueSetterWithIntType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.INT());
        setter.writeValue(writer, 0, 100);
        setter.writeValue(writer, 1, 200);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(100);
        assertThat(array.getInt(1)).isEqualTo(200);
    }

    @Test
    public void testValueSetterWithStringType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.STRING());
        setter.writeValue(writer, 0, BinaryString.fromString("hello"));
        setter.writeValue(writer, 1, BinaryString.fromString("world"));
        writer.complete();

        assertThat(array.getString(0)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(array.getString(1)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    public void testValueSetterWithDecimalType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.DECIMAL(5, 2));
        setter.writeValue(writer, 0, Decimal.fromUnscaledLong(123, 5, 2));
        setter.writeValue(writer, 1, Decimal.fromUnscaledLong(456, 5, 2));
        writer.complete();

        assertThat(array.getDecimal(0, 5, 2)).isEqualTo(Decimal.fromUnscaledLong(123, 5, 2));
        assertThat(array.getDecimal(1, 5, 2)).isEqualTo(Decimal.fromUnscaledLong(456, 5, 2));
    }

    @Test
    public void testValueSetterWithMapType() {
        BinaryArray array = new org.apache.fluss.row.array.PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter =
                BinaryWriter.createValueWriter(
                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                        BinaryRow.BinaryRowFormat.COMPACTED);

        GenericMap map1 =
                GenericMap.of(1, BinaryString.fromString("one"), 2, BinaryString.fromString("two"));
        GenericMap map2 =
                GenericMap.of(
                        3, BinaryString.fromString("three"), 4, BinaryString.fromString("four"));

        setter.writeValue(writer, 0, map1);
        setter.writeValue(writer, 1, map2);
        writer.complete();

        assertThat(array.getMap(0).size()).isEqualTo(2);
        assertThat(array.getMap(1).size()).isEqualTo(2);

        // Assert keys and values for map1
        InternalArray keys1 = array.getMap(0).keyArray();
        InternalArray values1 = array.getMap(0).valueArray();
        assertThat(keys1.getInt(0)).isEqualTo(1);
        assertThat(keys1.getInt(1)).isEqualTo(2);
        assertThat(values1.getString(0)).isEqualTo(BinaryString.fromString("one"));
        assertThat(values1.getString(1)).isEqualTo(BinaryString.fromString("two"));

        // Assert keys and values for map2
        InternalArray keys2 = array.getMap(1).keyArray();
        InternalArray values2 = array.getMap(1).valueArray();
        assertThat(keys2.getInt(0)).isEqualTo(3);
        assertThat(keys2.getInt(1)).isEqualTo(4);
        assertThat(values2.getString(0)).isEqualTo(BinaryString.fromString("three"));
        assertThat(values2.getString(1)).isEqualTo(BinaryString.fromString("four"));
    }

    @Test
    public void testValueSetterWithByteType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.TINYINT());
        setter.writeValue(writer, 0, (byte) 10);
        setter.writeValue(writer, 1, (byte) 20);
        writer.complete();

        assertThat(array.getByte(0)).isEqualTo((byte) 10);
        assertThat(array.getByte(1)).isEqualTo((byte) 20);
    }

    @Test
    public void testValueSetterWithShortType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 2);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.SMALLINT());
        setter.writeValue(writer, 0, (short) 100);
        setter.writeValue(writer, 1, (short) 200);
        writer.complete();

        assertThat(array.getShort(0)).isEqualTo((short) 100);
        assertThat(array.getShort(1)).isEqualTo((short) 200);
    }

    @Test
    public void testValueSetterWithLongType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.BIGINT());
        setter.writeValue(writer, 0, 1000L);
        setter.writeValue(writer, 1, 2000L);
        writer.complete();

        assertThat(array.getLong(0)).isEqualTo(1000L);
        assertThat(array.getLong(1)).isEqualTo(2000L);
    }

    @Test
    public void testValueSetterWithFloatType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.FLOAT());
        setter.writeValue(writer, 0, 1.5f);
        setter.writeValue(writer, 1, 2.5f);
        writer.complete();

        assertThat(array.getFloat(0)).isEqualTo(1.5f);
        assertThat(array.getFloat(1)).isEqualTo(2.5f);
    }

    @Test
    public void testValueSetterWithDoubleType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.DOUBLE());
        setter.writeValue(writer, 0, 1.1);
        setter.writeValue(writer, 1, 2.2);
        writer.complete();

        assertThat(array.getDouble(0)).isEqualTo(1.1);
        assertThat(array.getDouble(1)).isEqualTo(2.2);
    }

    @Test
    public void testValueSetterWithCharType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.CHAR(5));
        setter.writeValue(writer, 0, BinaryString.fromString("hello"));
        setter.writeValue(writer, 1, BinaryString.fromString("world"));
        writer.complete();

        assertThat(array.getChar(0, 5)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(array.getChar(1, 5)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    public void testValueSetterWithBinaryType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.BINARY(3));
        setter.writeValue(writer, 0, new byte[] {1, 2, 3});
        setter.writeValue(writer, 1, new byte[] {4, 5, 6});
        writer.complete();

        assertThat(array.getBinary(0, 3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(array.getBinary(1, 3)).isEqualTo(new byte[] {4, 5, 6});
    }

    @Test
    public void testValueSetterWithTimestampNtzType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.TIMESTAMP(3));
        setter.writeValue(writer, 0, TimestampNtz.fromMillis(1000L));
        setter.writeValue(writer, 1, TimestampNtz.fromMillis(2000L));
        writer.complete();

        assertThat(array.getTimestampNtz(0, 3)).isEqualTo(TimestampNtz.fromMillis(1000L));
        assertThat(array.getTimestampNtz(1, 3)).isEqualTo(TimestampNtz.fromMillis(2000L));
    }

    @Test
    public void testValueSetterWithTimestampLtzType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.TIMESTAMP_LTZ(3));
        setter.writeValue(writer, 0, TimestampLtz.fromEpochMillis(1000L));
        setter.writeValue(writer, 1, TimestampLtz.fromEpochMillis(2000L));
        writer.complete();

        assertThat(array.getTimestampLtz(0, 3)).isEqualTo(TimestampLtz.fromEpochMillis(1000L));
        assertThat(array.getTimestampLtz(1, 3)).isEqualTo(TimestampLtz.fromEpochMillis(2000L));
    }

    @Test
    public void testValueSetterWithDateType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.DATE());
        setter.writeValue(writer, 0, 18000);
        setter.writeValue(writer, 1, 18001);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(18000);
        assertThat(array.getInt(1)).isEqualTo(18001);
    }

    @Test
    public void testValueSetterWithTimeType() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        BinaryWriter.ValueWriter setter = createPrimitiveValueWriter(DataTypes.TIME());
        setter.writeValue(writer, 0, 3600000);
        setter.writeValue(writer, 1, 7200000);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(3600000);
        assertThat(array.getInt(1)).isEqualTo(7200000);
    }

    private static BinaryWriter.ValueWriter createPrimitiveValueWriter(DataType elementType) {
        // use null for row format if there is no nested row type
        return BinaryWriter.createValueWriter(elementType, null);
    }
}
