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

package org.apache.fluss.row;

import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ArrayWriter}. */
public class ArrayWriterTest {

    @Test
    public void testCreateValueSetterForAllTypes() {
        ArrayWriter.ValueSetter booleanSetter = ArrayWriter.createValueSetter(DataTypes.BOOLEAN());
        ArrayWriter.ValueSetter tinyintSetter = ArrayWriter.createValueSetter(DataTypes.TINYINT());
        ArrayWriter.ValueSetter smallintSetter =
                ArrayWriter.createValueSetter(DataTypes.SMALLINT());
        ArrayWriter.ValueSetter intSetter = ArrayWriter.createValueSetter(DataTypes.INT());
        ArrayWriter.ValueSetter bigintSetter = ArrayWriter.createValueSetter(DataTypes.BIGINT());
        ArrayWriter.ValueSetter floatSetter = ArrayWriter.createValueSetter(DataTypes.FLOAT());
        ArrayWriter.ValueSetter doubleSetter = ArrayWriter.createValueSetter(DataTypes.DOUBLE());
        ArrayWriter.ValueSetter stringSetter = ArrayWriter.createValueSetter(DataTypes.STRING());
        ArrayWriter.ValueSetter charSetter = ArrayWriter.createValueSetter(DataTypes.CHAR(10));
        ArrayWriter.ValueSetter binarySetter = ArrayWriter.createValueSetter(DataTypes.BINARY(10));
        ArrayWriter.ValueSetter decimalSetter =
                ArrayWriter.createValueSetter(DataTypes.DECIMAL(5, 2));
        ArrayWriter.ValueSetter timestampNtzSetter =
                ArrayWriter.createValueSetter(DataTypes.TIMESTAMP(3));
        ArrayWriter.ValueSetter timestampLtzSetter =
                ArrayWriter.createValueSetter(DataTypes.TIMESTAMP_LTZ(3));
        ArrayWriter.ValueSetter dateSetter = ArrayWriter.createValueSetter(DataTypes.DATE());
        ArrayWriter.ValueSetter timeSetter = ArrayWriter.createValueSetter(DataTypes.TIME());

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
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.BOOLEAN());
        setter.setValue(writer, 0, true);
        setter.setValue(writer, 1, false);
        writer.complete();

        assertThat(array.getBoolean(0)).isTrue();
        assertThat(array.getBoolean(1)).isFalse();
    }

    @Test
    public void testValueSetterWithIntType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.INT());
        setter.setValue(writer, 0, 100);
        setter.setValue(writer, 1, 200);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(100);
        assertThat(array.getInt(1)).isEqualTo(200);
    }

    @Test
    public void testValueSetterWithStringType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.STRING());
        setter.setValue(writer, 0, BinaryString.fromString("hello"));
        setter.setValue(writer, 1, BinaryString.fromString("world"));
        writer.complete();

        assertThat(array.getString(0)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(array.getString(1)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    public void testValueSetterWithDecimalType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.DECIMAL(5, 2));
        setter.setValue(writer, 0, Decimal.fromUnscaledLong(123, 5, 2));
        setter.setValue(writer, 1, Decimal.fromUnscaledLong(456, 5, 2));
        writer.complete();

        assertThat(array.getDecimal(0, 5, 2)).isEqualTo(Decimal.fromUnscaledLong(123, 5, 2));
        assertThat(array.getDecimal(1, 5, 2)).isEqualTo(Decimal.fromUnscaledLong(456, 5, 2));
    }

    @Test
    public void testCreateValueSetterForMapThrowsException() {
        assertThatThrownBy(
                        () ->
                                ArrayWriter.createValueSetter(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Map type is not supported yet");
    }

    @Test
    public void testCreateValueSetterForRowThrowsException() {
        assertThatThrownBy(
                        () ->
                                ArrayWriter.createValueSetter(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("a", DataTypes.INT()),
                                                DataTypes.FIELD("b", DataTypes.STRING()))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Row type is not supported yet");
    }

    @Test
    public void testDeprecatedWriteMethodForAllTypes() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 10, 8);

        ArrayWriter.write(writer, 0, true, DataTypes.BOOLEAN(), null);
        ArrayWriter.write(writer, 1, (byte) 10, DataTypes.TINYINT(), null);
        ArrayWriter.write(writer, 2, (short) 20, DataTypes.SMALLINT(), null);
        ArrayWriter.write(writer, 3, 30, DataTypes.INT(), null);
        ArrayWriter.write(writer, 4, 40L, DataTypes.BIGINT(), null);
        ArrayWriter.write(writer, 5, 1.5f, DataTypes.FLOAT(), null);
        ArrayWriter.write(writer, 6, 2.5, DataTypes.DOUBLE(), null);
        ArrayWriter.write(writer, 7, BinaryString.fromString("test"), DataTypes.STRING(), null);
        ArrayWriter.write(writer, 8, BinaryString.fromString("char"), DataTypes.CHAR(4), null);
        ArrayWriter.write(writer, 9, new byte[] {1, 2, 3}, DataTypes.BINARY(3), null);
        writer.complete();

        assertThat(array.getBoolean(0)).isTrue();
        assertThat(array.getByte(1)).isEqualTo((byte) 10);
        assertThat(array.getShort(2)).isEqualTo((short) 20);
        assertThat(array.getInt(3)).isEqualTo(30);
        assertThat(array.getLong(4)).isEqualTo(40L);
        assertThat(array.getFloat(5)).isEqualTo(1.5f);
        assertThat(array.getDouble(6)).isEqualTo(2.5);
        assertThat(array.getString(7)).isEqualTo(BinaryString.fromString("test"));
        assertThat(array.getChar(8, 4)).isEqualTo(BinaryString.fromString("char"));
        assertThat(array.getBinary(9, 3)).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    public void testDeprecatedWriteMethodForDecimal() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        Decimal decimal = Decimal.fromUnscaledLong(123, 5, 2);
        ArrayWriter.write(writer, 0, decimal, DataTypes.DECIMAL(5, 2), null);
        writer.complete();

        assertThat(array.getDecimal(0, 5, 2)).isEqualTo(decimal);
    }

    @Test
    public void testDeprecatedWriteMethodForTimestamp() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampNtz timestampNtz = TimestampNtz.fromMillis(1000L);
        TimestampLtz timestampLtz = TimestampLtz.fromEpochMillis(2000L);

        ArrayWriter.write(writer, 0, timestampNtz, DataTypes.TIMESTAMP(3), null);
        ArrayWriter.write(writer, 1, timestampLtz, DataTypes.TIMESTAMP_LTZ(3), null);
        writer.complete();

        assertThat(array.getTimestampNtz(0, 3)).isEqualTo(timestampNtz);
        assertThat(array.getTimestampLtz(1, 3)).isEqualTo(timestampLtz);
    }

    @Test
    public void testDeprecatedWriteMethodForDateAndTime() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        ArrayWriter.write(writer, 0, 18000, DataTypes.DATE(), null);
        ArrayWriter.write(writer, 1, 3600000, DataTypes.TIME(), null);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(18000);
        assertThat(array.getInt(1)).isEqualTo(3600000);
    }

    @Test
    public void testDeprecatedWriteMethodForMapThrowsException() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        assertThatThrownBy(
                        () ->
                                ArrayWriter.write(
                                        writer,
                                        0,
                                        null,
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                        null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Map type is not supported yet");
    }

    @Test
    public void testDeprecatedWriteMethodForRowThrowsException() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        assertThatThrownBy(
                        () ->
                                ArrayWriter.write(
                                        writer,
                                        0,
                                        null,
                                        DataTypes.ROW(
                                                DataTypes.FIELD("a", DataTypes.INT()),
                                                DataTypes.FIELD("b", DataTypes.STRING())),
                                        null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Row type is not supported yet");
    }

    @Test
    public void testValueSetterWithByteType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 1);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.TINYINT());
        setter.setValue(writer, 0, (byte) 10);
        setter.setValue(writer, 1, (byte) 20);
        writer.complete();

        assertThat(array.getByte(0)).isEqualTo((byte) 10);
        assertThat(array.getByte(1)).isEqualTo((byte) 20);
    }

    @Test
    public void testValueSetterWithShortType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 2);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.SMALLINT());
        setter.setValue(writer, 0, (short) 100);
        setter.setValue(writer, 1, (short) 200);
        writer.complete();

        assertThat(array.getShort(0)).isEqualTo((short) 100);
        assertThat(array.getShort(1)).isEqualTo((short) 200);
    }

    @Test
    public void testValueSetterWithLongType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.BIGINT());
        setter.setValue(writer, 0, 1000L);
        setter.setValue(writer, 1, 2000L);
        writer.complete();

        assertThat(array.getLong(0)).isEqualTo(1000L);
        assertThat(array.getLong(1)).isEqualTo(2000L);
    }

    @Test
    public void testValueSetterWithFloatType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.FLOAT());
        setter.setValue(writer, 0, 1.5f);
        setter.setValue(writer, 1, 2.5f);
        writer.complete();

        assertThat(array.getFloat(0)).isEqualTo(1.5f);
        assertThat(array.getFloat(1)).isEqualTo(2.5f);
    }

    @Test
    public void testValueSetterWithDoubleType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.DOUBLE());
        setter.setValue(writer, 0, 1.1);
        setter.setValue(writer, 1, 2.2);
        writer.complete();

        assertThat(array.getDouble(0)).isEqualTo(1.1);
        assertThat(array.getDouble(1)).isEqualTo(2.2);
    }

    @Test
    public void testValueSetterWithCharType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.CHAR(5));
        setter.setValue(writer, 0, BinaryString.fromString("hello"));
        setter.setValue(writer, 1, BinaryString.fromString("world"));
        writer.complete();

        assertThat(array.getChar(0, 5)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(array.getChar(1, 5)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    public void testValueSetterWithBinaryType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.BINARY(3));
        setter.setValue(writer, 0, new byte[] {1, 2, 3});
        setter.setValue(writer, 1, new byte[] {4, 5, 6});
        writer.complete();

        assertThat(array.getBinary(0, 3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(array.getBinary(1, 3)).isEqualTo(new byte[] {4, 5, 6});
    }

    @Test
    public void testValueSetterWithTimestampNtzType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.TIMESTAMP(3));
        setter.setValue(writer, 0, TimestampNtz.fromMillis(1000L));
        setter.setValue(writer, 1, TimestampNtz.fromMillis(2000L));
        writer.complete();

        assertThat(array.getTimestampNtz(0, 3)).isEqualTo(TimestampNtz.fromMillis(1000L));
        assertThat(array.getTimestampNtz(1, 3)).isEqualTo(TimestampNtz.fromMillis(2000L));
    }

    @Test
    public void testValueSetterWithTimestampLtzType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.TIMESTAMP_LTZ(3));
        setter.setValue(writer, 0, TimestampLtz.fromEpochMillis(1000L));
        setter.setValue(writer, 1, TimestampLtz.fromEpochMillis(2000L));
        writer.complete();

        assertThat(array.getTimestampLtz(0, 3)).isEqualTo(TimestampLtz.fromEpochMillis(1000L));
        assertThat(array.getTimestampLtz(1, 3)).isEqualTo(TimestampLtz.fromEpochMillis(2000L));
    }

    @Test
    public void testValueSetterWithDateType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.DATE());
        setter.setValue(writer, 0, 18000);
        setter.setValue(writer, 1, 18001);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(18000);
        assertThat(array.getInt(1)).isEqualTo(18001);
    }

    @Test
    public void testValueSetterWithTimeType() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        ArrayWriter.ValueSetter setter = ArrayWriter.createValueSetter(DataTypes.TIME());
        setter.setValue(writer, 0, 3600000);
        setter.setValue(writer, 1, 7200000);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(3600000);
        assertThat(array.getInt(1)).isEqualTo(7200000);
    }
}
