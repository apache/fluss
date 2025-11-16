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

/** Test for {@link InternalArray}. */
public class InternalArrayTest {

    @Test
    public void testElementGetterForChar() {
        Object[] array = {BinaryString.fromString("hello"), BinaryString.fromString("world"), null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.CHAR(10), 0);
        assertThat(getter.getElementOrNull(genericArray))
                .isEqualTo(BinaryString.fromString("hello"));

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.CHAR(10), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForString() {
        Object[] array = {BinaryString.fromString("test1"), BinaryString.fromString("test2"), null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.STRING(), 0);
        assertThat(getter.getElementOrNull(genericArray))
                .isEqualTo(BinaryString.fromString("test1"));

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.STRING(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForBoolean() {
        Object[] array = {true, false, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.BOOLEAN(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(true);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.BOOLEAN(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForBinary() {
        Object[] array = {new byte[] {1, 2}, new byte[] {3, 4}, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.BINARY(2), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(new byte[] {1, 2});

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.BINARY(2), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForDecimal() {
        Object[] array = {
            Decimal.fromUnscaledLong(123, 5, 2), Decimal.fromUnscaledLong(456, 5, 2), null
        };
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.DECIMAL(5, 2), 0);
        assertThat(getter.getElementOrNull(genericArray))
                .isEqualTo(Decimal.fromUnscaledLong(123, 5, 2));

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.DECIMAL(5, 2), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForTinyint() {
        Object[] array = {(byte) 1, (byte) 2, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.TINYINT(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo((byte) 1);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.TINYINT(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForSmallint() {
        Object[] array = {(short) 10, (short) 20, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.SMALLINT(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo((short) 10);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.SMALLINT(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForInteger() {
        Object[] array = {100, 200, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.INT(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(100);

        InternalArray.ElementGetter getter2 = InternalArray.createElementGetter(DataTypes.INT(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForDate() {
        Object[] array = {18000, 18001, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.DATE(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(18000);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.DATE(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForTime() {
        Object[] array = {3600000, 7200000, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.TIME(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(3600000);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.TIME(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForBigint() {
        Object[] array = {1000L, 2000L, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.BIGINT(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(1000L);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.BIGINT(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForFloat() {
        Object[] array = {1.5f, 2.5f, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.FLOAT(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(1.5f);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.FLOAT(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForDouble() {
        Object[] array = {1.1, 2.2, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.DOUBLE(), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(1.1);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.DOUBLE(), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForTimestampNtz() {
        Object[] array = {TimestampNtz.fromMillis(1000L), TimestampNtz.fromMillis(2000L), null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.TIMESTAMP(3), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(TimestampNtz.fromMillis(1000L));

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.TIMESTAMP(3), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForTimestampLtz() {
        Object[] array = {
            TimestampLtz.fromEpochMillis(1000L), TimestampLtz.fromEpochMillis(2000L), null
        };
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.TIMESTAMP_LTZ(3), 0);
        assertThat(getter.getElementOrNull(genericArray))
                .isEqualTo(TimestampLtz.fromEpochMillis(1000L));

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.TIMESTAMP_LTZ(3), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    @Test
    public void testElementGetterForArray() {
        GenericArray innerArray1 = new GenericArray(new int[] {1, 2, 3});
        GenericArray innerArray2 = new GenericArray(new int[] {4, 5, 6});
        Object[] array = {innerArray1, innerArray2, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.ARRAY(DataTypes.INT()), 0);
        assertThat(getter.getElementOrNull(genericArray)).isEqualTo(innerArray1);

        InternalArray.ElementGetter getter2 =
                InternalArray.createElementGetter(DataTypes.ARRAY(DataTypes.INT()), 2);
        assertThat(getter2.getElementOrNull(genericArray)).isNull();
    }

    // TODO: Uncomment when Row support is added in Issue #1974
    // @Test
    // public void testElementGetterForRow() { ... }

    // TODO: Uncomment when Map support is added in Issue #1973
    // @Test
    // public void testElementGetterForMap() { ... }

    @Test
    public void testBinaryArrayWithElementGetter() {
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(new int[] {10, 20, 30});

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.INT(), 0);
        assertThat(getter.getElementOrNull(binaryArray)).isEqualTo(10);

        InternalArray.ElementGetter getter1 = InternalArray.createElementGetter(DataTypes.INT(), 1);
        assertThat(getter1.getElementOrNull(binaryArray)).isEqualTo(20);

        InternalArray.ElementGetter getter2 = InternalArray.createElementGetter(DataTypes.INT(), 2);
        assertThat(getter2.getElementOrNull(binaryArray)).isEqualTo(30);
    }

    @Test
    public void testBinaryArrayWithNullElementGetter() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 10);
        writer.setNullInt(1);
        writer.writeInt(2, 30);
        writer.complete();

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.INT(), 0);
        assertThat(getter.getElementOrNull(array)).isEqualTo(10);

        InternalArray.ElementGetter getter1 = InternalArray.createElementGetter(DataTypes.INT(), 1);
        assertThat(getter1.getElementOrNull(array)).isNull();

        InternalArray.ElementGetter getter2 = InternalArray.createElementGetter(DataTypes.INT(), 2);
        assertThat(getter2.getElementOrNull(array)).isEqualTo(30);
    }

    @Test
    public void testElementGetterWithAllDataTypes() {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 10, 8);

        writer.writeBoolean(0, true);
        writer.writeByte(1, (byte) 1);
        writer.writeShort(2, (short) 10);
        writer.writeInt(3, 100);
        writer.writeLong(4, 1000L);
        writer.writeFloat(5, 1.5f);
        writer.writeDouble(6, 2.5);
        writer.writeString(7, BinaryString.fromString("test"));
        writer.writeDecimal(8, Decimal.fromUnscaledLong(123, 5, 2), 5);
        writer.writeTimestampNtz(9, TimestampNtz.fromMillis(1000L), 3);
        writer.complete();

        assertThat(
                        InternalArray.createElementGetter(DataTypes.BOOLEAN(), 0)
                                .getElementOrNull(array))
                .isEqualTo(true);
        assertThat(
                        InternalArray.createElementGetter(DataTypes.TINYINT(), 1)
                                .getElementOrNull(array))
                .isEqualTo((byte) 1);
        assertThat(
                        InternalArray.createElementGetter(DataTypes.SMALLINT(), 2)
                                .getElementOrNull(array))
                .isEqualTo((short) 10);
        assertThat(InternalArray.createElementGetter(DataTypes.INT(), 3).getElementOrNull(array))
                .isEqualTo(100);
        assertThat(InternalArray.createElementGetter(DataTypes.BIGINT(), 4).getElementOrNull(array))
                .isEqualTo(1000L);
        assertThat(InternalArray.createElementGetter(DataTypes.FLOAT(), 5).getElementOrNull(array))
                .isEqualTo(1.5f);
        assertThat(InternalArray.createElementGetter(DataTypes.DOUBLE(), 6).getElementOrNull(array))
                .isEqualTo(2.5);
        assertThat(InternalArray.createElementGetter(DataTypes.STRING(), 7).getElementOrNull(array))
                .isEqualTo(BinaryString.fromString("test"));
        assertThat(
                        InternalArray.createElementGetter(DataTypes.DECIMAL(5, 2), 8)
                                .getElementOrNull(array))
                .isEqualTo(Decimal.fromUnscaledLong(123, 5, 2));
        assertThat(
                        InternalArray.createElementGetter(DataTypes.TIMESTAMP(3), 9)
                                .getElementOrNull(array))
                .isEqualTo(TimestampNtz.fromMillis(1000L));
    }
}
