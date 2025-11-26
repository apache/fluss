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

import org.apache.fluss.row.array.PrimitiveBinaryArray;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InternalArray}. */
public class InternalArrayTest {

    @Test
    public void testElementGetterForChar() {
        Object[] array = {BinaryString.fromString("hello"), BinaryString.fromString("world"), null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.CHAR(10));

        assertThat(getter.getElementOrNull(genericArray, 0))
                .isEqualTo(BinaryString.fromString("hello"));
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForString() {
        Object[] array = {BinaryString.fromString("test1"), BinaryString.fromString("test2"), null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.STRING());

        assertThat(getter.getElementOrNull(genericArray, 0))
                .isEqualTo(BinaryString.fromString("test1"));
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForBoolean() {
        Object[] array = {true, false, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.BOOLEAN());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(true);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForBinary() {
        Object[] array = {new byte[] {1, 2}, new byte[] {3, 4}, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.BINARY(2));

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(new byte[] {1, 2});
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForBytes() {
        Object[] array = {new byte[] {1, 2, 3}, new byte[] {3, 4}, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.BYTES());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(getter.getElementOrNull(genericArray, 1)).isEqualTo(new byte[] {3, 4});
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForDecimal() {
        Object[] array = {
            Decimal.fromUnscaledLong(123, 5, 2), Decimal.fromUnscaledLong(456, 5, 2), null
        };
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.DECIMAL(5, 2));

        assertThat(getter.getElementOrNull(genericArray, 0))
                .isEqualTo(Decimal.fromUnscaledLong(123, 5, 2));
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForTinyint() {
        Object[] array = {(byte) 1, (byte) 2, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.TINYINT());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo((byte) 1);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForSmallint() {
        Object[] array = {(short) 10, (short) 20, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.SMALLINT());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo((short) 10);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForInteger() {
        Object[] array = {100, 200, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.INT());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(100);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForDate() {
        Object[] array = {18000, 18001, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.DATE());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(18000);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForTime() {
        Object[] array = {3600000, 7200000, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.TIME());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(3600000);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForBigint() {
        Object[] array = {1000L, 2000L, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.BIGINT());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(1000L);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForFloat() {
        Object[] array = {1.5f, 2.5f, null};
        GenericArray genericArray = new GenericArray(array);

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.FLOAT());
        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(1.5f);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForDouble() {
        Object[] array = {1.1, 2.2, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.DOUBLE());

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(1.1);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForTimestampNtz() {
        Object[] array = {TimestampNtz.fromMillis(1000L), TimestampNtz.fromMillis(2000L), null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.TIMESTAMP(3));

        assertThat(getter.getElementOrNull(genericArray, 0))
                .isEqualTo(TimestampNtz.fromMillis(1000L));
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForTimestampLtz() {
        Object[] array = {
            TimestampLtz.fromEpochMillis(1000L), TimestampLtz.fromEpochMillis(2000L), null
        };
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.TIMESTAMP_LTZ(3));

        assertThat(getter.getElementOrNull(genericArray, 0))
                .isEqualTo(TimestampLtz.fromEpochMillis(1000L));
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
    }

    @Test
    public void testElementGetterForArray() {
        GenericArray innerArray1 = new GenericArray(new int[] {1, 2, 3});
        GenericArray innerArray2 = new GenericArray(new int[] {4, 5, 6});
        Object[] array = {innerArray1, innerArray2, null};
        GenericArray genericArray = new GenericArray(array);
        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(DataTypes.ARRAY(DataTypes.INT()));

        assertThat(getter.getElementOrNull(genericArray, 0)).isEqualTo(innerArray1);
        assertThat(getter.getElementOrNull(genericArray, 2)).isNull();
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
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.INT());

        assertThat(getter.getElementOrNull(binaryArray, 0)).isEqualTo(10);
        assertThat(getter.getElementOrNull(binaryArray, 1)).isEqualTo(20);
        assertThat(getter.getElementOrNull(binaryArray, 2)).isEqualTo(30);
    }

    @Test
    public void testBinaryArrayWithNullElementGetter() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 10);
        writer.setNullInt(1);
        writer.writeInt(2, 30);
        writer.complete();

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.INT());
        assertThat(getter.getElementOrNull(array, 0)).isEqualTo(10);
        assertThat(getter.getElementOrNull(array, 1)).isNull();
        assertThat(getter.getElementOrNull(array, 2)).isEqualTo(30);
    }
}
