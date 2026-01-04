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

package org.apache.fluss.row.array;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryArrayWriter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompactedArray}. */
public class CompactedArrayTest {

    @Test
    public void testConstructorWithRowType() {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        CompactedArray array = new CompactedArray(rowType);

        assertThat(array).isNotNull();
    }

    @Test
    public void testConstructorWithArrayType() {
        DataType arrayType = DataTypes.ARRAY(DataTypes.INT());
        CompactedArray array = new CompactedArray(arrayType);

        assertThat(array).isNotNull();
    }

    @Test
    public void testConstructorWithMapType() {
        DataType mapType = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());
        CompactedArray array = new CompactedArray(mapType);

        assertThat(array).isNotNull();
    }

    @Test
    public void testGetRowWithRowType() {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());

        CompactedArray compactedArray = new CompactedArray(rowType);
        assertThat(compactedArray).isNotNull();
    }

    @Test
    public void testGetRowWithInvalidFieldCount() {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());

        CompactedArray compactedArray = new CompactedArray(rowType);

        BinaryArray intArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        compactedArray.pointTo(
                intArray.getSegments(), intArray.getOffset(), intArray.getSizeInBytes());

        assertThatThrownBy(() -> compactedArray.getRow(0, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected number of fields");
    }

    @Test
    public void testGetRowWithNonRowType() {
        DataType intType = DataTypes.INT();
        CompactedArray compactedArray = new CompactedArray(intType);

        BinaryArray intArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        compactedArray.pointTo(
                intArray.getSegments(), intArray.getOffset(), intArray.getSizeInBytes());

        assertThatThrownBy(() -> compactedArray.getRow(0, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Can not get row from Array of type");
    }

    @Test
    public void testCreateNestedArrayInstanceWithArrayType() {
        DataType innerArrayType = DataTypes.ARRAY(DataTypes.INT());
        DataType outerArrayType = DataTypes.ARRAY(innerArrayType);

        CompactedArray outerArray = new CompactedArray(outerArrayType);

        GenericArray innerArray1 = GenericArray.of(10, 20, 30);
        GenericArray innerArray2 = GenericArray.of(40, 50, 60);
        GenericArray genericOuterArray = GenericArray.of(innerArray1, innerArray2);

        ArraySerializer serializer =
                new ArraySerializer(innerArrayType, BinaryRow.BinaryRowFormat.COMPACTED);
        BinaryArray binaryOuterArray = serializer.toBinaryArray(genericOuterArray);

        outerArray.pointTo(
                binaryOuterArray.getSegments(),
                binaryOuterArray.getOffset(),
                binaryOuterArray.getSizeInBytes());

        assertThat(outerArray.size()).isEqualTo(2);
        InternalArray nestedArray = outerArray.getArray(0);
        assertThat(nestedArray).isNotNull();
    }

    @Test
    public void testCreateNestedArrayInstanceWithNonArrayType() {
        DataType intType = DataTypes.INT();
        CompactedArray compactedArray = new CompactedArray(intType);

        BinaryArray intArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        compactedArray.pointTo(
                intArray.getSegments(), intArray.getOffset(), intArray.getSizeInBytes());

        assertThatThrownBy(() -> compactedArray.getArray(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Can not get nested array from Array of type");
    }

    @Test
    public void testCreateNestedMapInstanceWithMapType() {
        DataType mapType = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());
        DataType arrayOfMapType = DataTypes.ARRAY(mapType);

        CompactedArray compactedArray = new CompactedArray(arrayOfMapType);

        assertThat(compactedArray).isNotNull();
    }

    @Test
    public void testCreateNestedMapInstanceWithNonMapType() {
        DataType intType = DataTypes.INT();
        CompactedArray compactedArray = new CompactedArray(intType);

        BinaryArray intArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        compactedArray.pointTo(
                intArray.getSegments(), intArray.getOffset(), intArray.getSizeInBytes());

        assertThatThrownBy(() -> compactedArray.getMap(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Can not get nested map from Array of type");
    }

    @Test
    public void testWithPrimitiveElements() {
        DataType intType = DataTypes.INT();
        CompactedArray compactedArray = new CompactedArray(intType);

        BinaryArray intArray = BinaryArray.fromPrimitiveArray(new int[] {100, 200, 300});
        compactedArray.pointTo(
                intArray.getSegments(), intArray.getOffset(), intArray.getSizeInBytes());

        assertThat(compactedArray.size()).isEqualTo(3);
        assertThat(compactedArray.getInt(0)).isEqualTo(100);
        assertThat(compactedArray.getInt(1)).isEqualTo(200);
        assertThat(compactedArray.getInt(2)).isEqualTo(300);
    }

    @Test
    public void testWithStringElements() {
        DataType stringType = DataTypes.STRING();
        CompactedArray compactedArray = new CompactedArray(stringType);

        BinaryArray stringArray = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(stringArray, 2, 8);
        writer.writeString(0, BinaryString.fromString("hello"));
        writer.writeString(1, BinaryString.fromString("world"));
        writer.complete();

        compactedArray.pointTo(
                stringArray.getSegments(), stringArray.getOffset(), stringArray.getSizeInBytes());

        assertThat(compactedArray.size()).isEqualTo(2);
        assertThat(compactedArray.getString(0)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(compactedArray.getString(1)).isEqualTo(BinaryString.fromString("world"));
    }
}
