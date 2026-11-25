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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.array.BinaryArraySplicer;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.variant.Variant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests Variant access and serialization through the row and array APIs. */
class VariantRowTest {

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testRoundTripAndNullSemantics(BinaryRowFormat format) {
        Variant value = Variant.fromJson("{\"name\":\"Fluss\",\"items\":[1,null,true]}");
        DataType[] types = {
            DataTypes.STRING(),
            DataTypes.VARIANT(),
            DataTypes.VARIANT(),
            DataTypes.VARIANT(),
            DataTypes.BYTES()
        };
        GenericRow input =
                GenericRow.of(
                        BinaryString.fromString("prefix"),
                        value,
                        Variant.ofNull(),
                        null,
                        new byte[] {1, 2, 3});
        RowSerializer serializer = new RowSerializer(types, format);
        BinaryRow encoded = serializer.toBinaryRow(input);
        // Exercise a non-zero row offset with other variable-length fields on both sides.
        byte[] bytes = new byte[encoded.getSizeInBytes() + 13];
        encoded.copyTo(bytes, 13);
        BinaryRow restored = encoded.copy();
        restored.pointTo(MemorySegment.wrap(bytes), 13, encoded.getSizeInBytes());
        for (InternalRow row : new InternalRow[] {input, restored, restored.copy()}) {
            assertThat(row.getVariant(1)).isEqualTo(value);
            assertThat(row.isNullAt(2)).isFalse();
            assertThat(row.getVariant(2).isNull()).isTrue();
            assertThat(row.isNullAt(3)).isTrue();
            assertThat(InternalRow.createFieldGetter(DataTypes.VARIANT(), 3).getFieldOrNull(row))
                    .isNull();
            assertThat(row.getString(0).toString()).isEqualTo("prefix");
            assertThat(row.getBytes(4)).containsExactly(1, 2, 3);
        }
        assertThat(InternalRow.getDataClass(DataTypes.VARIANT())).isEqualTo(Variant.class);
        assertThat(
                        AlignedRow.from(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("payload", DataTypes.VARIANT())),
                                        GenericRow.of(value))
                                .getVariant(0))
                .isEqualTo(value);
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testNestedValues(BinaryRowFormat format) {
        Variant value = Variant.fromJson("{\"a\":[1,2]}");
        DataType nestedType = DataTypes.ROW(DataTypes.FIELD("value", DataTypes.VARIANT()));
        DataType[] types = {
            nestedType,
            DataTypes.ARRAY(DataTypes.VARIANT()),
            DataTypes.MAP(DataTypes.STRING(), DataTypes.VARIANT()),
            DataTypes.ARRAY(nestedType)
        };
        InternalRow row =
                new RowSerializer(types, format)
                        .toBinaryRow(
                                GenericRow.of(
                                        GenericRow.of(value),
                                        new GenericArray(
                                                new Object[] {value, null, Variant.ofNull()}),
                                        GenericMap.of(BinaryString.fromString("key"), value),
                                        new GenericArray(new Object[] {GenericRow.of(value)})));
        assertThat(row.getRow(0, 1).getVariant(0)).isEqualTo(value);
        InternalArray array = row.getArray(1);
        assertThat(array.getVariant(0)).isEqualTo(value);
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(array.isNullAt(2)).isFalse();
        assertThat(array.getVariant(2).isNull()).isTrue();
        assertThat(
                        InternalArray.createElementGetter(DataTypes.VARIANT())
                                .getElementOrNull(array, 1))
                .isNull();
        assertThat(row.getMap(2).valueArray().getVariant(0)).isEqualTo(value);
        assertThat(row.getArray(3).getRow(0, 1).getVariant(0)).isEqualTo(value);
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testProjectedAndPaddedRows(BinaryRowFormat format) {
        Variant value = Variant.ofString("payload");
        InternalRow source = GenericRow.of(7, value, Variant.ofNull(), null);
        ProjectedRow projected = ProjectedRow.from(new int[] {1, 2, 3, -1}).replaceRow(source);
        PaddingRow padded = new PaddingRow(5).replaceRow(projected);
        DataType[] types = {
            DataTypes.VARIANT(),
            DataTypes.VARIANT(),
            DataTypes.VARIANT(),
            DataTypes.VARIANT(),
            DataTypes.VARIANT()
        };
        for (InternalRow row :
                new InternalRow[] {padded, new RowSerializer(types, format).toBinaryRow(padded)}) {
            assertThat(row.getVariant(0)).isEqualTo(value);
            assertThat(row.isNullAt(1)).isFalse();
            assertThat(row.getVariant(1).isNull()).isTrue();
            assertThat(row.isNullAt(2)).isTrue();
            assertThat(row.isNullAt(3)).isTrue();
            assertThat(row.isNullAt(4)).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testArraySplicing(BinaryRowFormat format) {
        Variant value = Variant.ofString("long enough to require a variable-length slot");
        BinaryArray array =
                new ArraySerializer(DataTypes.VARIANT(), format)
                        .toBinaryArray(
                                new GenericArray(
                                        new Object[] {
                                            value, null, Variant.ofNull(), Variant.ofInt(3)
                                        }));
        BinaryArray spliced =
                new BinaryArraySplicer(DataTypes.VARIANT())
                        .append(array, 2, 2)
                        .append(array, 0, 2)
                        .build();
        assertThat(spliced.size()).isEqualTo(4);
        assertThat(spliced.isNullAt(0)).isFalse();
        assertThat(spliced.getVariant(0).isNull()).isTrue();
        assertThat(spliced.getVariant(1).getInt()).isEqualTo(3);
        assertThat(spliced.getVariant(2)).isEqualTo(value);
        assertThat(spliced.isNullAt(3)).isTrue();
    }

    @Test
    void testDeepGettersDetachVariantBuffers() {
        Variant value = Variant.ofInt(7);
        Variant rowCopy =
                (Variant)
                        InternalRow.createDeepFieldGetter(DataTypes.VARIANT(), 0, true)
                                .getFieldOrNull(GenericRow.of(value));
        Variant arrayCopy =
                (Variant)
                        InternalArray.createDeepElementGetter(DataTypes.VARIANT(), true)
                                .getElementOrNull(new GenericArray(new Object[] {value}), 0);
        value.value()[1] = 9;
        assertThat(rowCopy.getInt()).isEqualTo(7);
        assertThat(arrayCopy.getInt()).isEqualTo(7);
    }

    @Test
    void testVariantRendering() {
        Variant value = Variant.fromJson("{\"x\":1}");
        GenericRow row =
                GenericRow.of(
                        value, new GenericArray(new Object[] {value, Variant.ofNull(), null}));
        assertThat(
                        InternalRow.createFieldToString(DataTypes.VARIANT(), 0)
                                .getFieldAsStringOrNull(row))
                .isEqualTo("{\"x\":1}");
        assertThat(
                        InternalRow.createFieldToString(DataTypes.ARRAY(DataTypes.VARIANT()), 1)
                                .getFieldAsStringOrNull(row))
                .isEqualTo("[{\"x\":1},null,null]");
        assertThat(
                        InternalRow.createFieldToString(DataTypes.VARIANT(), 0)
                                .getFieldAsStringOrNull(GenericRow.of((Object) null)))
                .isNull();
        assertThat(
                        InternalRow.createFieldToString(DataTypes.VARIANT(), 0)
                                .getFieldAsStringOrNull(GenericRow.of(Variant.ofNull())))
                .isEqualTo("null");
    }

    @Test
    void testNestedVariantRenderingKeepsDecimalPrecision() {
        String decimal = "12345678901234567890.123456789012345678";
        GenericRow row = GenericRow.of(new GenericArray(new Object[] {Variant.fromJson(decimal)}));
        assertThat(
                        InternalRow.createFieldToString(DataTypes.ARRAY(DataTypes.VARIANT()), 0)
                                .getFieldAsStringOrNull(row))
                .isEqualTo("[" + decimal + "]");
    }

    @Test
    void testExistingRowImplementationRejectsUnsupportedVariant() {
        assertThatThrownBy(() -> new ColumnarRow().getVariant(0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("VARIANT");
    }
}
