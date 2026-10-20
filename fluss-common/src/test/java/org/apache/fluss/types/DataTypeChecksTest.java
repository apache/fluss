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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.fluss.types.DataTypeChecks.equalsIgnoreFieldDescriptionsAndIds;
import static org.apache.fluss.types.DataTypeChecks.getFieldCount;
import static org.apache.fluss.types.DataTypeChecks.getFieldTypes;
import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Tests for the {@link org.apache.fluss.types.DataTypeChecks} utilities. */
public class DataTypeChecksTest {

    @ParameterizedTest
    @MethodSource("rowsWithDifferentFieldMetadata")
    void testEqualsIgnoreFieldDescriptionsAndIds(RowType differentMetadata) {
        RowType original =
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT(), "Original field", 1));
        List<DataType> originalTypes =
                Arrays.asList(
                        DataTypes.INT(),
                        original,
                        DataTypes.ARRAY(original),
                        DataTypes.MAP(original, original),
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "envelope",
                                        DataTypes.ARRAY(
                                                DataTypes.MAP(DataTypes.STRING(), original)),
                                        2)));
        List<DataType> equivalentTypes =
                Arrays.asList(
                        DataTypes.INT(),
                        differentMetadata,
                        DataTypes.ARRAY(differentMetadata),
                        DataTypes.MAP(differentMetadata, differentMetadata),
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "envelope",
                                        DataTypes.ARRAY(
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), differentMetadata)),
                                        20)));

        for (int i = 0; i < originalTypes.size(); i++) {
            assertThat(
                            equalsIgnoreFieldDescriptionsAndIds(
                                    originalTypes.get(i), equivalentTypes.get(i)))
                    .isTrue();
            assertThat(
                            equalsIgnoreFieldDescriptionsAndIds(
                                    equivalentTypes.get(i), originalTypes.get(i)))
                    .isTrue();
        }
    }

    private static Stream<RowType> rowsWithDifferentFieldMetadata() {
        return Stream.of(
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT(), "Updated field", 1)),
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT(), "Original field", 10)),
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT(), "Updated field", 10)),
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT(), null, 10)));
    }

    @Test
    void testEqualsIgnoreFieldDescriptionsAndIdsPreservesFieldNames() {
        RowType original = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));
        RowType renamed = DataTypes.ROW(DataTypes.FIELD("other", DataTypes.INT()));
        assertDifferentTypes(original, renamed);
        assertDifferentTypes(renamed, original);
        assertDifferentTypes(
                DataTypes.MAP(original, DataTypes.STRING()),
                DataTypes.MAP(renamed, DataTypes.STRING()));
        assertDifferentTypes(
                DataTypes.ROW(
                        DataTypes.FIELD("first", DataTypes.INT()),
                        DataTypes.FIELD("second", DataTypes.INT())),
                DataTypes.ROW(
                        DataTypes.FIELD("second", DataTypes.INT()),
                        DataTypes.FIELD("first", DataTypes.INT())));
    }

    @ParameterizedTest
    @MethodSource("differentTypeClasses")
    void testEqualsIgnoreFieldDescriptionsAndIdsWithDifferentTypeClasses(
            DataType first, DataType second) {
        assertDifferentTypes(first, second);
        assertDifferentTypes(second, first);
        assertThat(
                        equalsIgnoreFieldDescriptionsAndIds(
                                DataTypes.MAP(first, DataTypes.STRING()),
                                DataTypes.MAP(second, DataTypes.STRING())))
                .isFalse();
        assertThat(
                        equalsIgnoreFieldDescriptionsAndIds(
                                DataTypes.MAP(second, DataTypes.STRING()),
                                DataTypes.MAP(first, DataTypes.STRING())))
                .isFalse();
    }

    private static Stream<Arguments> differentTypeClasses() {
        DataType scalar = DataTypes.INT();
        DataType row = DataTypes.ROW(DataTypes.FIELD("value", DataTypes.INT()));
        DataType array = DataTypes.ARRAY(DataTypes.INT());
        DataType map = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        return Stream.of(
                arguments(scalar, row),
                arguments(scalar, array),
                arguments(scalar, map),
                arguments(row, array),
                arguments(row, map),
                arguments(array, map));
    }

    @Test
    void testEqualsIgnoreFieldDescriptionsAndIdsPreservesTypeDetails() {
        assertDifferentTypes(DataTypes.INT(), DataTypes.BIGINT());
        assertDifferentTypes(DataTypes.INT(), DataTypes.INT().copy(false));
        assertDifferentTypes(DataTypes.CHAR(1), DataTypes.CHAR(2));
        assertDifferentTypes(DataTypes.BINARY(1), DataTypes.BINARY(2));
        assertDifferentTypes(DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(11, 2));
        assertDifferentTypes(DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(10, 3));
        assertDifferentTypes(DataTypes.TIME(3), DataTypes.TIME(6));
        assertDifferentTypes(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6));
        assertDifferentTypes(DataTypes.TIMESTAMP_LTZ(3), DataTypes.TIMESTAMP_LTZ(6));
        assertDifferentTypes(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_LTZ(3));
        assertDifferentTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()));
        assertDifferentTypes(
                DataTypes.ARRAY(DataTypes.INT()), DataTypes.ARRAY(DataTypes.INT()).copy(false));
        assertDifferentTypes(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                DataTypes.MAP(DataTypes.INT(), DataTypes.INT()));
        assertDifferentTypes(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).copy(false));
        RowType row = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));
        assertDifferentTypes(row, row.copy(false));
        assertDifferentTypes(row, RowType.builder().build());
        assertDifferentTypes(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING())),
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("id", DataTypes.INT())));
    }

    @Test
    void testDataTypeVisitorForLengthExtract() {
        assertThat(getLength(new CharType(1))).isEqualTo(1);
        assertThat(getLength(new BinaryType(10))).isEqualTo(10);

        List<DataType> noLengthTypes =
                Arrays.asList(
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.BYTES(),
                        DataTypes.DECIMAL(2, 1),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DATE(),
                        DataTypes.TIME(),
                        DataTypes.TIMESTAMP(),
                        DataTypes.TIMESTAMP_LTZ(),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())));

        for (DataType noLengthType : noLengthTypes) {
            assertThatThrownBy(() -> getLength(noLengthType))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid use of extractor LengthExtractor");
        }
    }

    @Test
    void testDataTypeVisitorForPrecisionExtract() {
        assertThat(getPrecision(new DecimalType(5, 2))).isEqualTo(5);
        assertThat(getPrecision(new TimeType(5))).isEqualTo(5);
        assertThat(getPrecision(new TimestampType(5))).isEqualTo(5);
        assertThat(getPrecision(new LocalZonedTimestampType(9))).isEqualTo(9);

        List<DataType> noPrecisionTypes =
                Arrays.asList(
                        DataTypes.CHAR(5),
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.BINARY(5),
                        DataTypes.BYTES(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DATE(),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())));

        for (DataType noPrecisionType : noPrecisionTypes) {
            assertThatThrownBy(() -> getPrecision(noPrecisionType))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid use of extractor PrecisionExtractor");
        }
    }

    @Test
    void testDataTypeVisitorForScaleExtract() {
        assertThat(getScale(new DecimalType(5, 1))).isEqualTo(1);
        assertThat(getScale(new TinyIntType())).isEqualTo(0);
        assertThat(getScale(new SmallIntType())).isEqualTo(0);
        assertThat(getScale(new IntType())).isEqualTo(0);
        assertThat(getScale(new BigIntType())).isEqualTo(0);

        List<DataType> noScaleTypes =
                Arrays.asList(
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.BYTES(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DATE(),
                        DataTypes.TIME(),
                        DataTypes.TIMESTAMP(),
                        DataTypes.TIMESTAMP_LTZ(),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())));

        for (DataType noScaleType : noScaleTypes) {
            assertThatThrownBy(() -> getScale(noScaleType))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid use of extractor ScaleExtractor");
        }
    }

    @Test
    void testDataTypeVisitorForFieldCountExtract() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING()),
                                DataTypes.FIELD("c", DataTypes.BOOLEAN()));
        assertThat(getFieldCount(rowType)).isEqualTo(3);

        assertThat(getFieldCount(DataTypes.INT())).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.STRING())).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.BOOLEAN())).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.BIGINT())).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.ARRAY(DataTypes.INT()))).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))).isEqualTo(1);
    }

    @Test
    void testDataTypeVisitorForFieldTypesExtract() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING()),
                                DataTypes.FIELD("c", DataTypes.BOOLEAN()));
        List<DataType> fieldTypes = getFieldTypes(rowType);
        assertThat(fieldTypes).hasSize(3);
        assertThat(fieldTypes.get(0)).isEqualTo(DataTypes.INT());
        assertThat(fieldTypes.get(1)).isEqualTo(DataTypes.STRING());
        assertThat(fieldTypes.get(2)).isEqualTo(DataTypes.BOOLEAN());

        assertThatThrownBy(() -> getFieldTypes(DataTypes.INT()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid use of extractor FieldTypesExtractor");

        assertThatThrownBy(() -> getFieldTypes(DataTypes.STRING()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid use of extractor FieldTypesExtractor");

        assertThatThrownBy(() -> getFieldTypes(DataTypes.ARRAY(DataTypes.INT())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid use of extractor FieldTypesExtractor");
    }

    private static void assertDifferentTypes(DataType first, DataType second) {
        assertThat(equalsIgnoreFieldDescriptionsAndIds(first, second)).isFalse();
        assertThat(equalsIgnoreFieldDescriptionsAndIds(second, first)).isFalse();
        assertThat(
                        equalsIgnoreFieldDescriptionsAndIds(
                                DataTypes.ARRAY(first), DataTypes.ARRAY(second)))
                .isFalse();
        assertThat(
                        equalsIgnoreFieldDescriptionsAndIds(
                                DataTypes.MAP(DataTypes.STRING(), first),
                                DataTypes.MAP(DataTypes.STRING(), second)))
                .isFalse();
        assertThat(
                        equalsIgnoreFieldDescriptionsAndIds(
                                DataTypes.ROW(DataTypes.FIELD("a", first)),
                                DataTypes.ROW(DataTypes.FIELD("a", second))))
                .isFalse();
    }
}
