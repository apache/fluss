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

package com.alibaba.fluss.types;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.fluss.types.DataTypeChecks.getFieldCount;
import static com.alibaba.fluss.types.DataTypeChecks.getFieldTypes;
import static com.alibaba.fluss.types.DataTypeChecks.getLength;
import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;
import static com.alibaba.fluss.types.DataTypeChecks.getScale;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link com.alibaba.fluss.types.DataTypeChecks} utilities. */
public class DataTypeChecksTest {

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
    void testDataTypeVisitorForLengthExtractWithVarCharAndVarBinary() {
        // Test VarCharType
        assertThat(getLength(new VarCharType(10))).isEqualTo(10);
        assertThat(getLength(new VarCharType(100))).isEqualTo(100);
        assertThat(getLength(new VarCharType(VarCharType.MAX_LENGTH)))
                .isEqualTo(VarCharType.MAX_LENGTH);

        // Test VarBinaryType
        assertThat(getLength(new VarBinaryType(20))).isEqualTo(20);
        assertThat(getLength(new VarBinaryType(200))).isEqualTo(200);
        assertThat(getLength(new VarBinaryType(VarBinaryType.MAX_LENGTH)))
                .isEqualTo(VarBinaryType.MAX_LENGTH);
    }

    @Test
    void testDataTypeVisitorForFieldCountExtract() {
        // Test RowType
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.INT()),
                        DataTypes.FIELD("b", DataTypes.STRING()),
                        DataTypes.FIELD("c", DataTypes.BOOLEAN()));
        assertThat(getFieldCount(rowType)).isEqualTo(3);

        // Test empty RowType
        RowType emptyRowType = DataTypes.ROW(new DataField[0]);
        assertThat(getFieldCount(emptyRowType)).isEqualTo(0);

        // Test single field RowType
        RowType singleFieldRowType = DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT()));
        assertThat(getFieldCount(singleFieldRowType)).isEqualTo(1);

        // Test other types should return 1
        assertThat(getFieldCount(DataTypes.INT())).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.STRING())).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.BOOLEAN())).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.ARRAY(DataTypes.INT()))).isEqualTo(1);
        assertThat(getFieldCount(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))).isEqualTo(1);
    }

    @Test
    void testDataTypeVisitorForFieldTypesExtract() {
        // Test RowType
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.INT()),
                        DataTypes.FIELD("b", DataTypes.STRING()),
                        DataTypes.FIELD("c", DataTypes.BOOLEAN()));
        List<DataType> fieldTypes = getFieldTypes(rowType);
        assertThat(fieldTypes).hasSize(3);
        assertThat(fieldTypes.get(0)).isInstanceOf(IntType.class);
        assertThat(fieldTypes.get(1)).isInstanceOf(StringType.class);
        assertThat(fieldTypes.get(2)).isInstanceOf(BooleanType.class);

        // Test empty RowType
        RowType emptyRowType = DataTypes.ROW(new DataField[0]);
        List<DataType> emptyFieldTypes = getFieldTypes(emptyRowType);
        assertThat(emptyFieldTypes).isEmpty();

        // Test single field RowType
        RowType singleFieldRowType = DataTypes.ROW(DataTypes.FIELD("a", DataTypes.DOUBLE()));
        List<DataType> singleFieldTypes = getFieldTypes(singleFieldRowType);
        assertThat(singleFieldTypes).hasSize(1);
        assertThat(singleFieldTypes.get(0)).isInstanceOf(DoubleType.class);

        // Test other types should throw exception
        List<DataType> otherTypes =
                Arrays.asList(
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()));

        for (DataType otherType : otherTypes) {
            assertThatThrownBy(() -> getFieldTypes(otherType))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid use of extractor FieldTypesExtractor");
        }
    }
}
