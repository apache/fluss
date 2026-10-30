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

package org.apache.fluss.lake.delta.utils;

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussDataTypeToDeltaDataType}. */
class FlussDataTypeToDeltaDataTypeTest {

    @Test
    void testConvertAllSupportedTypes() {
        RowType rowType =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {
                            DataTypes.BOOLEAN(),
                            DataTypes.TINYINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.INT().copy(false),
                            DataTypes.BIGINT(),
                            DataTypes.FLOAT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DECIMAL(10, 2),
                            DataTypes.CHAR(3),
                            DataTypes.STRING(),
                            DataTypes.BINARY(4),
                            DataTypes.BYTES(),
                            DataTypes.DATE(),
                            DataTypes.TIMESTAMP(6),
                            DataTypes.TIMESTAMP_LTZ(3),
                            DataTypes.ARRAY(DataTypes.INT().copy(false)),
                            DataTypes.MAP(DataTypes.STRING().copy(false), DataTypes.BIGINT()),
                            DataTypes.ROW(DataTypes.FIELD("city", DataTypes.STRING()))
                        },
                        new String[] {
                            "f_boolean", "f_tinyint", "f_smallint", "f_int", "f_bigint",
                            "f_float", "f_double", "f_decimal", "f_char", "f_string",
                            "f_binary", "f_bytes", "f_date", "f_timestamp", "f_timestamp_ltz",
                            "f_array", "f_map", "f_row"
                        });

        StructType expected =
                new StructType()
                        .add("f_boolean", BooleanType.BOOLEAN, true)
                        .add("f_tinyint", ByteType.BYTE, true)
                        .add("f_smallint", ShortType.SHORT, true)
                        .add("f_int", IntegerType.INTEGER, false)
                        .add("f_bigint", LongType.LONG, true)
                        .add("f_float", FloatType.FLOAT, true)
                        .add("f_double", DoubleType.DOUBLE, true)
                        .add("f_decimal", new DecimalType(10, 2), true)
                        .add("f_char", StringType.STRING, true)
                        .add("f_string", StringType.STRING, true)
                        .add("f_binary", BinaryType.BINARY, true)
                        .add("f_bytes", BinaryType.BINARY, true)
                        .add("f_date", DateType.DATE, true)
                        .add("f_timestamp", TimestampNTZType.TIMESTAMP_NTZ, true)
                        .add("f_timestamp_ltz", TimestampType.TIMESTAMP, true)
                        .add("f_array", new ArrayType(IntegerType.INTEGER, false), true)
                        .add("f_map", new MapType(StringType.STRING, LongType.LONG, true), true)
                        .add(
                                "f_row",
                                new StructType()
                                        .add(new StructField("city", StringType.STRING, true)),
                                true);

        assertThat(FlussDataTypeToDeltaDataType.toDeltaSchema(rowType)).isEqualTo(expected);
    }

    @Test
    void testTimeTypeIsRejected() {
        RowType rowType = RowType.of(DataTypes.TIME());
        assertThatThrownBy(() -> FlussDataTypeToDeltaDataType.toDeltaSchema(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("TIME");
    }
}
