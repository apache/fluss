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

package org.apache.fluss.trino;

import org.apache.fluss.types.DataTypes;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.apache.fluss.trino.FlussTypeConverter.toTrinoType;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests logical type mapping independently of catalog access. */
final class FlussTypeConverterTest {
    @Test
    void testPrimitiveTypes() {
        assertThat(toTrinoType(DataTypes.BOOLEAN())).isEqualTo(BOOLEAN);
        assertThat(toTrinoType(DataTypes.TINYINT())).isEqualTo(TINYINT);
        assertThat(toTrinoType(DataTypes.SMALLINT())).isEqualTo(SMALLINT);
        assertThat(toTrinoType(DataTypes.INT())).isEqualTo(INTEGER);
        assertThat(toTrinoType(DataTypes.BIGINT())).isEqualTo(BIGINT);
        assertThat(toTrinoType(DataTypes.FLOAT())).isEqualTo(REAL);
        assertThat(toTrinoType(DataTypes.DOUBLE())).isEqualTo(DOUBLE);
        assertThat(toTrinoType(DataTypes.STRING())).isEqualTo(VARCHAR);
        assertThat(toTrinoType(DataTypes.BINARY(16))).isEqualTo(VARBINARY);
        assertThat(toTrinoType(DataTypes.BYTES())).isEqualTo(VARBINARY);
        assertThat(toTrinoType(DataTypes.DATE())).isEqualTo(DATE);
        assertThat(toTrinoType(DataTypes.INT().copy(false))).isEqualTo(INTEGER);
    }

    @Test
    void testDecimalPrecisionAndScale() {
        assertThat(toTrinoType(DataTypes.DECIMAL(10, 2))).isEqualTo(createDecimalType(10, 2));
        assertThat(toTrinoType(DataTypes.DECIMAL(38, 18))).isEqualTo(createDecimalType(38, 18));
    }

    @Test
    void testTemporalPrecision() {
        assertThat(toTrinoType(DataTypes.TIME(0))).isEqualTo(createTimeType(0));
        assertThat(toTrinoType(DataTypes.TIME(9))).isEqualTo(createTimeType(9));
        assertThat(toTrinoType(DataTypes.TIMESTAMP(3))).isEqualTo(createTimestampType(3));
        assertThat(toTrinoType(DataTypes.TIMESTAMP(9))).isEqualTo(createTimestampType(9));
        assertThat(toTrinoType(DataTypes.TIMESTAMP_LTZ(3)))
                .isEqualTo(createTimestampWithTimeZoneType(3));
        assertThat(toTrinoType(DataTypes.TIMESTAMP_LTZ(9)))
                .isEqualTo(createTimestampWithTimeZoneType(9));
    }

    @Test
    void testCharLengthBoundaries() {
        int maxChar = io.trino.spi.type.CharType.MAX_LENGTH;
        int maxVarchar = io.trino.spi.type.VarcharType.MAX_LENGTH;
        assertThat(toTrinoType(DataTypes.CHAR(1))).isEqualTo(createCharType(1));
        assertThat(toTrinoType(DataTypes.CHAR(maxChar))).isEqualTo(createCharType(maxChar));
        assertThat(toTrinoType(DataTypes.CHAR(maxChar + 1)))
                .isEqualTo(createVarcharType(maxChar + 1));
        assertThat(toTrinoType(DataTypes.CHAR(maxVarchar)))
                .isEqualTo(createVarcharType(maxVarchar));
        assertThat(toTrinoType(DataTypes.CHAR(Integer.MAX_VALUE))).isEqualTo(VARCHAR);
    }

    @Test
    void testNestedTypes() {
        RowType row =
                (RowType)
                        toTrinoType(
                                DataTypes.ROW(
                                        DataTypes.FIELD("UserID", DataTypes.BIGINT()),
                                        DataTypes.FIELD(
                                                "Tags", DataTypes.ARRAY(DataTypes.STRING()))));
        assertThat(row)
                .isEqualTo(
                        RowType.from(
                                Arrays.asList(
                                        RowType.field("UserID", BIGINT),
                                        RowType.field("Tags", new ArrayType(VARCHAR)))));

        MapType map =
                (MapType)
                        toTrinoType(
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.ARRAY(DataTypes.DECIMAL(12, 4))));
        assertThat(map.getKeyType()).isEqualTo(VARCHAR);
        assertThat(map.getValueType()).isEqualTo(new ArrayType(createDecimalType(12, 4)));
        assertThat(toTrinoType(DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))))
                .isEqualTo(new ArrayType(new ArrayType(INTEGER)));
    }
}
