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

package org.apache.fluss.cli.util;

import org.apache.fluss.types.DataTypes;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlTypeMapperTest {

    @Test
    void testTimestampWithoutTimeZoneAliases() {
        SqlDataTypeSpec timestampWithoutTimeZone =
                new SqlDataTypeSpec(
                        new SqlUserDefinedTypeNameSpec(
                                new SqlIdentifier("TIMESTAMP_WITHOUT_TIME_ZONE", SqlParserPos.ZERO),
                                SqlParserPos.ZERO),
                        SqlParserPos.ZERO);
        assertThat(SqlTypeMapper.toFlussDataType(timestampWithoutTimeZone))
                .isEqualTo(DataTypes.TIMESTAMP());

        SqlDataTypeSpec timestampNtz =
                new SqlDataTypeSpec(
                        new SqlUserDefinedTypeNameSpec(
                                new SqlIdentifier("TIMESTAMP_NTZ", SqlParserPos.ZERO),
                                SqlParserPos.ZERO),
                        SqlParserPos.ZERO);
        assertThat(SqlTypeMapper.toFlussDataType(timestampNtz)).isEqualTo(DataTypes.TIMESTAMP());
    }

    @Test
    void testNumericTypes() {
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("BOOLEAN")))
                .isEqualTo(DataTypes.BOOLEAN());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("BOOL"))).isEqualTo(DataTypes.BOOLEAN());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("TINYINT")))
                .isEqualTo(DataTypes.TINYINT());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("SMALLINT")))
                .isEqualTo(DataTypes.SMALLINT());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("INTEGER"))).isEqualTo(DataTypes.INT());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("INT"))).isEqualTo(DataTypes.INT());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("BIGINT"))).isEqualTo(DataTypes.BIGINT());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("LONG"))).isEqualTo(DataTypes.BIGINT());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("FLOAT"))).isEqualTo(DataTypes.FLOAT());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("DOUBLE"))).isEqualTo(DataTypes.DOUBLE());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("DOUBLE PRECISION")))
                .isEqualTo(DataTypes.DOUBLE());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("DECIMAL")))
                .isEqualTo(DataTypes.DECIMAL(10, 0));
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("NUMERIC")))
                .isEqualTo(DataTypes.DECIMAL(10, 0));
    }

    @Test
    void testCharacterAndBinaryTypes() {
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("CHAR"))).isEqualTo(DataTypes.CHAR(1));
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("VARCHAR")))
                .isEqualTo(DataTypes.STRING());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("STRING"))).isEqualTo(DataTypes.STRING());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("TEXT"))).isEqualTo(DataTypes.STRING());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("BINARY")))
                .isEqualTo(DataTypes.BINARY(1));
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("VARBINARY")))
                .isEqualTo(DataTypes.BYTES());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("BYTES"))).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testPrecisionAndScale() {
        SqlDataTypeSpec decimal =
                new SqlDataTypeSpec(
                        new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                                SqlTypeName.DECIMAL, 12, 3, null, SqlParserPos.ZERO),
                        SqlParserPos.ZERO);
        assertThat(SqlTypeMapper.toFlussDataType(decimal)).isEqualTo(DataTypes.DECIMAL(12, 3));

        SqlDataTypeSpec charType =
                new SqlDataTypeSpec(
                        new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                                SqlTypeName.CHAR, 4, -1, null, SqlParserPos.ZERO),
                        SqlParserPos.ZERO);
        assertThat(SqlTypeMapper.toFlussDataType(charType)).isEqualTo(DataTypes.CHAR(4));

        SqlDataTypeSpec timeType =
                new SqlDataTypeSpec(
                        new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                                SqlTypeName.TIME, 3, -1, null, SqlParserPos.ZERO),
                        SqlParserPos.ZERO);
        assertThat(SqlTypeMapper.toFlussDataType(timeType)).isEqualTo(DataTypes.TIME(3));
    }

    @Test
    void testTemporalTypes() {
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("DATE"))).isEqualTo(DataTypes.DATE());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("TIME"))).isEqualTo(DataTypes.TIME());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("TIMESTAMP")))
                .isEqualTo(DataTypes.TIMESTAMP());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("TIMESTAMP_LTZ")))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ());
        assertThat(SqlTypeMapper.toFlussDataType(typeSpec("TIMESTAMP WITH LOCAL TIME ZONE")))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ());
    }

    @Test
    void testUnsupportedType() {
        SqlDataTypeSpec spec = typeSpec("UNSUPPORTED");
        assertThatThrownBy(() -> SqlTypeMapper.toFlussDataType(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported SQL type");
    }

    private static SqlDataTypeSpec typeSpec(String typeName) {
        return new SqlDataTypeSpec(
                new SqlUserDefinedTypeNameSpec(
                        new SqlIdentifier(typeName, SqlParserPos.ZERO), SqlParserPos.ZERO),
                SqlParserPos.ZERO);
    }
}
