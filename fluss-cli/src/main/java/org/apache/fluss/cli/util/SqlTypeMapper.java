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

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;

/** Maps SQL data types to Fluss internal data types. */
public class SqlTypeMapper {

    public static DataType toFlussDataType(SqlDataTypeSpec dataTypeSpec) {
        String sqlType = extractTypeName(dataTypeSpec);
        Integer precision = extractPrecision(dataTypeSpec);
        Integer scale = extractScale(dataTypeSpec);

        switch (sqlType.toUpperCase()) {
            case "BOOLEAN":
            case "BOOL":
                return DataTypes.BOOLEAN();

            case "TINYINT":
                return DataTypes.TINYINT();

            case "SMALLINT":
                return DataTypes.SMALLINT();

            case "INTEGER":
            case "INT":
                return DataTypes.INT();

            case "BIGINT":
            case "LONG":
                return DataTypes.BIGINT();

            case "FLOAT":
                return DataTypes.FLOAT();

            case "DOUBLE":
            case "DOUBLE PRECISION":
                return DataTypes.DOUBLE();

            case "DECIMAL":
            case "NUMERIC":
                if (precision != null && scale != null) {
                    return DataTypes.DECIMAL(precision, scale);
                } else if (precision != null) {
                    return DataTypes.DECIMAL(precision, 0);
                }
                return DataTypes.DECIMAL(10, 0);

            case "CHAR":
                if (precision != null) {
                    return DataTypes.CHAR(precision);
                }
                return DataTypes.CHAR(1);

            case "VARCHAR":
            case "STRING":
            case "TEXT":
                return DataTypes.STRING();

            case "BINARY":
                if (precision != null) {
                    return DataTypes.BINARY(precision);
                }
                return DataTypes.BINARY(1);

            case "VARBINARY":
            case "BYTES":
                return DataTypes.BYTES();

            case "DATE":
                return DataTypes.DATE();

            case "TIME":
                if (precision != null) {
                    return DataTypes.TIME(precision);
                }
                return DataTypes.TIME();

            case "TIMESTAMP":
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
            case "TIMESTAMP WITHOUT TIME ZONE":
            case "TIMESTAMP_NTZ":
                if (precision != null) {
                    return DataTypes.TIMESTAMP(precision);
                }
                return DataTypes.TIMESTAMP();

            case "TIMESTAMP_LTZ":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
                if (precision != null) {
                    return DataTypes.TIMESTAMP_LTZ(precision);
                }
                return DataTypes.TIMESTAMP_LTZ();

            case "ARRAY":
                return parseArrayType(dataTypeSpec);

            case "MAP":
                return parseMapType(dataTypeSpec);

            case "ROW":
                return parseRowType(dataTypeSpec);

            default:
                throw new IllegalArgumentException("Unsupported SQL type: " + sqlType);
        }
    }

    private static String extractTypeName(SqlDataTypeSpec dataTypeSpec) {
        if (dataTypeSpec.getTypeName() instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) dataTypeSpec.getTypeName();
            return identifier.getSimple();
        }
        return dataTypeSpec.getTypeName().toString();
    }

    private static Integer extractPrecision(SqlDataTypeSpec dataTypeSpec) {
        SqlTypeNameSpec typeNameSpec = dataTypeSpec.getTypeNameSpec();
        if (typeNameSpec instanceof SqlBasicTypeNameSpec) {
            SqlBasicTypeNameSpec basicSpec = (SqlBasicTypeNameSpec) typeNameSpec;
            int precision = basicSpec.getPrecision();
            return precision == -1 ? null : precision;
        }
        return null;
    }

    private static Integer extractScale(SqlDataTypeSpec dataTypeSpec) {
        SqlTypeNameSpec typeNameSpec = dataTypeSpec.getTypeNameSpec();
        if (typeNameSpec instanceof SqlBasicTypeNameSpec) {
            SqlBasicTypeNameSpec basicSpec = (SqlBasicTypeNameSpec) typeNameSpec;
            int scale = basicSpec.getScale();
            return scale == -1 ? null : scale;
        }
        return null;
    }

    private static DataType parseArrayType(SqlDataTypeSpec dataTypeSpec) {
        SqlTypeNameSpec typeNameSpec = dataTypeSpec.getTypeNameSpec();

        if (typeNameSpec
                        instanceof
                        org.apache.flink.sql.parser.type.ExtendedSqlCollectionTypeNameSpec
                || typeNameSpec instanceof org.apache.calcite.sql.SqlCollectionTypeNameSpec) {
            org.apache.calcite.sql.SqlCollectionTypeNameSpec collectionSpec =
                    (org.apache.calcite.sql.SqlCollectionTypeNameSpec) typeNameSpec;
            SqlTypeNameSpec elementTypeNameSpec = collectionSpec.getElementTypeName();
            SqlDataTypeSpec elementTypeSpec =
                    new SqlDataTypeSpec(
                            elementTypeNameSpec, org.apache.calcite.sql.parser.SqlParserPos.ZERO);
            DataType elementType = toFlussDataType(elementTypeSpec);
            return DataTypes.ARRAY(elementType);
        }

        throw new IllegalArgumentException("Invalid ARRAY type specification");
    }

    private static DataType parseMapType(SqlDataTypeSpec dataTypeSpec) {
        SqlTypeNameSpec typeNameSpec = dataTypeSpec.getTypeNameSpec();

        if (typeNameSpec instanceof org.apache.flink.sql.parser.type.SqlMapTypeNameSpec) {
            org.apache.flink.sql.parser.type.SqlMapTypeNameSpec mapSpec =
                    (org.apache.flink.sql.parser.type.SqlMapTypeNameSpec) typeNameSpec;
            SqlDataTypeSpec keyTypeSpec = mapSpec.getKeyType();
            SqlDataTypeSpec valueTypeSpec = mapSpec.getValType();
            DataType keyType = toFlussDataType(keyTypeSpec);
            DataType valueType = toFlussDataType(valueTypeSpec);
            return DataTypes.MAP(keyType, valueType);
        } else if (typeNameSpec instanceof org.apache.calcite.sql.SqlMapTypeNameSpec) {
            org.apache.calcite.sql.SqlMapTypeNameSpec mapSpec =
                    (org.apache.calcite.sql.SqlMapTypeNameSpec) typeNameSpec;
            SqlDataTypeSpec keyTypeSpec = mapSpec.getKeyType();
            SqlDataTypeSpec valueTypeSpec = mapSpec.getValType();
            DataType keyType = toFlussDataType(keyTypeSpec);
            DataType valueType = toFlussDataType(valueTypeSpec);
            return DataTypes.MAP(keyType, valueType);
        }

        throw new IllegalArgumentException("Invalid MAP type specification");
    }

    private static DataType parseRowType(SqlDataTypeSpec dataTypeSpec) {
        SqlTypeNameSpec typeNameSpec = dataTypeSpec.getTypeNameSpec();

        if (typeNameSpec instanceof org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec
                || typeNameSpec instanceof org.apache.calcite.sql.SqlRowTypeNameSpec) {
            java.util.List<org.apache.calcite.sql.SqlIdentifier> fieldNames;
            java.util.List<SqlDataTypeSpec> fieldTypes;

            if (typeNameSpec
                    instanceof org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec) {
                org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec rowSpec =
                        (org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec) typeNameSpec;
                fieldNames = rowSpec.getFieldNames();
                fieldTypes = rowSpec.getFieldTypes();
            } else {
                org.apache.calcite.sql.SqlRowTypeNameSpec rowSpec =
                        (org.apache.calcite.sql.SqlRowTypeNameSpec) typeNameSpec;
                fieldNames = rowSpec.getFieldNames();
                fieldTypes = rowSpec.getFieldTypes();
            }

            java.util.List<org.apache.fluss.types.DataField> dataFields =
                    new java.util.ArrayList<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i).getSimple();
                DataType fieldType = toFlussDataType(fieldTypes.get(i));
                dataFields.add(new org.apache.fluss.types.DataField(fieldName, fieldType));
            }

            return DataTypes.ROW(dataFields.toArray(new org.apache.fluss.types.DataField[0]));
        }

        throw new IllegalArgumentException("Invalid ROW type specification");
    }
}
