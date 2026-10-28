/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;

import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Converts Fluss logical types to Trino types. */
final class FlussTypeConverter {

    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private FlussTypeConverter() {}

    static Type toTrinoType(DataType dataType) {
        checkNotNull(dataType, "dataType is null");

        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return BOOLEAN;
            case TINYINT:
                return TINYINT;
            case SMALLINT:
                return SMALLINT;
            case INTEGER:
                return INTEGER;
            case BIGINT:
                return BIGINT;
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case CHAR:
                {
                    CharType charType = (CharType) dataType;
                    int length = charType.getLength();
                    if (length <= io.trino.spi.type.CharType.MAX_LENGTH) {
                        return io.trino.spi.type.CharType.createCharType(length);
                    }
                    if (length <= VarcharType.MAX_LENGTH) {
                        return VarcharType.createVarcharType(length);
                    }
                    return VARCHAR;
                }

            case STRING:
                return VARCHAR;
            case BINARY:
            case BYTES:
                return VARBINARY;
            case DECIMAL:
                {
                    DecimalType decimalType = (DecimalType) dataType;
                    return io.trino.spi.type.DecimalType.createDecimalType(
                            decimalType.getPrecision(), decimalType.getScale());
                }
            case DATE:
                return DATE;
            case TIME_WITHOUT_TIME_ZONE:
                {
                    TimeType timeType = (TimeType) dataType;
                    return io.trino.spi.type.TimeType.createTimeType(timeType.getPrecision());
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    TimestampType timestampType = (TimestampType) dataType;
                    return io.trino.spi.type.TimestampType.createTimestampType(
                            timestampType.getPrecision());
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    LocalZonedTimestampType timestampType = (LocalZonedTimestampType) dataType;
                    return io.trino.spi.type.TimestampWithTimeZoneType
                            .createTimestampWithTimeZoneType(timestampType.getPrecision());
                }
            case ARRAY:
                {
                    ArrayType arrayType = (ArrayType) dataType;
                    return new io.trino.spi.type.ArrayType(toTrinoType(arrayType.getElementType()));
                }
            case MAP:
                {
                    MapType mapType = (MapType) dataType;
                    Type keyType = toTrinoType(mapType.getKeyType());
                    Type valueType = toTrinoType(mapType.getValueType());
                    if (!keyType.isComparable()) {
                        throw new TrinoException(
                                NOT_SUPPORTED,
                                "Unsupported Fluss map key type: "
                                        + mapType.getKeyType().asSummaryString());
                    }
                    return new io.trino.spi.type.MapType(keyType, valueType, TYPE_OPERATORS);
                }
            case ROW:
                {
                    RowType rowType = (RowType) dataType;
                    return io.trino.spi.type.RowType.from(
                            rowType.getFields().stream()
                                    .map(
                                            field ->
                                                    io.trino.spi.type.RowType.field(
                                                            field.getName(),
                                                            toTrinoType(field.getType())))
                                    .collect(Collectors.toList()));
                }
            default:
                throw new TrinoException(
                        NOT_SUPPORTED, "Unsupported Fluss type: " + dataType.asSummaryString());
        }
    }
}
