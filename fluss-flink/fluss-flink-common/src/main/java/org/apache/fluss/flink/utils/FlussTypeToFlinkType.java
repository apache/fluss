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

package org.apache.fluss.flink.utils;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypeVisitor;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;

/** Convert Fluss's {@link org.apache.fluss.types.DataType} to Flink's {@link DataType}. */
class FlussTypeToFlinkType implements DataTypeVisitor<DataType> {

    static final FlussTypeToFlinkType INSTANCE = new FlussTypeToFlinkType();

    @Override
    public DataType visit(CharType charType) {
        return withNullability(DataTypes.CHAR(charType.getLength()), charType.isNullable());
    }

    @Override
    public DataType visit(StringType stringType) {
        return withNullability(DataTypes.STRING(), stringType.isNullable());
    }

    @Override
    public DataType visit(BooleanType booleanType) {
        return withNullability(DataTypes.BOOLEAN(), booleanType.isNullable());
    }

    @Override
    public DataType visit(BinaryType binaryType) {
        return withNullability(DataTypes.BINARY(binaryType.getLength()), binaryType.isNullable());
    }

    @Override
    public DataType visit(BytesType bytesType) {
        return withNullability(DataTypes.BYTES(), bytesType.isNullable());
    }

    @Override
    public DataType visit(DecimalType decimalType) {
        return withNullability(
                DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale()),
                decimalType.isNullable());
    }

    @Override
    public DataType visit(TinyIntType tinyIntType) {
        return withNullability(DataTypes.TINYINT(), tinyIntType.isNullable());
    }

    @Override
    public DataType visit(SmallIntType smallIntType) {
        return withNullability(DataTypes.SMALLINT(), smallIntType.isNullable());
    }

    @Override
    public DataType visit(IntType intType) {
        return withNullability(DataTypes.INT(), intType.isNullable());
    }

    @Override
    public DataType visit(BigIntType bigIntType) {
        return withNullability(DataTypes.BIGINT(), bigIntType.isNullable());
    }

    @Override
    public DataType visit(FloatType floatType) {
        return withNullability(DataTypes.FLOAT(), floatType.isNullable());
    }

    @Override
    public DataType visit(DoubleType doubleType) {
        return withNullability(DataTypes.DOUBLE(), doubleType.isNullable());
    }

    @Override
    public DataType visit(DateType dateType) {
        return withNullability(DataTypes.DATE(), dateType.isNullable());
    }

    @Override
    public DataType visit(TimeType timeType) {
        return withNullability(DataTypes.TIME(), timeType.isNullable());
    }

    @Override
    public DataType visit(TimestampType timestampType) {
        return withNullability(
                DataTypes.TIMESTAMP(timestampType.getPrecision()), timestampType.isNullable());
    }

    @Override
    public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
        return withNullability(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(localZonedTimestampType.getPrecision()),
                localZonedTimestampType.isNullable());
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return withNullability(
                DataTypes.ARRAY(arrayType.getElementType().accept(this)), arrayType.isNullable());
    }

    @Override
    public DataType visit(MapType mapType) {
        return withNullability(
                DataTypes.MAP(
                        mapType.getKeyType().accept(this), mapType.getValueType().accept(this)),
                mapType.isNullable());
    }

    @Override
    public DataType visit(RowType rowType) {
        List<DataTypes.Field> dataFields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            DataTypes.Field dataTypeField;
            if (field.getDescription().isPresent()) {
                dataTypeField =
                        DataTypes.FIELD(
                                field.getName(),
                                field.getType().accept(this),
                                field.getDescription().get());
            } else {
                dataTypeField = DataTypes.FIELD(field.getName(), field.getType().accept(this));
            }
            dataFields.add(dataTypeField);
        }
        return withNullability(DataTypes.ROW(dataFields), rowType.isNullable());
    }

    private DataType withNullability(DataType flinkType, boolean nullable) {
        if (flinkType.getLogicalType().isNullable() != nullable) {
            return nullable ? flinkType.nullable() : flinkType.notNull();
        }
        return flinkType;
    }
}
