/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink;

import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypeVisitor;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

/** Convert from Fluss's data type to Paimon's data type. */
public class FlussDataTypeToPaimonDataType implements DataTypeVisitor<DataType> {

    public static final FlussDataTypeToPaimonDataType INSTANCE =
            new FlussDataTypeToPaimonDataType();

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
        org.apache.paimon.types.RowType.Builder rowTypeBuilder =
                org.apache.paimon.types.RowType.builder();
        for (DataField field : rowType.getFields()) {
            rowTypeBuilder.field(
                    field.getName(),
                    field.getType().accept(this),
                    field.getDescription().orElse(null));
        }
        return withNullability(rowTypeBuilder.build(), rowType.isNullable());
    }

    private DataType withNullability(DataType paimon, boolean nullable) {
        if (paimon.isNullable() != nullable) {
            return nullable ? paimon.nullable() : paimon.notNull();
        }
        return paimon;
    }
}
