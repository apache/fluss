/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.StringType;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import java.util.ArrayList;
import java.util.List;

/** Convert Flink's {@link LogicalType} to Fluss {@link DataType}. */
class FlinkTypeToFlussType extends LogicalTypeDefaultVisitor<DataType> {

    static final FlinkTypeToFlussType INSTANCE = new FlinkTypeToFlussType();

    @Override
    public DataType visit(CharType charType) {
        return new com.alibaba.fluss.types.CharType(charType.isNullable(), charType.getLength());
    }

    @Override
    public DataType visit(VarCharType varCharType) {
        if (varCharType.getLength() == Integer.MAX_VALUE) {
            return new StringType(varCharType.isNullable());
        } else {
            return defaultMethod(varCharType);
        }
    }

    @Override
    public DataType visit(BooleanType booleanType) {
        return new com.alibaba.fluss.types.BooleanType(booleanType.isNullable());
    }

    @Override
    public DataType visit(BinaryType binaryType) {
        return new com.alibaba.fluss.types.BinaryType(
                binaryType.isNullable(), binaryType.getLength());
    }

    @Override
    public DataType visit(VarBinaryType varBinaryType) {
        if (varBinaryType.getLength() == Integer.MAX_VALUE) {
            return new BytesType(varBinaryType.isNullable());
        } else {
            return defaultMethod(varBinaryType);
        }
    }

    @Override
    public DataType visit(DecimalType decimalType) {
        return new com.alibaba.fluss.types.DecimalType(
                decimalType.isNullable(), decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public DataType visit(TinyIntType tinyIntType) {
        return new com.alibaba.fluss.types.TinyIntType(tinyIntType.isNullable());
    }

    @Override
    public DataType visit(SmallIntType smallIntType) {
        return new com.alibaba.fluss.types.SmallIntType(smallIntType.isNullable());
    }

    @Override
    public DataType visit(IntType intType) {
        return new com.alibaba.fluss.types.IntType(intType.isNullable());
    }

    @Override
    public DataType visit(BigIntType bigIntType) {
        return new com.alibaba.fluss.types.BigIntType(bigIntType.isNullable());
    }

    @Override
    public DataType visit(FloatType floatType) {
        return new com.alibaba.fluss.types.FloatType(floatType.isNullable());
    }

    @Override
    public DataType visit(DoubleType doubleType) {
        return new com.alibaba.fluss.types.DoubleType(doubleType.isNullable());
    }

    @Override
    public DataType visit(DateType dateType) {
        return new com.alibaba.fluss.types.DateType(dateType.isNullable());
    }

    @Override
    public DataType visit(TimeType timeType) {
        return new com.alibaba.fluss.types.TimeType(timeType.isNullable(), timeType.getPrecision());
    }

    @Override
    public DataType visit(TimestampType timestampType) {
        return new com.alibaba.fluss.types.TimestampType(
                timestampType.isNullable(), timestampType.getPrecision());
    }

    @Override
    public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
        return new com.alibaba.fluss.types.LocalZonedTimestampType(
                localZonedTimestampType.isNullable(), localZonedTimestampType.getPrecision());
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return new com.alibaba.fluss.types.ArrayType(
                arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MapType mapType) {
        return new com.alibaba.fluss.types.MapType(
                mapType.isNullable(),
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    @Override
    public DataType visit(RowType rowType) {
        List<DataField> dataFields = new ArrayList<>();
        for (RowType.RowField field : rowType.getFields()) {
            DataType fieldType = field.getType().accept(this);
            dataFields.add(
                    new DataField(field.getName(), fieldType, field.getDescription().orElse(null)));
        }
        return new com.alibaba.fluss.types.RowType(rowType.isNullable(), dataFields);
    }

    @Override
    protected DataType defaultMethod(LogicalType logicalType) {
        throw new UnsupportedOperationException("Unsupported data type: " + logicalType);
    }
}
