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

import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.StringType;

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
        return new org.apache.fluss.types.CharType(charType.isNullable(), charType.getLength());
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
        return new org.apache.fluss.types.BooleanType(booleanType.isNullable());
    }

    @Override
    public DataType visit(BinaryType binaryType) {
        return new org.apache.fluss.types.BinaryType(
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
        return new org.apache.fluss.types.DecimalType(
                decimalType.isNullable(), decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public DataType visit(TinyIntType tinyIntType) {
        return new org.apache.fluss.types.TinyIntType(tinyIntType.isNullable());
    }

    @Override
    public DataType visit(SmallIntType smallIntType) {
        return new org.apache.fluss.types.SmallIntType(smallIntType.isNullable());
    }

    @Override
    public DataType visit(IntType intType) {
        return new org.apache.fluss.types.IntType(intType.isNullable());
    }

    @Override
    public DataType visit(BigIntType bigIntType) {
        return new org.apache.fluss.types.BigIntType(bigIntType.isNullable());
    }

    @Override
    public DataType visit(FloatType floatType) {
        return new org.apache.fluss.types.FloatType(floatType.isNullable());
    }

    @Override
    public DataType visit(DoubleType doubleType) {
        return new org.apache.fluss.types.DoubleType(doubleType.isNullable());
    }

    @Override
    public DataType visit(DateType dateType) {
        return new org.apache.fluss.types.DateType(dateType.isNullable());
    }

    @Override
    public DataType visit(TimeType timeType) {
        return new org.apache.fluss.types.TimeType(timeType.isNullable(), timeType.getPrecision());
    }

    @Override
    public DataType visit(TimestampType timestampType) {
        return new org.apache.fluss.types.TimestampType(
                timestampType.isNullable(), timestampType.getPrecision());
    }

    @Override
    public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
        return new org.apache.fluss.types.LocalZonedTimestampType(
                localZonedTimestampType.isNullable(), localZonedTimestampType.getPrecision());
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return new org.apache.fluss.types.ArrayType(
                arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MapType mapType) {
        return new org.apache.fluss.types.MapType(
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
        return new org.apache.fluss.types.RowType(rowType.isNullable(), dataFields);
    }

    @Override
    protected DataType defaultMethod(LogicalType logicalType) {
        throw new UnsupportedOperationException("Unsupported data type: " + logicalType);
    }
}
