/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.spark.utils;

import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/** Convert Fluss's {@link com.alibaba.fluss.types.DataType} to Spark's {@link DataType}. */
public class FlussTypeToSparkType implements DataTypeVisitor<DataType> {

    static final FlussTypeToSparkType INSTANCE = new FlussTypeToSparkType();

    @Override
    public DataType visit(CharType charType) {
        return new org.apache.spark.sql.types.CharType(charType.getLength());
    }

    @Override
    public DataType visit(StringType stringType) {
        return DataTypes.StringType;
    }

    @Override
    public DataType visit(BooleanType booleanType) {
        return DataTypes.BooleanType;
    }

    @Override
    public DataType visit(BinaryType binaryType) {
        return DataTypes.BinaryType;
    }

    @Override
    public DataType visit(BytesType bytesType) {
        return DataTypes.BinaryType;
    }

    @Override
    public DataType visit(DecimalType decimalType) {
        return DataTypes.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public DataType visit(TinyIntType tinyIntType) {
        return DataTypes.ByteType;
    }

    @Override
    public DataType visit(SmallIntType smallIntType) {
        return DataTypes.ShortType;
    }

    @Override
    public DataType visit(IntType intType) {
        return DataTypes.IntegerType;
    }

    @Override
    public DataType visit(BigIntType bigIntType) {
        return DataTypes.LongType;
    }

    @Override
    public DataType visit(FloatType floatType) {
        return DataTypes.FloatType;
    }

    @Override
    public DataType visit(DoubleType doubleType) {
        return DataTypes.DoubleType;
    }

    @Override
    public DataType visit(DateType dateType) {
        return DataTypes.DateType;
    }

    @Override
    public DataType visit(TimeType timeType) {
        // spark does not support Time type, use int to represent it
        return DataTypes.IntegerType;
    }

    @Override
    public DataType visit(TimestampType timestampType) {
        // without time zone (spark only support microsecond)
        return DataTypes.TimestampNTZType;
    }

    @Override
    public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
        // with local time zone (spark only support microsecond)
        return DataTypes.TimestampType;
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        // TODO: support ArrayType
        throw new UnsupportedOperationException("UnSupport ArrayType now");
    }

    @Override
    public DataType visit(MapType mapType) {
        // TODO: support MapType
        throw new UnsupportedOperationException("UnSupport MapType now");
    }

    @Override
    public DataType visit(RowType rowType) {
        // TODO: support RowType
        throw new UnsupportedOperationException("UnSupport RowType now");
    }
}
