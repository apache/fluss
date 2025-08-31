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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
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

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Convert from Fluss's data type to Iceberg's data type. */
public class FlussDataTypeToIcebergDataType implements DataTypeVisitor<Type> {

    public static final FlussDataTypeToIcebergDataType INSTANCE =
            new FlussDataTypeToIcebergDataType();

    @Override
    public Type visit(CharType charType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(StringType stringType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(BooleanType booleanType) {
        return Types.BooleanType.get();
    }

    @Override
    public Type visit(BinaryType binaryType) {
        return Types.BinaryType.get();
    }

    @Override
    public Type visit(BytesType bytesType) {
        return Types.BinaryType.get();
    }

    @Override
    public Type visit(DecimalType decimalType) {
        return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public Type visit(TinyIntType tinyIntType) {
        return Types.IntegerType.get();
    }

    @Override
    public Type visit(SmallIntType smallIntType) {
        return Types.IntegerType.get();
    }

    @Override
    public Type visit(IntType intType) {
        return Types.IntegerType.get();
    }

    @Override
    public Type visit(BigIntType bigIntType) {
        return Types.LongType.get();
    }

    @Override
    public Type visit(FloatType floatType) {
        return Types.FloatType.get();
    }

    @Override
    public Type visit(DoubleType doubleType) {
        return Types.DoubleType.get();
    }

    @Override
    public Type visit(DateType dateType) {
        return Types.DateType.get();
    }

    @Override
    public Type visit(TimeType timeType) {
        return Types.TimeType.get();
    }

    @Override
    public Type visit(TimestampType timestampType) {
        return Types.TimestampType.withoutZone();
    }

    @Override
    public Type visit(LocalZonedTimestampType localZonedTimestampType) {
        return Types.TimestampType.withZone();
    }

    @Override
    public Type visit(ArrayType arrayType) {
        throw new UnsupportedOperationException("Unsupported array type");
    }

    @Override
    public Type visit(MapType mapType) {
        throw new UnsupportedOperationException("Unsupported map type");
    }

    @Override
    public Type visit(RowType rowType) {
        throw new UnsupportedOperationException("Unsupported row type");
    }
}
