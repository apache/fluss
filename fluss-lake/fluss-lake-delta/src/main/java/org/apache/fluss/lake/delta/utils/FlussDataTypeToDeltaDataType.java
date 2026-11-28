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

package org.apache.fluss.lake.delta.utils;

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

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a Fluss data type to a Delta data type.
 *
 * <p>Fluss {@code TIMESTAMP} has no time zone and maps to Delta {@code TIMESTAMP_NTZ}, while Fluss
 * {@code TIMESTAMP_LTZ} maps to Delta {@code TIMESTAMP}, which is stored in UTC. Delta has no
 * {@code TIME} type, so tables with {@code TIME} columns are rejected.
 */
public class FlussDataTypeToDeltaDataType implements DataTypeVisitor<DataType> {

    public static final FlussDataTypeToDeltaDataType INSTANCE = new FlussDataTypeToDeltaDataType();

    private FlussDataTypeToDeltaDataType() {}

    /** Converts a Fluss row type to a Delta schema, keeping field order and nullability. */
    public static StructType toDeltaSchema(RowType rowType) {
        return (StructType) rowType.accept(INSTANCE);
    }

    @Override
    public DataType visit(CharType charType) {
        return io.delta.kernel.types.StringType.STRING;
    }

    @Override
    public DataType visit(StringType stringType) {
        return io.delta.kernel.types.StringType.STRING;
    }

    @Override
    public DataType visit(BooleanType booleanType) {
        return io.delta.kernel.types.BooleanType.BOOLEAN;
    }

    @Override
    public DataType visit(BinaryType binaryType) {
        return io.delta.kernel.types.BinaryType.BINARY;
    }

    @Override
    public DataType visit(BytesType bytesType) {
        return io.delta.kernel.types.BinaryType.BINARY;
    }

    @Override
    public DataType visit(DecimalType decimalType) {
        return new io.delta.kernel.types.DecimalType(
                decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public DataType visit(TinyIntType tinyIntType) {
        return io.delta.kernel.types.ByteType.BYTE;
    }

    @Override
    public DataType visit(SmallIntType smallIntType) {
        return io.delta.kernel.types.ShortType.SHORT;
    }

    @Override
    public DataType visit(IntType intType) {
        return io.delta.kernel.types.IntegerType.INTEGER;
    }

    @Override
    public DataType visit(BigIntType bigIntType) {
        return io.delta.kernel.types.LongType.LONG;
    }

    @Override
    public DataType visit(FloatType floatType) {
        return io.delta.kernel.types.FloatType.FLOAT;
    }

    @Override
    public DataType visit(DoubleType doubleType) {
        return io.delta.kernel.types.DoubleType.DOUBLE;
    }

    @Override
    public DataType visit(DateType dateType) {
        return io.delta.kernel.types.DateType.DATE;
    }

    @Override
    public DataType visit(TimeType timeType) {
        throw new UnsupportedOperationException("Delta Lake does not support the TIME type.");
    }

    @Override
    public DataType visit(TimestampType timestampType) {
        return io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ;
    }

    @Override
    public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
        return io.delta.kernel.types.TimestampType.TIMESTAMP;
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return new io.delta.kernel.types.ArrayType(
                arrayType.getElementType().accept(this), arrayType.getElementType().isNullable());
    }

    @Override
    public DataType visit(MapType mapType) {
        return new io.delta.kernel.types.MapType(
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this),
                mapType.getValueType().isNullable());
    }

    @Override
    public DataType visit(RowType rowType) {
        List<StructField> fields = new ArrayList<>(rowType.getFieldCount());
        for (DataField field : rowType.getFields()) {
            fields.add(
                    new StructField(
                            field.getName(),
                            field.getType().accept(this),
                            field.getType().isNullable()));
        }
        return new StructType(fields);
    }
}
