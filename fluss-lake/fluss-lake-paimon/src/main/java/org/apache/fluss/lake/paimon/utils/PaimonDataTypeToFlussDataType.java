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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.types.DataTypes;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

/**
 * Convert from Paimon's data type to Fluss's data type (inverse of {@link
 * FlussDataTypeToPaimonDataType}).
 */
public class PaimonDataTypeToFlussDataType
        extends DataTypeDefaultVisitor<org.apache.fluss.types.DataType> {

    public static final PaimonDataTypeToFlussDataType INSTANCE =
            new PaimonDataTypeToFlussDataType();

    @Override
    public org.apache.fluss.types.DataType visit(CharType charType) {
        return withNullability(DataTypes.CHAR(charType.getLength()), charType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(VarCharType varCharType) {
        return withNullability(DataTypes.STRING(), varCharType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(BooleanType booleanType) {
        return withNullability(DataTypes.BOOLEAN(), booleanType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(BinaryType binaryType) {
        return withNullability(DataTypes.BINARY(binaryType.getLength()), binaryType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(VarBinaryType varBinaryType) {
        return withNullability(DataTypes.BYTES(), varBinaryType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(DecimalType decimalType) {
        return withNullability(
                DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale()), decimalType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(TinyIntType tinyIntType) {
        return withNullability(DataTypes.TINYINT(), tinyIntType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(SmallIntType smallIntType) {
        return withNullability(DataTypes.SMALLINT(), smallIntType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(IntType intType) {
        return withNullability(DataTypes.INT(), intType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(BigIntType bigIntType) {
        return withNullability(DataTypes.BIGINT(), bigIntType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(FloatType floatType) {
        return withNullability(DataTypes.FLOAT(), floatType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(DoubleType doubleType) {
        return withNullability(DataTypes.DOUBLE(), doubleType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(DateType dateType) {
        return withNullability(DataTypes.DATE(), dateType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(TimeType timeType) {
        return withNullability(DataTypes.TIME(timeType.getPrecision()), timeType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(TimestampType timestampType) {
        return withNullability(DataTypes.TIMESTAMP(timestampType.getPrecision()), timestampType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(LocalZonedTimestampType localZonedTimestampType) {
        return withNullability(
                DataTypes.TIMESTAMP_LTZ(localZonedTimestampType.getPrecision()),
                localZonedTimestampType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(ArrayType arrayType) {
        return withNullability(DataTypes.ARRAY(arrayType.getElementType().accept(this)), arrayType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(MapType mapType) {
        return withNullability(
                DataTypes.MAP(
                        mapType.getKeyType().accept(this), mapType.getValueType().accept(this)),
                mapType);
    }

    @Override
    public org.apache.fluss.types.DataType visit(RowType rowType) {
        org.apache.fluss.types.RowType.Builder builder = org.apache.fluss.types.RowType.builder();
        for (DataField field : rowType.getFields()) {
            org.apache.fluss.types.DataType fieldType = field.type().accept(this);
            if (field.description() == null) {
                builder.field(field.name(), fieldType);
            } else {
                builder.field(field.name(), fieldType, field.description());
            }
        }
        return withNullability(builder.build(), rowType);
    }

    @Override
    protected org.apache.fluss.types.DataType defaultMethod(DataType dataType) {
        throw new UnsupportedOperationException(
                "Unsupported data type to convert to Fluss: " + dataType.getTypeRoot());
    }

    private static org.apache.fluss.types.DataType withNullability(
            org.apache.fluss.types.DataType flussType, DataType paimonType) {
        return flussType.copy(paimonType.isNullable());
    }
}
