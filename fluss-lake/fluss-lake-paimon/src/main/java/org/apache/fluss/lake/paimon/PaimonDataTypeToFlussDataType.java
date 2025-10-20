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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

/** Convert from Paimon's data type to Fluss's data type. */
public class PaimonDataTypeToFlussDataType implements DataTypeVisitor<DataType> {

    public static final PaimonDataTypeToFlussDataType INSTANCE =
            new PaimonDataTypeToFlussDataType();

    @Override
    public DataType visit(CharType charType) {
        return withNullability(DataTypes.CHAR(charType.getLength()), charType.isNullable());
    }

    @Override
    public DataType visit(VarCharType varCharType) {
        return withNullability(DataTypes.STRING(), varCharType.isNullable());
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
    public DataType visit(VarBinaryType varBinaryType) {
        return withNullability(DataTypes.BYTES(), varBinaryType.isNullable());
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
                DataTypes.TIMESTAMP_LTZ(localZonedTimestampType.getPrecision()),
                localZonedTimestampType.isNullable());
    }

    @Override
    public DataType visit(VariantType variantType) {
        throw new UnsupportedOperationException("VariantType is not supported for Fluss.");
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return withNullability(
                DataTypes.ARRAY(arrayType.getElementType().accept(this)), arrayType.isNullable());
    }

    @Override
    public DataType visit(MultisetType multisetType) {
        throw new UnsupportedOperationException("MultisetType is not supported for Fluss.");
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
        org.apache.fluss.types.RowType.Builder rowTypeBuilder =
                org.apache.fluss.types.RowType.builder();
        for (DataField field : rowType.getFields()) {
            rowTypeBuilder.field(field.name(), field.type().accept(this), field.description());
        }
        return withNullability(rowTypeBuilder.build(), rowType.isNullable());
    }

    private DataType withNullability(DataType fluss, boolean nullable) {
        if (fluss.isNullable() != nullable) {
            return nullable ? fluss.nullable() : fluss.notNull();
        }
        return fluss;
    }
}
