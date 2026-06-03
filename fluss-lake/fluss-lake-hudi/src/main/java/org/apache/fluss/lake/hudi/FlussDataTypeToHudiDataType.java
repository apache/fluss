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

package org.apache.fluss.lake.hudi;

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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import javax.annotation.concurrent.ThreadSafe;

import static org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils.FILE_SYSTEM_TYPE;
import static org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils.HIVE_META_STORE_TYPE;

/**
 * Converter from Fluss's data type to Flink's internal data type used by Hudi.
 *
 * <p>Hudi's Flink catalog accepts Flink {@link DataType} through Flink catalog APIs and then
 * handles the conversion to Hudi's internal schema itself. Therefore, Fluss only needs to bridge
 * its type system to Flink's type system here.
 */
@ThreadSafe
public class FlussDataTypeToHudiDataType implements DataTypeVisitor<DataType> {

    private final String catalogMode;

    private FlussDataTypeToHudiDataType(String catalogMode) {
        this.catalogMode = catalogMode;
    }

    public static final FlussDataTypeToHudiDataType DFS_INSTANCE =
            new FlussDataTypeToHudiDataType(FILE_SYSTEM_TYPE);
    public static final FlussDataTypeToHudiDataType HMS_INSTANCE =
            new FlussDataTypeToHudiDataType(HIVE_META_STORE_TYPE);

    @Override
    public DataType visit(CharType charType) {
        return withNullability(DataTypes.STRING(), charType.isNullable());
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
        // Hudi IntType covers 8/16/32-bit integers (consistent with internal conversion logic)
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
        return withNullability(DataTypes.TIME(timeType.getPrecision()), timeType.isNullable());
    }

    @Override
    public DataType visit(TimestampType timestampType) {
        return withNullability(
                DataTypes.TIMESTAMP(timestampType.getPrecision()), timestampType.isNullable());
    }

    @Override
    public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
        return HIVE_META_STORE_TYPE.equals(catalogMode)
                ? withNullability(DataTypes.BIGINT(), localZonedTimestampType.isNullable())
                : withNullability(
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                        localZonedTimestampType.isNullable());
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        DataType elementType = arrayType.getElementType().accept(this);
        return withNullability(DataTypes.ARRAY(elementType), arrayType.isNullable());
    }

    @Override
    public DataType visit(MapType mapType) {
        DataType keyType = mapType.getKeyType().accept(this);
        DataType valueType = mapType.getValueType().accept(this);
        return withNullability(DataTypes.MAP(keyType, valueType), mapType.isNullable());
    }

    @Override
    public DataType visit(RowType rowType) {
        DataTypes.Field[] fields = new DataTypes.Field[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            DataType fieldType = rowType.getTypeAt(i).accept(this);
            fields[i] = DataTypes.FIELD(fieldName, fieldType);
        }
        return withNullability(DataTypes.ROW(fields), rowType.isNullable());
    }

    private DataType withNullability(DataType flinkDataType, boolean nullable) {
        if (flinkDataType.getLogicalType().isNullable() != nullable) {
            return nullable ? flinkDataType.nullable() : flinkDataType.notNull();
        }
        return flinkDataType;
    }
}
