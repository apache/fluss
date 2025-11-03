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

package org.apache.fluss.connector.trino.typeutils;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
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

import io.airlift.log.Logger;
import io.airlift.log.Logger;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility class for converting between Fluss and Trino types.
 */
public class FlussTypeUtils {

    private static final Logger log = Logger.get(FlussTypeUtils.class);

    private FlussTypeUtils() {
        // utility class
    }

    /**
     * Convert Fluss DataType to Trino Type.
     */
    public static Type toTrinoType(DataType flussType, TypeManager typeManager) {
        DataTypeRoot typeRoot = flussType.getTypeRoot();
        
        switch (typeRoot) {
            case BOOLEAN:
                return io.trino.spi.type.BooleanType.BOOLEAN;
            case TINYINT:
                return TinyintType.TINYINT;
            case SMALLINT:
                return SmallintType.SMALLINT;
            case INTEGER:
                return IntegerType.INTEGER;
            case BIGINT:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return io.trino.spi.type.DoubleType.DOUBLE;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) flussType;
                return io.trino.spi.type.DecimalType.createDecimalType(
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case CHAR:
                CharType charType = (CharType) flussType;
                return io.trino.spi.type.CharType.createCharType(charType.getLength());
            case STRING:
                return VarcharType.VARCHAR;
            case BINARY:
                return VarbinaryType.VARBINARY;
            case BYTES:
                return VarbinaryType.VARBINARY;
            case DATE:
                return io.trino.spi.type.DateType.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                TimeType timeType = (TimeType) flussType;
                return io.trino.spi.type.TimeType.createTimeType(timeType.getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) flussType;
                return io.trino.spi.type.TimestampType.createTimestampType(timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedType = (LocalZonedTimestampType) flussType;
                return TimestampWithTimeZoneType.createTimestampWithTimeZoneType(
                        localZonedType.getPrecision());
            case ARRAY:
                ArrayType arrayType = (ArrayType) flussType;
                Type elementType = toTrinoType(arrayType.getElementType(), typeManager);
                return new io.trino.spi.type.ArrayType(elementType);
            case MAP:
                MapType mapType = (MapType) flussType;
                Type keyType = toTrinoType(mapType.getKeyType(), typeManager);
                Type valueType = toTrinoType(mapType.getValueType(), typeManager);
                return new io.trino.spi.type.MapType(keyType, valueType, typeManager.getTypeOperators());
            case ROW:
                RowType rowType = (RowType) flussType;
                return convertRowType(rowType, typeManager);
            default:
                log.warn("Unsupported Fluss type: %s, using VARCHAR as fallback", typeRoot);
                return VarcharType.VARCHAR;
        }
    }

    /**
     * Convert Fluss RowType to Trino RowType.
     */
    private static Type convertRowType(RowType rowType, TypeManager typeManager) {
        List<io.trino.spi.type.RowType.Field> fields = new ArrayList<>();
        
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            DataType fieldType = rowType.getTypeAt(i);
            Type trinoFieldType = toTrinoType(fieldType, typeManager);
            
            fields.add(io.trino.spi.type.RowType.field(fieldName, trinoFieldType));
        }
        
        return io.trino.spi.type.RowType.from(fields);
    }

    /**
     * Convert Trino Type to Fluss DataType.
     * 
     * <p>This method handles the conversion from Trino types to Fluss data types.
     * It supports all basic types as well as complex types like arrays, maps, and rows.
     */
    public static Optional<DataType> toFlussType(Type trinoType) {
        // String representation of the type
        String typeString = trinoType.getDisplayName().toLowerCase();
        
        if (trinoType instanceof io.trino.spi.type.BooleanType) {
            return Optional.of(new BooleanType());
        } else if (trinoType instanceof TinyintType) {
            return Optional.of(new TinyIntType());
        } else if (trinoType instanceof SmallintType) {
            return Optional.of(new SmallIntType());
        } else if (trinoType instanceof IntegerType) {
            return Optional.of(new IntType());
        } else if (trinoType instanceof BigintType) {
            return Optional.of(new BigIntType());
        } else if (trinoType instanceof RealType) {
            return Optional.of(new FloatType());
        } else if (trinoType instanceof io.trino.spi.type.DoubleType) {
            return Optional.of(new DoubleType());
        } else if (trinoType instanceof io.trino.spi.type.DecimalType) {
            io.trino.spi.type.DecimalType decimalType = (io.trino.spi.type.DecimalType) trinoType;
            return Optional.of(new DecimalType(
                    decimalType.getPrecision(),
                    decimalType.getScale()));
        } else if (trinoType instanceof io.trino.spi.type.CharType) {
            io.trino.spi.type.CharType charType = (io.trino.spi.type.CharType) trinoType;
            return Optional.of(new CharType(charType.getLength()));
        } else if (trinoType instanceof VarcharType) {
            return Optional.of(new StringType());
        } else if (trinoType instanceof VarbinaryType) {
            return Optional.of(new BytesType());
        } else if (trinoType instanceof io.trino.spi.type.DateType) {
            return Optional.of(new DateType());
        } else if (trinoType instanceof io.trino.spi.type.TimeType) {
            io.trino.spi.type.TimeType timeType = (io.trino.spi.type.TimeType) trinoType;
            return Optional.of(new TimeType(timeType.getPrecision()));
        } else if (trinoType instanceof io.trino.spi.type.TimestampType) {
            io.trino.spi.type.TimestampType timestampType = (io.trino.spi.type.TimestampType) trinoType;
            return Optional.of(new TimestampType(timestampType.getPrecision()));
        } else if (trinoType instanceof TimestampWithTimeZoneType) {
            TimestampWithTimeZoneType timestampTzType = (TimestampWithTimeZoneType) trinoType;
            return Optional.of(new LocalZonedTimestampType(timestampTzType.getPrecision()));
        } else if (trinoType instanceof io.trino.spi.type.ArrayType) {
            io.trino.spi.type.ArrayType arrayType = (io.trino.spi.type.ArrayType) trinoType;
            Optional<DataType> elementType = toFlussType(arrayType.getElementType());
            if (elementType.isPresent()) {
                return Optional.of(new ArrayType(elementType.get()));
            }
        } else if (trinoType instanceof io.trino.spi.type.MapType) {
            io.trino.spi.type.MapType mapType = (io.trino.spi.type.MapType) trinoType;
            Optional<DataType> keyType = toFlussType(mapType.getKeyType());
            Optional<DataType> valueType = toFlussType(mapType.getValueType());
            if (keyType.isPresent() && valueType.isPresent()) {
                return Optional.of(new MapType(keyType.get(), valueType.get()));
            }
        } else if (trinoType instanceof io.trino.spi.type.RowType) {
            io.trino.spi.type.RowType rowType = (io.trino.spi.type.RowType) trinoType;
            List<DataType> fieldTypes = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();
            boolean allFieldsSupported = true;
            
            for (io.trino.spi.type.RowType.Field field : rowType.getFields()) {
                Optional<DataType> fieldType = toFlussType(field.getType());
                if (fieldType.isPresent()) {
                    fieldTypes.add(fieldType.get());
                    fieldNames.add(field.getName().orElse("field" + fieldNames.size()));
                } else {
                    allFieldsSupported = false;
                    break;
                }
            }
            
            if (allFieldsSupported) {
                return Optional.of(new RowType(fieldNames, fieldTypes));
            }
        }
        
        log.warn("Unsupported Trino type conversion: %s", trinoType);
        return Optional.empty();
    }
}
