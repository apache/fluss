/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.converter;

import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.MapType;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.time.Instant;

/** Adapter class for converting Fluss InternalArray to Pojo Array. */
public class FlussArrayToPojoArray {
    private final InternalArray flussArray;
    private final DataType elementType;
    private final String fieldName;
    private final Class<?> pojoType;

    public FlussArrayToPojoArray(
            InternalArray flussArray, DataType elementType, String fieldName, Class<?> pojoType) {
        this.flussArray = flussArray;
        this.elementType = elementType;
        this.fieldName = fieldName;
        this.pojoType = pojoType != null ? pojoType : Object.class;
    }

    public Object convertArray() {
        if (flussArray == null) {
            return null;
        }

        int size = flussArray.size();
        // Create array with proper component type
        Object result = Array.newInstance(pojoType, size);
        for (int i = 0; i < size; i++) {
            Array.set(result, i, convertElementValue(i));
        }
        return result;
    }

    public @Nullable Object convertElementValue(int index) {
        if (flussArray.isNullAt(index)) {
            return null;
        }

        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return flussArray.getBoolean(index);
            case TINYINT:
                return flussArray.getByte(index);
            case SMALLINT:
                return flussArray.getShort(index);
            case INTEGER:
                return flussArray.getInt(index);
            case BIGINT:
                return flussArray.getLong(index);
            case FLOAT:
                return flussArray.getFloat(index);
            case DOUBLE:
                return flussArray.getDouble(index);
            case CHAR:
            case STRING:
                return FlussTypeToPojoTypeConverter.convertTextValue(
                        elementType, fieldName, String.class, flussArray.getString(index));
            case BINARY:
            case BYTES:
                return flussArray.getBytes(index);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) elementType;
                Decimal d =
                        flussArray.getDecimal(
                                index, decimalType.getPrecision(), decimalType.getScale());
                return FlussTypeToPojoTypeConverter.convertDecimalValue(d);
            case DATE:
                return FlussTypeToPojoTypeConverter.convertDateValue(flussArray.getInt(index));
            case TIME_WITHOUT_TIME_ZONE:
                return FlussTypeToPojoTypeConverter.convertTimeValue(flussArray.getInt(index));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(elementType);
                    return FlussTypeToPojoTypeConverter.convertTimestampNtzValue(
                            flussArray.getTimestampNtz(index, precision));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(elementType);
                    return FlussTypeToPojoTypeConverter.convertTimestampLtzValue(
                            flussArray.getTimestampLtz(index, precision), fieldName, Instant.class);
                }
            case ARRAY:
                ArrayType nestedArrayType = (ArrayType) elementType;
                InternalArray innerArray = flussArray.getArray(index);
                return innerArray == null
                        ? null
                        : new FlussArrayToPojoArray(
                                        innerArray,
                                        nestedArrayType.getElementType(),
                                        fieldName,
                                        pojoType.getComponentType())
                                .convertArray();
            case MAP:
                return new FlussMapToPojoMap(
                                flussArray.getMap(index), (MapType) elementType, fieldName)
                        .convertMap();
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported field type %s for field %s.",
                                elementType.getTypeRoot(), fieldName));
        }
    }
}
