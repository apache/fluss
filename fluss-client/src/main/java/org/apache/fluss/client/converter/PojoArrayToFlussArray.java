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

import org.apache.fluss.row.GenericArray;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DecimalType;

import javax.annotation.Nullable;

import java.util.Collection;

/** Adapter class for converting Pojo Array to Fluss InternalArray. */
public class PojoArrayToFlussArray {
    private final Object obj;
    private final DataType fieldType;
    private final String fieldName;

    public PojoArrayToFlussArray(Object obj, DataType fieldType, String fieldName) {
        this.obj = obj;
        this.fieldType = fieldType;
        this.fieldName = fieldName;
    }

    public GenericArray convertArray() {
        if (obj == null) {
            return null;
        }

        ArrayType arrayType = (ArrayType) fieldType;
        DataType elementType = arrayType.getElementType();

        // Handle primitive arrays directly
        if (obj instanceof Boolean[]) {
            return new GenericArray((Boolean[]) obj);
        } else if (obj instanceof Long[]) {
            return new GenericArray((Long[]) obj);
        } else if (obj instanceof Double[]) {
            return new GenericArray((Double[]) obj);
        } else if (obj instanceof Float[]) {
            return new GenericArray((Float[]) obj);
        } else if (obj instanceof Short[]) {
            return new GenericArray((Short[]) obj);
        } else if (obj instanceof Byte[]) {
            return new GenericArray((Byte[]) obj);
        }

        // Handle Object[] and java.util.Collection
        Object[] elements;
        if (obj instanceof Object[]) {
            elements = (Object[]) obj;
        } else if (obj instanceof Collection) {
            elements = ((Collection<?>) obj).toArray();
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s has unsupported array type: %s. Expected array or Collection.",
                            fieldName, obj.getClass().getName()));
        }

        Object[] converted = new Object[elements.length];
        for (int i = 0; i < elements.length; i++) {
            converted[i] = convertElementValue(elements[i], elementType);
        }
        return new GenericArray(converted);
    }

    private @Nullable Object convertElementValue(@Nullable Object obj, DataType elementType) {
        if (obj == null) {
            return null;
        }

        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case BYTES:
                return obj;
            case CHAR:
            case STRING:
                return ConverterCommons.convertTextValue(elementType, fieldName, obj);
            case DECIMAL:
                return ConverterCommons.convertDecimalValue(
                        (DecimalType) elementType, fieldName, obj);
            case DATE:
                return ConverterCommons.convertDateValue(fieldName, obj);
            case TIME_WITHOUT_TIME_ZONE:
                return ConverterCommons.convertTimeValue(fieldName, obj);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(elementType);
                    return ConverterCommons.convertTimestampNtzValue(precision, fieldName, obj);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(elementType);
                    return ConverterCommons.convertTimestampLtzValue(precision, fieldName, obj);
                }
            case ARRAY:
                return new PojoArrayToFlussArray(obj, elementType, fieldName).convertArray();
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported field type %s for field %s.",
                                elementType.getTypeRoot(), fieldName));
        }
    }
}
