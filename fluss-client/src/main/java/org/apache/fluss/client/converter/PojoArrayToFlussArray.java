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

import java.util.Collection;

import static org.apache.fluss.client.converter.PojoTypeToFlussTypeConverter.convertElementValue;

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

        // Handle primitive arrays
        if (obj instanceof boolean[]) {
            return new GenericArray((boolean[]) obj);
        } else if (obj instanceof long[]) {
            return new GenericArray((long[]) obj);
        } else if (obj instanceof double[]) {
            return new GenericArray((double[]) obj);
        } else if (obj instanceof float[]) {
            return new GenericArray((float[]) obj);
        } else if (obj instanceof short[]) {
            return new GenericArray((short[]) obj);
        } else if (obj instanceof byte[]) {
            return new GenericArray((byte[]) obj);
        } else if (obj instanceof int[]) {
            return new GenericArray((int[]) obj);
        }
        // Handle boxed wrapper arrays
        else if (obj instanceof Boolean[]) {
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
            converted[i] = convertElementValue(elements[i], elementType, fieldName);
        }
        return new GenericArray(converted);
    }
}
