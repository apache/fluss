/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;

/**
 * Factory for creating serializers for complex types.
 *
 * <p>Note: Currently only supports Array type for Issue #1972. Map and Row serializers will be
 * added in Issue #1973 and #1974.
 */
public class InternalSerializers {

    /**
     * Creates a serializer for the given data type.
     *
     * @param dataType the data type
     * @return a serializer for the type, or null for primitive types
     */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> create(DataType dataType) {
        if (dataType instanceof ArrayType) {
            return (Serializer<T>)
                    new InternalArraySerializer(((ArrayType) dataType).getElementType());
        } else {
            // TODO: Add Map serializer support in Issue #1973
            // TODO: Add Row serializer support in Issue #1974
            // For primitive types, return null as they are handled directly
            return null;
        }
    }
}
