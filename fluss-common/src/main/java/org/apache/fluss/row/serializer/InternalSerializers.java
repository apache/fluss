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

import org.apache.fluss.types.DataType;

/**
 * Temporary stub class for InternalSerializers to maintain compilation compatibility. TODO: Remove
 * this class and all references to it as per PR feedback. Different writers should handle their own
 * serialization formats.
 */
public class InternalSerializers {

    /**
     * Temporary method to create serializers. This should be removed.
     *
     * @param dataType the data type
     * @return a serializer
     */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> create(DataType dataType) {
        throw new UnsupportedOperationException(
                "InternalSerializers is deprecated. Each writer should handle its own serialization.");
    }
}
