/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.lakehouse.serializer;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.io.IOException;

/**
 * The SimpleVersionedSerializer interface = for serializing and deserializing objects with
 * versioning support. It provides methods to get the version, serialize an object to a byte array,
 * and deserialize a byte array to an object.
 *
 * @param <E> the type of the object to be serialized and deserialized
 * @since 0.7
 */
@PublicEvolving
public interface SimpleVersionedSerializer<E> {

    /**
     * Returns the version of the serializer.
     *
     * @return the version of the serializer
     */
    int getVersion();

    /**
     * Serializes the given object to a byte array.
     *
     * @param obj the object to serialize
     * @return the serialized byte array
     * @throws IOException if an I/O error occurs during serialization
     */
    byte[] serialize(E obj) throws IOException;

    /**
     * Deserializes the given byte array to an object.
     *
     * @param version the version of the serialized data
     * @param serialized the byte array to deserialize
     * @return the deserialized object
     * @throws IOException if an I/O error occurs during deserialization
     */
    E deserialize(int version, byte[] serialized) throws IOException;
}
