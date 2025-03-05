/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import java.io.IOException;
import java.util.Arrays;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is used to transfer (via serialization) objects whose classes are not available in the
 * system class loader. When those objects are deserialized without access to their special class
 * loader, the deserialization fails with a {@code ClassNotFoundException}.
 *
 * <p>To work around that issue, the SerializedValue serialized data immediately into a byte array.
 * When send through RPC or another service that uses serialization, only the byte array is
 * transferred. The object is deserialized later (upon access) and requires the accessor to provide
 * the corresponding class loader.
 *
 * @param <T> The type of the value held.
 */
public class SerializedValue<T> implements java.io.Serializable {

    private static final long serialVersionUID = -3564011643393683761L;

    /** The serialized data. */
    private final byte[] serializedData;

    protected SerializedValue(byte[] serializedData) {
        checkNotNull(serializedData, "Serialized data must not be null");
        checkArgument(serializedData.length != 0, "Serialized data must not be empty");
        this.serializedData = serializedData;
    }

    /**
     * Constructs a serialized value.
     *
     * @param value value to serialize
     * @throws NullPointerException if value is null
     * @throws IOException exception during serialization
     */
    public SerializedValue(T value) throws IOException {
        checkNotNull(value, "Value must not be null");
        this.serializedData = InstantiationUtils.serializeObject(value);
    }

    public T deserializeValue(ClassLoader loader) throws IOException, ClassNotFoundException {
        checkNotNull(loader, "No classloader has been passed");
        return InstantiationUtils.deserializeObject(serializedData, loader);
    }

    /**
     * Returns byte array for serialized data.
     *
     * @return Serialized data.
     */
    public byte[] getByteArray() {
        return serializedData;
    }

    /**
     * Constructs serialized value from serialized data.
     *
     * @param serializedData serialized data
     * @param <T> type
     * @return serialized value
     * @throws NullPointerException if serialized data is null
     * @throws IllegalArgumentException if serialized data is empty
     */
    public static <T> SerializedValue<T> fromBytes(byte[] serializedData) {
        return new SerializedValue<>(serializedData);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Arrays.hashCode(serializedData);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SerializedValue) {
            SerializedValue<?> other = (SerializedValue<?>) obj;
            return Arrays.equals(this.serializedData, other.serializedData);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "SerializedValue";
    }
}
