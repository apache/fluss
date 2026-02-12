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

package org.apache.fluss.flink.adapter;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Type information adapter which hides the different version of {@code createSerializer} method.
 *
 * <p>Implements both {@link #createSerializer(SerializerConfig)} and {@link
 * #createSerializer(ExecutionConfig)} by delegating to {@link
 * #createSerializer(TypeSerializerCreator)} with a creator that, given a {@link TypeInformation},
 * returns {@code typeInfo.createSerializer(config)}. Subclasses implement {@link
 * #createSerializer(TypeSerializerCreator)} by calling the creator with their inner type
 * information (e.g. row type) and wrapping the result. This way config is passed through to the
 * underlying TypeInformation.
 *
 * <p>See {@link org.apache.fluss.flink.sink.shuffle.StatisticsOrRecordTypeInformation} for usage.
 *
 * <p>TODO: remove this class when no longer supporting Flink versions that only have one of the two
 * createSerializer signatures.
 *
 * @param <T> the type described by this type information
 */
public abstract class TypeInformationAdapter<T> extends TypeInformation<T> {

    @Override
    public TypeSerializer<T> createSerializer(SerializerConfig config) {
        return createSerializer(typeInfo -> typeInfo.createSerializer(config));
    }

    @Override
    @Deprecated
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return createSerializer(typeInfo -> typeInfo.createSerializer(config));
    }

    /**
     * Creates the type serializer using the given creator. The creator captures the config from the
     * framework; subclasses call {@code creator.createSerializer(innerTypeInfo)} to obtain the
     * inner serializer and then wrap or return it.
     */
    protected abstract TypeSerializer<T> createSerializer(
            TypeSerializerCreator typeSerializerCreator);

    /**
     * Creator that, given a TypeInformation, returns its serializer for the current config. Passed
     * by the adapter so that config is forwarded to the underlying TypeInformation.
     */
    @FunctionalInterface
    public interface TypeSerializerCreator {
        TypeSerializer<?> createSerializer(TypeInformation<?> typeInfo);
    }
}
