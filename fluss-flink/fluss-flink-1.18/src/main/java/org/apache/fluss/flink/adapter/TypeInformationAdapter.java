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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Flink 1.18 variant of the type information adapter.
 *
 * <p>In Flink 1.18, {@link TypeInformation} only declares {@code
 * createSerializer(ExecutionConfig)}. This adapter implements that method and delegates to {@link
 * #createSerializer(TypeSerializerCreator)} with a creator that forwards the config, so subclasses
 * (e.g. in fluss-flink-common) can obtain the inner serializer and wrap it without touching
 * ExecutionConfig here.
 *
 * <p>When building for Flink 1.18, this class is used instead of the common adapter so that
 * SerializerConfig is not required on the classpath. See fluss-flink-common's
 * TypeInformationAdapter for the version that implements both createSerializer(SerializerConfig)
 * and createSerializer(ExecutionConfig).
 *
 * @param <T> the type described by this type information
 */
public abstract class TypeInformationAdapter<T> extends TypeInformation<T> {

    @Override
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
