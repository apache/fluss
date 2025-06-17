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

package com.alibaba.fluss.utils.json;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonEncoding;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;

/** A utility class that provide abilities for JSON serialization and deserialization. */
@Internal
public class JsonSerdeUtils {

    /**
     * Object mapper shared instance to serialize and deserialize the plan. Note that creating and
     * copying of object mappers is expensive and should be avoided.
     */
    static final ObjectMapper OBJECT_MAPPER_INSTANCE;

    static {
        OBJECT_MAPPER_INSTANCE = new ObjectMapper();
    }

    /**
     * Serialize the given value to a byte array with the specified json serializer. It potentially
     * reuses byte array for writing efficiently. The implementation is inspired from {@link
     * ObjectMapper#writeValueAsBytes(Object)}.
     */
    public static <T> byte[] writeValueAsBytes(T value, JsonSerializer<T> serializer) {
        try (ByteArrayBuilder bb =
                new ByteArrayBuilder(OBJECT_MAPPER_INSTANCE.getFactory()._getBufferRecycler())) {
            JsonGenerator generator = OBJECT_MAPPER_INSTANCE.createGenerator(bb, JsonEncoding.UTF8);
            serializer.serialize(value, generator);
            generator.close();
            final byte[] result = bb.toByteArray();
            bb.release();
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Deserialize a byte array to a value with the specified json deserializer. */
    public static <T> T readValue(byte[] json, JsonDeserializer<T> deserializer) {
        try {
            JsonNode jsonNode = OBJECT_MAPPER_INSTANCE.readTree(json);
            if (jsonNode.isMissingNode()) {
                throw new IOException("No content to map due to end-of-input.");
            }
            return deserializer.deserialize(OBJECT_MAPPER_INSTANCE.readTree(json));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private JsonSerdeUtils() {}
}
