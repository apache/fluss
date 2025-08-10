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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Abstract test base for json serde. */
public abstract class JsonSerdeTestBase<T> {

    protected abstract T[] createObjects();

    protected abstract String[] expectedJsons();

    /** Specific tests may override this to provide a custom equal assertion. */
    protected void assertEquals(T actual, T expected) {
        assertThat(actual).isEqualTo(expected);
    }

    private final JsonSerializer<T> serializer;
    private final JsonDeserializer<T> deserializer;

    protected <SD extends JsonSerializer<T> & JsonDeserializer<T>> JsonSerdeTestBase(SD serde) {
        this.serializer = serde;
        this.deserializer = serde;
    }

    @Test
    void testJsonSerde() throws IOException {
        T[] testObjects = createObjects();
        String[] expectedJsons = expectedJsons();
        assertThat(testObjects.length).isEqualTo(expectedJsons.length);
        for (int i = 0; i < testObjects.length; i++) {
            T value = testObjects[i];
            final byte[] json = JsonSerdeUtils.writeValueAsBytes(value, serializer);
            JsonNode jsonNode = JsonSerdeUtils.OBJECT_MAPPER_INSTANCE.readTree(json);
            checkFieldNameLowerCase(jsonNode);

            // assert the compatibility of json string while the json serde evolving
            assertThat(new String(json, StandardCharsets.UTF_8)).isEqualTo(expectedJsons[i]);

            final T actual = JsonSerdeUtils.readValue(json, deserializer);
            assertEquals(actual, value);
        }
    }

    private static void checkFieldNameLowerCase(JsonNode jsonNode) {
        jsonNode.fields()
                .forEachRemaining(
                        field -> {
                            String name = field.getKey();
                            assertThat(name)
                                    .as("json field name should be snake case, not camel case")
                                    .isEqualTo(camelToSnake(name));
                            checkFieldNameLowerCase(field.getValue());
                        });
    }

    public static String camelToSnake(String str) {
        // Regular Expression
        String regex = "([a-z])([A-Z]+)";

        // Replacement string
        String replacement = "$1_$2";

        // Replace the given regex
        // with replacement string
        // and convert it to lower case.
        return str.replaceAll(regex, replacement).toLowerCase();
    }
}
