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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PropertiesUtils}. */
class PropertiesUtilsTest {

    @Test
    void testAsPrefixedMap() {
        Map<String, String> properties = new HashMap<>();
        properties.put("host", "localhost");
        properties.put("port", "8080");

        Map<String, String> prefixed = PropertiesUtils.asPrefixedMap(properties, "server.");
        assertThat(prefixed).hasSize(2);
        assertThat(prefixed.get("server.host")).isEqualTo("localhost");
        assertThat(prefixed.get("server.port")).isEqualTo("8080");

        // Empty map
        assertThat(PropertiesUtils.asPrefixedMap(Collections.<String, Object>emptyMap(), "prefix."))
                .isEmpty();
    }

    @Test
    void testExtractAndRemovePrefix() {
        Map<String, String> original = new HashMap<>();
        original.put("server.host", "localhost");
        original.put("server.port", "8080");
        original.put("client.timeout", "5000");

        Map<String, String> extracted = PropertiesUtils.extractAndRemovePrefix(original, "server.");
        assertThat(extracted).hasSize(2);
        assertThat(extracted.get("host")).isEqualTo("localhost");
        assertThat(extracted.get("port")).isEqualTo("8080");
        assertThat(extracted.containsKey("client.timeout")).isFalse();

        // When prefix does not match any key
        Map<String, String> nonMatching =
                PropertiesUtils.extractAndRemovePrefix(original, "database.");
        assertThat(nonMatching).isEmpty();
    }

    @Test
    void testExtractPrefix() {
        Map<String, String> original = new HashMap<>();
        original.put("server.host", "localhost");
        original.put("server.port", "8080");
        original.put("client.timeout", "5000");

        Map<String, String> extracted = PropertiesUtils.extractPrefix(original, "server.");
        assertThat(extracted).hasSize(2);
        assertThat(extracted.get("server.host")).isEqualTo("localhost");
        assertThat(extracted.get("server.port")).isEqualTo("8080");
        assertThat(extracted.containsKey("client.timeout")).isFalse();

        // When prefix does not match
        assertThat(PropertiesUtils.extractPrefix(original, "database.")).isEmpty();
    }

    @Test
    void testExcludeByPrefix() {
        Map<String, String> original = new HashMap<>();
        original.put("server.host", "localhost");
        original.put("server.port", "8080");
        original.put("client.timeout", "5000");

        Map<String, String> remaining = PropertiesUtils.excludeByPrefix(original, "server.");
        assertThat(remaining).hasSize(1);
        assertThat(remaining.get("client.timeout")).isEqualTo("5000");

        // When prefix matches nothing
        Map<String, String> allRemaining = PropertiesUtils.excludeByPrefix(original, "database.");
        assertThat(allRemaining).hasSize(3);
    }
}
