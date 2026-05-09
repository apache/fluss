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

package org.apache.fluss.docs;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigOptionsDocGeneratorTest {

    @Test
    void testGeneratorProducesCorrectContent() throws Exception {
        String content = ConfigOptionsDocGenerator.generateMDXContent();

        // Verify structure
        assertThat(content).startsWith("{/* This file is auto-generated");

        // Verify known sections appear
        assertThat(content).contains("## Client\n");
        assertThat(content).contains("## KV\n");
        assertThat(content).contains("## ZooKeeper\n");

        // Verify a known option appears with correct format
        assertThat(content).contains("### `client.scanner.io.tmpdir`");

        // Verify OverrideDefault is applied (should show /tmp/fluss, not system path)
        assertThat(content).contains("* **Default**: `/tmp/fluss`");

        // Verify Duration formatting via ConfigDocUtils (not raw ISO-8601)
        assertThat(content).doesNotContain("PT15M");
        assertThat(content).contains("15 min"); // acl.notification.expiration-time

        // Verify no broken %s replacements
        assertThat(content).doesNotContain("refer to true https://");
    }

    @Test
    void testCleanDescriptionHandlesSpecialCharacters() {
        assertThat(ConfigOptionsDocGenerator.cleanDescription("a {b} <c>"))
                .isEqualTo("a &#123;b&#125; &lt;c>");
    }

    @Test
    void testCleanDescriptionHandlesNull() {
        assertThat(ConfigOptionsDocGenerator.cleanDescription(null))
                .isEqualTo("(No description provided.)");
    }

    @Test
    void testFormatDefaultValueUsesOverrideDefault() throws Exception {
        Field field = ConfigOptions.class.getField("CLIENT_SCANNER_IO_TMP_DIR");
        ConfigOption<?> option = (ConfigOption<?>) field.get(null);
        assertThat(ConfigOptionsDocGenerator.getFormattedDefaultValue(field, option))
                .isEqualTo("/tmp/fluss");
    }
}
