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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EncodingUtils}. */
class EncodingUtilsTest {

    @Test
    void testEscapeIdentifier() {
        // Plain identifier is wrapped in backticks
        assertThat(EncodingUtils.escapeIdentifier("myTable")).isEqualTo("`myTable`");
        // Identifier with existing backtick gets it doubled inside the wrapper
        assertThat(EncodingUtils.escapeIdentifier("my`Table")).isEqualTo("`my``Table`");
        // Empty string
        assertThat(EncodingUtils.escapeIdentifier("")).isEqualTo("``");
    }

    @Test
    void testEscapeBackticks() {
        // No backticks – unchanged
        assertThat(EncodingUtils.escapeBackticks("hello")).isEqualTo("hello");
        // Single backtick becomes double
        assertThat(EncodingUtils.escapeBackticks("a`b")).isEqualTo("a``b");
        // Multiple backticks
        assertThat(EncodingUtils.escapeBackticks("a`b`c")).isEqualTo("a``b``c");
        // Only backtick
        assertThat(EncodingUtils.escapeBackticks("`")).isEqualTo("``");
        // Empty string
        assertThat(EncodingUtils.escapeBackticks("")).isEqualTo("");
    }

    @Test
    void testEscapeSingleQuotes() {
        // No single quotes – unchanged
        assertThat(EncodingUtils.escapeSingleQuotes("hello")).isEqualTo("hello");
        // Single quote becomes two single quotes
        assertThat(EncodingUtils.escapeSingleQuotes("it's")).isEqualTo("it''s");
        // Multiple single quotes
        assertThat(EncodingUtils.escapeSingleQuotes("a'b'c")).isEqualTo("a''b''c");
        // Only a single quote
        assertThat(EncodingUtils.escapeSingleQuotes("'")).isEqualTo("''");
        // Empty string
        assertThat(EncodingUtils.escapeSingleQuotes("")).isEqualTo("");
    }
}
