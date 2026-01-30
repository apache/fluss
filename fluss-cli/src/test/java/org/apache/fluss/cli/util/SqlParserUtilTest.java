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

package org.apache.fluss.cli.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SqlParserUtil}. */
class SqlParserUtilTest {

    @Test
    void testStripTrailingSemicolon() {
        assertThat(SqlParserUtil.stripTrailingSemicolon("SELECT * FROM t;"))
                .isEqualTo("SELECT * FROM t");
        assertThat(SqlParserUtil.stripTrailingSemicolon("SELECT * FROM t"))
                .isEqualTo("SELECT * FROM t");
        assertThat(SqlParserUtil.stripTrailingSemicolon("SELECT * FROM t;"))
                .isEqualTo("SELECT * FROM t");
        assertThat(SqlParserUtil.stripTrailingSemicolon("")).isEmpty();
        assertThat(SqlParserUtil.stripTrailingSemicolon(";")).isEmpty();
    }

    @Test
    void testStripQuotes() {
        assertThat(SqlParserUtil.stripQuotes("'value'")).isEqualTo("value");
        assertThat(SqlParserUtil.stripQuotes("\"value\"")).isEqualTo("value");
        assertThat(SqlParserUtil.stripQuotes("`value`")).isEqualTo("value");
        assertThat(SqlParserUtil.stripQuotes("value")).isEqualTo("value");
        assertThat(SqlParserUtil.stripQuotes("'mixed\"")).isEqualTo("'mixed\"");
        assertThat(SqlParserUtil.stripQuotes("''")).isEmpty();
        assertThat(SqlParserUtil.stripQuotes("'")).isEqualTo("'");
    }

    @Test
    void testSplitFirstToken() {
        assertThat(SqlParserUtil.splitFirstToken("CREATE TABLE t"))
                .containsExactly("CREATE", "TABLE t");
        assertThat(SqlParserUtil.splitFirstToken("  CREATE  TABLE t"))
                .containsExactly("CREATE", " TABLE t");
        assertThat(SqlParserUtil.splitFirstToken("SINGLETOKEN")).containsExactly("SINGLETOKEN", "");
        assertThat(SqlParserUtil.splitFirstToken("")).containsExactly("", "");
        assertThat(SqlParserUtil.splitFirstToken("   ")).containsExactly("", "");
    }

    @Test
    void testExtractParenthesizedContent() {
        assertThat(SqlParserUtil.extractParenthesizedContent("(a, b, c)")).isEqualTo("a, b, c");
        assertThat(SqlParserUtil.extractParenthesizedContent("(nested (parens) here)"))
                .isEqualTo("nested (parens) here");
        assertThat(SqlParserUtil.extractParenthesizedContent("()")).isEmpty();
        assertThat(SqlParserUtil.extractParenthesizedContent("no parens")).isEmpty();
        assertThat(SqlParserUtil.extractParenthesizedContent("(unclosed")).isEmpty();
    }

    @Test
    void testSplitCommaSeparated() {
        assertThat(SqlParserUtil.splitCommaSeparated("a, b, c")).containsExactly("a", "b", "c");
        assertThat(SqlParserUtil.splitCommaSeparated("'quoted, value', normal"))
                .containsExactly("'quoted, value'", "normal");
        assertThat(SqlParserUtil.splitCommaSeparated("func(a, b), other"))
                .containsExactly("func(a, b)", "other");
        assertThat(SqlParserUtil.splitCommaSeparated("a,b,c")).containsExactly("a", "b", "c");
        assertThat(SqlParserUtil.splitCommaSeparated("single")).containsExactly("single");
        assertThat(SqlParserUtil.splitCommaSeparated("")).isEmpty();
    }

    @Test
    void testSplitCommaSeparatedComplex() {
        // Nested quotes and parens
        assertThat(SqlParserUtil.splitCommaSeparated("func('a, b'), \"x, y\", (m, n)"))
                .containsExactly("func('a, b')", "\"x, y\"", "(m, n)");

        // Multiple nesting levels
        assertThat(SqlParserUtil.splitCommaSeparated("outer(inner('a, b'), c), d"))
                .containsExactly("outer(inner('a, b'), c)", "d");
    }

    @Test
    void testParseKeyValueMap() {
        Map<String, String> result = SqlParserUtil.parseKeyValueMap("key1=value1, key2='value2'");
        assertThat(result).containsEntry("key1", "value1");
        assertThat(result).containsEntry("key2", "value2");

        result = SqlParserUtil.parseKeyValueMap("k1=v1");
        assertThat(result).containsEntry("k1", "v1");

        result = SqlParserUtil.parseKeyValueMap("");
        assertThat(result).isEmpty();

        result = SqlParserUtil.parseKeyValueMap("invalid");
        assertThat(result).isEmpty();
    }

    @Test
    void testParseKeyList() {
        List<String> result = SqlParserUtil.parseKeyList("(key1, key2, key3)");
        assertThat(result).containsExactly("key1", "key2", "key3");

        result = SqlParserUtil.parseKeyList("(key1)");
        assertThat(result).containsExactly("key1");

        result = SqlParserUtil.parseKeyList("()");
        assertThat(result).isEmpty();

        result = SqlParserUtil.parseKeyList("key1, key2");
        assertThat(result).containsExactly("key1", "key2");
    }

    @Test
    void testParseTablePath() throws Exception {
        org.apache.fluss.metadata.TablePath result = SqlParserUtil.parseTablePath("db.table");
        assertThat(result.getDatabaseName()).isEqualTo("db");
        assertThat(result.getTableName()).isEqualTo("table");

        assertThatThrownBy(() -> SqlParserUtil.parseTablePath("table"))
                .isInstanceOf(Exception.class);

        result = SqlParserUtil.parseTablePath("`my.db`.`my.table`");
        assertThat(result.getDatabaseName()).isEqualTo("my.db");
        assertThat(result.getTableName()).isEqualTo("my.table");
    }

    @Test
    void testParseTablePathWithDefault() throws Exception {
        org.apache.fluss.metadata.TablePath result =
                SqlParserUtil.parseTablePathWithDefault("db.table", "default_db");
        assertThat(result.getDatabaseName()).isEqualTo("db");
        assertThat(result.getTableName()).isEqualTo("table");

        result = SqlParserUtil.parseTablePathWithDefault("table", "default_db");
        assertThat(result.getDatabaseName()).isEqualTo("default_db");
        assertThat(result.getTableName()).isEqualTo("table");

        assertThatThrownBy(() -> SqlParserUtil.parseTablePathWithDefault("table", null))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testParseTimestampLiteral() throws Exception {
        long result1 = SqlParserUtil.parseTimestampLiteral("2023-01-15T12:30:45Z");
        assertThat(result1).isGreaterThan(0);

        long result2 = SqlParserUtil.parseTimestampLiteral("2023-01-15 12:30:45");
        assertThat(result2).isGreaterThan(0);

        assertThatThrownBy(() -> SqlParserUtil.parseTimestampLiteral("invalid"))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testStripQuotesEdgeCases() {
        // Empty quotes
        assertThat(SqlParserUtil.stripQuotes("''")).isEmpty();
        assertThat(SqlParserUtil.stripQuotes("\"\"")).isEmpty();
        assertThat(SqlParserUtil.stripQuotes("``")).isEmpty();

        // Escaped quotes (should not strip)
        assertThat(SqlParserUtil.stripQuotes("\\'value\\'")).isEqualTo("\\'value\\'");

        // Whitespace
        assertThat(SqlParserUtil.stripQuotes("'  value  '")).isEqualTo("  value  ");
    }

    @Test
    void testSplitCommaSeparatedEmptyElements() {
        assertThat(SqlParserUtil.splitCommaSeparated("a,,c")).containsExactly("a", "", "c");
        assertThat(SqlParserUtil.splitCommaSeparated(",,")).containsExactly("", "", "");
    }

    @Test
    void testParseKeyValueMapQuotedValues() {
        Map<String, String> result =
                SqlParserUtil.parseKeyValueMap("key1='value with spaces', key2=\"another, value\"");
        assertThat(result).containsEntry("key1", "value with spaces");
        assertThat(result).containsEntry("key2", "another, value");
    }

    @Test
    void testParseTablePathMultipleDots() throws Exception {
        org.apache.fluss.metadata.TablePath result =
                SqlParserUtil.parseTablePath("db.schema.table");
        assertThat(result.getDatabaseName()).isEqualTo("db");
        assertThat(result.getTableName()).isEqualTo("schema.table");
    }
}
