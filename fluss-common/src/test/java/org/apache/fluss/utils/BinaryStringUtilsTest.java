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

import org.apache.fluss.row.BinaryString;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinaryStringUtils}. */
public class BinaryStringUtilsTest {

    @Test
    public void testConcat() {
        // Test basic concatenation
        BinaryString s1 = BinaryString.fromString("hello");
        BinaryString s2 = BinaryString.fromString(" ");
        BinaryString s3 = BinaryString.fromString("world");

        BinaryString result = BinaryStringUtils.concat(s1, s2, s3);
        assertThat(result.toString()).isEqualTo("hello world");

        // Test concatenation with empty string
        BinaryString empty = BinaryString.fromString("");
        result = BinaryStringUtils.concat(s1, empty, s3);
        assertThat(result.toString()).isEqualTo("helloworld");

        // Test concatenation with single string
        result = BinaryStringUtils.concat(s1);
        assertThat(result.toString()).isEqualTo("hello");

        // Test concatenation with null
        result = BinaryStringUtils.concat(s1, null, s3);
        assertThat(result).isNull();

        // Test concatenation with Chinese characters
        BinaryString chinese1 = BinaryString.fromString("你好");
        BinaryString chinese2 = BinaryString.fromString("世界");
        result = BinaryStringUtils.concat(chinese1, chinese2);
        assertThat(result.toString()).isEqualTo("你好世界");

        // Test concatenation with mixed ASCII and multi-byte characters
        BinaryString mixed1 = BinaryString.fromString("Hello");
        BinaryString mixed2 = BinaryString.fromString("世界");
        BinaryString mixed3 = BinaryString.fromString("!");
        result = BinaryStringUtils.concat(mixed1, mixed2, mixed3);
        assertThat(result.toString()).isEqualTo("Hello世界!");
    }

    @Test
    public void testConcatWs() {
        BinaryString sep = BinaryString.fromString(",");
        BinaryString s1 = BinaryString.fromString("a");
        BinaryString s2 = BinaryString.fromString("b");
        BinaryString s3 = BinaryString.fromString("c");

        // Test basic concatenation with separator
        BinaryString result = BinaryStringUtils.concatWs(sep, s1, s2, s3);
        assertThat(result.toString()).isEqualTo("a,b,c");

        // Test with null separator
        result = BinaryStringUtils.concatWs(null, s1, s2, s3);
        assertThat(result).isNull();

        // Test with null values in inputs (should skip nulls)
        result = BinaryStringUtils.concatWs(sep, s1, null, s3);
        assertThat(result.toString()).isEqualTo("a,c");

        // Test with all null inputs
        result = BinaryStringUtils.concatWs(sep, null, null, null);
        assertThat(result).isEqualTo(BinaryString.EMPTY_UTF8);

        // Test with single input
        result = BinaryStringUtils.concatWs(sep, s1);
        assertThat(result.toString()).isEqualTo("a");

        // Test with empty strings (should not skip empty strings)
        BinaryString empty = BinaryString.fromString("");
        result = BinaryStringUtils.concatWs(sep, s1, empty, s3);
        assertThat(result.toString()).isEqualTo("a,,c");

        // Test with different separator
        BinaryString dashSep = BinaryString.fromString("-");
        result = BinaryStringUtils.concatWs(dashSep, s1, s2, s3);
        assertThat(result.toString()).isEqualTo("a-b-c");

        // Test with multi-character separator
        BinaryString multiSep = BinaryString.fromString(" | ");
        result = BinaryStringUtils.concatWs(multiSep, s1, s2, s3);
        assertThat(result.toString()).isEqualTo("a | b | c");

        // Test with Chinese characters
        BinaryString chineseSep = BinaryString.fromString("，");
        BinaryString chinese1 = BinaryString.fromString("你好");
        BinaryString chinese2 = BinaryString.fromString("世界");
        result = BinaryStringUtils.concatWs(chineseSep, chinese1, chinese2);
        assertThat(result.toString()).isEqualTo("你好，世界");
    }

    @Test
    public void testConcatIterable() {
        BinaryString s1 = BinaryString.fromString("a");
        BinaryString s2 = BinaryString.fromString("b");
        BinaryString s3 = BinaryString.fromString("c");

        // Test with iterable
        BinaryString result = BinaryStringUtils.concat(java.util.Arrays.asList(s1, s2, s3));
        assertThat(result.toString()).isEqualTo("abc");

        // Test with null in iterable
        result = BinaryStringUtils.concat(java.util.Arrays.asList(s1, null, s3));
        assertThat(result).isNull();
    }

    @Test
    public void testConcatWsIterable() {
        BinaryString sep = BinaryString.fromString(",");
        BinaryString s1 = BinaryString.fromString("a");
        BinaryString s2 = BinaryString.fromString("b");
        BinaryString s3 = BinaryString.fromString("c");

        // Test with iterable
        BinaryString result = BinaryStringUtils.concatWs(sep, java.util.Arrays.asList(s1, s2, s3));
        assertThat(result.toString()).isEqualTo("a,b,c");

        // Test with null values in iterable (should skip nulls)
        result = BinaryStringUtils.concatWs(sep, java.util.Arrays.asList(s1, null, s3));
        assertThat(result.toString()).isEqualTo("a,c");
    }

    @Test
    public void testConcatPerformance() {
        // Test to ensure concat works correctly for repeated concatenations
        // simulating listagg aggregation
        BinaryString delimiter = BinaryString.fromString(",");
        BinaryString accumulator = BinaryString.fromString("item1");

        for (int i = 2; i <= 10; i++) {
            BinaryString newItem = BinaryString.fromString("item" + i);
            accumulator = BinaryStringUtils.concat(accumulator, delimiter, newItem);
        }

        assertThat(accumulator.toString())
                .isEqualTo("item1,item2,item3,item4,item5,item6,item7,item8,item9,item10");
    }
}
