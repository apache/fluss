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

import java.time.DayOfWeek;

import static org.apache.fluss.utils.StringUtils.isNullOrWhitespaceOnly;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.fluss.utils.StringUtils} class. */
public class StringUtilsTest {

    @Test
    void testIsNullOrWhitespaceOnly() {
        assertThat(isNullOrWhitespaceOnly(null)).isTrue();
        assertThat(isNullOrWhitespaceOnly("")).isTrue();
        assertThat(isNullOrWhitespaceOnly("    ")).isTrue();
        assertThat(isNullOrWhitespaceOnly("hell o")).isFalse();
        assertThat(isNullOrWhitespaceOnly("hello")).isFalse();
    }

    @Test
    void testArrayAwareToString() {
        assertThat(StringUtils.arrayAwareToString(null)).isEqualTo("null");

        assertThat(StringUtils.arrayAwareToString(DayOfWeek.MONDAY)).isEqualTo("MONDAY");

        assertThat(StringUtils.arrayAwareToString(new int[] {1, 2, 3})).isEqualTo("[1, 2, 3]");

        assertThat(StringUtils.arrayAwareToString(new byte[][] {{4, 5, 6}, null, {}}))
                .isEqualTo("[[4, 5, 6], null, []]");

        assertThat(
                        StringUtils.arrayAwareToString(
                                new Object[] {new Integer[] {4, 5, 6}, null, DayOfWeek.MONDAY}))
                .isEqualTo("[[4, 5, 6], null, MONDAY]");
    }
}
