/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import org.junit.jupiter.api.Test;

import static com.alibaba.fluss.utils.DateTimeUtils.parseDate;
import static com.alibaba.fluss.utils.DateTimeUtils.parseTime;
import static org.assertj.core.api.Assertions.assertThat;

/** Test of {@link com.alibaba.fluss.utils.DateTimeUtils}. */
public class DateTimeUtilsTest {

    @Test
    void testParseDate() {
        String input = "2023-10-25";
        Integer result = parseDate(input);
        assertThat(result.intValue()).isEqualTo(19655);

        input = "2023-10-25 09:30:00.0";
        result = parseDate(input);
        assertThat(result.intValue()).isEqualTo(19655);

        input = "22023-10";
        result = parseDate(input);
        assertThat(result).isNull();

        input = "abcd-10-25";
        result = parseDate(input);
        assertThat(result).isNull();

        input = "2023-ab-25";
        result = parseDate(input);
        assertThat(result).isNull();

        input = "2023-10-ab";
        result = parseDate(input);
        assertThat(result).isNull();
    }

    @Test
    void testParseTime() {
        String input = "09:30:00.0";
        Integer result = parseTime(input);
        assertThat(result.intValue()).isEqualTo(34200000);

        input = "09:30:00.0+01:00";
        result = parseTime(input);
        assertThat(result.intValue()).isEqualTo(37800000);

        input = "09:30:00.0-01:00";
        result = parseTime(input);
        assertThat(result.intValue()).isEqualTo(30600000);

        input = "09:30";
        result = parseTime(input);
        assertThat(result.intValue()).isEqualTo(34200000);

        input = "ab:30:00.0";
        result = parseTime(input);
        assertThat(result).isNull();

        input = "09:cd:00.0";
        result = parseTime(input);
        assertThat(result).isNull();

        input = "09:30:ef";
        result = parseTime(input);
        assertThat(result).isNull();

        input = "09:30:00.abcd";
        result = parseTime(input);
        assertThat(result.intValue()).isEqualTo(34200001);
    }
}
