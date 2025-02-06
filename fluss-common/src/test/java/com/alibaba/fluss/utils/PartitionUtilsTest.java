/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.exception.PartitionSpecInvalidException;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;

import static com.alibaba.fluss.utils.PartitionUtils.generateAutoPartitionName;
import static com.alibaba.fluss.utils.PartitionUtils.getPartitionName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartitionUtils}. */
class PartitionUtilsTest {

    @Test
    void testGetPartitionName() {
        assertThat(getPartitionName(Collections.singletonList("a"), Collections.singletonList("1")))
                .isEqualTo("1");

        assertThat(getPartitionName(Arrays.asList("a", "b"), Arrays.asList("1", "2")))
                .isEqualTo("1$2");

        assertThatThrownBy(
                        () ->
                                getPartitionName(
                                        Arrays.asList("a", "b", "c"), Arrays.asList("1", "2")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The number of partition keys and partition specs should be the same.");

        assertThatThrownBy(
                        () -> getPartitionName(Arrays.asList("a", "b"), Arrays.asList("$1", "2")))
                .isInstanceOf(PartitionSpecInvalidException.class)
                .hasMessageContaining(
                        "The value of partition key should not contains separator: '$'");
    }

    @Test
    void testGenerateAutoPartitionName() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 11, 11, 11, 11);
        ZoneId zoneId = ZoneId.of("UTC-8");
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId);

        // for year
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.YEAR,
                new int[] {-1, 0, 1, 2, 3},
                new String[] {"2023", "2024", "2025", "2026", "2027"});

        // for quarter
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.QUARTER,
                new int[] {-1, 0, 1, 2, 3},
                new String[] {"20243", "20244", "20251", "20252", "20253"});

        // for month
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.MONTH,
                new int[] {-1, 0, 1, 2, 3},
                new String[] {"202410", "202411", "202412", "202501", "202502"});

        // for day
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.DAY,
                new int[] {-1, 0, 1, 2, 3, 20},
                new String[] {
                    "20241110", "20241111", "20241112", "20241113", "20241114", "20241201"
                });

        // for hour
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.HOUR,
                new int[] {-2, -1, 0, 1, 2, 3, 13},
                new String[] {
                    "2024111109",
                    "2024111110",
                    "2024111111",
                    "2024111112",
                    "2024111113",
                    "2024111114",
                    "2024111200"
                });
    }

    void testGenerateAutoPartitionName(
            ZonedDateTime zonedDateTime,
            AutoPartitionTimeUnit autoPartitionTimeUnit,
            int[] offsets,
            String[] expected) {
        for (int i = 0; i < offsets.length; i++) {
            String partitionString =
                    generateAutoPartitionName(
                            Collections.singletonList("ds"),
                            zonedDateTime,
                            offsets[i],
                            autoPartitionTimeUnit);
            assertThat(partitionString).isEqualTo(expected[i]);
        }
    }
}
