/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.serializer;

import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.utils.DateTimeUtils;
import com.alibaba.fluss.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TimestampLtzSerializer} with precision 6. */
public class TimestampLtzSerializer6Test extends SerializerTestBase<TimestampLtz> {

    @Override
    protected TimestampLtzSerializer createSerializer() {
        return new TimestampLtzSerializer(6);
    }

    @Override
    protected boolean deepEquals(TimestampLtz t1, TimestampLtz t2) {
        return t1.equals(t2);
    }

    @Override
    protected TimestampLtz[] getTestData() {
        // For precision 6 (non-compact mode), include timestamps with non-zero nanoOfMillisecond
        return new TimestampLtz[] {
            TimestampLtz.fromEpochMillis(1, 100000),
            TimestampLtz.fromEpochMillis(2, 200000),
            TimestampLtz.fromEpochMillis(3, 300000),
            TimestampLtz.fromEpochMillis(4, 400000),
            TimestampLtz.fromEpochMillis(0, 0),
            TimestampLtz.fromEpochMillis(1234567890L, 500000),
            TimestampLtz.fromEpochMillis(1000000000L, 750000),
            TimestampLtz.fromEpochMillis(2000000000L, 999999),
            TimestampLtz.fromEpochMillis(-1, 100000),
            TimestampLtz.fromEpochMillis(-2, 200000),
            TimestampLtz.fromEpochMillis(-3, 300000),
            TimestampLtz.fromEpochMillis(-4, 400000),
            TimestampLtz.fromEpochMillis(-1234567890L, 500000),
            TimestampLtz.fromEpochMillis(-1000000000L, 750000),
            TimestampLtz.fromEpochMillis(-2000000000L, 999999)
        };
    }

    @Override
    protected List<Pair<TimestampLtz, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of(
                        DateTimeUtils.parseTimestampLtzData("2019-01-01 00:00:00.000000", 6),
                        "2019-01-01 00:00:00.000000"),
                Pair.of(
                        DateTimeUtils.parseTimestampLtzData("2019-01-01 00:00:00.000001", 6),
                        "2019-01-01 00:00:00.000001"));
    }

    @Test
    void testEqualsAndHashCode() {
        TimestampLtzSerializer serializer1 = new TimestampLtzSerializer(6);
        TimestampLtzSerializer serializer2 = new TimestampLtzSerializer(6);
        TimestampLtzSerializer serializer3 = new TimestampLtzSerializer(0);

        assertThat(serializer1).isEqualTo(serializer2);
        assertThat(serializer1).isNotEqualTo(serializer3);
        assertThat(serializer1).isNotEqualTo(null);
        assertThat(serializer1).isNotEqualTo("string");

        assertThat(serializer1.hashCode()).isEqualTo(serializer2.hashCode());
        assertThat(serializer1.hashCode()).isNotEqualTo(serializer3.hashCode());
    }

    @Test
    void testNonCompactMode() {
        TimestampLtzSerializer serializer = createSerializer();
        // Precision 6 is not compact mode
        assertThat(TimestampLtz.isCompact(6)).isFalse();
    }
}
