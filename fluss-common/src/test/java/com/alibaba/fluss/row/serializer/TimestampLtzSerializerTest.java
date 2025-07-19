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

/** Test for {@link TimestampLtzSerializer} with precision 0. */
public class TimestampLtzSerializerTest extends SerializerTestBase<TimestampLtz> {

    @Override
    protected TimestampLtzSerializer createSerializer() {
        return new TimestampLtzSerializer(0);
    }

    @Override
    protected boolean deepEquals(TimestampLtz t1, TimestampLtz t2) {
        return t1.equals(t2);
    }

    @Override
    protected TimestampLtz[] getTestData() {
        // For precision 0 (compact mode), nanoOfMillisecond must be 0
        return new TimestampLtz[] {
            TimestampLtz.fromEpochMillis(0),
            TimestampLtz.fromEpochMillis(1),
            TimestampLtz.fromEpochMillis(1000),
            TimestampLtz.fromEpochMillis(1000000),
            TimestampLtz.fromEpochMillis(1000000000),
            TimestampLtz.fromEpochMillis(Long.MAX_VALUE),
            TimestampLtz.fromEpochMillis(Long.MIN_VALUE),
            TimestampLtz.fromEpochMillis(-1),
            TimestampLtz.fromEpochMillis(-1000),
            TimestampLtz.fromEpochMillis(-1000000),
            TimestampLtz.fromEpochMillis(-1000000000),
            TimestampLtz.fromEpochMillis(-Long.MAX_VALUE),
            TimestampLtz.fromEpochMillis(-Long.MIN_VALUE),
            TimestampLtz.fromEpochMillis(123456789),
            TimestampLtz.fromEpochMillis(-123456789),
            TimestampLtz.fromEpochMillis(999999999),
            TimestampLtz.fromEpochMillis(-999999999)
        };
    }

    @Override
    protected List<Pair<TimestampLtz, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of(
                        DateTimeUtils.parseTimestampLtzData("2019-01-01 00:00:00", 0),
                        "2019-01-01 00:00:00"),
                Pair.of(
                        DateTimeUtils.parseTimestampLtzData("2019-01-01 00:00:01", 0),
                        "2019-01-01 00:00:01"));
    }

    @Test
    void testEqualsAndHashCode() {
        TimestampLtzSerializer serializer1 = new TimestampLtzSerializer(0);
        TimestampLtzSerializer serializer2 = new TimestampLtzSerializer(0);
        TimestampLtzSerializer serializer3 = new TimestampLtzSerializer(6);

        assertThat(serializer1).isEqualTo(serializer2);
        assertThat(serializer1).isNotEqualTo(serializer3);
        assertThat(serializer1).isNotEqualTo(null);
        assertThat(serializer1).isNotEqualTo("string");

        assertThat(serializer1.hashCode()).isEqualTo(serializer2.hashCode());
        assertThat(serializer1.hashCode()).isNotEqualTo(serializer3.hashCode());
    }

    @Test
    void testCompactMode() {
        TimestampLtzSerializer serializer = createSerializer();
        // Precision 0 is compact mode
        assertThat(TimestampLtz.isCompact(0)).isTrue();
    }
}
