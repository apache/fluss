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

import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.utils.DateTimeUtils;
import com.alibaba.fluss.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TimestampNtzSerializer} with precision 0 and precision 6. */
public class TimestampNtzSerializerTest extends SerializerTestBase<TimestampNtz> {

    @Override
    protected Serializer<TimestampNtz> createSerializer() {
        return new TimestampNtzSerializer(0);
    }

    @Override
    protected boolean deepEquals(TimestampNtz t1, TimestampNtz t2) {
        return t1.equals(t2);
    }

    @Override
    protected TimestampNtz[] getTestData() {
        // For precision 0 (compact mode), only use timestamps with nanoOfMillisecond = 0
        return new TimestampNtz[] {
            TimestampNtz.fromMillis(1),
            TimestampNtz.fromMillis(2),
            TimestampNtz.fromMillis(3),
            TimestampNtz.fromMillis(4),
            TimestampNtz.fromMillis(0),
            TimestampNtz.fromMillis(1234567890L),
            TimestampNtz.fromMillis(1000000000L),
            TimestampNtz.fromMillis(2000000000L),
            TimestampNtz.fromMillis(-1),
            TimestampNtz.fromMillis(-2),
            TimestampNtz.fromMillis(-3),
            TimestampNtz.fromMillis(-4),
            TimestampNtz.fromMillis(-1234567890L),
            TimestampNtz.fromMillis(-1000000000L),
            TimestampNtz.fromMillis(-2000000000L)
        };
    }

    @Override
    protected List<Pair<TimestampNtz, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of(
                        DateTimeUtils.parseTimestampNtzData("2019-01-01 00:00:00", 0),
                        "2019-01-01 00:00:00"),
                Pair.of(
                        DateTimeUtils.parseTimestampNtzData("2019-01-01 00:00:01", 0),
                        "2019-01-01 00:00:01"));
    }

    @Test
    void testEqualsAndHashCode() {
        TimestampNtzSerializer serializer1 = new TimestampNtzSerializer(0);
        TimestampNtzSerializer serializer2 = new TimestampNtzSerializer(0);
        TimestampNtzSerializer serializer3 = new TimestampNtzSerializer(6);

        assertThat(serializer1).isEqualTo(serializer2);
        assertThat(serializer1).isNotEqualTo(serializer3);
        assertThat(serializer1).isNotEqualTo(null);
        assertThat(serializer1).isNotEqualTo("string");

        assertThat(serializer1.hashCode()).isEqualTo(serializer2.hashCode());
        assertThat(serializer1.hashCode()).isNotEqualTo(serializer3.hashCode());
    }

    @Test
    void testCompactMode() {
        Serializer<TimestampNtz> serializer = createSerializer();
        // Precision 0 is compact mode
        assertThat(TimestampNtz.isCompact(0)).isTrue();
    }

    // Tests for precision 6 (non-compact mode)

    @Test
    void testEqualsAndHashCodePrecision6() {
        TimestampNtzSerializer serializer1 = new TimestampNtzSerializer(6);
        TimestampNtzSerializer serializer2 = new TimestampNtzSerializer(6);
        TimestampNtzSerializer serializer3 = new TimestampNtzSerializer(0);

        assertThat(serializer1).isEqualTo(serializer2);
        assertThat(serializer1).isNotEqualTo(serializer3);
        assertThat(serializer1).isNotEqualTo(null);
        assertThat(serializer1).isNotEqualTo("string");

        assertThat(serializer1.hashCode()).isEqualTo(serializer2.hashCode());
        assertThat(serializer1.hashCode()).isNotEqualTo(serializer3.hashCode());
    }

    @Test
    void testNonCompactMode() {
        Serializer<TimestampNtz> serializer = new TimestampNtzSerializer(6);
        // Precision 6 is not compact mode
        assertThat(TimestampNtz.isCompact(6)).isFalse();
    }

    @Test
    void testPrecision6Data() {
        Serializer<TimestampNtz> serializer = new TimestampNtzSerializer(6);

        // For precision 6 (non-compact mode), include timestamps with non-zero nanoOfMillisecond
        TimestampNtz[] testData =
                new TimestampNtz[] {
                    TimestampNtz.fromMillis(1, 100000),
                    TimestampNtz.fromMillis(2, 200000),
                    TimestampNtz.fromMillis(3, 300000),
                    TimestampNtz.fromMillis(4, 400000),
                    TimestampNtz.fromMillis(0, 0),
                    TimestampNtz.fromMillis(1234567890L, 500000),
                    TimestampNtz.fromMillis(1000000000L, 750000),
                    TimestampNtz.fromMillis(2000000000L, 999999),
                    TimestampNtz.fromMillis(-1, 100000),
                    TimestampNtz.fromMillis(-2, 200000),
                    TimestampNtz.fromMillis(-3, 300000),
                    TimestampNtz.fromMillis(-4, 400000),
                    TimestampNtz.fromMillis(-1234567890L, 500000),
                    TimestampNtz.fromMillis(-1000000000L, 750000),
                    TimestampNtz.fromMillis(-2000000000L, 999999)
                };

        // Test serialization and deserialization
        for (TimestampNtz timestamp : testData) {
            TimestampNtz copied = serializer.copy(timestamp);
            assertThat(deepEquals(timestamp, copied)).isTrue();
        }
    }

    @Test
    void testPrecision6SerializableToString() {
        Serializer<TimestampNtz> serializer = new TimestampNtzSerializer(6);

        List<Pair<TimestampNtz, String>> testData =
                Arrays.asList(
                        Pair.of(
                                DateTimeUtils.parseTimestampNtzData(
                                        "2019-01-01 00:00:00.000000", 6),
                                "2019-01-01 00:00:00.000000"),
                        Pair.of(
                                DateTimeUtils.parseTimestampNtzData(
                                        "2019-01-01 00:00:00.000001", 6),
                                "2019-01-01 00:00:00.000001"));

        for (Pair<TimestampNtz, String> pair : testData) {
            String result = serializer.serializeToString(pair.getLeft());
            assertThat(result).isEqualTo(pair.getRight());
        }
    }
}
