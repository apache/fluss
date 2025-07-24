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

/** Test for {@link TimestampNtzSerializer}. */
public abstract class TimestampNtzSerializerTest extends SerializerTestBase<TimestampNtz> {

    @Override
    protected Serializer<TimestampNtz> createSerializer() {
        return new TimestampNtzSerializer(getPrecision());
    }

    @Override
    protected boolean deepEquals(TimestampNtz t1, TimestampNtz t2) {
        return t1.equals(t2);
    }

    @Override
    protected TimestampNtz[] getTestData() {
        int precision = getPrecision();
        boolean isCompact = TimestampNtz.isCompact(precision);

        if (isCompact) {
            // For compact precisions (0, 3), only use timestamps with nanoOfMillisecond = 0
            return new TimestampNtz[] {
                TimestampNtz.fromMillis(1),
                TimestampNtz.fromMillis(2),
                TimestampNtz.fromMillis(3),
                TimestampNtz.fromMillis(4),
                TimestampNtz.fromMillis(0),
                TimestampNtz.fromMillis(1234567890L),
                // Additional test data for better coverage
                TimestampNtz.fromMillis(1000000000L),
                TimestampNtz.fromMillis(2000000000L)
            };
        } else {
            // For non-compact precisions (6, 8), include timestamps with non-zero nanoOfMillisecond
            return new TimestampNtz[] {
                TimestampNtz.fromMillis(1, 100000),
                TimestampNtz.fromMillis(2, 200000),
                TimestampNtz.fromMillis(3, 300000),
                TimestampNtz.fromMillis(4, 400000),
                TimestampNtz.fromMillis(0, 0),
                TimestampNtz.fromMillis(1234567890L, 500000),
                // Additional test data for better coverage
                TimestampNtz.fromMillis(1000000000L, 750000),
                TimestampNtz.fromMillis(2000000000L, 999999)
            };
        }
    }

    protected abstract int getPrecision();

    /** Test for TimestampNtzSerializer with precision 0. */
    public static final class TimestampNtzSerializer0Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }

        @Override
        protected List<Pair<TimestampNtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00", getPrecision()),
                            "2019-01-01 00:00:00"),
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:01", getPrecision()),
                            "2019-01-01 00:00:01"));
        }

        @Test
        void testTimestampNtzSerializer0() {
            // This test will inherit all test methods from the parent class
        }
    }

    /** Test for TimestampNtzSerializer with precision 3. */
    public static final class TimestampNtzSerializer3Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }

        @Override
        protected List<Pair<TimestampNtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.000", getPrecision()),
                            "2019-01-01 00:00:00.000"),
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.001", getPrecision()),
                            "2019-01-01 00:00:00.001"));
        }

        @Test
        void testTimestampNtzSerializer3() {
            // This test will inherit all test methods from the parent class
        }
    }

    /** Test for TimestampNtzSerializer with precision 6. */
    public static final class TimestampNtzSerializer6Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }

        @Override
        protected List<Pair<TimestampNtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.000000", getPrecision()),
                            "2019-01-01 00:00:00.000000"),
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.000001", getPrecision()),
                            "2019-01-01 00:00:00.000001"));
        }

        @Test
        void testTimestampNtzSerializer6() {
            // This test will inherit all test methods from the parent class
        }
    }

    /** Test for TimestampNtzSerializer with precision 8. */
    public static final class TimestampNtzSerializer8Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }

        @Test
        void testTimestampNtzSerializer8() {
            // This test will inherit all test methods from the parent class
        }

        @Test
        void testDuplicate() {
            Serializer<TimestampNtz> original = createSerializer();
            Serializer<TimestampNtz> duplicated = original.duplicate();

            assertThat(duplicated).isNotSameAs(original);
            assertThat(duplicated).isEqualTo(original);
            assertThat(duplicated.hashCode()).isEqualTo(original.hashCode());
        }

        @Test
        void testEqualsAndHashCode() {
            TimestampNtzSerializer serializer1 = new TimestampNtzSerializer(8);
            TimestampNtzSerializer serializer2 = new TimestampNtzSerializer(8);
            TimestampNtzSerializer serializer3 = new TimestampNtzSerializer(6);

            assertThat(serializer1).isEqualTo(serializer2);
            assertThat(serializer1).isNotEqualTo(serializer3);
            assertThat(serializer1).isNotEqualTo(null);
            assertThat(serializer1).isNotEqualTo("string");

            assertThat(serializer1.hashCode()).isEqualTo(serializer2.hashCode());
            assertThat(serializer1.hashCode()).isNotEqualTo(serializer3.hashCode());
        }
    }

    /** Test for TimestampNtzSerializer with precision 1. */
    public static final class TimestampNtzSerializer1Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 1;
        }

        @Test
        void testTimestampNtzSerializer1() {
            // This test will inherit all test methods from the parent class
        }

        @Test
        void testCompactMode() {
            Serializer<TimestampNtz> serializer = createSerializer();
            // Precision 1 is compact mode
            assertThat(TimestampNtz.isCompact(1)).isTrue();
        }

        @Test
        void testNonCompactMode() {
            Serializer<TimestampNtz> serializer = createSerializer();
            // Precision 9 is not compact mode
            assertThat(TimestampNtz.isCompact(9)).isFalse();
        }
    }

    /** Test for TimestampNtzSerializer with precision 9. */
    public static final class TimestampNtzSerializer9Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 9;
        }

        @Test
        void testTimestampNtzSerializer9() {
            // This test will inherit all test methods from the parent class
        }

        @Test
        void testNonCompactMode() {
            Serializer<TimestampNtz> serializer = createSerializer();
            // Precision 9 is not compact mode
            assertThat(TimestampNtz.isCompact(9)).isFalse();
        }
    }
}
