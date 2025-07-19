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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TimestampLtzSerializer}. */
public abstract class TimestampLtzSerializerTest extends SerializerTestBase<TimestampLtz> {

    @Override
    protected TimestampLtzSerializer createSerializer() {
        return new TimestampLtzSerializer(getPrecision());
    }

    @Override
    protected boolean deepEquals(TimestampLtz t1, TimestampLtz t2) {
        return t1.equals(t2);
    }

    protected abstract int getPrecision();

    @Override
    protected TimestampLtz[] getTestData() {
        return new TimestampLtz[] {
            TimestampLtz.fromEpochMillis(0, getPrecision()),
            TimestampLtz.fromEpochMillis(1, getPrecision()),
            TimestampLtz.fromEpochMillis(1000, getPrecision()),
            TimestampLtz.fromEpochMillis(1000000, getPrecision()),
            TimestampLtz.fromEpochMillis(1000000000, getPrecision()),
            TimestampLtz.fromEpochMillis(Long.MAX_VALUE, getPrecision()),
            TimestampLtz.fromEpochMillis(Long.MIN_VALUE, getPrecision()),
            TimestampLtz.fromEpochMillis(-1, getPrecision()),
            TimestampLtz.fromEpochMillis(-1000, getPrecision()),
            TimestampLtz.fromEpochMillis(-1000000, getPrecision()),
            TimestampLtz.fromEpochMillis(-1000000000, getPrecision()),
            TimestampLtz.fromEpochMillis(-Long.MAX_VALUE, getPrecision()),
            TimestampLtz.fromEpochMillis(-Long.MIN_VALUE, getPrecision()),
            TimestampLtz.fromEpochMillis(123456789, getPrecision()),
            TimestampLtz.fromEpochMillis(-123456789, getPrecision()),
            TimestampLtz.fromEpochMillis(999999999, getPrecision()),
            TimestampLtz.fromEpochMillis(-999999999, getPrecision()),
            TimestampLtz.fromEpochMillis(0, getPrecision()),
            TimestampLtz.fromEpochMillis(1, getPrecision()),
            TimestampLtz.fromEpochMillis(2, getPrecision()),
            TimestampLtz.fromEpochMillis(3, getPrecision()),
            TimestampLtz.fromEpochMillis(4, getPrecision()),
            TimestampLtz.fromEpochMillis(5, getPrecision()),
            TimestampLtz.fromEpochMillis(6, getPrecision()),
            TimestampLtz.fromEpochMillis(7, getPrecision()),
            TimestampLtz.fromEpochMillis(8, getPrecision()),
            TimestampLtz.fromEpochMillis(9, getPrecision()),
            TimestampLtz.fromEpochMillis(10, getPrecision())
        };
    }

    /** Test for TimestampLtzSerializer with precision 0. */
    public static final class TimestampLtzSerializer0Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }

        @Test
        void testTimestampLtzSerializer0() {
            // This test will inherit all test methods from the parent class
        }
    }

    /** Test for TimestampLtzSerializer with precision 3. */
    public static final class TimestampLtzSerializer3Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }

        @Override
        protected TimestampLtz[] getTestData() {
            // For precision 3 (compact mode), nanoOfMillisecond must be 0
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
                TimestampLtz.fromEpochMillis(-999999999),
                TimestampLtz.fromEpochMillis(0),
                TimestampLtz.fromEpochMillis(1),
                TimestampLtz.fromEpochMillis(2),
                TimestampLtz.fromEpochMillis(3),
                TimestampLtz.fromEpochMillis(4),
                TimestampLtz.fromEpochMillis(5),
                TimestampLtz.fromEpochMillis(6),
                TimestampLtz.fromEpochMillis(7),
                TimestampLtz.fromEpochMillis(8),
                TimestampLtz.fromEpochMillis(9),
                TimestampLtz.fromEpochMillis(10)
            };
        }

        @Test
        void testTimestampLtzSerializer3() {
            // This test will inherit all test methods from the parent class
        }
    }

    /** Test for TimestampLtzSerializer with precision 6. */
    public static final class TimestampLtzSerializer6Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }

        @Test
        void testTimestampLtzSerializer6() {
            // This test will inherit all test methods from the parent class
        }
    }

    /** Test for TimestampLtzSerializer with precision 8. */
    public static final class TimestampLtzSerializer8Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }

        @Test
        void testTimestampLtzSerializer8() {
            // This test will inherit all test methods from the parent class
        }

        @Test
        void testDuplicate() {
            Serializer<TimestampLtz> original = createSerializer();
            Serializer<TimestampLtz> duplicated = original.duplicate();

            assertThat(duplicated).isNotSameAs(original);
            assertThat(duplicated).isEqualTo(original);
            assertThat(duplicated.hashCode()).isEqualTo(original.hashCode());
        }

        @Test
        void testEqualsAndHashCode() {
            TimestampLtzSerializer serializer1 = new TimestampLtzSerializer(8);
            TimestampLtzSerializer serializer2 = new TimestampLtzSerializer(8);
            TimestampLtzSerializer serializer3 = new TimestampLtzSerializer(6);

            assertThat(serializer1).isEqualTo(serializer2);
            assertThat(serializer1).isNotEqualTo(serializer3);
            assertThat(serializer1).isNotEqualTo(null);
            assertThat(serializer1).isNotEqualTo("string");

            assertThat(serializer1.hashCode()).isEqualTo(serializer2.hashCode());
            assertThat(serializer1.hashCode()).isNotEqualTo(serializer3.hashCode());
        }
    }

    /** Test for TimestampLtzSerializer with precision 1. */
    public static final class TimestampLtzSerializer1Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 1;
        }

        @Override
        protected TimestampLtz[] getTestData() {
            // For precision 1 (compact mode), nanoOfMillisecond must be 0
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
                TimestampLtz.fromEpochMillis(-999999999),
                TimestampLtz.fromEpochMillis(0),
                TimestampLtz.fromEpochMillis(1),
                TimestampLtz.fromEpochMillis(2),
                TimestampLtz.fromEpochMillis(3),
                TimestampLtz.fromEpochMillis(4),
                TimestampLtz.fromEpochMillis(5),
                TimestampLtz.fromEpochMillis(6),
                TimestampLtz.fromEpochMillis(7),
                TimestampLtz.fromEpochMillis(8),
                TimestampLtz.fromEpochMillis(9),
                TimestampLtz.fromEpochMillis(10)
            };
        }

        @Test
        void testTimestampLtzSerializer1() {
            // This test will inherit all test methods from the parent class
        }

        @Test
        void testCompactMode() {
            TimestampLtzSerializer serializer = createSerializer();
            // Precision 1 is compact mode
            assertThat(TimestampLtz.isCompact(1)).isTrue();
        }
    }

    /** Test for TimestampLtzSerializer with precision 9. */
    public static final class TimestampLtzSerializer9Test extends TimestampLtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 9;
        }

        @Test
        void testTimestampLtzSerializer9() {
            // This test will inherit all test methods from the parent class
        }

        @Test
        void testNonCompactMode() {
            TimestampLtzSerializer serializer = createSerializer();
            // Precision 9 is not compact mode
            assertThat(TimestampLtz.isCompact(9)).isFalse();
        }
    }
}
