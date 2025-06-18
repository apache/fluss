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

package com.alibaba.fluss.row;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.row.TimestampLtz} class. */
public class TimestampLtzTest {

    @Test
    public void testNormal() {
        // From long to TimestampData and vice versa.
        assertThat(TimestampLtz.fromEpochMillis(1123L).getEpochMillisecond()).isEqualTo(1123L);
        assertThat(TimestampLtz.fromEpochMillis(-1123L).getEpochMillisecond()).isEqualTo(-1123L);

        assertThat(TimestampLtz.fromEpochMillis(1123L, 45678).getEpochMillisecond())
                .isEqualTo(1123L);
        assertThat(TimestampLtz.fromEpochMillis(1123L, 45678).getNanoOfMillisecond())
                .isEqualTo(45678);

        assertThat(TimestampLtz.fromEpochMillis(-1123L, 45678).getEpochMillisecond())
                .isEqualTo(-1123L);
        assertThat(TimestampLtz.fromEpochMillis(-1123L, 45678).getNanoOfMillisecond())
                .isEqualTo(45678);

        // From Instant to TimestampData and vice versa.
        Instant instant1 = Instant.ofEpochMilli(123L);
        Instant instant2 = Instant.ofEpochSecond(0L, 123456789L);
        Instant instant3 = Instant.ofEpochSecond(-2L, 123456789L);

        assertThat(TimestampLtz.fromInstant(instant1).toInstant()).isEqualTo(instant1);
        assertThat(TimestampLtz.fromInstant(instant2).toInstant()).isEqualTo(instant2);
        assertThat(TimestampLtz.fromInstant(instant3).toInstant()).isEqualTo(instant3);
    }

    @Test
    public void testDaylightSavingTime() {
        TimeZone tz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

        Instant instant = Instant.ofEpochSecond(0L, 123456789L);
        assertThat(TimestampLtz.fromInstant(instant).toInstant()).isEqualTo(instant);

        TimeZone.setDefault(tz);
    }

    @Test
    public void testToString() {
        assertThat(TimestampLtz.fromEpochMillis(123L).toString())
                .isEqualTo("1970-01-01T00:00:00.123Z");
        assertThat(TimestampLtz.fromEpochMillis(123L, 456789).toString())
                .isEqualTo("1970-01-01T00:00:00.123456789Z");

        assertThat(TimestampLtz.fromEpochMillis(-123L).toString())
                .isEqualTo("1969-12-31T23:59:59.877Z");
        assertThat(TimestampLtz.fromEpochMillis(-123L, 456789).toString())
                .isEqualTo("1969-12-31T23:59:59.877456789Z");

        Instant instant = Instant.ofEpochSecond(0L, 123456789L);
        assertThat(TimestampLtz.fromInstant(instant).toString())
                .isEqualTo("1970-01-01T00:00:00.123456789Z");
    }

    @Test
    public void testToMicros() {
        Instant instant = Instant.ofEpochSecond(0L, 123456789L);
        assertThat(TimestampLtz.fromInstant(instant).toString())
                .isEqualTo("1970-01-01T00:00:00.123456789Z");
        assertThat(
                        TimestampLtz.fromEpochMicros(
                                        TimestampLtz.fromInstant(instant).toEpochMicros())
                                .toString())
                .isEqualTo("1970-01-01T00:00:00.123456Z");
    }

    @Test
    void testIsCompact() {
        assertThat(TimestampLtz.isCompact(1)).isTrue();
        assertThat(TimestampLtz.isCompact(3)).isTrue();
        assertThat(TimestampLtz.isCompact(5)).isFalse();
    }
}
