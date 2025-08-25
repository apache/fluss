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

package org.apache.fluss.row;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.row.TimestampNtz} class. */
public class TimestampNtzTest {
    @Test
    public void testNormal() {
        // From LocalDateTime to TimestampData and vice versa.
        LocalDateTime ldt19 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
        LocalDateTime ldt16 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456000);
        LocalDateTime ldt13 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123000000);
        LocalDateTime ldt10 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);

        assertThat(TimestampNtz.fromLocalDateTime(ldt19).toLocalDateTime()).isEqualTo(ldt19);
        assertThat(TimestampNtz.fromLocalDateTime(ldt16).toLocalDateTime()).isEqualTo(ldt16);
        assertThat(TimestampNtz.fromLocalDateTime(ldt13).toLocalDateTime()).isEqualTo(ldt13);
        assertThat(TimestampNtz.fromLocalDateTime(ldt10).toLocalDateTime()).isEqualTo(ldt10);

        LocalDateTime ldt2 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);
        assertThat(TimestampNtz.fromLocalDateTime(ldt2).toLocalDateTime()).isEqualTo(ldt2);

        LocalDateTime ldt3 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
        assertThat(TimestampNtz.fromLocalDateTime(ldt3).toLocalDateTime()).isEqualTo(ldt3);
    }

    @Test
    public void testDaylightSavingTime() {
        TimeZone tz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

        LocalDateTime localDateTime = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
        assertThat(TimestampNtz.fromLocalDateTime(localDateTime).toLocalDateTime())
                .isEqualTo(localDateTime);

        TimeZone.setDefault(tz);
    }

    @Test
    public void testToString() {
        LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
        assertThat(TimestampNtz.fromLocalDateTime(ldt).toString())
                .isEqualTo("1969-01-02T00:00:00.123456789");
    }

    @Test
    void testIsCompact() {
        assertThat(TimestampNtz.isCompact(1)).isTrue();
        assertThat(TimestampNtz.isCompact(3)).isTrue();
        assertThat(TimestampNtz.isCompact(5)).isFalse();
    }

    @Test
    void testEquals() {
        LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
        TimestampNtz timestampNtz1 = TimestampNtz.fromLocalDateTime(ldt);
        TimestampNtz timestampNtz2 = TimestampNtz.fromMillis(-31449599877L, 456789);
        assertThat(timestampNtz1).isEqualTo(timestampNtz2);
    }

    @Test
    void testHashCode() {
        LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
        TimestampNtz timestampNtz = TimestampNtz.fromLocalDateTime(ldt);
        assertThat(timestampNtz.hashCode()).isEqualTo(-19523278);
    }
}
