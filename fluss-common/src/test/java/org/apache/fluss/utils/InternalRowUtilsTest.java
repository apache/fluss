/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InternalRowUtils}. */
public class InternalRowUtilsTest {

    @Test
    public void testCompare() {
        // test DECIMAL data type
        Decimal xDecimalData = Decimal.fromBigDecimal(new BigDecimal("12.34"), 4, 2);
        Decimal yDecimalData = Decimal.fromBigDecimal(new BigDecimal("13.14"), 4, 2);
        assertThat(InternalRowUtils.compare(xDecimalData, yDecimalData, DataTypeRoot.DECIMAL))
                .isLessThan(0);

        // test DOUBLE data type
        double xDouble = 13.14;
        double yDouble = 12.13;
        assertThat(InternalRowUtils.compare(xDouble, yDouble, DataTypeRoot.DOUBLE))
                .isGreaterThan(0);

        // test TIMESTAMP_WITHOUT_TIME_ZONE data type
        TimestampNtz xTimestamp = TimestampNtz.fromLocalDateTime(LocalDateTime.now());
        TimestampNtz yTimestamp = TimestampNtz.fromLocalDateTime(xTimestamp.toLocalDateTime());
        assertThat(
                        InternalRowUtils.compare(
                                xTimestamp, yTimestamp, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE))
                .isEqualTo(0);

        // test TIME_WITHOUT_TIME_ZONE data type
        assertThat(InternalRowUtils.compare(165, 168, DataTypeRoot.TIME_WITHOUT_TIME_ZONE))
                .isLessThan(0);

        // test STRING type (fluss uses STRING instead of VARCHAR)
        assertThat(
                        InternalRowUtils.compare(
                                BinaryString.fromString("a"),
                                BinaryString.fromString("b"),
                                DataTypeRoot.STRING))
                .isLessThan(0);

        // test CHAR type
        assertThat(
                        InternalRowUtils.compare(
                                BinaryString.fromString("a"),
                                BinaryString.fromString("b"),
                                DataTypeRoot.CHAR))
                .isLessThan(0);

        // test TIMESTAMP_WITH_LOCAL_TIME_ZONE data type
        long currentMillis = System.currentTimeMillis();
        TimestampLtz xLtz = TimestampLtz.fromEpochMillis(currentMillis);
        TimestampLtz yLtz = TimestampLtz.fromEpochMillis(xLtz.getEpochMillisecond());
        assertThat(
                        InternalRowUtils.compare(
                                xLtz, yLtz, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
                .isEqualTo(0);

        // test TINYINT
        assertThat(InternalRowUtils.compare((byte) 5, (byte) 10, DataTypeRoot.TINYINT))
                .isLessThan(0);

        // test SMALLINT
        assertThat(InternalRowUtils.compare((short) 5, (short) 10, DataTypeRoot.SMALLINT))
                .isLessThan(0);

        // test INTEGER
        assertThat(InternalRowUtils.compare(5, 10, DataTypeRoot.INTEGER)).isLessThan(0);

        // test BIGINT
        assertThat(InternalRowUtils.compare(5L, 10L, DataTypeRoot.BIGINT)).isLessThan(0);

        // test FLOAT
        assertThat(InternalRowUtils.compare(5.0f, 10.0f, DataTypeRoot.FLOAT)).isLessThan(0);

        // test DATE
        assertThat(InternalRowUtils.compare(100, 200, DataTypeRoot.DATE)).isLessThan(0);

        // test BINARY
        byte[] xBinary = new byte[] {1, 2, 3};
        byte[] yBinary = new byte[] {1, 2, 4};
        assertThat(InternalRowUtils.compare(xBinary, yBinary, DataTypeRoot.BINARY)).isLessThan(0);

        // test BYTES
        byte[] xBytes = new byte[] {1, 2};
        byte[] yBytes = new byte[] {1, 2, 3};
        assertThat(InternalRowUtils.compare(xBytes, yBytes, DataTypeRoot.BYTES)).isLessThan(0);
    }
}
