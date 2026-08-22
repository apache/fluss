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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks the serialized statistics block against the checked-in reference so that other language
 * clients (e.g. fluss-rust) can verify byte-for-byte compatibility. The reference lives in {@code
 * src/test/resources/encoding/statistics_block.hex}; CHAR is excluded because the collector does
 * not record CHAR bounds.
 */
public class LogRecordBatchStatisticsCompatibilityTest {

    private static final RowType ROW_TYPE =
            DataTypes.ROW(
                    new DataField("bool", DataTypes.BOOLEAN()),
                    new DataField("i8", DataTypes.TINYINT()),
                    new DataField("i16", DataTypes.SMALLINT()),
                    new DataField("i32", DataTypes.INT()),
                    new DataField("i64", DataTypes.BIGINT()),
                    new DataField("f32", DataTypes.FLOAT()),
                    new DataField("f64", DataTypes.DOUBLE()),
                    new DataField("str", DataTypes.STRING()),
                    new DataField("dec5", DataTypes.DECIMAL(5, 2)),
                    new DataField("dec20", DataTypes.DECIMAL(20, 3)),
                    new DataField("date", DataTypes.DATE()),
                    new DataField("time", DataTypes.TIME()),
                    new DataField("ts3", DataTypes.TIMESTAMP(3)),
                    new DataField("ts6", DataTypes.TIMESTAMP(6)),
                    new DataField("ltz3", DataTypes.TIMESTAMP_LTZ(3)),
                    new DataField("ltz6", DataTypes.TIMESTAMP_LTZ(6)),
                    new DataField("strnull", DataTypes.STRING()));

    private static List<Object[]> rows() {
        return Arrays.asList(
                new Object[] {
                    true,
                    (byte) 1,
                    (short) 100,
                    10,
                    1000L,
                    1.5f,
                    3.25,
                    "banana",
                    Decimal.fromBigDecimal(new BigDecimal("123.45"), 5, 2),
                    Decimal.fromBigDecimal(new BigDecimal("12345678.901"), 20, 3),
                    19000,
                    3600000,
                    TimestampNtz.fromMillis(1700000000123L),
                    TimestampNtz.fromMillis(1700000000123L, 456000),
                    TimestampLtz.fromEpochMillis(1700000000123L),
                    TimestampLtz.fromEpochMillis(1700000000123L, 456000),
                    null
                },
                new Object[] {
                    false,
                    (byte) -3,
                    (short) 200,
                    null,
                    -2000L,
                    -2.5f,
                    1.25,
                    "apple",
                    Decimal.fromBigDecimal(new BigDecimal("67.89"), 5, 2),
                    Decimal.fromBigDecimal(new BigDecimal("1.234"), 20, 3),
                    18000,
                    7200000,
                    TimestampNtz.fromMillis(1600000000000L),
                    TimestampNtz.fromMillis(1600000000000L, 1000),
                    TimestampLtz.fromEpochMillis(1600000000000L),
                    TimestampLtz.fromEpochMillis(1600000000000L, 1000),
                    null
                },
                new Object[] {
                    true,
                    (byte) 7,
                    (short) -50,
                    30,
                    3000L,
                    0.5f,
                    9.75,
                    "cherry",
                    Decimal.fromBigDecimal(new BigDecimal("500.00"), 5, 2),
                    Decimal.fromBigDecimal(new BigDecimal("99999999999999.999"), 20, 3),
                    20000,
                    1800000,
                    TimestampNtz.fromMillis(1800000000999L),
                    TimestampNtz.fromMillis(1800000000999L, 999000),
                    TimestampLtz.fromEpochMillis(1800000000999L),
                    TimestampLtz.fromEpochMillis(1800000000999L, 999000),
                    null
                });
    }

    @Test
    void testStatisticsBlockMatchesReference() throws Exception {
        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(
                        ROW_TYPE,
                        LogRecordBatchStatisticsTestUtils.createAllColumnsStatsMapping(ROW_TYPE));
        for (Object[] data : rows()) {
            collector.processRow(DataTestUtils.row(data));
        }

        MemorySegment segment = MemorySegment.allocateHeapMemory(4096);
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));
        byte[] bytes = new byte[bytesWritten];
        segment.get(0, bytes, 0, bytesWritten);
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        String expected = readReferenceHex();
        assertThat(hex.toString()).isEqualTo(expected);
    }

    private static String readReferenceHex() throws Exception {
        try (InputStream in =
                LogRecordBatchStatisticsCompatibilityTest.class.getResourceAsStream(
                        "/encoding/statistics_block.hex")) {
            assertThat(in).as("missing resource encoding/statistics_block.hex").isNotNull();
            byte[] buffer = new byte[8192];
            int length = in.read(buffer);
            return new String(buffer, 0, length, StandardCharsets.UTF_8).trim();
        }
    }
}
