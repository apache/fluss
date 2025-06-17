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

package com.alibaba.fluss.flink.utils;

import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.TimeZone;

import static com.alibaba.fluss.flink.FlinkConnectorOptions.SCAN_STARTUP_TIMESTAMP;
import static com.alibaba.fluss.flink.utils.FlinkConnectorOptionsUtils.parseTimestamp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkConnectorOptionsUtils}. */
class FlinkConnectorOptionsUtilTest {
    @Test
    void testParseTimestamp() {
        assertThat(
                        parseTimestamp(
                                "1702134552000",
                                SCAN_STARTUP_TIMESTAMP.key(),
                                ZoneId.systemDefault()))
                .isEqualTo(1702134552000L);

        assertThat(
                        parseTimestamp(
                                "2023-12-09 23:09:12",
                                SCAN_STARTUP_TIMESTAMP.key(),
                                TimeZone.getTimeZone("Asia/Shanghai").toZoneId()))
                .isEqualTo(1702134552000L);

        assertThatThrownBy(
                        () ->
                                parseTimestamp(
                                        "2023-12-09T23:09:12",
                                        SCAN_STARTUP_TIMESTAMP.key(),
                                        ZoneId.systemDefault()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Invalid properties 'scan.startup.timestamp' should follow the format 'yyyy-MM-dd HH:mm:ss' "
                                + "or 'timestamp', but is '2023-12-09T23:09:12'. "
                                + "You can config like: '2023-12-09 23:09:12' or '1678883047356'.");
    }
}
