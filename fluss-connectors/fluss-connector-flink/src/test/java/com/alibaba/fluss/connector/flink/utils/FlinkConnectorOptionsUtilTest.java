/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.utils;

import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.SCAN_STARTUP_TIMESTAMP;
import static com.alibaba.fluss.connector.flink.utils.FlinkConnectorOptionsUtils.parseTimestamp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkConnectorOptionsUtils}. */
class FlinkConnectorOptionsUtilTest {
    private final long expectedTimestamp = 1702156152000L;

    @Test
    void testParseTimestamp() {
        assertThat(
                        parseTimestamp(
                                "1702156152000",
                                SCAN_STARTUP_TIMESTAMP.key(),
                                ZoneId.systemDefault()))
                .isEqualTo(expectedTimestamp);

        assertThat(
                        parseTimestamp(
                                "2023-12-09 23:09:12",
                                SCAN_STARTUP_TIMESTAMP.key(),
                                ZoneId.systemDefault()))
                .isEqualTo(expectedTimestamp);

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
