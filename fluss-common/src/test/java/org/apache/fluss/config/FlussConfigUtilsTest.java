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

package org.apache.fluss.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.fluss.config.FlussConfigUtils.CLIENT_OPTIONS;
import static org.apache.fluss.config.FlussConfigUtils.TABLE_OPTIONS;
import static org.apache.fluss.config.FlussConfigUtils.extractConfigOptions;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlussConfigUtils}. */
class FlussConfigUtilsTest {

    @Test
    void testExtractOptions() {
        Map<String, ConfigOption<?>> tableOptions = extractConfigOptions("table.");
        assertThat(tableOptions).isNotEmpty();
        tableOptions.forEach(
                (k, v) -> {
                    assertThat(k).startsWith("table.");
                    assertThat(v.key()).startsWith("table.");
                });
        assertThat(tableOptions.size()).isEqualTo(TABLE_OPTIONS.size());

        Map<String, ConfigOption<?>> clientOptions = extractConfigOptions("client.");
        assertThat(clientOptions).isNotEmpty();
        clientOptions.forEach(
                (k, v) -> {
                    assertThat(k).startsWith("client.");
                    assertThat(v.key()).startsWith("client.");
                });
        assertThat(clientOptions.size()).isEqualTo(CLIENT_OPTIONS.size());
    }
}
