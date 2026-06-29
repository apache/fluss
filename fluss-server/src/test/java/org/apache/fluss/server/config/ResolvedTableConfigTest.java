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

package org.apache.fluss.server.config;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ResolvedTableConfig}. */
class ResolvedTableConfigTest {

    @Test
    void testLogSegmentSizeFallsBackToServerConfig() {
        Configuration tableConf = new Configuration();
        Configuration serverConf = new Configuration();
        serverConf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("8kb"));

        ResolvedTableConfig tableConfig = new ResolvedTableConfig(tableConf, serverConf);

        assertThat(tableConfig.getLogSegmentSize()).isEqualTo(MemorySize.parse("8kb"));

        tableConf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("4kb"));

        assertThat(tableConfig.getLogSegmentSize()).isEqualTo(MemorySize.parse("4kb"));
    }
}
