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
        serverConf.set(ConfigOptions.LOG_INDEX_FILE_SIZE, MemorySize.parse("2kb"));

        ResolvedTableConfig tableConfig = new ResolvedTableConfig(tableConf, serverConf);

        assertThat(tableConfig.getLogSegmentSize()).isEqualTo(MemorySize.parse("8kb"));
        assertThat(tableConfig.getLogIndexFileSize()).isEqualTo(MemorySize.parse("2kb"));

        tableConf.set(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE, MemorySize.parse("4kb"));
        tableConf.set(ConfigOptions.TABLE_LOG_INDEX_FILE_SIZE, MemorySize.parse("1kb"));

        assertThat(tableConfig.getLogSegmentSize()).isEqualTo(MemorySize.parse("4kb"));
        assertThat(tableConfig.getLogIndexFileSize()).isEqualTo(MemorySize.parse("1kb"));
    }

    @Test
    void testKvOptionsFallBackToServerConfig() {
        Configuration tableConf = new Configuration();
        Configuration serverConf = new Configuration();
        serverConf.set(ConfigOptions.KV_WRITE_BATCH_SIZE, MemorySize.parse("16mb"));
        serverConf.set(ConfigOptions.KV_MAX_BACKGROUND_THREADS, 8);
        serverConf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("256mb"));
        serverConf.set(ConfigOptions.KV_MAX_WRITE_BUFFER_NUMBER, 4);

        ResolvedTableConfig tableConfig = new ResolvedTableConfig(tableConf, serverConf);

        assertThat(tableConfig.getKvWriteBatchSize()).isEqualTo(MemorySize.parse("16mb"));
        assertThat(tableConfig.getKvMaxBackgroundThreads()).isEqualTo(8);
        assertThat(tableConfig.getKvWriteBufferSize()).isEqualTo(MemorySize.parse("256mb"));
        assertThat(tableConfig.getKvMaxWriteBufferNumber()).isEqualTo(4);

        tableConf.set(ConfigOptions.TABLE_KV_WRITE_BATCH_SIZE, MemorySize.parse("8mb"));
        tableConf.set(ConfigOptions.TABLE_KV_MAX_BACKGROUND_THREADS, 3);
        tableConf.set(ConfigOptions.TABLE_KV_WRITE_BUFFER_SIZE, MemorySize.parse("128mb"));
        tableConf.set(ConfigOptions.TABLE_KV_MAX_WRITE_BUFFER_NUMBER, 2);

        assertThat(tableConfig.getKvWriteBatchSize()).isEqualTo(MemorySize.parse("8mb"));
        assertThat(tableConfig.getKvMaxBackgroundThreads()).isEqualTo(3);
        assertThat(tableConfig.getKvWriteBufferSize()).isEqualTo(MemorySize.parse("128mb"));
        assertThat(tableConfig.getKvMaxWriteBufferNumber()).isEqualTo(2);
    }
}
