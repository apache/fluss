/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConsumerOffsetTableConfig}. */
class ConsumerOffsetTableConfigTest {

    @Test
    void testDefaultsComeFromConfigOptions() {
        ConsumerOffsetTableConfig config = ConsumerOffsetTableConfig.from(new Configuration());

        assertThat(config.getMaxMetadataSize())
                .isEqualTo(
                        Math.toIntExact(
                                ConfigOptions.OFFSET_METADATA_MAX_BYTES
                                        .defaultValue()
                                        .getBytes()));
        assertThat(config.getOffsetsRetentionMs())
                .isEqualTo(ConfigOptions.OFFSETS_RETENTION_MS.defaultValue().toMillis());
        assertThat(config.getOffsetsTableNumBuckets())
                .isEqualTo(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS.defaultValue());
        assertThat(config.getOffsetsTableReplicationFactor())
                .isEqualTo(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR.defaultValue());
        assertThat(config.getOffsetCommitTimeoutMs())
                .isEqualTo(
                        Math.toIntExact(
                                ConfigOptions.OFFSET_COMMIT_TIMEOUT_MS
                                        .defaultValue()
                                        .toMillis()));
    }

    @Test
    void testLoadConfiguredValues() {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.OFFSET_METADATA_MAX_BYTES.key(), "1KB");
        configuration.setString(ConfigOptions.OFFSETS_RETENTION_MS.key(), "1min");
        configuration.set(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS, 16);
        configuration.set(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR, 2);
        configuration.setString(ConfigOptions.OFFSET_COMMIT_TIMEOUT_MS.key(), "3s");

        ConsumerOffsetTableConfig config = ConsumerOffsetTableConfig.from(configuration);

        assertThat(config.getMaxMetadataSize()).isEqualTo(1024);
        assertThat(config.getOffsetsRetentionMs()).isEqualTo(60_000L);
        assertThat(config.getOffsetsTableNumBuckets()).isEqualTo(16);
        assertThat(config.getOffsetsTableReplicationFactor()).isEqualTo(2);
        assertThat(config.getOffsetCommitTimeoutMs()).isEqualTo(3000);
    }
}
