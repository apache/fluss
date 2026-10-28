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
import org.apache.fluss.config.MemorySize;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConsumerOffsetTableConfig}. */
class ConsumerOffsetTableConfigTest {

    @Test
    void testDefaultsComeFromConfigOptions() {
        ConsumerOffsetTableConfig config = ConsumerOffsetTableConfig.from(new Configuration());

        assertThat(config.getMaxMetadataSize())
                .isEqualTo(ConfigOptions.OFFSET_METADATA_MAX_BYTES.defaultValue());
        assertThat(config.getOffsetsRetention())
                .isEqualTo(ConfigOptions.OFFSETS_RETENTION.defaultValue());
        assertThat(config.getOffsetsTableNumBuckets())
                .isEqualTo(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS.defaultValue());
        assertThat(config.getOffsetsTableReplicationFactor())
                .isEqualTo(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR.defaultValue());
        assertThat(config.getOffsetCommitTimeout())
                .isEqualTo(ConfigOptions.OFFSET_COMMIT_TIMEOUT.defaultValue());
    }

    @Test
    void testLoadConfiguredValues() {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.OFFSET_METADATA_MAX_BYTES.key(), "1KB");
        configuration.setString(ConfigOptions.OFFSETS_RETENTION.key(), "1min");
        configuration.set(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS, 16);
        configuration.set(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR, 2);
        configuration.setString(ConfigOptions.OFFSET_COMMIT_TIMEOUT.key(), "3s");

        ConsumerOffsetTableConfig config = ConsumerOffsetTableConfig.from(configuration);

        assertThat(config.getMaxMetadataSize()).isEqualTo(MemorySize.parse("1KB"));
        assertThat(config.getOffsetsRetention()).isEqualTo(Duration.ofMinutes(1));
        assertThat(config.getOffsetsTableNumBuckets()).isEqualTo(16);
        assertThat(config.getOffsetsTableReplicationFactor()).isEqualTo(2);
        assertThat(config.getOffsetCommitTimeout()).isEqualTo(Duration.ofSeconds(3));
    }
}
