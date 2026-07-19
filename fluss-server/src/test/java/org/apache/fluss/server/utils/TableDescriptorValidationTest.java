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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableDescriptorValidation}. */
class TableDescriptorValidationTest {

    @Test
    void testValidateLogSegmentFileSizeTableProperty() {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1")
                        .property(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE.key(), "1kb")
                        .property(ConfigOptions.TABLE_LOG_INDEX_FILE_SIZE.key(), "1kb")
                        .property(ConfigOptions.TABLE_KV_WRITE_BUFFER_SIZE.key(), "256mb")
                        .property(ConfigOptions.TABLE_KV_MAX_WRITE_BUFFER_NUMBER.key(), "4")
                        .property(ConfigOptions.TABLE_KV_WRITE_BATCH_SIZE.key(), "16mb")
                        .property(ConfigOptions.TABLE_KV_MAX_BACKGROUND_THREADS.key(), "8")
                        .property(ConfigOptions.TABLE_KV_MAX_OPEN_FILES.key(), "32")
                        .property(ConfigOptions.TABLE_KV_LOG_LEVEL.key(), "warn_level")
                        .property(ConfigOptions.TABLE_KV_COMPRESSION_PER_LEVEL.key(), "lz4,zstd")
                        .property(ConfigOptions.TABLE_KV_BLOCK_CACHE_SIZE.key(), "64mb")
                        .property(ConfigOptions.TABLE_KV_USE_BLOOM_FILTER.key(), "true")
                        .build();

        TableDescriptorValidation.validateTableDescriptor(tableDescriptor, 1, null);
    }

    @Test
    void testValidateLogSegmentFileSizeTablePropertyTooLarge() {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1")
                        .property(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE.key(), "3g")
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, 1, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Invalid configuration for table.log.segment.file-size, it must be less than or equal");
    }

    @Test
    void testValidateUnknownTableOption() {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1")
                        .property("table.future.option", "value")
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, 1, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("not a recognized Fluss table property");
    }
}
