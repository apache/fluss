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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for table-level log config options in {@link TableDescriptorValidation}. */
class TableDescriptorValidationTest {

    private static final Schema SCHEMA = Schema.newBuilder().column("id", DataTypes.INT()).build();

    /** Builds a minimal valid {@link TableDescriptor} with the given extra property. */
    private static TableDescriptor descriptorWithProperty(String key, String value) {
        return TableDescriptor.builder()
                .schema(SCHEMA)
                .distributedBy(1)
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1")
                .property(key, value)
                .build();
    }

    @Test
    void testTableLogSegmentFileSizeValidValue() {
        // A valid value (e.g. 16m) should not throw.
        TableDescriptor descriptor =
                descriptorWithProperty(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE.key(), "16m");
        // Should succeed without exception
        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testTableLogSegmentFileSizeOverflowThrows() {
        // Integer.MAX_VALUE bytes = 2147483647 bytes; set one byte more.
        long overflowBytes = (long) Integer.MAX_VALUE + 1;
        String overflowValue = overflowBytes + "b";
        TableDescriptor descriptor =
                descriptorWithProperty(
                        ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE.key(), overflowValue);
        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE.key())
                .hasMessageContaining(String.valueOf(Integer.MAX_VALUE));
    }

    @Test
    void testTableLogSegmentFileSizeAtMaxIntegerAllowed() {
        // Exactly Integer.MAX_VALUE bytes should be accepted.
        MemorySize maxSize = new MemorySize(Integer.MAX_VALUE);
        TableDescriptor descriptor =
                descriptorWithProperty(
                        ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE.key(), maxSize.toString());
        // Should succeed without exception
        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testTableLogIndexFileSizeValidValue() {
        TableDescriptor descriptor =
                descriptorWithProperty(ConfigOptions.TABLE_LOG_INDEX_FILE_SIZE.key(), "8m");
        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testTableLogIndexIntervalSizeValidValue() {
        TableDescriptor descriptor =
                descriptorWithProperty(ConfigOptions.TABLE_LOG_INDEX_INTERVAL_SIZE.key(), "4096b");
        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testTableLogFilePreallocateValidValue() {
        TableDescriptor descriptor =
                descriptorWithProperty(ConfigOptions.TABLE_LOG_FILE_PREALLOCATE.key(), "true");
        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testTableLogFlushIntervalMessagesValidValue() {
        TableDescriptor descriptor =
                descriptorWithProperty(
                        ConfigOptions.TABLE_LOG_FLUSH_INTERVAL_MESSAGES.key(), "100");
        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testTableLogOptionsRecognizedAsTableOptions() {
        // All five new options must be recognised as valid table properties (i.e. present in
        // TABLE_OPTIONS); otherwise validateTableDescriptor would throw InvalidConfigException
        // with "not a recognized Fluss table property".
        assertThat(org.apache.fluss.config.FlussConfigUtils.TABLE_OPTIONS.keySet())
                .contains(
                        ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE.key(),
                        ConfigOptions.TABLE_LOG_INDEX_FILE_SIZE.key(),
                        ConfigOptions.TABLE_LOG_INDEX_INTERVAL_SIZE.key(),
                        ConfigOptions.TABLE_LOG_FILE_PREALLOCATE.key(),
                        ConfigOptions.TABLE_LOG_FLUSH_INTERVAL_MESSAGES.key());
    }
}
