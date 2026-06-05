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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableDescriptorValidationTest {

    @Test
    void validTimeFormatIsAccepted() {
        TableDescriptor descriptor =
                dayPartitionedBuilder()
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy-MM-dd")
                        .build();
        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    @Test
    void blankTimeFormatIsRejected() {
        TableDescriptor descriptor =
                dayPartitionedBuilder()
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "  ")
                        .build();
        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key())
                .hasMessageContaining("must not be empty");
    }

    @Test
    void malformedTimeFormatIsRejected() {
        TableDescriptor descriptor =
                dayPartitionedBuilder()
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy-MM-dd'")
                        .build();
        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Invalid time format");
    }

    @Test
    void timeFormatWithDisallowedCharactersIsRejected() {
        TableDescriptor descriptor =
                dayPartitionedBuilder()
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy/MM/dd")
                        .build();
        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Allowed characters: ASCII alphanumerics, '_' and '-'");
    }

    @Test
    void timeFormatWithInsufficientPrecisionIsRejected() {
        TableDescriptor descriptor =
                hourPartitionedBuilder()
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy-MM-dd")
                        .build();
        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("lacks the precision");
    }

    @Test
    void hourTimeFormatIsAccepted() {
        TableDescriptor descriptor =
                hourPartitionedBuilder()
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(),
                                "yyyy-MM-dd-HH")
                        .build();
        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    private static TableDescriptor.Builder dayPartitionedBuilder() {
        return autoPartitionedBuilder(AutoPartitionTimeUnit.DAY);
    }

    private static TableDescriptor.Builder hourPartitionedBuilder() {
        return autoPartitionedBuilder(AutoPartitionTimeUnit.HOUR);
    }

    private static TableDescriptor.Builder autoPartitionedBuilder(AutoPartitionTimeUnit timeUnit) {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("dt", DataTypes.STRING())
                                .build())
                .distributedBy(1)
                .partitionedBy("dt")
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, timeUnit)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 0);
    }

    private static void validate(TableDescriptor descriptor) {
        TableDescriptorValidation.validateTableDescriptor(descriptor, Integer.MAX_VALUE, null);
    }
}
