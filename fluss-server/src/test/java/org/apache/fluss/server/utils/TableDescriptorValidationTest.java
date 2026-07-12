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
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestData;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableDescriptorValidation}. */
class TableDescriptorValidationTest {

    @Test
    void testCreatePkTableWithRowTTL() {
        TableDescriptorValidation.validateTableDescriptor(
                pkTableWithProperties(
                        ConfigOptions.TABLE_KV_ROW_TTL.key(),
                        "1 h",
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(KV_FORMAT_VERSION_3)),
                100,
                null);
    }

    @Test
    void testCreateLogTableWithRowTTLFails() {
        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        logTableWithProperty(
                                                ConfigOptions.TABLE_KV_ROW_TTL.key(), "1 h"),
                                        100,
                                        null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_ROW_TTL.key())
                .hasMessageContaining("primary key");
    }

    @ParameterizedTest
    @MethodSource("supportedRowTTLTimeColumnTypes")
    void testCreateTableWithSupportedRowTTLTimeColumn(DataType timeColumnType) {
        TableDescriptor descriptor = pkTableWithRowTTLTimeColumn("event_time", timeColumnType);

        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testCreateTableWithMissingRowTTLTimeColumnFails() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TestData.DATA1_SCHEMA_PK)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_KV_ROW_TTL.key(), "1 h")
                        .property(ConfigOptions.TABLE_KV_ROW_TTL_TIME_COLUMN.key(), "event_time")
                        .property(
                                ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                                String.valueOf(KV_FORMAT_VERSION_3))
                        .build()
                        .withReplicationFactor(3);

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_ROW_TTL_TIME_COLUMN.key());
    }

    @ParameterizedTest
    @MethodSource("unsupportedRowTTLTimeColumnTypes")
    void testCreateTableWithUnsupportedRowTTLTimeColumnFails(DataType timeColumnType) {
        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        pkTableWithRowTTLTimeColumn("event_time", timeColumnType),
                                        100,
                                        null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_ROW_TTL_TIME_COLUMN.key())
                .hasMessageContaining("BIGINT")
                .hasMessageContaining("TIMESTAMP_LTZ");
    }

    @Test
    void testCreateTableWithLargeRowTTL() {
        TableDescriptor descriptor =
                pkTableWithProperties(
                        ConfigOptions.TABLE_KV_ROW_TTL.key(),
                        "3000000000 s",
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(KV_FORMAT_VERSION_3));

        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testCreateTableWithRowTTLMillisOverflowFails() {
        TableDescriptor descriptor =
                pkTableWithProperties(
                        ConfigOptions.TABLE_KV_ROW_TTL.key(),
                        "9223372036854776 s",
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(KV_FORMAT_VERSION_3));

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_ROW_TTL.key())
                .hasMessageContaining("exceeds");
    }

    @Test
    void testCreateRowTTLTableWithKvFormatVersion2Fails() {
        TableDescriptor descriptor =
                pkTableWithProperties(
                        ConfigOptions.TABLE_KV_ROW_TTL.key(),
                        "1 h",
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(KV_FORMAT_VERSION_2));

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_FORMAT_VERSION.key())
                .hasMessageContaining(ConfigOptions.TABLE_KV_ROW_TTL.key());
    }

    @Test
    void testCreateNonRowTTLTableWithKvFormatVersion3Fails() {
        TableDescriptor descriptor =
                pkTableWithProperty(
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(KV_FORMAT_VERSION_3));

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_FORMAT_VERSION.key())
                .hasMessageContaining(ConfigOptions.TABLE_KV_ROW_TTL.key());
    }

    @Test
    void testAlterRowTTLFails() {
        TableInfo currentTable =
                TableInfo.of(
                        TablePath.of("db", "t"),
                        1L,
                        1,
                        pkTableWithProperty(ConfigOptions.TABLE_KV_ROW_TTL.key(), "1 h"),
                        "file://remote",
                        1L,
                        1L);

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateAlterTableProperties(
                                        currentTable,
                                        Collections.singleton(
                                                ConfigOptions.TABLE_KV_ROW_TTL.key())))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_ROW_TTL.key());
    }

    private static Stream<Arguments> supportedRowTTLTimeColumnTypes() {
        return Stream.of(Arguments.of(DataTypes.BIGINT()), Arguments.of(DataTypes.TIMESTAMP_LTZ()));
    }

    private static Stream<Arguments> unsupportedRowTTLTimeColumnTypes() {
        return Stream.of(Arguments.of(DataTypes.STRING()), Arguments.of(DataTypes.TIMESTAMP()));
    }

    private TableDescriptor pkTableWithProperty(String key, String value) {
        return TableDescriptor.builder()
                .schema(TestData.DATA1_SCHEMA_PK)
                .distributedBy(3)
                .property(key, value)
                .build()
                .withReplicationFactor(3);
    }

    private TableDescriptor pkTableWithProperties(
            String key1, String value1, String key2, String value2) {
        Map<String, String> properties = new HashMap<>();
        properties.put(key1, value1);
        properties.put(key2, value2);
        return TableDescriptor.builder()
                .schema(TestData.DATA1_SCHEMA_PK)
                .distributedBy(3)
                .properties(properties)
                .build()
                .withReplicationFactor(3);
    }

    private TableDescriptor pkTableWithRowTTLTimeColumn(
            String timeColumn, DataType timeColumnType) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(timeColumn, timeColumnType)
                        .primaryKey("id")
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3)
                .property(ConfigOptions.TABLE_KV_ROW_TTL.key(), "1 h")
                .property(ConfigOptions.TABLE_KV_ROW_TTL_TIME_COLUMN.key(), timeColumn)
                .property(
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(KV_FORMAT_VERSION_3))
                .build()
                .withReplicationFactor(3);
    }

    private TableDescriptor logTableWithProperty(String key, String value) {
        return TableDescriptor.builder()
                .schema(TestData.DATA1_SCHEMA)
                .distributedBy(3)
                .property(key, value)
                .build()
                .withReplicationFactor(3);
    }
}
