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
import org.apache.fluss.row.encode.KvValueLayout;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableDescriptorValidation}. */
class TableDescriptorValidationTest {

    @Test
    void testCreateLogTableWithKvTTLFails() {
        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        logTableWithProperty(
                                                ConfigOptions.TABLE_KV_TTL.key(), "1 h"),
                                        100,
                                        null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_TTL.key())
                .hasMessageContaining("primary key");
    }

    @ParameterizedTest
    @MethodSource("supportedKvTTLTimeColumnTypes")
    void testCreateTableWithSupportedKvTTLTimeColumn(DataType timeColumnType) {
        TableDescriptor descriptor = pkTableWithKvTTLTimeColumn("event_time", timeColumnType);

        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testCreateTableWithMissingKvTTLTimeColumnFails() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TestData.DATA1_SCHEMA_PK)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_KV_TTL.key(), "1 h")
                        .property(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(), "event_time")
                        .property(
                                ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                                String.valueOf(KvValueLayout.TAGGED.version()))
                        .build()
                        .withReplicationFactor(3);

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key());
    }

    @Test
    void testKvTTLTimeColumnRequiresKvTTL() {
        TableDescriptor descriptor =
                pkTableWithProperty(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(), "event_time");

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key())
                .hasMessageContaining(ConfigOptions.TABLE_KV_TTL.key());
    }

    @ParameterizedTest
    @MethodSource("unsupportedKvTTLTimeColumnTypes")
    void testCreateTableWithUnsupportedKvTTLTimeColumnFails(DataType timeColumnType) {
        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        pkTableWithKvTTLTimeColumn("event_time", timeColumnType),
                                        100,
                                        null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key())
                .hasMessageContaining("BIGINT")
                .hasMessageContaining("TIMESTAMP_LTZ");
    }

    @Test
    void testCreateTableWithLargeKvTTL() {
        TableDescriptor descriptor =
                pkTableWithProperties(
                        ConfigOptions.TABLE_KV_TTL.key(),
                        "3000000000 s",
                        ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                        String.valueOf(KvValueLayout.TAGGED.version()));

        TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
    }

    @Test
    void testCreateTableWithKvTTLMillisOverflowFails() {
        TableDescriptor descriptor =
                pkTableWithProperties(
                        ConfigOptions.TABLE_KV_TTL.key(),
                        "9223372036854776 s",
                        ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                        String.valueOf(KvValueLayout.TAGGED.version()));

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_TTL.key())
                .hasMessageContaining("exceeds");
    }

    @Test
    void testKvFormatVersionStillRejectsValuesAboveVersionTwo() {
        TableDescriptor descriptor =
                pkTableWithProperty(
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(KV_FORMAT_VERSION_2 + 1));

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, 100, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("maximum supported version is " + KV_FORMAT_VERSION_2);
    }

    @ParameterizedTest(name = "layoutVersion={0}, rowTtl={1}, valid={2}")
    @MethodSource("kvValueLayoutMatrix")
    void testKvValueLayoutMatrix(Integer layoutVersion, boolean rowTtl, boolean valid) {
        Map<String, String> properties = new HashMap<>();
        if (layoutVersion != null) {
            properties.put(
                    ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                    String.valueOf(layoutVersion));
        }
        if (rowTtl) {
            properties.put(ConfigOptions.TABLE_KV_TTL.key(), "1 h");
        }
        TableDescriptor descriptor = pkTableWithProperties(properties);

        if (valid) {
            TableDescriptorValidation.validateTableDescriptor(descriptor, 100, null);
        } else {
            assertThatThrownBy(
                            () ->
                                    TableDescriptorValidation.validateTableDescriptor(
                                            descriptor, 100, null))
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining(ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key());
        }
    }

    @Test
    void testAlterKvTTLFails() {
        TableInfo currentTable =
                TableInfo.of(
                        TablePath.of("db", "t"),
                        1L,
                        1,
                        pkTableWithProperty(ConfigOptions.TABLE_KV_TTL.key(), "1 h"),
                        "file://remote",
                        1L,
                        1L);

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateAlterTableProperties(
                                        currentTable,
                                        Collections.singleton(ConfigOptions.TABLE_KV_TTL.key())))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_TTL.key());
    }

    @Test
    void testAlterValueLayoutVersionFails() {
        TableInfo currentTable = plainTableInfo();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateAlterTableProperties(
                                        currentTable,
                                        Collections.singleton(
                                                ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key())))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key());
    }

    private static Stream<Arguments> supportedKvTTLTimeColumnTypes() {
        return Stream.of(
                Arguments.of(DataTypes.BIGINT()),
                Arguments.of(DataTypes.TIMESTAMP()),
                Arguments.of(DataTypes.TIMESTAMP_LTZ()));
    }

    private static Stream<Arguments> unsupportedKvTTLTimeColumnTypes() {
        return Stream.of(Arguments.of(DataTypes.STRING()));
    }

    private static Stream<Arguments> kvValueLayoutMatrix() {
        return Stream.of(
                Arguments.of(null, false, true),
                Arguments.of(null, true, false),
                Arguments.of(KvValueLayout.PLAIN.version(), false, true),
                Arguments.of(KvValueLayout.PLAIN.version(), true, false),
                Arguments.of(KvValueLayout.TAGGED.version(), true, true),
                Arguments.of(KvValueLayout.TAGGED.version(), false, false),
                Arguments.of(999, true, false));
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
        return pkTableWithProperties(properties);
    }

    private TableDescriptor pkTableWithProperties(Map<String, String> properties) {
        return TableDescriptor.builder()
                .schema(TestData.DATA1_SCHEMA_PK)
                .distributedBy(3)
                .properties(properties)
                .build()
                .withReplicationFactor(3);
    }

    private TableDescriptor pkTableWithKvTTLTimeColumn(String timeColumn, DataType timeColumnType) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(timeColumn, timeColumnType)
                        .primaryKey("id")
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3)
                .property(ConfigOptions.TABLE_KV_TTL.key(), "1 h")
                .property(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(), timeColumn)
                .property(
                        ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                        String.valueOf(KvValueLayout.TAGGED.version()))
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

    private TableInfo plainTableInfo() {
        return TableInfo.of(
                TablePath.of("db", "t"),
                1L,
                1,
                pkTableWithProperty(
                        ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                        String.valueOf(KvValueLayout.PLAIN.version())),
                "file://remote",
                1L,
                1L);
    }
}
