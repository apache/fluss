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
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.PartitionTransform;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TransformType;
import org.apache.fluss.record.TestData;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    @Test
    void testCustomLakePathValidation() {
        // invalid database and table names
        assertThatThrownBy(
                        () ->
                                validate(
                                        tableDescriptorWithLakeName(
                                                ConfigOptions.TABLE_DATALAKE_DATABASE_NAME,
                                                "../lake_db"),
                                        DataLakeFormat.PAIMON))
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining("../lake_db");

        assertThatThrownBy(
                        () ->
                                validate(
                                        tableDescriptorWithLakeName(
                                                ConfigOptions.TABLE_DATALAKE_TABLE_NAME,
                                                "/tmp/lake_table"),
                                        DataLakeFormat.PAIMON))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("/tmp/lake_table");

        // valid database and table names
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME, "lake_db-1")
                        .property(ConfigOptions.TABLE_DATALAKE_TABLE_NAME, "lake_table-1")
                        .build();

        assertThatCode(() -> validate(tableDescriptor, DataLakeFormat.PAIMON))
                .doesNotThrowAnyException();

        // custom lake paths are not supported for Iceberg
        assertThatThrownBy(
                        () ->
                                validate(
                                        tableDescriptorWithLakeName(
                                                ConfigOptions.TABLE_DATALAKE_TABLE_NAME,
                                                "lake_table"),
                                        DataLakeFormat.ICEBERG))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Custom lake table path is only supported for Paimon");
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

    private static void validate(TableDescriptor tableDescriptor, DataLakeFormat dataLakeFormat) {
        TableDescriptorValidation.validateTableDescriptor(tableDescriptor, 100, dataLakeFormat);
    }

    private static TableDescriptor tableDescriptorWithLakeName(
            ConfigOption<String> configOption, String lakeName) {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                .distributedBy(1)
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                .property(configOption, lakeName)
                .build();
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

    @Test
    void testImplicitPartitionAllowsAutoPartition() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .build();

        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    @Test
    void testImplicitPartitionAllowsExplicitVirtualAutoPartitionKey() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "event_day")
                        .build();

        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    @Test
    void testRejectsUnknownAutoPartitionKey() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "missing_key")
                        .build();

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("missing_key")
                .hasMessageContaining("does not exist");
    }

    @Test
    void testImplicitPartitionDatalakeValidationIsDelegatedToLakeCatalog() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();

        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    @Test
    void testImplicitPartitionAllowsAlterDatalakeEnabled() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY).build();
        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "test_table"), 1, 1, descriptor, "/tmp/test", 1, 1);

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateAlterTableProperties(
                                        tableInfo,
                                        Collections.singleton(
                                                ConfigOptions.TABLE_DATALAKE_ENABLED.key())))
                .doesNotThrowAnyException();
    }

    @Test
    void testDateTruncRejectsDateHour() {
        TableDescriptor descriptor =
                dateTruncDescriptor(
                        "event_date",
                        DataTypes.DATE().copy(false),
                        "event_hour",
                        AutoPartitionTimeUnit.HOUR);

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessage("DATE_TRUNC partition transform does not support DATE + HOUR.");
    }

    @Test
    void testDateTruncAcceptsSupportedSourceTypes() {
        assertThatCode(
                        () ->
                                validate(
                                        dateTruncDescriptor(
                                                "event_date",
                                                DataTypes.DATE().copy(false),
                                                "event_day",
                                                AutoPartitionTimeUnit.DAY)))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                validate(
                                        dateTruncDescriptor(
                                                "event_time",
                                                DataTypes.TIMESTAMP().copy(false),
                                                "event_hour",
                                                AutoPartitionTimeUnit.HOUR)))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                validate(
                                        dateTruncDescriptor(
                                                        "event_ltz",
                                                        DataTypes.TIMESTAMP_LTZ().copy(false),
                                                        "event_hour",
                                                        AutoPartitionTimeUnit.HOUR)
                                                .withResolvedPartitionExpressionTimeZone(
                                                        ZoneId.of("UTC"))))
                .doesNotThrowAnyException();
    }

    @Test
    void testDateTruncRejectsUnsupportedSourceType() {
        TableDescriptor descriptor =
                dateTruncDescriptor(
                        "event_time",
                        DataTypes.STRING().copy(false),
                        "event_day",
                        AutoPartitionTimeUnit.DAY);

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "DATE_TRUNC partition transform does not support source column 'event_time'");
    }

    @Test
    void testRejectsUnsupportedTransformImplementation() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                        .build())
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day", new UnsupportedPartitionTransform())))
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .build();

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessage("Unsupported partition transform type: DATE_TRUNC");
    }

    @Test
    void testVirtualPartitionKeyDoesNotNeedPhysicalSchemaColumn() {
        TableDescriptor descriptor =
                implicitPartitionDescriptor("event_day", AutoPartitionTimeUnit.DAY).build();

        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    @Test
    void testRejectsInvalidVirtualPartitionKeyName() {
        assertThatThrownBy(
                        () ->
                                validate(
                                        implicitPartitionDescriptor("", AutoPartitionTimeUnit.DAY)
                                                .build()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Virtual partition spec key '' is invalid");

        assertThatThrownBy(
                        () ->
                                validate(
                                        implicitPartitionDescriptor(
                                                        "__event_day", AutoPartitionTimeUnit.DAY)
                                                .build()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Virtual partition spec key '__event_day' is invalid");

        assertThatThrownBy(
                        () ->
                                validate(
                                        implicitPartitionDescriptor(
                                                        "event$day", AutoPartitionTimeUnit.DAY)
                                                .build()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Virtual partition spec key 'event$day' is invalid");
    }

    @Test
    void testRejectsSystemColumnVirtualPartitionKey() {
        for (String systemColumn :
                Arrays.asList(
                        TableDescriptor.OFFSET_COLUMN_NAME,
                        TableDescriptor.TIMESTAMP_COLUMN_NAME,
                        TableDescriptor.BUCKET_COLUMN_NAME,
                        TableDescriptor.CHANGE_TYPE_COLUMN,
                        TableDescriptor.LOG_OFFSET_COLUMN,
                        TableDescriptor.COMMIT_TIMESTAMP_COLUMN)) {
            assertThatThrownBy(
                            () ->
                                    validate(
                                            implicitPartitionDescriptor(
                                                            systemColumn, AutoPartitionTimeUnit.DAY)
                                                    .build()))
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessage(
                            "Virtual partition spec key '"
                                    + systemColumn
                                    + "' is reserved for system columns.");
        }
    }

    private static TableDescriptor.Builder implicitPartitionDescriptor(
            String virtualPartitionKey, AutoPartitionTimeUnit timeUnit) {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build())
                .partitionedByKeys(
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        virtualPartitionKey,
                                        DateTruncPartitionTransform.of("event_time", timeUnit))))
                .distributedBy(1)
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1);
    }

    private static TableDescriptor dateTruncDescriptor(
            String sourceColumn,
            DataType sourceType,
            String virtualPartitionKey,
            AutoPartitionTimeUnit timeUnit) {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column(sourceColumn, sourceType).build())
                .partitionedByKeys(
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        virtualPartitionKey,
                                        DateTruncPartitionTransform.of(sourceColumn, timeUnit))))
                .distributedBy(1)
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                .build();
    }

    private static void validate(TableDescriptor descriptor) {
        TableDescriptorValidation.validateTableDescriptor(descriptor, 10, null);
    }

    private static class UnsupportedPartitionTransform implements PartitionTransform {
        private static final long serialVersionUID = 1L;

        @Override
        public TransformType getType() {
            return TransformType.DATE_TRUNC;
        }

        @Override
        public List<String> getSourceColumns() {
            return Collections.singletonList("event_time");
        }
    }
}
