/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metadata;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.KafkaKeyConversionType;
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the shared DDL and Kafka conversion contract. */
class KafkaTableSchemaTest {

    @Test
    void testExplicitOptInAndDisabledTables() {
        TableDescriptor disabled =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .build())
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "binary")
                        .build();
        assertThat(KafkaTableSchema.isEnabled(disabled)).isFalse();
        assertThatCode(() -> KafkaTableSchema.validate(disabled)).doesNotThrowAnyException();
        assertThatThrownBy(() -> KafkaTableSchema.resolve(disabled))
                .hasMessageContaining("disabled");
        TableDescriptor arbitrary =
                TableDescriptor.builder(disabled)
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT().copy(false))
                                        .primaryKey("id")
                                        .build())
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "future-format")
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                        .build();
        assertThatCode(() -> KafkaTableSchema.validate(arbitrary)).doesNotThrowAnyException();
    }

    @Test
    void testEnabledFlagParsing() {
        TableDescriptor.Builder builder = enabled(valueSchema(), "binary");
        for (String value : Arrays.asList("true", "TRUE", "TrUe")) {
            assertThat(
                            KafkaTableSchema.isEnabled(
                                    builder.property("table.kafka.enabled", value).build()))
                    .isTrue();
        }
        for (String value : Arrays.asList("false", "FALSE", "FaLsE")) {
            assertThat(
                            KafkaTableSchema.isEnabled(
                                    builder.property("table.kafka.enabled", value).build()))
                    .isFalse();
        }
        for (String value : Arrays.asList("", "yes", "1", " true", "false ")) {
            TableDescriptor table = builder.property("table.kafka.enabled", value).build();
            assertThatThrownBy(() -> KafkaTableSchema.isEnabled(table))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("table.kafka.enabled");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testFixedValueColumnWithOptionalSystemFields(String format) {
        DataType valueType = format.equals("binary") ? DataTypes.BYTES() : DataTypes.STRING();
        assertInvalid(
                enabled(
                                Schema.newBuilder()
                                        .column("_kafka_value", valueType.copy(false))
                                        .build(),
                                format)
                        .build(),
                "nullable fields");
        for (int mask = 0; mask < 16; mask++) {
            Schema.Builder schema = Schema.newBuilder().column("_kafka_value", valueType);
            if ((mask & 1) != 0) {
                schema.column("_kafka_key", DataTypes.BYTES());
            }
            if ((mask & 2) != 0) {
                schema.column(
                        "_kafka_headers", DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
            }
            if ((mask & 4) != 0) {
                schema.column("_kafka_rescue", DataTypes.STRING());
            }
            if ((mask & 8) != 0) {
                schema.column("_kafka_metadata", metadataType());
            }
            TableDescriptor table = enabled(schema.build(), format).build();
            KafkaTableSchema resolved = KafkaTableSchema.resolve(table);
            assertThat(resolved.valueProjection().positions()).containsExactly(0);
            assertThat(resolved.valueProjection().dataTypeAt(0).isNullable()).isTrue();
            assertThat(resolved.keyPosition() >= 0).isEqualTo((mask & 1) != 0);
            assertThat(resolved.headersPosition() >= 0).isEqualTo((mask & 2) != 0);
            assertThat(resolved.rescuePosition() >= 0).isEqualTo((mask & 4) != 0);
            assertThat(resolved.metadataPosition() >= 0).isEqualTo((mask & 8) != 0);
            assertThat(table.getProperties())
                    .doesNotContainKeys(
                            "table.kafka.convert.key.type", "table.kafka.convert.value.layout");
        }
    }

    @Test
    void testDefaultJsonMappingPreservesNestedTypesAndExcludesSystemColumns() {
        RowType nested = DataTypes.ROW(DataTypes.FIELD("_kafka_key", DataTypes.STRING()));
        TableDescriptor table =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT())
                                        .column("_kafka_rescue", DataTypes.STRING())
                                        .column("_kafka_metadata", metadataType())
                                        .column("payload", nested)
                                        .column("_kafka_key", DataTypes.BYTES())
                                        .column(
                                                "_kafka_headers",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.BYTES()))
                                        .column("custom_rescue", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                        .build();
        KafkaTableSchema resolved = KafkaTableSchema.resolve(table);
        assertThat(resolved.valueConversionType()).isEqualTo(KafkaValueConversionType.PARSE_JSON);
        assertThat(resolved.valueConversionLayout()).isEqualTo(KafkaValueConversionLayout.FLAT);
        assertThat(resolved.valueProjection().positions()).containsExactly(0, 3, 6);
        assertThat(resolved.valueProjection().dataTypeAt(1)).isEqualTo(nested);
        assertThat(resolved.rescuePosition()).isEqualTo(1);
        assertThat(resolved.metadataPosition()).isEqualTo(2);
        assertThat(table.getProperties()).containsOnlyKeys("table.kafka.enabled");
    }

    @Test
    void testNestedJsonTargetsOneRow() {
        RowType row =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "details",
                                DataTypes.ROW(
                                        DataTypes.FIELD("_kafka_rescue", DataTypes.STRING()))));
        TableDescriptor table =
                enabled(
                                Schema.newBuilder()
                                        .column("_kafka_rescue", DataTypes.STRING())
                                        .column("_kafka_metadata", metadataType())
                                        .column("_kafka_value", row)
                                        .build(),
                                "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), "nested")
                        .build();
        KafkaTableSchema resolved = KafkaTableSchema.resolve(table);
        assertThat(resolved.valueConversionLayout()).isEqualTo(KafkaValueConversionLayout.NESTED);
        assertThat(resolved.valueProjection().positions()).containsExactly(2);
        assertThat(resolved.valueProjection().dataTypeAt(0)).isEqualTo(row);
        assertThat(resolved.rescuePosition()).isZero();
        assertThat(resolved.metadataPosition()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"flat", "nested"})
    void testJsonRejectsNonStringMapKeysRecursively(String layout) {
        for (DataType key : Arrays.asList(DataTypes.INT(), DataTypes.CHAR(10), DataTypes.BYTES())) {
            DataType invalidMap = DataTypes.MAP(key, DataTypes.STRING());
            for (DataType type :
                    Arrays.asList(
                            invalidMap,
                            DataTypes.ARRAY(invalidMap),
                            DataTypes.ROW(DataTypes.FIELD("attributes", invalidMap)),
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(invalidMap)))) {
                TableDescriptor table = jsonTable(layout, type);
                assertInvalid(table, "only supports STRING map keys");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"flat", "nested"})
    void testJsonAcceptsAllScalarTypesInNestedContainers(String layout) {
        for (DataType scalar :
                Arrays.asList(
                        DataTypes.CHAR(10),
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.BINARY(4),
                        DataTypes.BYTES(),
                        DataTypes.DECIMAL(18, 2),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DATE(),
                        DataTypes.TIME(3),
                        DataTypes.TIMESTAMP(9),
                        DataTypes.TIMESTAMP_LTZ(9))) {
            DataType type =
                    DataTypes.ARRAY(
                            DataTypes.MAP(
                                    DataTypes.STRING().copy(false),
                                    DataTypes.ROW(DataTypes.FIELD("_kafka_key", scalar))));
            TableDescriptor table = jsonTable(layout, type);
            assertThatCode(() -> KafkaTableSchema.validate(table)).doesNotThrowAnyException();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"flat", "nested"})
    void testJsonBusinessNestingDepthBoundary(String layout) {
        // Exercise every recursive edge, including map values (keys do not add a level).
        DataType boundary = DataTypes.STRING();
        for (int depth = 0; depth < KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH; depth++) {
            if (depth % 3 == 0) {
                boundary = DataTypes.ARRAY(boundary);
            } else if (depth % 3 == 1) {
                boundary = DataTypes.MAP(DataTypes.STRING(), boundary);
            } else {
                boundary = DataTypes.ROW(DataTypes.FIELD("child", boundary));
            }
        }
        TableDescriptor valid = jsonTable(layout, boundary);
        assertThatCode(() -> KafkaTableSchema.validate(valid)).doesNotThrowAnyException();
        TableDescriptor tooDeep = jsonTable(layout, DataTypes.ARRAY(boundary));
        assertInvalid(tooDeep, "maximum nesting depth of 64");
    }

    private static TableDescriptor jsonTable(String layout, DataType businessType) {
        Schema schema =
                "flat".equals(layout)
                        ? Schema.newBuilder().column("payload", businessType).build()
                        : Schema.newBuilder()
                                .column(
                                        "_kafka_value",
                                        DataTypes.ROW(DataTypes.FIELD("payload", businessType)))
                                .build();
        return enabled(schema, "parse_json")
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), layout)
                .build();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "_kafka_key",
                "_kafka_value",
                "_kafka_headers",
                "_kafka_metadata",
                "_kafka_rescue"
            })
    void testSystemNamesAreBusinessFieldsInsideNestedValue(String name) {
        assertThat(KafkaTableSchema.isReservedName(name)).isTrue();
        RowType valueType = DataTypes.ROW(DataTypes.FIELD(name, DataTypes.STRING()));
        TableDescriptor nested =
                enabled(
                                Schema.newBuilder()
                                        .column("_kafka_key", DataTypes.BYTES())
                                        .column("_kafka_value", valueType)
                                        .build(),
                                "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), "nested")
                        .build();
        KafkaTableSchema resolved = KafkaTableSchema.resolve(nested);
        assertThat(resolved.keyPosition()).isZero();
        assertThat(resolved.valueProjection().positions()).containsExactly(1);
        assertThat(resolved.valueProjection().dataTypeAt(0)).isEqualTo(valueType);
    }

    @ParameterizedTest
    @ValueSource(strings = {"raw", "avro", ""})
    void testRejectsUnsupportedValueTypes(String type) {
        assertThatThrownBy(() -> KafkaTableSchema.resolve(enabled(valueSchema(), type).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table.kafka.convert.value.type");
    }

    @Test
    void testRejectsValueTargetConflicts() {
        assertInvalid(
                TableDescriptor.builder()
                        .schema(valueSchema())
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                        .build(),
                "flat JSON");
        assertInvalid(
                enabled(Schema.newBuilder().column("body", DataTypes.BYTES()).build(), "binary")
                        .build(),
                "Missing required value column");
        assertInvalid(enabled(valueSchema(), "string").build(), "must be STRING");
        assertInvalid(
                enabled(valueSchema(), "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), "nested")
                        .build(),
                "must be ROW");
        assertInvalid(enabled(valueSchema(), "parse_json").build(), "flat JSON");
        assertInvalid(
                enabled(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .column("unused", DataTypes.STRING())
                                        .build(),
                                "binary")
                        .build(),
                "no value mapping");
        assertInvalid(
                enabled(
                                Schema.newBuilder()
                                        .column(
                                                "_kafka_value",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("id", DataTypes.STRING())))
                                        .column("unused", DataTypes.STRING())
                                        .build(),
                                "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), "nested")
                        .build(),
                "no value mapping");
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testNonJsonIgnoresDefaultAndExplicitLayouts(String type) {
        Schema schema =
                Schema.newBuilder()
                        .column(
                                "_kafka_value",
                                "binary".equals(type) ? DataTypes.BYTES() : DataTypes.STRING())
                        .build();
        TableDescriptor table = enabled(schema, type).build();
        KafkaTableSchema expected = KafkaTableSchema.resolve(table);
        assertThat(expected.valueConversionLayout()).isNull();
        assertThat(expected.valueProjection().positions()).containsExactly(0);
        assertThat(table.getProperties()).doesNotContainKey("table.kafka.convert.value.layout");
        for (KafkaValueConversionLayout layout : KafkaValueConversionLayout.values()) {
            assertThat(
                            KafkaTableSchema.resolve(
                                    TableDescriptor.builder(table)
                                            .property(
                                                    ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT,
                                                    layout)
                                            .build()))
                    .isEqualTo(expected);
        }
    }

    @Test
    void testRejectsInvalidJsonLayout() {
        TableDescriptor table =
                enabled(valueSchema(), "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), "unknown")
                        .build();
        assertThatThrownBy(() -> KafkaTableSchema.resolve(table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table.kafka.convert.value.layout");
    }

    @Test
    void testSystemColumnTypes() {
        for (DataType invalid : Arrays.asList(DataTypes.STRING(), DataTypes.BINARY(8))) {
            assertInvalid(withSystem("_kafka_key", invalid), "_kafka_key must be BYTES");
        }
        for (DataType invalid :
                Arrays.asList(
                        DataTypes.BYTES(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.BYTES()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))) {
            assertInvalid(withSystem("_kafka_headers", invalid), "MAP<STRING, BYTES>");
        }
        assertInvalid(withSystem("_kafka_rescue", DataTypes.INT()), "nullable STRING");
        assertInvalid(
                withSystem("_kafka_rescue", DataTypes.STRING().copy(false)), "nullable STRING");
    }

    @Test
    void testSystemFieldsRequireNullableTypes() {
        assertInvalid(withSystem("_kafka_key", DataTypes.BYTES().copy(false)), "_kafka_key");
        assertInvalid(
                withSystem(
                        "_kafka_headers",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()).copy(false)),
                "_kafka_headers");
        assertInvalid(
                withSystem(
                        "_kafka_headers",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES().copy(false))),
                "_kafka_headers<value>");
        assertInvalid(withSystem("_kafka_metadata", metadataType().copy(false)), "_kafka_metadata");
        for (int i = 0; i < metadataType().getFieldCount(); i++) {
            RowType.Builder metadata = RowType.builder();
            for (int j = 0; j < metadataType().getFieldCount(); j++) {
                metadata.field(
                        metadataType().getFields().get(j).getName(),
                        metadataType().getTypeAt(j).copy(i != j));
            }
            assertInvalid(
                    withSystem("_kafka_metadata", metadata.build()),
                    "_kafka_metadata." + metadataType().getFields().get(i).getName());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"flat", "nested"})
    void testJsonFieldsRequireNullableTypesRecursively(String layout) {
        DataType notNull = DataTypes.INT().copy(false);
        for (DataType type :
                Arrays.asList(
                        notNull,
                        DataTypes.ROW(DataTypes.FIELD("child", notNull)),
                        DataTypes.ROW(DataTypes.FIELD("child", DataTypes.INT())).copy(false),
                        DataTypes.ARRAY(notNull),
                        DataTypes.ARRAY(DataTypes.INT()).copy(false),
                        DataTypes.MAP(DataTypes.STRING(), notNull),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).copy(false))) {
            assertInvalid(jsonTable(layout, type), "nullable fields");
        }
        if ("nested".equals(layout)) {
            assertInvalid(
                    enabled(
                                    Schema.newBuilder()
                                            .column(
                                                    "_kafka_value",
                                                    DataTypes.ROW(
                                                                    DataTypes.FIELD(
                                                                            "id", DataTypes.INT()))
                                                            .copy(false))
                                            .build(),
                                    "parse_json")
                            .property(
                                    ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT,
                                    KafkaValueConversionLayout.NESTED)
                            .build(),
                    "_kafka_value");
        }
    }

    @Test
    void testOptionalBinaryKeyConversion() {
        TableDescriptor table = withSystem("_kafka_key", DataTypes.BYTES());
        TableDescriptor explicit =
                TableDescriptor.builder(table)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE,
                                KafkaKeyConversionType.BINARY)
                        .build();
        assertThat(KafkaTableSchema.resolve(explicit)).isEqualTo(KafkaTableSchema.resolve(table));
        assertThat(table.getProperties()).doesNotContainKey("table.kafka.convert.key.type");
        assertInvalid(
                enabled(valueSchema(), "binary")
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE,
                                KafkaKeyConversionType.BINARY)
                        .build(),
                "requires _kafka_key BYTES");
        assertThatThrownBy(
                        () ->
                                KafkaTableSchema.resolve(
                                        TableDescriptor.builder(table)
                                                .property("table.kafka.convert.key.type", "string")
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table.kafka.convert.key.type");
    }

    @Test
    void testMetadataRejectsInvalidStructureAndOffsetField() {
        for (DataType invalid :
                Arrays.asList(
                        DataTypes.STRING(),
                        DataTypes.ROW(
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                DataTypes.FIELD("timestamp_type", DataTypes.INT())),
                        DataTypes.ROW(
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                DataTypes.FIELD("partition", DataTypes.INT()),
                                DataTypes.FIELD("offset", DataTypes.BIGINT())),
                        DataTypes.ROW(
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                DataTypes.FIELD("partition", DataTypes.INT())),
                        DataTypes.ROW(
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(6)),
                                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                DataTypes.FIELD("partition", DataTypes.INT())),
                        DataTypes.ROW(
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                DataTypes.FIELD("timestamp_type", DataTypes.STRING()),
                                DataTypes.FIELD("partition", DataTypes.INT())),
                        DataTypes.ROW(
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                DataTypes.FIELD("partition", DataTypes.BIGINT())),
                        DataTypes.ROW(
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                DataTypes.FIELD("partition", DataTypes.INT()),
                                DataTypes.FIELD("timestamp_type", DataTypes.INT())),
                        DataTypes.ROW(
                                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                DataTypes.FIELD("bucket", DataTypes.INT())))) {
            assertInvalid(
                    withSystem("_kafka_metadata", invalid),
                    "ROW<timestamp TIMESTAMP_LTZ(3), timestamp_type INT, partition INT>");
        }
    }

    @Test
    void testUnrelatedPropertiesDoNotChangeMapping() {
        TableDescriptor table = enabled(valueSchema(), "binary").build();
        KafkaTableSchema expected = KafkaTableSchema.resolve(table);
        // Storage option registration is checked by the server, not by this shared resolver.
        assertThat(
                        KafkaTableSchema.resolve(
                                TableDescriptor.builder(table)
                                        .property("table.kafka.future-option", "anything")
                                        .build()))
                .isEqualTo(expected);
        for (String key :
                Arrays.asList(
                        "kafka.value.format",
                        "fluss.value.format",
                        "table.kafka.convert.value.type",
                        "owner")) {
            assertThat(
                            KafkaTableSchema.resolve(
                                    TableDescriptor.builder(table)
                                            .customProperty(key, "anything")
                                            .build()))
                    .isEqualTo(expected);
        }
    }

    @Test
    void testUnsupportedTableKinds() {
        Schema keyed =
                Schema.newBuilder()
                        .column("_kafka_value", DataTypes.BYTES().copy(false))
                        .primaryKey("_kafka_value")
                        .build();
        assertInvalid(enabled(keyed, "binary").build(), "without a primary key");
        assertInvalid(
                enabled(valueSchema(), "binary").partitionedBy("_kafka_value").build(),
                "non-partitioned");
        assertInvalid(
                enabled(valueSchema(), "binary").logFormat(LogFormat.INDEXED).build(), "Arrow");
    }

    private static RowType metadataType() {
        return DataTypes.ROW(
                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                DataTypes.FIELD("partition", DataTypes.INT()));
    }

    private static TableDescriptor withSystem(String name, DataType type) {
        return enabled(
                        Schema.newBuilder()
                                .column("_kafka_value", DataTypes.BYTES())
                                .column(name, type)
                                .build(),
                        "binary")
                .build();
    }

    private static Schema valueSchema() {
        return Schema.newBuilder().column("_kafka_value", DataTypes.BYTES()).build();
    }

    private static TableDescriptor.Builder enabled(Schema schema, String type) {
        return TableDescriptor.builder()
                .schema(schema)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), type);
    }

    private static void assertInvalid(TableDescriptor table, String message) {
        assertThatThrownBy(() -> KafkaTableSchema.resolve(table))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(message);
    }
}
