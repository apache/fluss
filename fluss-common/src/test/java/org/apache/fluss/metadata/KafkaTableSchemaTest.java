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
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.KafkaTableSchema.ValueLayout;
import org.apache.fluss.metadata.KafkaTableSchema.ValueType;
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
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "binary")
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
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "future-format")
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                        .build();
        assertThatCode(() -> KafkaTableSchema.validate(arbitrary)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testFixedValueColumnWithOptionalSystemFields(String format) {
        DataType valueType = format.equals("binary") ? DataTypes.BYTES() : DataTypes.STRING();
        for (int mask = 0; mask < 16; mask++) {
            Schema.Builder schema =
                    Schema.newBuilder().column("_kafka_value", valueType.copy(false));
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
            assertThat(resolved.valueProjection().dataTypeAt(0).isNullable()).isFalse();
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
    void testJsonFlatPreservesNestedTypesAndExcludesSystemColumns() {
        RowType nested = DataTypes.ROW(DataTypes.FIELD("_kafka_key", DataTypes.STRING()));
        TableDescriptor table =
                enabled(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT().copy(false))
                                        .column("_kafka_rescue", DataTypes.STRING())
                                        .column("_kafka_metadata", metadataType())
                                        .column("payload", nested)
                                        .column("_kafka_key", DataTypes.BYTES())
                                        .column(
                                                "_kafka_headers",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.BYTES()))
                                        .column("custom_rescue", DataTypes.STRING())
                                        .build(),
                                "parse_json")
                        .build();
        KafkaTableSchema resolved = KafkaTableSchema.resolve(table);
        assertThat(resolved.valueType()).isEqualTo(ValueType.PARSE_JSON);
        assertThat(resolved.valueLayout()).isEqualTo(ValueLayout.FLAT);
        assertThat(resolved.valueProjection().positions()).containsExactly(0, 3, 6);
        assertThat(resolved.valueProjection().dataTypeAt(1)).isEqualTo(nested);
        assertThat(resolved.rescuePosition()).isEqualTo(1);
        assertThat(resolved.metadataPosition()).isEqualTo(2);
        assertThat(table.getProperties()).doesNotContainKey("table.kafka.convert.value.layout");
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
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, "nested")
                        .build();
        KafkaTableSchema resolved = KafkaTableSchema.resolve(table);
        assertThat(resolved.valueLayout()).isEqualTo(ValueLayout.NESTED);
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
                                    DataTypes.ROW(
                                            DataTypes.FIELD("_kafka_key", scalar.copy(false)))));
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
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, layout)
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
    void testReservedNamesAtValueMappingLevel(String name) {
        assertThat(KafkaTableSchema.isReservedName(name)).isTrue();
        TableDescriptor nested =
                enabled(
                                Schema.newBuilder()
                                        .column(
                                                "_kafka_value",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(name, DataTypes.STRING())))
                                        .build(),
                                "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, "nested")
                        .build();
        assertInvalid(nested, "Reserved name");
    }

    @ParameterizedTest
    @ValueSource(strings = {"raw", "avro", ""})
    void testRejectsUnsupportedValueTypes(String type) {
        assertInvalid(enabled(valueSchema(), type).build(), "table.kafka.convert.value.type");
    }

    @Test
    void testRejectsMissingValueTypeAndTargetConflicts() {
        assertInvalid(
                TableDescriptor.builder()
                        .schema(valueSchema())
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                        .build(),
                "Missing required");
        assertInvalid(
                enabled(Schema.newBuilder().column("body", DataTypes.BYTES()).build(), "binary")
                        .build(),
                "Missing required value column");
        assertInvalid(enabled(valueSchema(), "string").build(), "must be STRING");
        assertInvalid(
                enabled(valueSchema(), "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, "nested")
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
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, "nested")
                        .build(),
                "no value mapping");
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "string"})
    void testNonJsonRejectsExplicitLayout(String type) {
        assertInvalid(
                enabled(valueSchema(), type)
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, "flat")
                        .build(),
                "only supported for parse_json");
    }

    @Test
    void testInvalidLayoutAndKeyConversion() {
        assertInvalid(
                enabled(valueSchema(), "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, "unknown")
                        .build(),
                "value.layout");
        assertInvalid(
                enabled(valueSchema(), "binary")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE, "binary")
                        .build(),
                "requires _kafka_key");
        Schema schema =
                Schema.newBuilder()
                        .column("_kafka_value", DataTypes.BYTES())
                        .column("_kafka_key", DataTypes.BYTES())
                        .build();
        assertInvalid(
                enabled(schema, "binary")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE, "string")
                        .build(),
                "only supports binary");
        assertThat(
                        KafkaTableSchema.resolve(
                                        enabled(schema, "binary")
                                                .property(
                                                        ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE,
                                                        "binary")
                                                .build())
                                .keyPosition())
                .isEqualTo(1);
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
    void testMetadataPreservesDeclaredNullability() {
        RowType metadata =
                DataTypes.ROW(
                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                        DataTypes.FIELD("timestamp_type", DataTypes.INT().copy(false)),
                        DataTypes.FIELD("partition", DataTypes.INT().copy(false)));
        for (boolean nullable : new boolean[] {true, false}) {
            DataType type = metadata.copy(nullable);
            TableDescriptor table = withSystem("_kafka_metadata", type);
            KafkaTableSchema resolved = KafkaTableSchema.resolve(table);
            assertThat(resolved.rowType().getTypeAt(resolved.metadataPosition())).isEqualTo(type);
        }
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
    void testUnknownAndPrototypeOptions() {
        for (String key :
                Arrays.asList("table.kafka.unknown", "table.kafka.convert.rescue-column")) {
            assertInvalid(
                    enabled(valueSchema(), "binary").property(key, "anything").build(),
                    "Unsupported Kafka table property");
        }
        for (String key :
                Arrays.asList(
                        "kafka.value.format",
                        "kafka.key.fields",
                        "kafka.metadata.headers.column",
                        "fluss.value.format",
                        "table.kafka.convert.value.type")) {
            assertInvalid(
                    enabled(valueSchema(), "binary").customProperty(key, "raw").build(),
                    "Unsupported Kafka custom property");
        }
        assertThat(
                        KafkaTableSchema.resolve(
                                enabled(valueSchema(), "binary")
                                        .customProperty("owner", "team")
                                        .build()))
                .isNotNull();
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
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, type);
    }

    private static void assertInvalid(TableDescriptor table, String message) {
        assertThatThrownBy(() -> KafkaTableSchema.resolve(table))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(message);
    }
}
