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

package org.apache.fluss.kafka.schema;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests JSON table admission shared by Metadata and Produce. */
class KafkaJsonSchemaTest {
    private final KafkaTopicSchemaResolver resolver = new KafkaTopicSchemaResolver();

    @Test
    void testFlatJsonProjectsOnlyBusinessColumnsAroundOptionalSystemColumns() {
        TableDescriptor descriptor =
                descriptor(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("_kafka_key", DataTypes.BYTES())
                                        .column(
                                                "_kafka_headers",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.BYTES()))
                                        .column("amount", DataTypes.DECIMAL(8, 2))
                                        .column("_kafka_rescue", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE.key(), "binary")
                        .build();
        KafkaTableSchema schema = resolver.resolve(descriptor);
        assertThat(schema).isEqualTo(KafkaTableSchema.resolve(descriptor));
        assertThat(schema.valueConversionType()).isEqualTo(KafkaValueConversionType.PARSE_JSON);
        assertThat(schema.valueConversionLayout()).isEqualTo(KafkaValueConversionLayout.FLAT);
        assertThat(schema.valueProjection().positions()).containsExactly(0, 3);
        assertThat(schema.keyPosition()).isEqualTo(1);
        assertThat(schema.headersPosition()).isEqualTo(2);
        assertThat(schema.rescuePosition()).isEqualTo(4);
        assertThat(schema.rowType()).isEqualTo(descriptor.getSchema().getRowType());
    }

    @Test
    void testNestedJsonProjectsItsPhysicalValueContainer() {
        RowType value =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()));
        KafkaTableSchema schema =
                resolver.resolve(
                        descriptor(
                                        Schema.newBuilder()
                                                .column("_kafka_key", DataTypes.BYTES())
                                                .column("_kafka_value", value)
                                                .column("_kafka_rescue", DataTypes.STRING())
                                                .build())
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(),
                                        "nested")
                                .build());
        assertThat(schema.valueConversionLayout()).isEqualTo(KafkaValueConversionLayout.NESTED);
        assertThat(schema.valueProjection().positions()).containsExactly(1);
        assertThat(schema.valueProjection().dataTypeAt(0)).isEqualTo(value);
        assertThat(schema.keyPosition()).isZero();
        assertThat(schema.rescuePosition()).isEqualTo(2);
    }

    @Test
    void testJsonContractRequiresExplicitEnablementAndIgnoresCustomProperties() {
        TableDescriptor.Builder table =
                descriptor(Schema.newBuilder().column("id", DataTypes.INT()).build());
        TableDescriptor disabled = table.property(ConfigOptions.TABLE_KAFKA_ENABLED, false).build();
        assertThat(resolver.isEnabled(disabled)).isFalse();
        assertThatThrownBy(() -> resolver.resolve(disabled))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("disabled");
        for (String option :
                Arrays.asList(
                        "kafka.value.format",
                        "kafka.key.fields",
                        "table.kafka.convert.value.type")) {
            TableDescriptor withCustomProperty =
                    descriptor(disabled.getSchema()).customProperty(option, "json").build();
            assertThat(withCustomProperty.getCustomProperties()).containsEntry(option, "json");
            assertThat(resolver.resolve(withCustomProperty).valueConversionType())
                    .isEqualTo(KafkaValueConversionType.PARSE_JSON);
        }
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        descriptor(disabled.getSchema())
                                                .property(
                                                        ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE
                                                                .key(),
                                                        "string")
                                                .build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("Could not parse value 'string'");
    }

    @Test
    void testNestedJsonRejectsUnmappedTopLevelBusinessColumns() {
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        descriptor(
                                                        Schema.newBuilder()
                                                                .column(
                                                                        "_kafka_value",
                                                                        DataTypes.ROW(
                                                                                DataTypes.FIELD(
                                                                                        "id",
                                                                                        DataTypes
                                                                                                .INT())))
                                                                .column("other", DataTypes.STRING())
                                                                .build())
                                                .property(
                                                        ConfigOptions
                                                                .TABLE_KAFKA_CONVERT_VALUE_LAYOUT
                                                                .key(),
                                                        "nested")
                                                .build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("no value mapping");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "_kafka_key",
                "_kafka_headers",
                "_kafka_metadata",
                "_kafka_rescue",
                "_kafka_value"
            })
    void testNestedJsonRejectsReservedRootBusinessNames(String name) {
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        descriptor(
                                                        Schema.newBuilder()
                                                                .column(
                                                                        "_kafka_value",
                                                                        DataTypes.ROW(
                                                                                DataTypes.FIELD(
                                                                                        name,
                                                                                        DataTypes
                                                                                                .STRING())))
                                                                .build())
                                                .property(
                                                        ConfigOptions
                                                                .TABLE_KAFKA_CONVERT_VALUE_LAYOUT
                                                                .key(),
                                                        "nested")
                                                .build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("Reserved name");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testComplexBusinessFieldsRemainUnavailableUntilRecursiveConversion(boolean nested) {
        for (DataType type :
                Arrays.asList(
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ROW(DataTypes.FIELD("child", DataTypes.INT())))) {
            Schema schema =
                    nested
                            ? Schema.newBuilder()
                                    .column(
                                            "_kafka_value",
                                            DataTypes.ROW(DataTypes.FIELD("items", type)))
                                    .build()
                            : Schema.newBuilder().column("items", type).build();
            TableDescriptor table =
                    descriptor(schema)
                            .property(
                                    ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(),
                                    nested ? "nested" : "flat")
                            .build();
            // The persisted contract is valid, but this gateway stage must report INVALID_TOPIC
            // before accepting records for a decoder that it does not yet implement.
            assertThat(KafkaTableSchema.resolve(table).valueConversionType())
                    .isEqualTo(KafkaValueConversionType.PARSE_JSON);
            assertThatThrownBy(() -> resolver.resolve(table))
                    .isInstanceOf(KafkaTopicSchemaException.class);
        }
    }

    private static TableDescriptor.Builder descriptor(Schema schema) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "parse_json");
    }
}
