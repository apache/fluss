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

package org.apache.fluss.kafka.schema;

import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the DDL contract independently of Kafka request handling and record decoding. */
public class KafkaTopicSchemaResolverTest {

    private final KafkaTopicSchemaResolver resolver = new KafkaTopicSchemaResolver();

    @Test
    public void testRawMappingSurvivesTableMetadataSerialization() {
        Schema schema =
                Schema.newBuilder()
                        .column("message", DataTypes.BYTES())
                        .column("received_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                        .column("attributes", headersType())
                        .column("message_key", DataTypes.BYTES())
                        .build();
        TableDescriptor table =
                table(schema, "raw")
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "message_key")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "received_at")
                        .customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "attributes")
                        .build();
        KafkaTopicSchema mapping =
                resolver.resolve(TableDescriptor.fromJsonBytes(table.toJsonBytes()));

        assertThat(mapping.rowType()).isEqualTo(schema.getRowType());
        assertThat(mapping.keyFormat()).isEqualTo(KafkaDataFormat.RAW);
        assertThat(mapping.keyProjection().positions()).containsExactly(3);
        assertThat(mapping.keyProjection().nameAt(0)).isEqualTo("message_key");
        assertThat(mapping.valueProjection().positions()).containsExactly(0);
        assertThat(mapping.valueProjection().dataTypeAt(0)).isEqualTo(DataTypes.BYTES());
        assertThat(mapping.timestampPosition()).isEqualTo(1);
        assertThat(mapping.headersPosition()).isEqualTo(2);
        assertThat(mapping).isEqualTo(resolver.resolve(table));
        assertThatThrownBy(() -> mapping.keyProjection().positions().add(1))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testValueOnlyStringAndNullableColumns() {
        for (boolean nullable : new boolean[] {true, false}) {
            KafkaTopicSchema mapping =
                    resolver.resolve(
                            table(
                                            Schema.newBuilder()
                                                    .column(
                                                            "body",
                                                            DataTypes.STRING().copy(nullable))
                                                    .build(),
                                            " STRING ")
                                    .build());
            assertThat(mapping.keyFormat()).isNull();
            assertThat(mapping.keyProjection().isEmpty()).isTrue();
            assertThat(mapping.valueFormat()).isEqualTo(KafkaDataFormat.STRING);
            assertThat(mapping.valueProjection().positions()).containsExactly(0);
            assertThat(mapping.valueProjection().dataTypeAt(0).isNullable()).isEqualTo(nullable);
            assertThat(mapping.timestampPosition()).isEqualTo(-1);
            assertThat(mapping.headersPosition()).isEqualTo(-1);
        }
    }

    @Test
    public void testMixedFormatsAndMetadataAreOptional() {
        KafkaTopicSchema mapping =
                resolver.resolve(
                        keyValueTable()
                                .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                                .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "id")
                                .customProperty(
                                        KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, " except_key ")
                                .build());
        assertThat(mapping.keyFormat()).isEqualTo(KafkaDataFormat.STRING);
        assertThat(mapping.valueFormat()).isEqualTo(KafkaDataFormat.RAW);
        assertThat(mapping.keyProjection().positions()).containsExactly(0);
        assertThat(mapping.valueProjection().positions()).containsExactly(1);
    }

    @Test
    public void testRejectsUnsupportedTableKinds() {
        Schema primaryKeySchema =
                Schema.newBuilder()
                        .column("id", DataTypes.STRING().copy(false))
                        .primaryKey("id")
                        .build();
        assertInvalid(table(primaryKeySchema, "string"), "must be a log table");
        assertInvalid(keyValueTable().partitionedBy("id"), "Partitioned Fluss tables");
        assertInvalid(valueTable().logFormat(LogFormat.INDEXED), "Arrow log format");
    }

    @Test
    public void testRequiresExplicitValueFormat() {
        assertInvalid(
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("body", DataTypes.BYTES()).build())
                        .distributedBy(1)
                        .customProperty("fluss.value.format", "raw"),
                KafkaDataFormat.VALUE_FORMAT_CONFIG);
    }

    @ParameterizedTest
    @ValueSource(strings = {"json", "avro", ""})
    public void testRejectsUnavailableFormats(String format) {
        assertInvalid(
                valueTable().customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, format),
                "Unsupported Kafka data format");
        assertInvalid(
                keyValueTable().customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, format),
                "Unsupported Kafka data format");
    }

    @Test
    public void testRejectsUnsupportedKafkaOptions() {
        assertInvalid(
                valueTable().customProperty("kafka.value.rescue-column", "body"),
                "Unsupported Kafka table property");
        assertInvalid(
                valueTable().customProperty("kafka.key.field", "body"),
                "Unsupported Kafka table property");
        assertThat(resolver.resolve(valueTable().customProperty("owner", "team").build()))
                .isNotNull();
    }

    @Test
    public void testKeyFormatAndFieldMustBeSpecifiedTogether() {
        assertInvalid(
                keyValueTable().customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "id"),
                "requires kafka.key.format");
        assertInvalid(
                keyValueTable().customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string"),
                "kafka.key.fields");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "missing", "id,id", "id,", ",id", "id,,body"})
    public void testRejectsInvalidKeyFields(String fields) {
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        keyValueTable()
                                                .customProperty(
                                                        KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                                                .customProperty(
                                                        KafkaDataFormat.KEY_FIELDS_CONFIG, fields)
                                                .customProperty(
                                                        KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG,
                                                        "EXCEPT_KEY")
                                                .build()))
                .isInstanceOf(KafkaTopicSchemaException.class);
    }

    @Test
    public void testRejectsAmbiguousAndEmptyValueProjections() {
        assertInvalid(
                keyValueTable()
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "id"),
                "requires");
        assertInvalid(
                valueTable().customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "bad"),
                "Expected ALL or EXCEPT_KEY");
        assertInvalid(
                valueTable()
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "body")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY"),
                "at least one Fluss column");
        assertInvalid(keyValueTable(), "exactly one Fluss field");
        assertInvalid(
                keyValueTable()
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "id,body")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY"),
                "at least one Fluss column");
    }

    @Test
    public void testRejectsWrongPhysicalTypes() {
        assertInvalid(
                valueTable().customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string"),
                "must be STRING");
        assertInvalid(
                keyValueTable()
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "id")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY"),
                "must be BYTES");
    }

    @Test
    public void testRejectsWrongTimestampTypes() {
        for (DataType type :
                new DataType[] {
                    DataTypes.STRING(),
                    DataTypes.TIMESTAMP_LTZ(3),
                    DataTypes.TIMESTAMP_LTZ(6).copy(false)
                }) {
            assertInvalid(
                    table(
                                    Schema.newBuilder()
                                            .column("body", DataTypes.BYTES())
                                            .column("ts", type)
                                            .build(),
                                    "raw")
                            .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "ts"),
                    "TIMESTAMP_LTZ(3) NOT NULL");
        }
    }

    @Test
    public void testRejectsWrongHeaderTypes() {
        for (DataType type :
                new DataType[] {
                    DataTypes.STRING(),
                    headersType().copy(false),
                    DataTypes.ARRAY(DataTypes.STRING()),
                    DataTypes.ARRAY(
                            DataTypes.ROW(
                                    DataTypes.FIELD("key", DataTypes.STRING()),
                                    DataTypes.FIELD("value", DataTypes.BYTES()))),
                    DataTypes.ARRAY(
                            DataTypes.ROW(
                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                    DataTypes.FIELD("value", DataTypes.BYTES().copy(false))))
                }) {
            assertInvalid(
                    table(
                                    Schema.newBuilder()
                                            .column("body", DataTypes.BYTES())
                                            .column("attributes", type)
                                            .build(),
                                    "raw")
                            .customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "attributes"),
                    "headers");
        }
    }

    @Test
    public void testRejectsConflictingOrMissingMetadataColumns() {
        assertInvalid(
                valueTable().customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "missing"),
                "does not exist");
        assertInvalid(
                valueTable().customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, " "),
                "must not be empty");
        TableDescriptor.Builder table =
                table(
                                Schema.newBuilder()
                                        .column("body", DataTypes.BYTES())
                                        .column("ts", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                                        .build(),
                                "raw")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "ts");
        assertInvalid(
                TableDescriptor.builder(table.build())
                        .customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "ts"),
                "same Fluss column");
        assertInvalid(
                TableDescriptor.builder(table.build())
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "ts"),
                "metadata column");
    }

    private void assertInvalid(TableDescriptor.Builder table, String message) {
        assertThatThrownBy(() -> resolver.resolve(table.build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining(message);
    }

    private static TableDescriptor.Builder keyValueTable() {
        return table(
                Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("body", DataTypes.BYTES())
                        .build(),
                "raw");
    }

    private static TableDescriptor.Builder valueTable() {
        return table(Schema.newBuilder().column("body", DataTypes.BYTES()).build(), "raw");
    }

    private static TableDescriptor.Builder table(Schema schema, String valueFormat) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(2)
                .logFormat(LogFormat.ARROW)
                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, valueFormat);
    }

    private static DataType headersType() {
        return DataTypes.ARRAY(
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("value", DataTypes.BYTES())));
    }
}
