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
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests table-level Kafka format and field projection resolution. */
public class KafkaTopicSchemaResolverTest {

    private final KafkaTopicSchemaResolver resolver = new KafkaTopicSchemaResolver();

    @Test
    public void testResolvesUnifiedRawContract() {
        TableDescriptor descriptor =
                baseDescriptor(defaultSchema())
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "record_key")
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "event_time")
                        .customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "headers")
                        .build();

        KafkaTopicSchema topicSchema = resolver.resolve(tableInfo(descriptor));

        assertThat(topicSchema.keyFormat()).isEqualTo(KafkaDataFormat.RAW);
        assertThat(topicSchema.keyProjection().positions()).containsExactly(0);
        assertThat(topicSchema.valueFormat()).isEqualTo(KafkaDataFormat.RAW);
        assertThat(topicSchema.valueProjection().positions()).containsExactly(1);
        assertThat(topicSchema.timestampPosition()).isEqualTo(2);
        assertThat(topicSchema.headersPosition()).isEqualTo(3);
    }

    @Test
    public void testAcceptsNullableHeaderNameFromFlinkCatalog() {
        TableDescriptor descriptor =
                baseDescriptor(defaultSchemaWithNullableHeaderName())
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "record_key")
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "event_time")
                        .customProperty(KafkaDataFormat.HEADERS_COLUMN_CONFIG, "headers")
                        .build();

        KafkaTopicSchema topicSchema = resolver.resolve(tableInfo(descriptor));

        assertThat(topicSchema.headersPosition()).isEqualTo(3);
    }

    @Test
    public void testResolvesJsonValueProjection() {
        Schema schema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.STRING().copy(false))
                        .column("customer_id", DataTypes.BIGINT())
                        .column("amount", DataTypes.DECIMAL(18, 2))
                        .column("event_time", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                        .build();
        TableDescriptor descriptor =
                baseDescriptor(schema)
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "order_id")
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "event_time")
                        .build();

        KafkaTopicSchema topicSchema = resolver.resolve(tableInfo(descriptor));

        assertThat(topicSchema.keyProjection().positions()).containsExactly(0);
        assertThat(topicSchema.valueFormat()).isEqualTo(KafkaDataFormat.JSON);
        assertThat(topicSchema.valueProjection().positions()).containsExactly(1, 2);
        assertThat(topicSchema.timestampPosition()).isEqualTo(3);
        assertThat(topicSchema.headersPosition()).isEqualTo(-1);
    }

    @Test
    public void testResolvesJsonRescueColumn() {
        Schema schema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.STRING().copy(false))
                        .column("amount", DataTypes.DECIMAL(18, 2))
                        .column("kafka_rescue", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                baseDescriptor(schema)
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "order_id")
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG, "kafka_rescue")
                        .build();

        KafkaTopicSchema topicSchema = resolver.resolve(tableInfo(descriptor));

        assertThat(topicSchema.valueProjection().positions()).containsExactly(1, 2);
        assertThat(topicSchema.valueRescueColumn()).isEqualTo("kafka_rescue");
    }

    @Test
    public void testRejectsInvalidJsonRescueColumnContract() {
        assertInvalidRescueColumn(DataTypes.BYTES(), "must be nullable STRING");
        assertInvalidRescueColumn(DataTypes.STRING().copy(false), "must be nullable STRING");

        TableDescriptor nonJsonDescriptor =
                baseDescriptor(
                                Schema.newBuilder()
                                        .column("payload", DataTypes.STRING())
                                        .column("kafka_rescue", DataTypes.STRING())
                                        .build())
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG, "kafka_rescue")
                        .build();
        assertThatThrownBy(() -> resolver.resolve(tableInfo(nonJsonDescriptor)))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("only supported for Kafka JSON values");
    }

    @Test
    public void testDoesNotReadPrototypeFlussProperties() {
        TableDescriptor descriptor =
                baseDescriptor(defaultSchema())
                        .customProperty("fluss.key.format", "raw")
                        .customProperty("fluss.value.format", "raw")
                        .build();

        assertThatThrownBy(() -> resolver.resolve(tableInfo(descriptor)))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining(KafkaDataFormat.VALUE_FORMAT_CONFIG);
    }

    @Test
    public void testRejectsJsonAllWhenKeyFieldsExist() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                baseDescriptor(schema)
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "id")
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                        .build();

        assertThatThrownBy(() -> resolver.resolve(tableInfo(descriptor)))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("requires")
                .hasMessageContaining("EXCEPT_KEY");
    }

    @Test
    public void testRejectsInvalidMetadataType() {
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.STRING())
                        .column("event_time", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                baseDescriptor(schema)
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "event_time")
                        .build();

        assertThatThrownBy(() -> resolver.resolve(tableInfo(descriptor)))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("TIMESTAMP_LTZ(3) NOT NULL");
    }

    private static Schema defaultSchema() {
        return Schema.newBuilder()
                .column("record_key", DataTypes.BYTES())
                .column("payload", DataTypes.BYTES())
                .column("event_time", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                .column(
                        "headers",
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING().copy(false)),
                                        DataTypes.FIELD("value", DataTypes.BYTES()))))
                .build();
    }

    private static Schema defaultSchemaWithNullableHeaderName() {
        return Schema.newBuilder()
                .column("record_key", DataTypes.BYTES())
                .column("payload", DataTypes.BYTES())
                .column("event_time", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                .column(
                        "headers",
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("value", DataTypes.BYTES()))))
                .build();
    }

    private void assertInvalidRescueColumn(
            org.apache.fluss.types.DataType rescueType, String expectedMessage) {
        TableDescriptor descriptor =
                baseDescriptor(
                                Schema.newBuilder()
                                        .column("value", DataTypes.BIGINT())
                                        .column("kafka_rescue", rescueType)
                                        .build())
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                        .customProperty(KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG, "kafka_rescue")
                        .build();

        assertThatThrownBy(() -> resolver.resolve(tableInfo(descriptor)))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining(expectedMessage);
    }

    private static TableDescriptor.Builder baseDescriptor(Schema schema) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(1)
                .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW);
    }

    private static TableInfo tableInfo(TableDescriptor descriptor) {
        return TableInfo.of(TablePath.of("kafka", "topic"), 1L, 1, descriptor, null, 1L, 1L);
    }
}
