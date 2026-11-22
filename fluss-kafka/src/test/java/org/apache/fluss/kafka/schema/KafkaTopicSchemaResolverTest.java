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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Kafka adapter's use of the shared DDL contract. */
class KafkaTopicSchemaResolverTest {
    private final KafkaTopicSchemaResolver resolver = new KafkaTopicSchemaResolver();

    @Test
    void testCompatibleSchemaDoesNotEnableTable() {
        assertThat(resolver.isEnabled(table().build())).isFalse();
        assertThatThrownBy(() -> resolver.resolve(table().build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("disabled");
    }

    @Test
    void testInvalidEnabledContractIsNotTreatedAsDisabled() {
        TableDescriptor table =
                table().property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "schema_latest")
                        .build();
        assertThat(resolver.isEnabled(table)).isTrue();
        assertThatThrownBy(() -> resolver.resolve(table))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("schema_latest");
    }

    @Test
    void testMalformedPersistedEnableFlagIsAnExplicitContractError() {
        TableDescriptor table = table().property("table.kafka.enabled", "maybe").build();
        assertThatThrownBy(() -> resolver.isEnabled(table))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("table.kafka.enabled");
    }

    private static TableDescriptor.Builder table() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("_kafka_value", DataTypes.BYTES()).build())
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "binary");
    }
}
