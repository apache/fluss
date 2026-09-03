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

package org.apache.fluss.kafka;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Kafka configuration. */
public class KafkaConfigsTest {
    @Test
    public void testFromMap() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(ConfigOptions.KAFKA_ENABLED.key(), "true");
        map.put(ConfigOptions.KAFKA_LISTENER_NAMES.key(), "kafka,kafka_sasl");
        map.put(ConfigOptions.KAFKA_DATABASE.key(), "fluss");
        map.put(ConfigOptions.KAFKA_DEFAULT_KEY_FORMAT.key(), "string");
        map.put(ConfigOptions.KAFKA_DEFAULT_VALUE_FORMAT.key(), "string");
        Configuration configuration = Configuration.fromMap(map);

        assertThat(configuration.getBoolean(ConfigOptions.KAFKA_ENABLED)).isTrue();
        assertThat(configuration.get(ConfigOptions.KAFKA_LISTENER_NAMES))
                .isEqualTo(Arrays.asList("kafka", "kafka_sasl"));
        assertThat(configuration.getString(ConfigOptions.KAFKA_DATABASE)).isEqualTo("fluss");
        assertThat(configuration.getString(ConfigOptions.KAFKA_DEFAULT_KEY_FORMAT))
                .isEqualTo("string");
        assertThat(configuration.getString(ConfigOptions.KAFKA_DEFAULT_VALUE_FORMAT))
                .isEqualTo("string");
    }

    @Test
    public void testFromDefault() throws Exception {
        Configuration configuration = Configuration.fromMap(new HashMap<>());
        assertThat(configuration.getBoolean(ConfigOptions.KAFKA_ENABLED)).isFalse();
        assertThat(configuration.get(ConfigOptions.KAFKA_LISTENER_NAMES))
                .isEqualTo(Collections.singletonList("KAFKA"));
        assertThat(configuration.getString(ConfigOptions.KAFKA_DATABASE)).isEqualTo("kafka");
        assertThat(configuration.getString(ConfigOptions.KAFKA_DEFAULT_KEY_FORMAT))
                .isEqualTo("raw");
        assertThat(configuration.getString(ConfigOptions.KAFKA_DEFAULT_VALUE_FORMAT))
                .isEqualTo("raw");
    }

    @Test
    public void testKafkaSaslPlainConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "sasl"));
        configuration.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("PLAIN"));
        configuration.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS,
                Collections.singletonMap("writer", "writer-secret"));

        new KafkaProtocolPlugin().setup(configuration);
    }

    @Test
    public void testKafkaSaslRequiresPlainMechanism() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "sasl"));
        configuration.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("SCRAM-SHA-256"));

        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(configuration))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("require PLAIN");
    }

    @Test
    public void testKafkaListenerRejectsNonSaslAuthenticationPlugin() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "custom"));

        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(configuration))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("supports only PLAINTEXT or SASL authentication");
    }

    @Test
    public void testKafkaListenerAcceptsExplicitPlaintextProtocol() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "PLAINTEXT"));

        new KafkaProtocolPlugin().setup(configuration);
    }
}
