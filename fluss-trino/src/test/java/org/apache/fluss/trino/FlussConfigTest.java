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

package org.apache.fluss.trino;

import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;

import io.airlift.configuration.ConfigurationFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests connector configuration through Airlift's configuration binding. */
final class FlussConfigTest {
    @Test
    void testDefaults() {
        assertRecordedDefaults(
                recordDefaults(FlussConfig.class)
                        .setBootstrapServers(null)
                        .setSecurityProtocol(null)
                        .setSaslMechanism(null)
                        .setSaslUsername(null)
                        .setSaslPassword(null));
    }

    @Test
    void testExplicitPropertyMappings() {
        Map<String, String> properties =
                ImmutableMap.of(
                        "bootstrap.servers", "localhost:9123,localhost:9124",
                        "client.security.protocol", "SASL",
                        "client.security.sasl.mechanism", "PLAIN",
                        "client.security.sasl.username", "test-user",
                        "client.security.sasl.password", "test-password");
        FlussConfig expected =
                new FlussConfig()
                        .setBootstrapServers("localhost:9123,localhost:9124")
                        .setSecurityProtocol("SASL")
                        .setSaslMechanism("PLAIN")
                        .setSaslUsername("test-user")
                        .setSaslPassword("test-password");
        assertFullMapping(properties, expected);
    }

    @Test
    void testBootstrapServersRequired() {
        assertThatThrownBy(
                        () ->
                                new ConfigurationFactory(Collections.emptyMap())
                                        .build(FlussConfig.class))
                .hasMessageContaining("bootstrap.servers");
        assertThatThrownBy(
                        () ->
                                new ConfigurationFactory(ImmutableMap.of("bootstrap.servers", ""))
                                        .build(FlussConfig.class))
                .hasMessageContaining("bootstrap.servers");
    }

    @Test
    void testSecurityConfigurationIsOptional() {
        FlussConfig config =
                new ConfigurationFactory(ImmutableMap.of("bootstrap.servers", "localhost:9123"))
                        .build(FlussConfig.class);
        assertThat(config.getSecurityProtocol()).isEmpty();
        assertThat(config.getSaslMechanism()).isEmpty();
        assertThat(config.getSaslUsername()).isEmpty();
        assertThat(config.getSaslPassword()).isEmpty();
    }
}
