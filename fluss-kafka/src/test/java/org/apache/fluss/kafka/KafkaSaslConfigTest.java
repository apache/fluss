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
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Kafka listener authentication configuration. */
public class KafkaSaslConfigTest {
    @Test
    public void testKafkaPluginPreservesSuperUserCredentialProtection() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.SUPER_USERS, "User:root");
        config.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS,
                Collections.singletonMap("root", "old-secret"));
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(config);
        Configuration changed = new Configuration(config);
        changed.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS,
                Collections.singletonMap("root", "new-secret"));
        assertThatThrownBy(() -> plugin.validate(changed, new FlussPrincipal("operator", "User")))
                .isInstanceOf(AuthorizationException.class);
        plugin.validate(changed, new FlussPrincipal("root", "User"));
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
}
