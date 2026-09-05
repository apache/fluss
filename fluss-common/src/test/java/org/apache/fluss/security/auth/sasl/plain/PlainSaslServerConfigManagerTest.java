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

package org.apache.fluss.security.auth.sasl.plain;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlainSaslServerConfigManagerTest {

    @Test
    void testInitialCredentialMapIsMergedWithJaasConfig() {
        Configuration initialConfiguration = new Configuration();
        initialConfiguration.setString(
                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG,
                PlainLoginModule.class.getName()
                        + " required user_admin=\"old-secret\" user_alice=\"alice-secret\";");
        initialConfiguration.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS,
                credentials("admin", "new-secret", "bob", "bob-secret"));

        PlainSaslServerConfigManager manager =
                new PlainSaslServerConfigManager(initialConfiguration);

        assertThat(
                        manager.getConfiguration()
                                .getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG))
                .isEqualTo(
                        PlainLoginModule.class.getName()
                                + " required user_admin=\"new-secret\""
                                + " user_alice=\"alice-secret\""
                                + " user_bob=\"bob-secret\";");
        assertThat(initialConfiguration.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG))
                .contains("user_admin=\"old-secret\"")
                .doesNotContain("user_bob");
    }

    @Test
    void testReconfigureUpdatesStableManagedConfiguration() {
        Configuration initialConfiguration = new Configuration();
        initialConfiguration.setString(
                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG,
                PlainLoginModule.class.getName() + " required user_admin=\"admin-secret\";");
        PlainSaslServerConfigManager manager =
                new PlainSaslServerConfigManager(initialConfiguration);
        Configuration managedConfiguration = manager.getConfiguration();

        Configuration addBobConfiguration = new Configuration();
        addBobConfiguration.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS, credentials("bob", "bob-secret"));
        manager.validate(addBobConfiguration);
        manager.reconfigure(addBobConfiguration);

        assertThat(manager.getConfiguration()).isSameAs(managedConfiguration);
        assertThat(managedConfiguration.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG))
                .isEqualTo(
                        PlainLoginModule.class.getName()
                                + " required user_admin=\"admin-secret\""
                                + " user_bob=\"bob-secret\";");

        Configuration removeBobConfiguration = new Configuration();
        manager.validate(removeBobConfiguration);
        manager.reconfigure(removeBobConfiguration);

        assertThat(manager.getConfiguration()).isSameAs(managedConfiguration);
        assertThat(managedConfiguration.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG))
                .isEqualTo(
                        PlainLoginModule.class.getName()
                                + " required user_admin=\"admin-secret\";");
    }

    @Test
    void testValidationDoesNotApplyCredentials() {
        PlainSaslServerConfigManager manager =
                new PlainSaslServerConfigManager(new Configuration());
        Configuration managedConfiguration = manager.getConfiguration();
        Configuration newConfiguration = new Configuration();
        newConfiguration.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS, credentials("alice", "alice-secret"));

        manager.validate(newConfiguration);

        assertThat(managedConfiguration.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG))
                .isNull();
    }

    @Test
    void testInitialConfigurationRejectsInvalidCredentials() {
        Configuration invalidUsername = new Configuration();
        invalidUsername.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS, credentials("user-name", "secret"));
        assertThatThrownBy(() -> new PlainSaslServerConfigManager(invalidUsername))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("username 'user-name' contains invalid characters");

        Configuration invalidPassword = new Configuration();
        invalidPassword.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS, credentials("user", "pass;word"));
        assertThatThrownBy(() -> new PlainSaslServerConfigManager(invalidPassword))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("password for user 'user' contains invalid characters");

        Configuration emptyPassword = new Configuration();
        emptyPassword.set(ConfigOptions.SERVER_SASL_CREDENTIALS, credentials("user", ""));
        assertThatThrownBy(() -> new PlainSaslServerConfigManager(emptyPassword))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("password for user 'user' must not be empty");
    }

    @Test
    void testRejectsMalformedCredentialMapString() {
        PlainSaslServerConfigManager manager =
                new PlainSaslServerConfigManager(new Configuration());
        Configuration malformedConfiguration = new Configuration();
        malformedConfiguration.setString(
                ConfigOptions.SERVER_SASL_CREDENTIALS.key(), "bob:pass,word");

        assertThatThrownBy(() -> manager.validate(malformedConfiguration))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Failed to parse security.sasl.plain.credentials")
                .hasMessageNotContaining("pass,word");
    }

    @Test
    void testReconfigureRejectsInvalidCredentialsWithoutMutatingManagedConfiguration() {
        Configuration initialConfiguration = new Configuration();
        initialConfiguration.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS, credentials("admin", "admin-secret"));
        PlainSaslServerConfigManager manager =
                new PlainSaslServerConfigManager(initialConfiguration);
        String effectiveJaas =
                manager.getConfiguration().getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG);
        Configuration invalidConfiguration = new Configuration();
        invalidConfiguration.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS, credentials("bob", "pass;word"));

        assertThatThrownBy(() -> manager.reconfigure(invalidConfiguration))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("password for user 'bob' contains invalid characters");
        assertThat(
                        manager.getConfiguration()
                                .getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG))
                .isEqualTo(effectiveJaas);
    }

    private static Map<String, String> credentials(String... values) {
        Map<String, String> credentials = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            credentials.put(values[i], values[i + 1]);
        }
        return credentials;
    }
}
