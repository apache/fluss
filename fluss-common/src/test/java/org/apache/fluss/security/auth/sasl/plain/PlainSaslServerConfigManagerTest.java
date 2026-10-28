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

        String effectiveJaas =
                managedConfiguration.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG);
        Configuration invalid = new Configuration();
        invalid.set(ConfigOptions.SERVER_SASL_CREDENTIALS, credentials("bob", "pass;word"));
        assertThatThrownBy(() -> manager.reconfigure(invalid)).isInstanceOf(ConfigException.class);
        assertThat(managedConfiguration.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG))
                .isEqualTo(effectiveJaas);

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
    void testDuplicateCredentialsDoNotLeakPasswords() {
        PlainSaslServerConfigManager manager =
                new PlainSaslServerConfigManager(new Configuration());
        Configuration duplicateCredentials = new Configuration();
        duplicateCredentials.setString(
                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                "alice:first-secret,alice:second-secret");

        assertThatThrownBy(() -> manager.validate(duplicateCredentials))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Failed to parse security.sasl.plain.credentials.")
                .hasNoCause();
        assertThat(
                        manager.getConfiguration()
                                .getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG))
                .isNull();
    }

    private static Map<String, String> credentials(String... values) {
        Map<String, String> credentials = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            credentials.put(values[i], values[i + 1]);
        }
        return credentials;
    }
}
