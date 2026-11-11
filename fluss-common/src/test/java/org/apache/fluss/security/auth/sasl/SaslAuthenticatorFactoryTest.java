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

package org.apache.fluss.security.auth.sasl;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.SaslAuthenticationPlugin;
import org.apache.fluss.security.auth.sasl.authenticator.SaslClientAuthenticator;
import org.apache.fluss.security.auth.sasl.jaas.JaasContext;
import org.apache.fluss.security.auth.sasl.jaas.LoginManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SaslAuthenticatorFactory}. */
class SaslAuthenticatorFactoryTest {

    @Test
    void testCreatesIndependentClientAuthenticators() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.CLIENT_SASL_MECHANISM, "plain");

        ClientAuthenticator firstClient =
                SaslAuthenticatorFactory.createClientAuthenticator(configuration);
        ClientAuthenticator secondClient =
                SaslAuthenticatorFactory.createClientAuthenticator(configuration);

        assertThat(firstClient).isNotSameAs(secondClient);
        assertThat(firstClient.protocol()).isEqualTo("PLAIN");
    }

    @ParameterizedTest
    @ValueSource(strings = {"unknown", "DIGEST-MD5"})
    void testRejectsUnsupportedClientMechanismDuringInitialization(String mechanism) {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.CLIENT_SASL_MECHANISM, mechanism);
        String normalizedMechanism = mechanism.toUpperCase(Locale.ROOT);

        try (ClientAuthenticator authenticator =
                        SaslAuthenticatorFactory.createClientAuthenticator(configuration);
                ClientAuthenticator directAuthenticator =
                        new SaslClientAuthenticator(configuration)) {
            assertThat(authenticator.protocol()).isEqualTo(normalizedMechanism);
            assertThat(authenticator.isCompleted()).isFalse();
            assertThatThrownBy(() -> authenticator.initialize(() -> "localhost"))
                    .isInstanceOf(AuthenticationException.class)
                    .hasMessage(
                            "Unable to find a matching SASL mechanism for " + normalizedMechanism);
            assertThatThrownBy(() -> directAuthenticator.initialize(() -> "localhost"))
                    .isInstanceOf(AuthenticationException.class)
                    .hasMessage(
                            "Unable to find a matching SASL mechanism for " + normalizedMechanism);
        }
    }

    @Test
    void testServerAuthenticatorReleasesLoginManager() throws Exception {
        String jaasConfig =
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_factory=\"factory-secret\";";
        Configuration configuration = new Configuration();
        configuration.setString("security.sasl.enabled.mechanisms", "PLAIN");
        configuration.setString("security.sasl.plain.jaas.config", jaasConfig);
        JaasContext jaasContext = JaasContext.loadServerContext("CLIENT", jaasConfig);
        ServerAuthenticator.AuthenticateContext context =
                new ServerAuthenticator.AuthenticateContext() {
                    @Override
                    public String ipAddress() {
                        return "localhost";
                    }

                    @Override
                    public String listenerName() {
                        return "CLIENT";
                    }

                    @Override
                    public String protocol() {
                        return "PLAIN";
                    }
                };

        LoginManager secondLogin;
        try (ServerAuthenticator authenticator =
                new SaslAuthenticationPlugin().createServerAuthenticator(configuration)) {
            authenticator.initialize(context);
            LoginManager firstLogin = LoginManager.acquireLoginManager(jaasContext);
            firstLogin.release();

            authenticator.initialize(context);
            secondLogin = LoginManager.acquireLoginManager(jaasContext);
            secondLogin.release();
            assertThat(secondLogin).isNotSameAs(firstLogin);
        }

        LoginManager newLogin = LoginManager.acquireLoginManager(jaasContext);
        try {
            assertThat(newLogin).isNotSameAs(secondLogin);
        } finally {
            newLogin.release();
        }
    }
}
