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

package org.apache.fluss.security.auth;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.ValidationException;
import org.apache.fluss.security.auth.TestIdentifierAuthenticationPlugin.TestIdentifierClientAuthenticator;
import org.apache.fluss.security.auth.TestIdentifierAuthenticationPlugin.TestIdentifierServerAuthenticator;
import org.apache.fluss.utils.ParentResourceBlockingClassLoader;
import org.apache.fluss.utils.TemporaryClassLoaderContext;

import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AuthenticationFactory}. */
public class AuthenticationFactoryTest {

    @Test
    void testConflictingAuthenticationPlugin() {
        Configuration configuration = new Configuration();
        configuration.setString("client.security.protocol", "conflicting");
        configuration.setString("security.protocol.map", "FLUSS:conflicting");
        String errorMsg =
                "Multiple plugins for the same protocol 'conflicting' are found in the classpath.\n\n"
                        + "Available plugins are:\n\n"
                        + "org.apache.fluss.security.auth.TestConflictingAuthenticationPlugin1\n"
                        + "org.apache.fluss.security.auth.TestConflictingAuthenticationPlugin2";
        assertThatThrownBy(
                        () -> AuthenticationFactory.loadClientAuthenticatorSupplier(configuration))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining(errorMsg);
        assertThatThrownBy(
                        () -> AuthenticationFactory.loadServerAuthenticatorSuppliers(configuration))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining(errorMsg);
    }

    @Test
    void testNoAuthenticationPlugin() {
        Configuration configuration = new Configuration();
        configuration.setString("client.security.protocol", "no_authentication");
        configuration.setString("security.protocol.map", "FLUSS:no_authentication");
        String errorMsg =
                "No plugin for the protocol 'no_authentication' is found in the classpath.";
        assertThatThrownBy(
                        () -> AuthenticationFactory.loadClientAuthenticatorSupplier(configuration))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining(errorMsg);
        assertThatThrownBy(
                        () -> AuthenticationFactory.loadServerAuthenticatorSuppliers(configuration))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining(errorMsg);
    }

    @Test
    void testIdentifierCaseInsensitive() {
        Configuration configuration = new Configuration();
        configuration.setString("client.security.protocol", "SSL_TEST");
        configuration.setString("security.protocol.map", "FLUSS:SSL_TEST");
        assertThat(AuthenticationFactory.loadClientAuthenticatorSupplier(configuration).get())
                .isInstanceOf(TestIdentifierClientAuthenticator.class);
        assertThat(
                        AuthenticationFactory.loadServerAuthenticatorSuppliers(configuration)
                                .values()
                                .stream()
                                .findAny()
                                .get()
                                .get())
                .isInstanceOf(TestIdentifierServerAuthenticator.class);

        Configuration configuration2 = new Configuration();
        configuration2.setString("client.security.protocol", "ssl_test");
        configuration2.setString("security.protocol.map", "FLUSS:ssl_test");
        assertThat(AuthenticationFactory.loadClientAuthenticatorSupplier(configuration2).get())
                .isInstanceOf(TestIdentifierClientAuthenticator.class);
        assertThat(
                        AuthenticationFactory.loadServerAuthenticatorSuppliers(configuration)
                                .values()
                                .stream()
                                .findAny()
                                .get()
                                .get())
                .isInstanceOf(TestIdentifierServerAuthenticator.class);
    }

    @Test
    void testNotIncludedInThreadContextClassloader() {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(new ParentResourceBlockingClassLoader(new URL[0]))) {
            Configuration configuration = new Configuration();
            configuration.setString("client.security.protocol", "SSL_TEST");
            configuration.setString("security.protocol.map", "FLUSS:SSL_TEST");
            assertThat(AuthenticationFactory.loadClientAuthenticatorSupplier(configuration).get())
                    .isInstanceOf(TestIdentifierClientAuthenticator.class);
        }
    }
}
