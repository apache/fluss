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

package org.apache.fluss.security.auth.ssl;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MutualTlsAuthenticationPlugin}. */
class MutualTlsAuthenticationPluginTest {

    private final MutualTlsAuthenticationPlugin plugin = new MutualTlsAuthenticationPlugin();

    @Test
    void testClientSendsOneEmptyToken() {
        ClientAuthenticator authenticator = plugin.createClientAuthenticator(new Configuration());
        assertThat(authenticator.protocol()).isEqualTo("mTLS");
        // complete before the first call, the client would send no authenticate request at all.
        assertThat(authenticator.isCompleted()).isFalse();

        assertThat(authenticator.authenticate(new byte[0])).isNotNull().isEmpty();
        assertThat(authenticator.isCompleted()).isTrue();
    }

    @Test
    void testServerRejectsConnectionWithoutTls() {
        ServerAuthenticator authenticator = plugin.createServerAuthenticator(new Configuration());
        authenticator.initialize(
                new ServerAuthenticator.AuthenticateContext() {
                    @Override
                    public String ipAddress() {
                        return "127.0.0.1";
                    }

                    @Override
                    public String listenerName() {
                        return "CLIENT";
                    }

                    @Override
                    public String protocol() {
                        return "mTLS";
                    }
                });

        assertThatThrownBy(() -> authenticator.evaluateResponse(new byte[0]))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("'CLIENT'")
                .hasMessageContaining("requires TLS transport");
        assertThat(authenticator.isCompleted()).isFalse();
    }
}
