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

package org.apache.fluss.security.auth.impersonation;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ImpersonationServerAuthenticator}. */
class ImpersonationServerAuthenticatorTest {

    private static final String PROXY = "proxy";
    private static final String PROXY_PASSWORD = "proxyPassword";
    private static final String WILDCARD_PROXY = "gateway";
    private static final String WILDCARD_PROXY_PASSWORD = "gatewayPassword";
    private static final String ALICE = "alice";
    private static final String ALICE_PASSWORD = "alicePassword";
    private static final String BOB = "bob";
    private static final String ROOT = "root";

    @Test
    void testActsAsAuthenticatedUserWhenNoOtherUserRequested() throws Exception {
        ServerAuthenticator authenticator = authenticate(PROXY, PROXY_PASSWORD, null);

        assertThat(authenticator.createPrincipal()).isEqualTo(new FlussPrincipal(PROXY, "User"));
    }

    @Test
    void testActsAsAllowedUser() throws Exception {
        // the target user needs no credentials of its own
        ServerAuthenticator authenticator = authenticate(PROXY, PROXY_PASSWORD, ALICE);

        assertThat(authenticator.createPrincipal()).isEqualTo(new FlussPrincipal(ALICE, "User"));
    }

    @Test
    void testWildcardAllowsAnyUser() throws Exception {
        ServerAuthenticator authenticator =
                authenticate(WILDCARD_PROXY, WILDCARD_PROXY_PASSWORD, BOB);

        assertThat(authenticator.createPrincipal()).isEqualTo(new FlussPrincipal(BOB, "User"));
    }

    @Test
    void testActingAsAnotherUserRequiresAGrant() {
        // alice is not listed as a proxy user at all
        assertThatThrownBy(() -> authenticate(ALICE, ALICE_PASSWORD, BOB))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Authentication failed: user 'alice' is not allowed to act as 'bob'");

        // proxy is listed, but bob is not among the users it may act as
        assertThatThrownBy(() -> authenticate(PROXY, PROXY_PASSWORD, BOB))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Authentication failed: user 'proxy' is not allowed to act as 'bob'");
    }

    @Test
    void testSuperUserCanNeverBeImpersonated() {
        // 'gateway' is granted the '*' wildcard, which must still not cover super users.
        assertThatThrownBy(() -> authenticate(WILDCARD_PROXY, WILDCARD_PROXY_PASSWORD, ROOT))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage(
                        "Authentication failed: user 'gateway' is not allowed to act as 'root' because it is a super user");
    }

    private static ServerAuthenticator authenticate(
            String username, String password, String actAsUser) throws Exception {
        ServerAuthenticator authenticator =
                new ImpersonationAuthenticationPlugin()
                        .createServerAuthenticator(serverConfiguration());
        authenticator.initialize(new TestingAuthenticateContext());
        authenticator.evaluateResponse(saslMessage(actAsUser, username, password));
        assertThat(authenticator.isCompleted()).isTrue();
        return authenticator;
    }

    private static Configuration serverConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("PLAIN"));
        configuration.setString(
                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG,
                String.format(
                        "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required"
                                + " user_%s=\"%s\" user_%s=\"%s\" user_%s=\"%s\";",
                        PROXY,
                        PROXY_PASSWORD,
                        WILDCARD_PROXY,
                        WILDCARD_PROXY_PASSWORD,
                        ALICE,
                        ALICE_PASSWORD));
        Map<String, String> proxyUsers = new HashMap<>();
        proxyUsers.put(PROXY, ALICE + ";carol");
        proxyUsers.put(WILDCARD_PROXY, "*");
        configuration.set(ConfigOptions.SERVER_IMPERSONATION_PROXY_USERS, proxyUsers);
        configuration.set(ConfigOptions.SUPER_USERS, "User:" + ROOT);
        return configuration;
    }

    private static byte[] saslMessage(String actAsUser, String username, String password) {
        String nul = "\u0000";
        String authorizationId = actAsUser == null ? "" : actAsUser;
        return String.format("%s%s%s%s%s", authorizationId, nul, username, nul, password)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static class TestingAuthenticateContext
            implements ServerAuthenticator.AuthenticateContext {

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
            return "PLAIN";
        }
    }
}
