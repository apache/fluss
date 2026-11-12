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
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.SaslServerAuthenticator;
import org.apache.fluss.security.auth.sasl.jaas.JaasContext;
import org.apache.fluss.security.auth.sasl.jaas.LoginManager;
import org.apache.fluss.security.auth.sasl.jaas.SaslServerFactory;
import org.apache.fluss.security.auth.sasl.jaas.TestJaasConfig;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.security.auth.sasl.plain.PlainSaslProxyAuthorization.proxyUsersOption;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PlainSaslServer}. */
public class PlainSaslServerTest {

    private static final String USER_A = "userA";
    private static final String PASSWORD_A = "passwordA";
    private static final String USER_B = "userB";
    private static final String PASSWORD_B = "passwordB";

    private static final String PROXY = "proxy";
    private static final String PROXY_PASSWORD = "proxyPassword";

    private SaslServer saslServer;

    @BeforeEach
    public void setUp() {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        Map<String, String> options = new HashMap<>();
        options.put("user_" + USER_A, PASSWORD_A);
        options.put("user_" + USER_B, PASSWORD_B);
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), options);
        JaasContext jaasContext =
                new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        saslServer =
                SaslServerFactory.createSaslServer(
                        "PLAIN",
                        jaasContext.configurationEntries(),
                        PlainSaslProxyAuthorization.disabled());
    }

    @Test
    public void testNoAuthorizationIdSpecified() throws SaslException {
        assertThat(saslServer.evaluateResponse(saslMessage("", USER_A, PASSWORD_A))).isEmpty();
        assertThat(saslServer.evaluateResponse(saslMessage("", USER_B, PASSWORD_B))).isEmpty();
    }

    @Test
    public void testClientServerAuthentication() throws Exception {
        JaasContext clientContext =
                JaasContext.loadClientContext(
                        PlainLoginModule.class.getName()
                                + " required username=\""
                                + USER_A
                                + "\" password=\""
                                + PASSWORD_A
                                + "\";");
        LoginManager clientLoginManager = LoginManager.acquireLoginManager(clientContext);
        try {
            SaslClient saslClient =
                    SaslServerFactory.createSaslClient(
                            "PLAIN", "127.0.0.1", Collections.emptyMap(), clientLoginManager);
            assertThat(saslClient).isNotNull();
            try {
                assertThat(saslServer).isInstanceOf(PlainSaslServer.class);
                assertThat(saslClient.hasInitialResponse()).isTrue();
                byte[] response = saslClient.evaluateChallenge(new byte[0]);
                assertThat(response).isEqualTo(saslMessage("", USER_A, PASSWORD_A));
                assertThat(saslServer.evaluateResponse(response)).isEmpty();
                assertThat(saslClient.isComplete()).isTrue();
                assertThat(saslServer.isComplete()).isTrue();
                assertThat(saslServer.getAuthorizationID()).isEqualTo(USER_A);
            } finally {
                saslClient.dispose();
            }
        } finally {
            clientLoginManager.release();
        }
    }

    @Test
    public void testUnsupportedServerMechanism() {
        assertThatThrownBy(
                        () ->
                                SaslServerFactory.createSaslServer(
                                        "SCRAM-SHA-256",
                                        Collections.emptyList(),
                                        PlainSaslProxyAuthorization.disabled()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported mechanism: SCRAM-SHA-256");
    }

    @Test
    public void testAuthorizationIdEqualsAuthenticationId() throws SaslException {
        assertThat(saslServer.evaluateResponse(saslMessage(USER_A, USER_A, PASSWORD_A))).isEmpty();
        assertThat(saslServer.evaluateResponse(saslMessage(USER_B, USER_B, PASSWORD_B))).isEmpty();
    }

    @Test
    public void testAuthorizationIdNotEqualsAuthenticationId() {
        assertThatThrownBy(
                        () -> saslServer.evaluateResponse(saslMessage(USER_A, USER_B, PASSWORD_B)))
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("Authentication failed: user 'userB' is not allowed to act as 'userA'");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAuthorizationCallbackRejected(boolean unsupported) {
        SaslServer server =
                new PlainSaslServer(
                        callbacks -> {
                            for (Callback callback : callbacks) {
                                if (callback instanceof PlainAuthenticateCallback) {
                                    ((PlainAuthenticateCallback) callback).authenticated(true);
                                } else if (callback instanceof AuthorizeCallback && unsupported) {
                                    throw new UnsupportedCallbackException(callback);
                                }
                            }
                        });
        assertThatThrownBy(() -> server.evaluateResponse(saslMessage(USER_A, USER_B, PASSWORD_B)))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("Authentication failed:");
        assertThat(server.isComplete()).isFalse();
    }

    @Test
    public void testInvalidPassword() {
        assertThatThrownBy(
                        () -> saslServer.evaluateResponse(saslMessage(USER_A, USER_A, PASSWORD_B)))
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("Authentication failed: Invalid username or password");
    }

    @Test
    public void testPlainCredentialVerification() throws Exception {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry(
                "credentials", "other", Collections.singletonMap("user_" + USER_A, "other-secret"));
        jaasConfig.addEntry(
                "credentials",
                PlainLoginModule.class.getName(),
                Collections.singletonMap("user_" + USER_B, PASSWORD_B));
        jaasConfig.addEntry(
                "credentials",
                PlainLoginModule.class.getName(),
                Collections.singletonMap("user_" + USER_A, PASSWORD_A));
        PlainServerCallbackHandler verifier = new PlainServerCallbackHandler();
        verifier.configure(
                "PLAIN", Arrays.asList(jaasConfig.getAppConfigurationEntry("credentials")));
        assertThat(verifyCredentials(verifier, USER_A, PASSWORD_A.toCharArray())).isTrue();
        assertThat(verifyCredentials(verifier, USER_B, PASSWORD_B.toCharArray())).isTrue();
        assertThat(verifyCredentials(verifier, USER_A, "other-secret".toCharArray())).isFalse();
        assertThat(verifyCredentials(verifier, USER_A, PASSWORD_B.toCharArray())).isFalse();
        assertThat(verifyCredentials(verifier, "unknown", PASSWORD_A.toCharArray())).isFalse();
        assertThat(verifyCredentials(verifier, null, PASSWORD_A.toCharArray())).isFalse();
    }

    @Test
    public void testEmptyTokens() {
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("", "", "")))
                .hasMessage("Authentication failed: username not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("", "", "p")))
                .hasMessage("Authentication failed: username not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("", "u", "")))
                .hasMessage("Authentication failed: password not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("a", "", "")))
                .hasMessage("Authentication failed: username not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("a", "", "p")))
                .hasMessage("Authentication failed: username not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("a", "u", "")))
                .hasMessage("Authentication failed: password not specified");
        String nul = "\u0000";
        assertThatThrownBy(
                        () ->
                                saslServer.evaluateResponse(
                                        String.format("%s%s%s%s%s%s", "a", nul, "u", nul, "p", nul)
                                                .getBytes(StandardCharsets.UTF_8)))
                .hasMessage("Invalid SASL/PLAIN response: expected 3 tokens, got 4");
        assertThatThrownBy(
                        () ->
                                saslServer.evaluateResponse(
                                        String.format("%s%s%s", "", nul, "u")
                                                .getBytes(StandardCharsets.UTF_8)))
                .hasMessage("Invalid SASL/PLAIN response: expected 3 tokens, got 2");
    }

    @Test
    void testWildcardGrantEscapesTheTargetInTheAuditLog() throws Exception {
        Configuration configuration = serverConfiguration();
        configuration.set(proxyUsersOption("CLIENT"), Collections.singletonMap(PROXY, "*"));
        List<String> messages = new ArrayList<>();
        AbstractAppender appender =
                new AbstractAppender("proxy-audit", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        Logger logger = (Logger) LogManager.getLogger(PlainSaslProxyAuthorization.class);
        Level previousLevel = logger.getLevel();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        try (ServerAuthenticator server = createServer(configuration)) {
            String target = "line\r\nbreak";
            server.evaluateResponse(saslMessage(target, PROXY, PROXY_PASSWORD));
            assertThat(server.createPrincipal()).isEqualTo(new FlussPrincipal(target, "User"));
            assertThat(messages)
                    .containsExactly(
                            "User 'proxy' authenticated on listener 'CLIENT' and is acting as 'line\\r\\nbreak'");
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
    }

    @Test
    void testNoProxyGrantsRejectsImpersonation() throws Exception {
        Configuration configuration = serverConfiguration();
        configuration.set(proxyUsersOption("CLIENT"), Collections.emptyMap());
        try (ServerAuthenticator server = createServer(configuration)) {
            assertThatThrownBy(
                            () ->
                                    server.evaluateResponse(
                                            saslMessage("alice", PROXY, PROXY_PASSWORD)))
                    .isInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("is not allowed to act as 'alice'");
            assertThat(server.isCompleted()).isFalse();
        }
    }

    @ParameterizedTest
    @CsvSource({
        "proxy, proxyPassword, alice.smith, true,",
        "proxy, proxyPassword, aliceXsmith, true, is not allowed to act as",
        "proxy, proxyPassword, Alice.smith, true,",
        "proxy, proxyPassword, Alice.smith, false, is not allowed to act as",
        "alice, alicePassword, bob, true, is not allowed to act as",
        "proxy, alicePassword, bob, true, Invalid username or password"
    })
    void testProxyGrant(
            String username, String password, String target, boolean ignoreCase, String denial)
            throws Exception {
        Configuration configuration = serverConfiguration();
        configuration.set(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE, ignoreCase);
        try (ServerAuthenticator server = createServer(configuration)) {
            byte[] token = saslMessage(target, username, password);
            if (denial == null) {
                server.evaluateResponse(token);
                assertThat(server.createPrincipal()).isEqualTo(new FlussPrincipal(target, "User"));
            } else {
                assertThatThrownBy(() -> server.evaluateResponse(token))
                        .isInstanceOf(AuthenticationException.class)
                        .hasMessageContaining(denial);
                assertThat(server.isCompleted()).isFalse();
            }
        }
    }

    // The U+0130 rows guard against case folding that diverges from equalsIgnoreCase:
    // U+0130 lowercases to "i" + combining dot above, which is a different ACL principal.
    @ParameterizedTest
    @CsvSource({
        "root, root, User:root, false, false, because it is a super user",
        "*, ROOT, USER:root, true, false, because it is a super user",
        "alice, alice, Group:alice, true, true,",
        "\u0130, i\u0307, User:root, true, false, is not allowed to act as",
        "\u0130, \u0130, User:root, true, true,"
    })
    void testProxyTargetUsesAclPrincipalMatching(
            String grant,
            String target,
            String superUser,
            boolean ignoreCase,
            boolean allowed,
            String denial)
            throws Exception {
        Configuration configuration = serverConfiguration();
        configuration.set(ConfigOptions.SUPER_USERS, superUser);
        configuration.set(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE, ignoreCase);
        configuration.set(proxyUsersOption("CLIENT"), Collections.singletonMap(PROXY, grant));
        try (ServerAuthenticator server = createServer(configuration)) {
            byte[] token = saslMessage(target, PROXY, PROXY_PASSWORD);
            if (allowed) {
                server.evaluateResponse(token);
                assertThat(server.createPrincipal()).isEqualTo(new FlussPrincipal(target, "User"));
            } else {
                assertThatThrownBy(() -> server.evaluateResponse(token))
                        .isInstanceOf(AuthenticationException.class)
                        .hasMessageContaining(denial);
                assertThatThrownBy(server::createPrincipal)
                        .isInstanceOf(IllegalStateException.class);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false, '', proxy", "true, alice, alice"})
    void testClientAuthorizationId(boolean useJaas, String target, String expectedPrincipal)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "SASL");
        configuration.set(ConfigOptions.CLIENT_SASL_AUTHORIZATION_ID, target);
        if (useJaas) {
            configuration.set(
                    ConfigOptions.CLIENT_SASL_JAAS_CONFIG,
                    PlainLoginModule.class.getName()
                            + " required username=\"proxy\" password=\"proxyPassword\";");
        } else {
            configuration.set(ConfigOptions.CLIENT_SASL_JAAS_USERNAME, PROXY);
            configuration.set(ConfigOptions.CLIENT_SASL_JAAS_PASSWORD, PROXY_PASSWORD);
        }

        try (ClientAuthenticator client =
                        AuthenticationFactory.loadClientAuthenticatorSupplier(configuration).get();
                ServerAuthenticator server = createServer(serverConfiguration())) {
            client.initialize(() -> "127.0.0.1");
            byte[] token = client.authenticate(new byte[0]);
            assertThat(token).isEqualTo(saslMessage(target, PROXY, PROXY_PASSWORD));
            server.evaluateResponse(token);
            assertThat(server.createPrincipal().getName()).isEqualTo(expectedPrincipal);
        }
    }

    private byte[] saslMessage(String authorizationId, String userName, String password) {
        String nul = "\u0000";
        String message =
                String.format(
                        "%s%s%s%s%s",
                        authorizationId == null ? "" : authorizationId,
                        nul,
                        userName,
                        nul,
                        password);
        return message.getBytes(StandardCharsets.UTF_8);
    }

    private ServerAuthenticator createServer(Configuration configuration) {
        ServerAuthenticator authenticator = new SaslServerAuthenticator(configuration);
        authenticator.initialize(new TestingAuthenticateContext());
        return authenticator;
    }

    private static Configuration serverConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("CLIENT", "SASL"));
        configuration.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("PLAIN"));
        configuration.setString(
                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG,
                PlainLoginModule.class.getName()
                        + " required user_proxy=\"proxyPassword\" user_alice=\"alicePassword\";");
        configuration.set(
                proxyUsersOption("CLIENT"),
                Collections.singletonMap(PROXY, "alice;alice.smith;bob"));
        return configuration;
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

    private static boolean verifyCredentials(
            PlainServerCallbackHandler handler, String user, char[] password) throws Exception {
        PlainAuthenticateCallback callback = new PlainAuthenticateCallback(password);
        handler.handle(
                user == null
                        ? new Callback[] {callback}
                        : new Callback[] {new NameCallback("username", user), callback});
        return callback.authenticated();
    }
}
