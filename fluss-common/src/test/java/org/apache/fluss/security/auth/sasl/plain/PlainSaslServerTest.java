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
import org.apache.fluss.config.GlobalConfiguration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.SaslAuthorization;
import org.apache.fluss.security.auth.sasl.authenticator.SaslAuthenticationPlugin;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
    private static final String WILDCARD_PROXY = "gateway";
    private static final String WILDCARD_PROXY_PASSWORD = "gatewayPassword";
    private static final String ALICE = "alice";
    private static final String ALICE_PASSWORD = "alicePassword";
    private static final String BOB = "bob";
    private static final String ROOT = "root";

    private final List<ServerAuthenticator> authenticators = new ArrayList<>();

    private SaslServer saslServer;
    private LoginManager loginManager;

    @BeforeEach
    public void setUp() throws Exception {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        Map<String, String> options = new HashMap<>();
        options.put("user_" + USER_A, PASSWORD_A);
        options.put("user_" + USER_B, PASSWORD_B);
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), options);
        JaasContext jaasContext =
                new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        loginManager = LoginManager.acquireLoginManager(jaasContext);
        saslServer =
                SaslServerFactory.createSaslServer(
                        "PLAIN",
                        "127.0.0.1",
                        options,
                        loginManager,
                        jaasContext.configurationEntries());
    }

    @AfterEach
    public void tearDown() throws Exception {
        loginManager.release();
        for (ServerAuthenticator authenticator : authenticators) {
            authenticator.close();
        }
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
                                        "127.0.0.1",
                                        Collections.emptyMap(),
                                        loginManager,
                                        Collections.emptyList()))
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

    @ParameterizedTest
    @ValueSource(strings = {BOB, "line\r\nbreak"})
    void testWildcardAllowsAnyUser(String target) {
        List<String> messages = new ArrayList<>();
        AbstractAppender appender =
                new AbstractAppender("proxy-audit", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        Logger logger = (Logger) LogManager.getLogger(SaslAuthorization.class);
        Level previousLevel = logger.getLevel();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        try {
            ServerAuthenticator authenticator =
                    authenticate(WILDCARD_PROXY, WILDCARD_PROXY_PASSWORD, target);
            assertThat(authenticator.createPrincipal())
                    .isEqualTo(new FlussPrincipal(target, "User"));
            assertThat(messages)
                    .containsExactly(
                            "User 'gateway' authenticated and is acting as '"
                                    + (BOB.equals(target) ? BOB : "line\\r\\nbreak")
                                    + "'");
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"tenant_a_alice", "alice.smith", "semi;colon"})
    void testRegexAndLiteralGrants(String target) {
        ServerAuthenticator server = createServer(regexConfiguration());
        server.evaluateResponse(saslMessage(target, PROXY, PROXY_PASSWORD));
        assertThat(server.createPrincipal()).isEqualTo(new FlussPrincipal(target, "User"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"tenant_b_alice", "xtenant_a_alice", "tenant_a_alice!", "aliceXsmith"})
    void testRegexRequiresFullMatchAndPreservesLiteralNames(String target) {
        ServerAuthenticator server = createServer(regexConfiguration());
        assertThatThrownBy(
                        () -> server.evaluateResponse(saslMessage(target, PROXY, PROXY_PASSWORD)))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("is not allowed to act as");
        assertThat(server.isCompleted()).isFalse();
    }

    @Test
    void testQuotedRegexConfigurationRoundTrip(@TempDir Path directory) throws Exception {
        Files.write(
                directory.resolve("server.yaml"),
                Collections.singletonList(
                        "security.sasl.proxy-users: 'proxy:''regex:^(?:tenant_a_)[a-z]{1,8}$'''"),
                StandardCharsets.UTF_8);
        Configuration configuration = serverConfiguration();
        configuration.addAll(GlobalConfiguration.loadConfiguration(directory.toString(), null));
        configuration.set(
                ConfigOptions.SERVER_SASL_PROXY_USERS,
                configuration.get(ConfigOptions.SERVER_SASL_PROXY_USERS));
        ServerAuthenticator server = createServer(Configuration.fromMap(configuration.toMap()));
        server.evaluateResponse(saslMessage("tenant_a_alice", PROXY, PROXY_PASSWORD));
        assertThat(server.createPrincipal().getName()).isEqualTo("tenant_a_alice");
    }

    @ParameterizedTest
    @ValueSource(strings = {"regex:[", "*;regex:["})
    void testInvalidRegexRejectedWhenLoadingSuppliers(String grant) {
        Configuration configuration = regexConfiguration();
        configuration.set(
                ConfigOptions.SERVER_SASL_PROXY_USERS, Collections.singletonMap(PROXY, grant));
        assertThatThrownBy(
                        () -> AuthenticationFactory.loadServerAuthenticatorSuppliers(configuration))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.SERVER_SASL_PROXY_USERS.key())
                .hasMessageContaining(PROXY)
                .hasMessageContaining("regex:[");
    }

    @Test
    void testSupplierSharesStaticRulesWithIndependentAuthenticationState() throws Exception {
        Configuration configuration = regexConfiguration();
        Supplier<ServerAuthenticator> supplier =
                AuthenticationFactory.loadServerAuthenticatorSuppliers(configuration).get("CLIENT");
        // Grants are static, while new connections must read updated credentials.
        configuration.set(
                ConfigOptions.SERVER_SASL_PROXY_USERS, Collections.singletonMap(PROXY, "regex:["));
        try (ServerAuthenticator first = supplier.get()) {
            first.initialize(new TestingAuthenticateContext());
            first.evaluateResponse(saslMessage("tenant_a_alice", PROXY, PROXY_PASSWORD));
            configuration.setString(
                    "security.sasl.listener.name.client.plain.jaas.config",
                    "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_proxy=\"updated-password\";");
            try (ServerAuthenticator second = supplier.get()) {
                assertThat(second).isNotSameAs(first);
                assertThat(second.isCompleted()).isFalse();
                second.initialize(new TestingAuthenticateContext());
                assertThatThrownBy(
                                () ->
                                        second.evaluateResponse(
                                                saslMessage("tenant_a_bob", PROXY, PROXY_PASSWORD)))
                        .isInstanceOf(AuthenticationException.class)
                        .hasMessageContaining("Invalid username or password");
                second.evaluateResponse(saslMessage("tenant_a_bob", PROXY, "updated-password"));
                assertThat(second.createPrincipal().getName()).isEqualTo("tenant_a_bob");
                assertThat(first.createPrincipal().getName()).isEqualTo("tenant_a_alice");
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "alice, alicePassword, alice",
        "proxy, proxyPassword, alice",
        "proxy, proxyPassword,"
    })
    void testActingAsAnotherUserRequiresAGrant(String username, String password, String grant) {
        Configuration configuration = serverConfiguration();
        if (grant == null) {
            configuration.removeConfig(ConfigOptions.SERVER_SASL_PROXY_USERS);
        } else {
            configuration.set(
                    ConfigOptions.SERVER_SASL_PROXY_USERS, Collections.singletonMap(PROXY, grant));
        }
        ServerAuthenticator server = createServer(configuration);
        assertThatThrownBy(() -> server.evaluateResponse(saslMessage(BOB, username, password)))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage(
                        "Authentication failed: user '"
                                + username
                                + "' is not allowed to act as 'bob'");
        assertThat(server.isCompleted()).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
        "root, root, false",
        "*, root, false",
        "regex:(?i)root, root, false",
        "ROOT, ROOT, true",
        "*, Root, true",
        "regex:(?i)root, ROOT, true"
    })
    void testSuperUserCannotBeImpersonated(String grant, String target, boolean ignoreCase) {
        Configuration configuration = serverConfiguration();
        configuration.set(ConfigOptions.SUPER_USERS, "Role:root");
        configuration.set(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE, ignoreCase);
        configuration.set(
                ConfigOptions.SERVER_SASL_PROXY_USERS, Collections.singletonMap(PROXY, grant));
        ServerAuthenticator server = createServer(configuration);
        assertThatThrownBy(
                        () -> server.evaluateResponse(saslMessage(target, PROXY, PROXY_PASSWORD)))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("because it is a super user");
        assertThat(server.isCompleted()).isFalse();
        assertThatThrownBy(server::createPrincipal).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @CsvSource({"proxy, alicePassword", "'', proxyPassword", "proxy, ''", "unknown, proxyPassword"})
    void testProxyMustAuthenticateWithItsOwnPassword(String username, String password) {
        ServerAuthenticator authenticator = createServer(serverConfiguration());

        assertThatThrownBy(
                        () ->
                                authenticator.evaluateResponse(
                                        saslMessage(ALICE, username, password)))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("Authentication failed");
        assertThat(authenticator.isCompleted()).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", PROXY, ALICE})
    void testClientServerAuthorization(String target) throws Exception {
        Configuration configuration = clientConfiguration();
        if (target != null) {
            configuration.set(ConfigOptions.CLIENT_SASL_AUTHORIZATION_ID, target);
        }
        try (ClientAuthenticator client =
                AuthenticationFactory.loadClientAuthenticatorSupplier(configuration).get()) {
            client.initialize(() -> "127.0.0.1");
            ServerAuthenticator server = createServer(serverConfiguration());
            server.matchProtocol(client.protocol());
            byte[] token = client.authenticate(new byte[0]);
            assertThat(token).isEqualTo(saslMessage(target, PROXY, PROXY_PASSWORD));
            server.evaluateResponse(token);
            assertThat(client.isCompleted()).isTrue();
            assertThat(server.isCompleted()).isTrue();
            assertThat(server.createPrincipal().getName())
                    .isEqualTo(target == null || target.isEmpty() ? PROXY : target);
            server.initialize(new TestingAuthenticateContext());
            assertThat(server.isCompleted()).isFalse();
            server.evaluateResponse(saslMessage(null, PROXY, PROXY_PASSWORD));
            assertThat(server.createPrincipal().getName()).isEqualTo(PROXY);
            server.close();
            assertThat(server.isCompleted()).isFalse();
        }
    }

    @Test
    void testJaasClientCanRequestAuthorization() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "SASL");
        configuration.set(
                ConfigOptions.CLIENT_SASL_JAAS_CONFIG,
                PlainLoginModule.class.getName()
                        + " required username=\"proxy\" password=\"proxyPassword\";");
        configuration.set(ConfigOptions.CLIENT_SASL_AUTHORIZATION_ID, ALICE);
        try (ClientAuthenticator client =
                AuthenticationFactory.loadClientAuthenticatorSupplier(configuration).get()) {
            client.initialize(() -> "127.0.0.1");
            ServerAuthenticator server = createServer(serverConfiguration());
            server.evaluateResponse(client.authenticate(new byte[0]));
            assertThat(server.createPrincipal().getName()).isEqualTo(ALICE);
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

    private ServerAuthenticator authenticate(String username, String password, String actAsUser) {
        ServerAuthenticator authenticator = createServer(serverConfiguration());
        authenticator.evaluateResponse(saslMessage(actAsUser, username, password));
        assertThat(authenticator.isCompleted()).isTrue();
        return authenticator;
    }

    private ServerAuthenticator createServer(Configuration configuration) {
        ServerAuthenticator authenticator =
                new SaslAuthenticationPlugin().createServerAuthenticator(configuration);
        authenticators.add(authenticator);
        authenticator.initialize(new TestingAuthenticateContext());
        return authenticator;
    }

    private static Configuration regexConfiguration() {
        Configuration configuration = serverConfiguration();
        configuration.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:SASL");
        configuration.setString(
                ConfigOptions.SERVER_SASL_PROXY_USERS.key(),
                "proxy:'alice.smith;regex:tenant_a_[a-z0-9_]+;regex:semi\\x3Bcolon'");
        return configuration;
    }

    private static Configuration clientConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "SASL");
        configuration.set(ConfigOptions.CLIENT_SASL_JAAS_USERNAME, PROXY);
        configuration.set(ConfigOptions.CLIENT_SASL_JAAS_PASSWORD, PROXY_PASSWORD);
        return configuration;
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
        configuration.set(ConfigOptions.SERVER_SASL_PROXY_USERS, proxyUsers);
        configuration.set(ConfigOptions.SUPER_USERS, "User:" + ROOT);
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
