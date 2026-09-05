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

package org.apache.fluss.kafka.security;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.SaslServerAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServerConfigManager;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the per-connection Kafka SASL state machine. */
public class KafkaSaslConnectionTest {

    @Test
    public void testPlaintextConnectionIsImmediatelyReady() {
        KafkaSaslConnection connection = KafkaSaslConnection.plaintext();

        assertThat(connection.authenticationEnabled()).isFalse();
        assertThat(connection.isReady()).isTrue();
        assertThat(connection.principal()).isEqualTo(FlussPrincipal.ANONYMOUS);
        assertThat(connection.isRequestAllowed(ApiKeys.METADATA)).isTrue();
        assertThat(connection.shouldClose()).isFalse();
    }

    @Test
    public void testSuccessfulAuthenticationTransitionsToReady() {
        TestingServerAuthenticator authenticator = new TestingServerAuthenticator();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);

        assertThat(connection.authenticationEnabled()).isTrue();
        assertThat(connection.isAwaitingHandshake()).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.API_VERSIONS)).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.SASL_HANDSHAKE)).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.SASL_AUTHENTICATE)).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.METADATA)).isFalse();

        connection.beginAuthentication(
                KafkaSaslConnection.PLAIN_MECHANISM,
                "KAFKA",
                new InetSocketAddress("127.0.0.1", 9092));

        assertThat(connection.isAuthenticating()).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.API_VERSIONS)).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.SASL_AUTHENTICATE)).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.SASL_HANDSHAKE)).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.METADATA)).isFalse();
        assertThat(authenticator.listenerName).isEqualTo("KAFKA");
        assertThat(authenticator.ipAddress).isEqualTo("127.0.0.1");
        assertThat(authenticator.mechanism).isEqualTo(KafkaSaslConnection.PLAIN_MECHANISM);

        byte[] challenge = connection.authenticate(bytes("valid-token"));

        assertThat(challenge).isEmpty();
        assertThat(connection.isReady()).isTrue();
        assertThat(connection.principal()).isEqualTo(new FlussPrincipal("alice", "User"));
        assertThat(connection.isRequestAllowed(ApiKeys.PRODUCE)).isTrue();
        assertThat(connection.shouldClose()).isFalse();
        assertThat(authenticator.closed).isTrue();
    }

    @Test
    public void testFlussPlainAuthenticatorAcceptsKafkaPlainToken() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(KafkaSaslConnection.PLAIN_MECHANISM));
        configuration.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS,
                Collections.singletonMap("writer", "writer-secret"));
        PlainSaslServerConfigManager configManager =
                new PlainSaslServerConfigManager(configuration);
        KafkaSaslConnection connection =
                KafkaSaslConnection.sasl(
                        () -> new SaslServerAuthenticator(configManager.getConfiguration()));

        connection.beginAuthentication(
                KafkaSaslConnection.PLAIN_MECHANISM,
                "KAFKA",
                new InetSocketAddress("127.0.0.1", 9092));
        connection.authenticate(bytes("\u0000writer\u0000writer-secret"));

        assertThat(connection.isReady()).isTrue();
        assertThat(connection.principal()).isEqualTo(new FlussPrincipal("writer", "User"));
    }

    @Test
    public void testAuthenticationFailureRequiresConnectionClose() {
        TestingServerAuthenticator authenticator = new TestingServerAuthenticator();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        connection.beginAuthentication(
                KafkaSaslConnection.PLAIN_MECHANISM,
                "KAFKA",
                new InetSocketAddress("127.0.0.1", 9092));

        assertThatThrownBy(() -> connection.authenticate(bytes("bad-token")))
                .isInstanceOf(AuthenticationException.class);

        assertThat(connection.shouldClose()).isTrue();
        assertThat(connection.isReady()).isFalse();
        assertThat(connection.principal()).isEqualTo(FlussPrincipal.ANONYMOUS);
        assertThat(authenticator.closed).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.API_VERSIONS)).isFalse();
    }

    @Test
    public void testCloseReleasesAuthenticator() {
        TestingServerAuthenticator authenticator = new TestingServerAuthenticator();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        connection.beginAuthentication(
                KafkaSaslConnection.PLAIN_MECHANISM,
                "KAFKA",
                new InetSocketAddress("127.0.0.1", 9092));

        connection.close();

        assertThat(authenticator.closed).isTrue();
        assertThat(connection.shouldClose()).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.SASL_AUTHENTICATE)).isFalse();
    }

    @Test
    public void testCloseWaitsForInProgressAuthentication() throws Exception {
        BlockingServerAuthenticator authenticator = new BlockingServerAuthenticator();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        connection.beginAuthentication(
                KafkaSaslConnection.PLAIN_MECHANISM,
                "KAFKA",
                new InetSocketAddress("127.0.0.1", 9092));
        CompletableFuture<byte[]> authentication =
                CompletableFuture.supplyAsync(() -> connection.authenticate(bytes("token")));

        assertThat(authenticator.entered.await(10, TimeUnit.SECONDS)).isTrue();
        CountDownLatch closeStarted = new CountDownLatch(1);
        CompletableFuture<Void> closeFuture =
                CompletableFuture.runAsync(
                        () -> {
                            closeStarted.countDown();
                            connection.close();
                        });
        assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
        try {
            assertThatThrownBy(() -> closeFuture.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
        } finally {
            authenticator.proceed.countDown();
        }

        assertThat(authentication.get(10, TimeUnit.SECONDS)).isEmpty();
        closeFuture.get(10, TimeUnit.SECONDS);
        assertThat(connection.shouldClose()).isTrue();
        assertThat(connection.isReady()).isFalse();
        assertThat(authenticator.closeCount).hasValue(1);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class TestingServerAuthenticator implements ServerAuthenticator {
        private String listenerName;
        private String ipAddress;
        private String mechanism;
        private boolean completed;
        private boolean closed;

        @Override
        public String protocol() {
            return "sasl";
        }

        @Override
        public void initialize(AuthenticateContext context) {
            listenerName = context.listenerName();
            ipAddress = context.ipAddress();
            mechanism = context.protocol();
        }

        @Override
        public byte[] evaluateResponse(byte[] token) {
            if (!java.util.Arrays.equals(token, bytes("valid-token"))) {
                throw new AuthenticationException("Invalid credentials");
            }
            completed = true;
            return new byte[0];
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        @Override
        public FlussPrincipal createPrincipal() {
            return new FlussPrincipal("alice", "User");
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class BlockingServerAuthenticator implements ServerAuthenticator {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch proceed = new CountDownLatch(1);
        private final AtomicInteger closeCount = new AtomicInteger();
        private boolean completed;

        @Override
        public String protocol() {
            return "sasl";
        }

        @Override
        public byte[] evaluateResponse(byte[] token) {
            entered.countDown();
            try {
                proceed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AuthenticationException("Interrupted while testing authentication.", e);
            }
            completed = true;
            return new byte[0];
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        @Override
        public FlussPrincipal createPrincipal() {
            return new FlussPrincipal("alice", "User");
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }
}
