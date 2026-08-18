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

package org.apache.fluss.security.auth.sasl.oauthbearer;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.Password;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.RetriableAuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_MECHANISM;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_CLIENT_ID;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_CLIENT_SECRET;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_REQUEST_TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_TOKEN_ENDPOINT;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_EXPECTED_AUDIENCES;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_ENDPOINT;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_REFRESH_MIN_INTERVAL;
import static org.apache.fluss.security.auth.sasl.oauthbearer.OAuthBearerClientAuthenticator.OAUTHBEARER_MECHANISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the OAuth bearer token and JWKS authentication path. */
class OAuthBearerAuthenticationTest {
    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final AtomicInteger jwksRequests = new AtomicInteger();
    private final AtomicReference<Response> tokenResponse = new AtomicReference<>();
    private final AtomicReference<Response> jwksResponse = new AtomicReference<>();

    private HttpServer server;

    @BeforeEach
    void beforeEach() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/token",
                exchange -> {
                    tokenRequests.incrementAndGet();
                    respond(exchange, tokenResponse.get());
                });
        server.createContext(
                "/jwks",
                exchange -> {
                    jwksRequests.incrementAndGet();
                    respond(exchange, jwksResponse.get());
                });
        server.start();
    }

    @AfterEach
    void afterEach() {
        server.stop(0);
    }

    @Test
    void testClientAuthenticatorFetchesTokenForEachConnection() {
        tokenResponse.set(
                new Response(200, "{\"access_token\":\"token\",\"token_type\":\"Bearer\"}"));

        OAuthBearerClientAuthenticator first =
                new OAuthBearerClientAuthenticator(clientConfiguration());
        OAuthBearerClientAuthenticator second =
                new OAuthBearerClientAuthenticator(clientConfiguration());
        first.initialize(null);
        second.initialize(null);

        byte[] firstResponse = first.authenticate(new byte[0]);
        first.initialize(null);
        assertThat(first.authenticate(new byte[0])).isEqualTo(firstResponse);
        assertThat(second.authenticate(new byte[0])).isEqualTo(firstResponse);
        assertThat(tokenRequests).hasValue(2);
    }

    @Test
    void testTokenEndpointFailureFailsAuthentication() {
        tokenResponse.set(new Response(503, "unavailable"));
        OAuthBearerClientAuthenticator authenticator =
                new OAuthBearerClientAuthenticator(clientConfiguration());
        authenticator.initialize(null);

        assertThatThrownBy(() -> authenticator.authenticate(new byte[0]))
                .isInstanceOf(RetriableAuthenticationException.class);
        assertThat(tokenRequests).hasValue(1);
    }

    @Test
    void testJwtValidationAndJwksRotation() throws Exception {
        long nowSeconds = System.currentTimeMillis() / 1000;
        KeyPair firstKey = generateRsaKey();
        KeyPair secondKey = generateRsaKey();
        jwksResponse.set(new Response(200, jwks("first", firstKey)));
        Configuration configuration = serverConfiguration();
        configuration.set(SERVER_SASL_OAUTHBEARER_JWKS_REFRESH_MIN_INTERVAL, Duration.ZERO);
        OAuthBearerJwksResolver resolver = new OAuthBearerJwksResolver(configuration);
        OAuthBearerJwtValidator validator = new OAuthBearerJwtValidator(configuration, resolver);

        String firstToken =
                signedToken("first", firstKey, "subject", "issuer", "fluss", nowSeconds + 60);
        OAuthBearerJwtValidator.ValidatedToken validated = validator.validate(firstToken);
        assertThat(validated.subject()).isEqualTo("subject");
        assertThat(jwksRequests).hasValue(1);
        validator.validate(firstToken);
        assertThat(jwksRequests).hasValue(1);

        jwksResponse.set(new Response(200, jwks("second", secondKey)));
        String secondToken =
                signedToken("second", secondKey, "subject-2", "issuer", "fluss", nowSeconds + 60);
        assertThat(validator.validate(secondToken).subject()).isEqualTo("subject-2");
        assertThat(jwksRequests).hasValue(2);

        String wrongAudience =
                signedToken("second", secondKey, "subject-2", "issuer", "other", nowSeconds + 60);
        assertThatThrownBy(() -> validator.validate(wrongAudience))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("audience");

        String expiredToken =
                signedToken("second", secondKey, "subject-2", "issuer", "fluss", nowSeconds - 1);
        assertThatThrownBy(() -> validator.validate(expiredToken))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("expired");
    }

    @Test
    void testClientAndServerAuthenticators() throws Exception {
        KeyPair keyPair = generateRsaKey();
        long expirationSeconds = System.currentTimeMillis() / 1000 + 60;
        String token =
                signedToken(
                        "key", keyPair, "service-account", "issuer", "fluss", expirationSeconds);
        tokenResponse.set(tokenResponse(token, 60));
        jwksResponse.set(new Response(200, jwks("key", keyPair)));

        OAuthBearerClientAuthenticator client =
                new OAuthBearerClientAuthenticator(clientConfiguration());
        OAuthBearerServerAuthenticator oauthServer =
                new OAuthBearerServerAuthenticator(
                        serverConfiguration(), new OAuthBearerJwksResolver(serverConfiguration()));

        client.initialize(null);
        byte[] initialResponse = client.authenticate(new byte[0]);
        assertThat(client.isCompleted()).isTrue();
        assertThat(oauthServer.evaluateResponse(initialResponse)).isNull();
        assertThat(oauthServer.isCompleted()).isTrue();
        assertThat(oauthServer.createPrincipal().getName()).isEqualTo("service-account");
        assertThat(oauthServer.createPrincipal().getType()).isEqualTo(FlussPrincipal.USER_TYPE);
    }

    @Test
    void testServerAuthenticatorBeforeAuthentication() {
        Configuration configuration = serverConfiguration();
        OAuthBearerServerAuthenticator oauthServer =
                new OAuthBearerServerAuthenticator(
                        configuration, new OAuthBearerJwksResolver(configuration));

        assertThat(oauthServer.protocol()).isEqualTo(OAUTHBEARER_MECHANISM);
        assertThat(oauthServer.isCompleted()).isFalse();
        oauthServer.validateSession();
        assertThatThrownBy(oauthServer::createPrincipal)
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("SASL OAUTHBEARER authentication is not completed");
    }

    @Test
    void testPluginFetchesTokenPerConnectionAndSharesJwksAcrossConnections() throws Exception {
        KeyPair keyPair = generateRsaKey();
        String token =
                signedToken(
                        "key",
                        keyPair,
                        "service-account",
                        "issuer",
                        "fluss",
                        System.currentTimeMillis() / 1000 + 60);
        tokenResponse.set(tokenResponse(token, 60));
        jwksResponse.set(new Response(200, jwks("key", keyPair)));

        Configuration clientConfiguration = clientConfiguration();
        clientConfiguration.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "SASL");
        clientConfiguration.set(CLIENT_SASL_MECHANISM, OAUTHBEARER_MECHANISM);
        Supplier<ClientAuthenticator> clientSupplier =
                AuthenticationFactory.loadClientAuthenticatorSupplier(clientConfiguration);
        ClientAuthenticator firstClient = clientSupplier.get();
        ClientAuthenticator secondClient = clientSupplier.get();
        firstClient.initialize(null);
        secondClient.initialize(null);
        byte[] firstResponse = firstClient.authenticate(new byte[0]);
        byte[] secondResponse = secondClient.authenticate(new byte[0]);
        assertThat(firstResponse).isEqualTo(secondResponse);
        assertThat(tokenRequests).hasValue(2);

        Configuration serverConfiguration = serverConfiguration();
        serverConfiguration.set(
                SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(OAUTHBEARER_MECHANISM));
        serverConfiguration.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:SASL,INTERNAL:SASL");
        Map<String, Supplier<ServerAuthenticator>> serverSuppliers =
                AuthenticationFactory.loadServerAuthenticatorSuppliers(serverConfiguration);
        ServerAuthenticator firstServer = serverSuppliers.get("CLIENT").get();
        ServerAuthenticator secondServer = serverSuppliers.get("INTERNAL").get();
        firstServer.initialize(authenticateContext("CLIENT"));
        secondServer.initialize(authenticateContext("INTERNAL"));
        firstServer.evaluateResponse(firstResponse);
        secondServer.evaluateResponse(secondResponse);
        assertThat(jwksRequests).hasValue(1);
    }

    @Test
    void testJwksFailureFailsAuthentication() throws Exception {
        jwksResponse.set(new Response(503, "unavailable"));
        Configuration configuration = serverConfiguration();
        OAuthBearerJwksResolver resolver = new OAuthBearerJwksResolver(configuration);
        OAuthBearerJwtValidator validator = new OAuthBearerJwtValidator(configuration, resolver);
        String token =
                signedToken(
                        "unknown",
                        generateRsaKey(),
                        "subject",
                        "issuer",
                        "fluss",
                        System.currentTimeMillis() / 1000 + 60);

        assertThatThrownBy(() -> validator.validate(token))
                .isInstanceOf(AuthenticationException.class);
        assertThat(jwksRequests).hasValue(1);

        assertThatThrownBy(() -> validator.validate(token))
                .isInstanceOf(AuthenticationException.class);
        assertThat(jwksRequests).hasValue(2);
    }

    private Configuration clientConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(CLIENT_SASL_OAUTHBEARER_TOKEN_ENDPOINT, endpoint("/token"));
        configuration.set(CLIENT_SASL_OAUTHBEARER_CLIENT_ID, "client");
        configuration.set(CLIENT_SASL_OAUTHBEARER_CLIENT_SECRET, new Password("secret"));
        configuration.set(CLIENT_SASL_OAUTHBEARER_REQUEST_TIMEOUT, Duration.ofSeconds(5));
        return configuration;
    }

    private Configuration serverConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(SERVER_SASL_OAUTHBEARER_JWKS_ENDPOINT, endpoint("/jwks"));
        configuration.set(SERVER_SASL_OAUTHBEARER_EXPECTED_ISSUER, "issuer");
        configuration.set(
                SERVER_SASL_OAUTHBEARER_EXPECTED_AUDIENCES, Arrays.asList("fluss", "fluss-admin"));
        return configuration;
    }

    private String endpoint(String path) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }

    private static ServerAuthenticator.AuthenticateContext authenticateContext(
            String listenerName) {
        return new ServerAuthenticator.AuthenticateContext() {
            @Override
            public String ipAddress() {
                return "127.0.0.1";
            }

            @Override
            public String listenerName() {
                return listenerName;
            }

            @Override
            public String protocol() {
                return OAUTHBEARER_MECHANISM;
            }
        };
    }

    private static Response tokenResponse(String token, long expiresIn) {
        return new Response(
                200,
                "{\"access_token\":\""
                        + token
                        + "\",\"token_type\":\"Bearer\",\"expires_in\":"
                        + expiresIn
                        + "}");
    }

    private static String signedToken(
            String keyId,
            KeyPair keyPair,
            String subject,
            String issuer,
            String audience,
            long expirationSeconds)
            throws Exception {
        String unsigned =
                encode("{\"alg\":\"RS256\",\"kid\":\"" + keyId + "\"}")
                        + "."
                        + encode(
                                "{\"sub\":\""
                                        + subject
                                        + "\",\"iss\":\""
                                        + issuer
                                        + "\",\"aud\":\""
                                        + audience
                                        + "\",\"exp\":"
                                        + expirationSeconds
                                        + "}");
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate());
        signature.update(unsigned.getBytes(StandardCharsets.US_ASCII));
        return unsigned
                + "."
                + Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign());
    }

    private static String jwks(String keyId, KeyPair keyPair) {
        RSAPublicKey key = (RSAPublicKey) keyPair.getPublic();
        return "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\""
                + keyId
                + "\",\"use\":\"sig\",\"alg\":\"RS256\",\"n\":\""
                + encodeUnsigned(key.getModulus())
                + "\",\"e\":\""
                + encodeUnsigned(key.getPublicExponent())
                + "\"}]}";
    }

    private static KeyPair generateRsaKey() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String encodeUnsigned(BigInteger value) {
        byte[] bytes = value.toByteArray();
        if (bytes.length > 1 && bytes[0] == 0) {
            bytes = Arrays.copyOfRange(bytes, 1, bytes.length);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static void respond(HttpExchange exchange, Response response) throws IOException {
        byte[] body = response.body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(response.status, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static final class Response {
        private final int status;
        private final String body;

        private Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
