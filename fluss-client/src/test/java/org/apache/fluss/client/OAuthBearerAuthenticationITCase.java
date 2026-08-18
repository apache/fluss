/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.client;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.Password;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_MECHANISM;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_CLIENT_ID;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_CLIENT_SECRET;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_TOKEN_ENDPOINT;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SECURITY_PROTOCOL;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_EXPECTED_AUDIENCES;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_ENDPOINT;
import static org.apache.fluss.security.auth.sasl.oauthbearer.OAuthBearerClientAuthenticator.OAUTHBEARER_MECHANISM;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;

/** End-to-end test for SASL/OAUTHBEARER authentication. */
class OAuthBearerAuthenticationITCase {
    private static final String KEY_ID = "test-key";
    private static final String ISSUER = "test-issuer";
    private static final String AUDIENCE = "fluss";

    @Test
    void testOAuthBearerAuthenticationWithTableOperations() throws Exception {
        try (TestingIdentityProvider identityProvider = new TestingIdentityProvider()) {
            FlussClusterExtension clusterExtension =
                    FlussClusterExtension.builder()
                            .setNumOfTabletServers(1)
                            .setCoordinatorServerListeners(
                                    "FLUSS://localhost:0, CLIENT://localhost:0")
                            .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                            .setClusterConf(serverConfiguration(identityProvider))
                            .build();

            try {
                clusterExtension.start();
                verifyTableOperations(clusterExtension, identityProvider);
            } finally {
                clusterExtension.close();
            }
        }
    }

    private static void verifyTableOperations(
            FlussClusterExtension clusterExtension, TestingIdentityProvider identityProvider)
            throws Exception {
        Configuration clientConfiguration = clusterExtension.getClientConfig("CLIENT");
        clientConfiguration.set(CLIENT_SECURITY_PROTOCOL, "SASL");
        clientConfiguration.set(CLIENT_SASL_MECHANISM, OAUTHBEARER_MECHANISM);
        clientConfiguration.set(
                CLIENT_SASL_OAUTHBEARER_TOKEN_ENDPOINT, identityProvider.tokenEndpoint());
        clientConfiguration.set(CLIENT_SASL_OAUTHBEARER_CLIENT_ID, "test-client");
        clientConfiguration.set(CLIENT_SASL_OAUTHBEARER_CLIENT_SECRET, new Password("test-secret"));

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
        TablePath tablePath = TablePath.of(BUILTIN_DATABASE, "oauth_bearer_e2e");

        try (Connection connection = ConnectionFactory.createConnection(clientConfiguration);
                Admin admin = connection.getAdmin()) {
            admin.createTable(tablePath, tableDescriptor, false).get();
            try {
                try (Table table = connection.getTable(tablePath)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    writer.upsert(row(1, "value")).get();
                    writer.flush();

                    Lookuper lookuper = table.newLookup().createLookuper();
                    InternalRow actualRow = lookuper.lookup(row(1)).get().getSingletonRow();
                    assertThatRow(actualRow)
                            .withSchema(schema.getRowType())
                            .isEqualTo(row(1, "value"));
                }
            } finally {
                admin.dropTable(tablePath, true).get();
            }
        }
    }

    private static Configuration serverConfiguration(TestingIdentityProvider identityProvider) {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "FLUSS:PLAINTEXT,CLIENT:SASL");
        configuration.set(
                SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(OAUTHBEARER_MECHANISM));
        configuration.set(SERVER_SASL_OAUTHBEARER_JWKS_ENDPOINT, identityProvider.jwksEndpoint());
        configuration.set(SERVER_SASL_OAUTHBEARER_EXPECTED_ISSUER, ISSUER);
        configuration.set(
                SERVER_SASL_OAUTHBEARER_EXPECTED_AUDIENCES, Collections.singletonList(AUDIENCE));
        return configuration;
    }

    private static final class TestingIdentityProvider implements AutoCloseable {
        private final HttpServer server;

        private TestingIdentityProvider() throws Exception {
            KeyPair keyPair = generateRsaKey();
            String token =
                    signedToken(keyPair, "test-client", System.currentTimeMillis() / 1000 + 300);
            String tokenResponse =
                    "{\"access_token\":\""
                            + token
                            + "\",\"token_type\":\"Bearer\",\"expires_in\":300}";

            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/token", exchange -> respond(exchange, tokenResponse));
            server.createContext("/jwks", exchange -> respond(exchange, jwks(keyPair)));
            server.start();
        }

        private String tokenEndpoint() {
            return endpoint("/token");
        }

        private String jwksEndpoint() {
            return endpoint("/jwks");
        }

        private String endpoint(String path) {
            return "http://127.0.0.1:" + server.getAddress().getPort() + path;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static String signedToken(KeyPair keyPair, String subject, long expirationSeconds)
            throws Exception {
        String unsigned =
                encode("{\"alg\":\"RS256\",\"kid\":\"" + KEY_ID + "\"}")
                        + "."
                        + encode(
                                "{\"sub\":\""
                                        + subject
                                        + "\",\"iss\":\""
                                        + ISSUER
                                        + "\",\"aud\":\""
                                        + AUDIENCE
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

    private static String jwks(KeyPair keyPair) {
        RSAPublicKey key = (RSAPublicKey) keyPair.getPublic();
        return "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\""
                + KEY_ID
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

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }
}
