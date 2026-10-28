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

package org.apache.fluss.fs.gs;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.gs.token.GSSecurityTokenReceiver;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.utils.IOUtils;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the GCS security token round trip against a mock token and storage server: the server mints
 * a read-only token for the connector's identity, and a client without credentials sends it.
 */
@ResourceLock("gs-security-token")
class GSSecurityTokenITCase {

    private static final String READ_ONLY_SCOPE =
            "https://www.googleapis.com/auth/devstorage.read_only";

    private static final String KEY_FILE_EMAIL = "key-file-sa@test-project.iam.gserviceaccount.com";

    private static final String ADC_EMAIL = "adc-sa@test-project.iam.gserviceaccount.com";

    /** Access token the mock token server returns, see {@code create-token.json}. */
    private static final String MOCK_SERVER_TOKEN = "token";

    private static final URI BUCKET_URI = URI.create("gs://test-bucket/");

    private static final FsPath OBJECT_PATH = new FsPath("gs://test-bucket/fluss/object");

    private static MockAuthServer mockServer;
    private static Map<String, String> originalEnv;
    private static Path keyFile;
    private static Path adcKeyFile;

    @BeforeAll
    static void setup() throws IOException {
        originalEnv = new HashMap<>(System.getenv());
        mockServer = MockAuthServer.create();
        keyFile = serviceAccountKeyFile(KEY_FILE_EMAIL);
        adcKeyFile = serviceAccountKeyFile(ADC_EMAIL);
    }

    @AfterAll
    static void tearDown() throws IOException {
        mockServer.close();
        CommonTestUtils.setEnv(originalEnv);
        Files.deleteIfExists(keyFile);
        Files.deleteIfExists(adcKeyFile);
    }

    @BeforeEach
    void beforeEach() {
        GSSecurityTokenReceiver.reset();
        mockServer.clearRecordedRequests();
        setEnvironment(null);
    }

    @AfterEach
    void afterEach() {
        GSSecurityTokenReceiver.reset();
    }

    // ------------------------------------------------------------------------------------------
    // Server side: which identity mints the token
    // ------------------------------------------------------------------------------------------

    @Test
    void keyFileMintsReadOnlyToken() throws Exception {
        Configuration server = serverConfig();
        server.setString("fs.gs.auth.service.account.json.keyfile", keyFile.toString());

        ObtainedSecurityToken token = obtainToken(server);

        assertThat(tokenValue(token)).isEqualTo(MOCK_SERVER_TOKEN);
        assertThat(token.getValidUntil().get()).isGreaterThan(System.currentTimeMillis());
        assertThat(mockServer.requestedScopes()).containsExactly(READ_ONLY_SCOPE);
        assertThat(mockServer.requestedIssuers()).containsExactly(KEY_FILE_EMAIL);
    }

    @Test
    void authTypeDoesNotChangeTheIdentity() throws Exception {
        // The bundled connector ignores fs.gs.auth.type and writes with the key file, so the
        // token must come from the key file as well.
        setEnvironment(adcKeyFile);
        Configuration server = serverConfig();
        server.setString("fs.gs.auth.type", "APPLICATION_DEFAULT");
        server.setString("fs.gs.auth.service.account.json.keyfile", keyFile.toString());

        obtainToken(server);

        assertThat(mockServer.requestedIssuers()).containsExactly(KEY_FILE_EMAIL);
    }

    @Test
    void applicationDefaultCredentialsMintReadOnlyToken() throws Exception {
        setEnvironment(adcKeyFile);

        ObtainedSecurityToken token = obtainToken(serverConfig());

        assertThat(tokenValue(token)).isEqualTo(MOCK_SERVER_TOKEN);
        assertThat(mockServer.requestedScopes()).containsExactly(READ_ONLY_SCOPE);
        assertThat(mockServer.requestedIssuers()).containsExactly(ADC_EMAIL);
    }

    @Test
    void metadataServerMintsReadOnlyToken() throws Exception {
        ObtainedSecurityToken token = obtainToken(serverConfig());

        assertThat(tokenValue(token)).isEqualTo(MOCK_SERVER_TOKEN);
        assertThat(token.getValidUntil().get()).isGreaterThan(System.currentTimeMillis());
        // The connector's metadata credential ignores scopes, so the read-only token is requested
        // separately after it.
        assertThat(mockServer.requestedScopes()).containsExactly("", READ_ONLY_SCOPE);
    }

    @Test
    void userCredentialsAreRejected() throws Exception {
        // OAuth user credentials keep the scopes granted at login, so they cannot be limited to
        // read-only. They are only used when no service account is configured.
        Configuration server = serverConfig();
        server.setString("fs.gs.auth.client.id", "test-client.apps.googleusercontent.com");
        server.setString("fs.gs.auth.client.secret", "test-secret");
        server.setString("fs.gs.auth.refresh.token", "test-refresh-token");
        FileSystem serverFileSystem = createFileSystem(server);

        assertThatThrownBy(serverFileSystem::obtainSecurityToken)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Only a service account");
    }

    // ------------------------------------------------------------------------------------------
    // Client side: which credentials the connector sends to storage
    // ------------------------------------------------------------------------------------------

    @Test
    void clientWithoutCredentialsSendsTheReceivedToken() throws Exception {
        receiveToken("received-token", System.currentTimeMillis() + 3_600_000L);

        FileSystem client = createFileSystem(clientConfig());
        client.exists(OBJECT_PATH);

        assertThat(mockServer.storageAuthorizations())
                .isNotEmpty()
                .allMatch("Bearer received-token"::equals);
    }

    @Test
    void renewedTokenReachesAnExistingFileSystem() throws Exception {
        // The connector refreshes a token that expires within 60 seconds before using it.
        receiveToken("expiring-token", System.currentTimeMillis() + 30_000L);
        FileSystem client = createFileSystem(clientConfig());

        receiveToken("renewed-token", System.currentTimeMillis() + 3_600_000L);
        client.exists(OBJECT_PATH);

        assertThat(mockServer.storageAuthorizations())
                .isNotEmpty()
                .allMatch("Bearer renewed-token"::equals);
    }

    @Test
    void clientWithKeyFileIgnoresTheReceivedToken() throws Exception {
        receiveToken("received-token", System.currentTimeMillis() + 3_600_000L);
        Configuration client = clientConfig();
        client.setString("fs.gs.token.server.url", "http://" + mockEndpoint() + "/token");
        client.setString("fs.gs.auth.service.account.json.keyfile", keyFile.toString());

        createFileSystem(client).exists(OBJECT_PATH);

        assertThat(mockServer.storageAuthorizations())
                .isNotEmpty()
                .allMatch(("Bearer " + MOCK_SERVER_TOKEN)::equals);
    }

    @Test
    void clientWithCustomAccessTokenProviderKeepsIt() throws Exception {
        receiveToken("received-token", System.currentTimeMillis() + 3_600_000L);
        Configuration client = clientConfig();
        client.setString(
                "fs.gs.auth.access.token.provider.impl",
                TestingAccessTokenProvider.class.getName());

        createFileSystem(client).exists(OBJECT_PATH);

        assertThat(mockServer.storageAuthorizations())
                .isNotEmpty()
                .allMatch(("Bearer " + TestingAccessTokenProvider.TOKEN)::equals);
    }

    @Test
    void clientWithoutReceivedTokenKeepsAmbientCredentials() throws Exception {
        // E.g. the server cannot issue tokens: the client keeps using the metadata server.
        createFileSystem(clientConfig()).exists(OBJECT_PATH);

        assertThat(mockServer.storageAuthorizations())
                .isNotEmpty()
                .allMatch(("Bearer " + MOCK_SERVER_TOKEN)::equals);
    }

    // ------------------------------------------------------------------------------------------

    /** Returns a fixed token, standing in for an access token provider a user configured. */
    public static class TestingAccessTokenProvider implements AccessTokenProvider {

        static final String TOKEN = "custom-provider-token";

        private org.apache.hadoop.conf.Configuration conf;

        @Override
        public AccessToken getAccessToken() {
            return new AccessToken(TOKEN, System.currentTimeMillis() + 3_600_000L);
        }

        @Override
        public void refresh() {}

        @Override
        public void setConf(org.apache.hadoop.conf.Configuration conf) {
            this.conf = conf;
        }

        @Override
        public org.apache.hadoop.conf.Configuration getConf() {
            return conf;
        }
    }

    private static ObtainedSecurityToken obtainToken(Configuration server) throws IOException {
        FileSystem serverFileSystem = createFileSystem(server);
        // Only record the requests made to mint the token, not those of connector start-up.
        mockServer.clearRecordedRequests();
        ObtainedSecurityToken token = serverFileSystem.obtainSecurityToken();
        assertThat(token.getScheme()).isEqualTo("gs");
        return token;
    }

    private static FileSystem createFileSystem(Configuration config) throws IOException {
        return new GSFileSystemPlugin().create(BUCKET_URI, config);
    }

    private static String tokenValue(ObtainedSecurityToken token) {
        return CredentialsJsonSerde.fromJson(token.getToken()).getSecurityToken();
    }

    private static void receiveToken(String tokenValue, long validUntil) throws Exception {
        new GSSecurityTokenReceiver()
                .onNewTokensObtained(
                        new ObtainedSecurityToken(
                                "gs",
                                CredentialsJsonSerde.toJson(new Credentials("", "", tokenValue)),
                                validUntil,
                                new HashMap<>()));
    }

    private static Configuration clientConfig() {
        Configuration config = new Configuration();
        config.setString("fs.gs.storage.root.url", "http://" + mockEndpoint());
        return config;
    }

    private static Configuration serverConfig() {
        Configuration config = clientConfig();
        config.setString("fs.gs.token.server.url", "http://" + mockEndpoint() + "/token");
        return config;
    }

    private static String mockEndpoint() {
        return "127.0.0.1:" + mockServer.getPort();
    }

    /**
     * Points the metadata server at the mock, and sets {@code GOOGLE_APPLICATION_CREDENTIALS} when
     * a key file is given.
     */
    private static void setEnvironment(Path applicationDefaultCredentials) {
        Map<String, String> env = new HashMap<>(originalEnv);
        env.remove("GOOGLE_APPLICATION_CREDENTIALS");
        env.put("GCE_METADATA_HOST", mockEndpoint());
        if (applicationDefaultCredentials != null) {
            env.put("GOOGLE_APPLICATION_CREDENTIALS", applicationDefaultCredentials.toString());
        }
        CommonTestUtils.setEnv(env);
    }

    private static Path serviceAccountKeyFile(String clientEmail) throws IOException {
        InputStream input =
                GSSecurityTokenITCase.class
                        .getClassLoader()
                        .getResourceAsStream("fake-service-account.json");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        IOUtils.copyBytes(input, buffer, true);
        String json =
                new String(buffer.toByteArray(), StandardCharsets.UTF_8)
                        .replace("test-sa@test-project.iam.gserviceaccount.com", clientEmail);
        Path file = Files.createTempFile("fake-service-account", ".json");
        Files.write(file, json.getBytes(StandardCharsets.UTF_8));
        return file;
    }
}
