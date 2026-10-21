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
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.gs.token.GSAccessTokenProvider;
import org.apache.fluss.fs.gs.token.GSSecurityTokenProvider;
import org.apache.fluss.fs.gs.token.GSSecurityTokenReceiver;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.testutils.common.CommonTestUtils;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.fluss.fs.FileSystem.WriteMode.NO_OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that validate the behavior of the Google Cloud Storage File System Plugin. */
@ResourceLock("gs-security-token")
class GSFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static MockAuthServer mockGSServer;
    private static FileSystem fileSystem;

    @BeforeAll
    static void setup() throws IOException {
        mockGSServer = MockAuthServer.create();
        fileSystem = createFileSystem();
    }

    @Override
    protected FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath("gs://test-bucket/fluss");
    }

    private static FileSystem createFileSystem() throws IOException {
        Map<String, String> map = new HashMap<>();
        map.put("GCE_METADATA_HOST", "localhost:8080");
        CommonTestUtils.setEnv(map);

        String path =
                GSFileSystemPlugin.class
                        .getClassLoader()
                        .getResource("fake-service-account.json")
                        .getPath();

        GSFileSystemPlugin gsFileSystemPlugin = new GSFileSystemPlugin();
        Configuration configuration = new Configuration();
        configuration.setString("fs.gs.storage.root.url", "http://localhost:8080");
        configuration.setString("fs.gs.token.server.url", "http://localhost:8080/token");
        configuration.setString("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
        configuration.setString("fs.gs.auth.service.account.json.keyfile", path);
        configuration.setString("fs.gs.inputstream.support.gzip.encoding.enable", "false");

        FileSystem fileSystem =
                gsFileSystemPlugin.create(URI.create("gs://test-bucket/flusspath"), configuration);

        applyInMemoryStorage(fileSystem);

        return fileSystem;
    }

    private static void applyInMemoryStorage(FileSystem fileSystem) throws IOException {
        try {
            Object fs = FieldUtils.readField(fileSystem, "fs", true);
            final InMemoryGoogleCloudStorage inMemoryGoogleCloudStorage =
                    new InMemoryGoogleCloudStorage();
            GoogleCloudStorageFileSystem googleCloudStorageFileSystem =
                    new GoogleCloudStorageFileSystem(
                            googleCloudStorageOptions -> inMemoryGoogleCloudStorage,
                            GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                                    .setCloudStorageOptions(inMemoryGoogleCloudStorage.getOptions())
                                    .build());

            inMemoryGoogleCloudStorage.createBucket("test-bucket");
            Supplier<GoogleCloudStorageFileSystem> gsFs = () -> googleCloudStorageFileSystem;

            FieldUtils.writeField(fs, "gcsFsSupplier", gsFs, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void tearDown() throws IOException {
        GSSecurityTokenReceiver.reset();
        mockGSServer.close();
    }

    @Test
    void receivedTokenCanBeUsedByAClientWithoutTheServiceAccountKey() throws Exception {
        ObtainedSecurityToken obtainedSecurityToken =
                new ObtainedSecurityToken(
                        "gs",
                        CredentialsJsonSerde.toJson(new Credentials("", "", "token")),
                        System.currentTimeMillis() + 3_600_000L,
                        new HashMap<String, String>());
        assertThat(obtainedSecurityToken.getScheme()).isEqualTo("gs");

        GSSecurityTokenReceiver receiver = new GSSecurityTokenReceiver();
        receiver.onNewTokensObtained(obtainedSecurityToken);

        GSFileSystemPlugin plugin = new GSFileSystemPlugin();
        Configuration clientConfig = new Configuration();
        clientConfig.setString("fs.gs.storage.root.url", "http://localhost:8080");
        clientConfig.setString("fs.gs.inputstream.support.gzip.encoding.enable", "false");
        FileSystem clientFileSystem =
                plugin.create(URI.create("gs://test-bucket/flusspath"), clientConfig);
        applyInMemoryStorage(clientFileSystem);

        FsPath path = new FsPath("gs://test-bucket/fluss-token/hello");
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        try (FSDataOutputStream outputStream = clientFileSystem.create(path, NO_OVERWRITE)) {
            outputStream.write(payload);
        }
        try (FSDataInputStream inputStream = clientFileSystem.open(path)) {
            byte[] read = new byte[payload.length];
            assertThat(inputStream.read(read)).isEqualTo(payload.length);
            assertThat(read).isEqualTo(payload);
        }
    }

    @Test
    void serviceAccountKeyRefreshesAnAccessToken() throws Exception {
        ObtainedSecurityToken obtained = fileSystem.obtainSecurityToken();
        assertThat(obtained.getScheme()).isEqualTo("gs");
        Credentials credentials = CredentialsJsonSerde.fromJson(obtained.getToken());
        assertThat(credentials.getSecurityToken()).isEqualTo("token");
        assertThat(obtained.getValidUntil().get()).isGreaterThan(System.currentTimeMillis());

        GSSecurityTokenReceiver.reset();
        new GSSecurityTokenReceiver().onNewTokensObtained(obtained);
        assertThat(new GSAccessTokenProvider().getAccessToken().getToken()).isEqualTo("token");
    }

    @Test
    void computeEngineRefreshesAnAccessTokenFromMetadata() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration(false);
        hadoopConfig.set("fs.gs.auth.type", "COMPUTE_ENGINE");
        ObtainedSecurityToken obtained =
                new GSSecurityTokenProvider("gs", hadoopConfig).obtainSecurityToken();
        Credentials credentials = CredentialsJsonSerde.fromJson(obtained.getToken());
        assertThat(credentials.getSecurityToken()).isEqualTo("token");
        assertThat(obtained.getValidUntil().get()).isGreaterThan(System.currentTimeMillis());
    }
}
