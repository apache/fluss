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

package org.apache.fluss.fs.s3.minio;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystem.WriteMode;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.s3.token.NoDelegationAWSCredentialsProvider;
import org.apache.fluss.fs.s3.token.S3DelegationTokenReceiver;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * FS S3 IT tests using MinIO test container.
 */
class S3FileSystemMinioITCase extends MinioITBase {
    private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemMinioITCase.class);

    private Configuration serverConf;

    @BeforeEach
    void setupServerConfiguration() {
        serverConf = getMinioFlussConfig();
    }

    private void publishProviderCredentials() {
        System.setProperty(
                SystemPropertyCredentialsProvider.ACCESS_KEY_PROPERTY,
                MinioTestContainer.ROOT_USER);
        System.setProperty(
                SystemPropertyCredentialsProvider.SECRET_KEY_PROPERTY,
                MinioTestContainer.ROOT_PASSWORD);
    }

    private void clearProviderCredentials() {
        System.clearProperty(SystemPropertyCredentialsProvider.ACCESS_KEY_PROPERTY);
        System.clearProperty(SystemPropertyCredentialsProvider.SECRET_KEY_PROPERTY);
    }

    @Test
    void testAssumeRoleTokenDelegation() throws Exception {
        // Server side configuration setup
        serverConf.setString("fs.s3a.access.key", MinioTestContainer.ROOT_USER);
        serverConf.setString("fs.s3a.secret.key", MinioTestContainer.ROOT_PASSWORD);
        // Delegation credentials provider class
        publishProviderCredentials();
        serverConf.setString(
                "fs.delegation.s3a.aws.credentials.provider",
                SystemPropertyCredentialsProvider.class.getName());
        // AssumeRole is the only STS action MinIO supports; provide any role ARN.
        serverConf.setString(
                "fs.delegation.s3a.assumed.role.arn",
                "arn:aws:iam::000000000000:role/fluss-configured-provider-test");
        serverConf.setString("fs.delegation.s3a.assumed.role.sts.endpoint", MINIO.getStsEndpoint());

        FsPath path = roundTrip(serverConf);

        // Assert delegation token
        ObtainedSecurityToken token = path.getFileSystem().obtainSecurityToken();

        assertThat(token.isEmpty()).isFalse();
        assertThat(token.getScheme()).isEqualTo("s3");
        assertThat(token.getAdditionInfos())
                .containsEntry("fs.s3a.region", MinioTestContainer.DEFAULT_REGION)
                .containsEntry("fs.s3a.endpoint", MINIO.getEndpoint());

        // Assert client side token receiver, confirm the client configurations can touch the bucket.
        S3DelegationTokenReceiver receiver = new S3DelegationTokenReceiver();
        receiver.onNewTokensObtained(token);

        Configuration clientConf = getMinioFlussConfig();
        roundTrip(clientConf);

        clearProviderCredentials();
    }

    @Test
    void testUseStaticCredentialsForDelegation() throws Exception {
        // This is previous behavior, the server initializes the S3 filesystem with plain AK/SK, obtains a session
        // token via MinIO's GetSessionToken STS endpoint, hands the token to client.
        serverConf.setString("fs.s3a.access.key", MinioTestContainer.ROOT_USER);
        serverConf.setString("fs.s3a.secret.key", MinioTestContainer.ROOT_PASSWORD);
        serverConf.setString("fs.s3a.assumed.role.sts.endpoint", MINIO.getStsEndpoint());
        // MinIO STS only supports AssumeRole, so provide a role ARN to trigger STS.
        serverConf.setString("fs.s3a.assumed.role.arn", MinioTestContainer.DUMMY_ROLE_ARN);

        FsPath path = roundTrip(serverConf);

        ObtainedSecurityToken token = path.getFileSystem().obtainSecurityToken();
        assertThat(token.isEmpty()).isFalse();
        assertThat(token.getScheme()).isEqualTo("s3");

        // Client side
        S3DelegationTokenReceiver receiver = new S3DelegationTokenReceiver();
        receiver.onNewTokensObtained(token);

        Configuration clientConf = getMinioFlussConfig();
        clientConf.setString("fs.s3a.path.style.access", "true");
        roundTrip(clientConf);
    }

    @Test
    void testServerKeysInvalidDelegationKeysValid() throws Exception {
        // Server credentials are invalid
        serverConf.setString("fs.s3a.access.key", "bogus-main-ak");
        serverConf.setString("fs.s3a.secret.key", "bogus-main-sk");

        // Delegation has valid credentials.
        serverConf.setString("fs.delegation.s3a.access.key", MinioTestContainer.ROOT_USER);
        serverConf.setString("fs.delegation.s3a.secret.key", MinioTestContainer.ROOT_PASSWORD);
        serverConf.setString("fs.s3a.assumed.role.sts.endpoint", MINIO.getStsEndpoint());
        serverConf.setString("fs.s3a.assumed.role.arn", MinioTestContainer.DUMMY_ROLE_ARN);

        assertThatThrownBy(() -> roundTrip(serverConf))
                .isInstanceOf(Exception.class);

        FsPath path = getBasePath("test-file-" + UUID.randomUUID() + ".txt");
        ObtainedSecurityToken token = path.getFileSystem().obtainSecurityToken();
        assertThat(token.isEmpty())
                .as("delegation creds must drive STS; otherwise bogus main creds would fail")
                .isFalse();

        // The token and delegated credentials should work on client side.
        S3DelegationTokenReceiver receiver = new S3DelegationTokenReceiver();
        receiver.onNewTokensObtained(token);

        Configuration clientConf = getMinioFlussConfig();
        roundTrip(clientConf);
    }

    @Test
    void testNoDelegationSettingsWithInvalidServerKeys() {
        serverConf.setString("fs.s3a.access.key", "bogus-main-ak");
        serverConf.setString("fs.s3a.secret.key", "bogus-main-sk");
        serverConf.setString("fs.s3a.assumed.role.sts.endpoint", MINIO.getStsEndpoint());
        serverConf.setString("fs.s3a.assumed.role.arn", MinioTestContainer.DUMMY_ROLE_ARN);
        // No fs.delegation.s3a.* keys, in this case the server creds must be used, which are bogus.

        FileSystem.initialize(serverConf, null);
        FsPath path = getBasePath("test-file-" + UUID.randomUUID() + ".txt");
        assertThatThrownBy(() -> path.getFileSystem().obtainSecurityToken())
                .isInstanceOf(Exception.class);
    }

    @Test
    void testNoDelegationProviderClassReturnsEmptyToken() throws Exception {
        serverConf.setString("fs.s3a.access.key", MinioTestContainer.ROOT_USER);
        serverConf.setString("fs.s3a.secret.key", MinioTestContainer.ROOT_PASSWORD);
        serverConf.setString(
                "fs.delegation.s3a.aws.credentials.provider",
                NoDelegationAWSCredentialsProvider.class.getName());

        FsPath path = roundTrip(serverConf);

        // Assert empty token for delegation
        ObtainedSecurityToken token = path.getFileSystem().obtainSecurityToken();
        assertThat(token.isEmpty()).isTrue();
        assertThat(token.getScheme()).isEqualTo("s3");
    }

    private static FsPath roundTrip(final Configuration conf) throws Exception {
        FileSystem.initialize(conf, null);
        FsPath path = getBasePath("minio-bucket-file-" + UUID.randomUUID() + ".txt");
        FileSystem fs = path.getFileSystem();

        LOG.info("Round-trip operation on fs path: '{}'", path);

        byte[] payload = "test-round-trip-data".getBytes(StandardCharsets.UTF_8);

        try (FSDataOutputStream out = fs.create(path, WriteMode.OVERWRITE)) {
            out.write(payload);
        }

        try (FSDataInputStream in = fs.open(path)) {
            byte[] buf = new byte[payload.length];
            assertThat(in.read(buf)).isEqualTo(payload.length);
            assertThat(buf).isEqualTo(payload);
        }
        assertThat(fs.delete(path, false)).isTrue();

        return path;
    }
}
