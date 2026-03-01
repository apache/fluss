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

package org.apache.fluss.fs.s3;

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.s3.token.S3DelegationTokenProvider;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3FileSystemPlugin}. */
public class S3FileSystemPluginTest {

    @Nested
    class TestStaticMethods {
        @Test
        void testIsClient() {
            Configuration configuration;
            boolean isClient;
            // Client
            // No options provided
            configuration = new Configuration();
            isClient = S3FileSystemPlugin.isClient(configuration);
            assertThat(isClient).isTrue();

            // Options provided with "client.fs." prefix
            configuration = new Configuration();
            configuration.setString("client.fs.s3.access-key", "fluss");
            configuration.setString("client.fs.s3.secret-key", "12345678");
            isClient = S3FileSystemPlugin.isClient(configuration);
            assertThat(isClient).isTrue();

            // Server
            // Activated token delegation
            configuration = new Configuration();
            configuration.setString("s3.access-key", "fluss");
            configuration.setString("s3.secret-key", "12345678");
            configuration.setString("s3.endpoint", "s3://fluss-data/");
            configuration.setString("s3.region", "us-east-1");
            isClient = S3FileSystemPlugin.isClient(configuration);
            assertThat(isClient).isFalse();

            configuration = new Configuration();
            configuration.setBoolean(ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION, true);
            configuration.setString("s3.access-key", "fluss");
            configuration.setString("s3.secret-key", "12345678");
            configuration.setString("s3.endpoint", "s3://fluss-data/");
            configuration.setString("s3.region", "us-east-1");
            isClient = S3FileSystemPlugin.isClient(configuration);
            assertThat(isClient).isFalse();

            // Example: Deactivated token delegation
            configuration = new Configuration();
            configuration.setBoolean(ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION, false);
            isClient = S3FileSystemPlugin.isClient(configuration);
            assertThat(isClient).isFalse();

            configuration = new Configuration();
            configuration.setBoolean(ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION, false);
            configuration.setString("s3.access-key", "fluss");
            configuration.setString("s3.secret-key", "12345678");
            configuration.setString("s3.endpoint", "http://minio:9000");
            configuration.setString("s3.path-style-access", "true");
            isClient = S3FileSystemPlugin.isClient(configuration);
            assertThat(isClient).isFalse();

            // Invalid
            configuration = new Configuration();
            configuration.setString("s3.access-key", "fluss");
            configuration.setString("client.fs.s3.secret-key", "12345678");
            Configuration finalConfiguration = configuration;
            assertThatThrownBy(() -> S3FileSystemPlugin.isClient(finalConfiguration))
                    .isInstanceOf(InvalidConfigException.class);
        }
    }

    @Nested
    class TestNonStaticMethods {

        private final URI fsUri = new FsPath("s3://test-bucket/tests-" + UUID.randomUUID()).toUri();

        private S3FileSystemPlugin s3FileSystemPlugin;

        @BeforeEach
        void beforeEach() {
            s3FileSystemPlugin = new S3FileSystemPlugin();
        }

        @Test
        void testDeactivateTokenDelegationProcess() throws IOException {
            Configuration config = new Configuration();
            config.set(ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION, false);
            FileSystem fileSystem = s3FileSystemPlugin.create(fsUri, config);
            ObtainedSecurityToken obtainedSecurityToken = fileSystem.obtainSecurityToken();
            assertThat(obtainedSecurityToken).isNotNull();
            assertThat(obtainedSecurityToken.getToken()).isNotNull();
            assertThat(obtainedSecurityToken.getToken().length).isEqualTo(0);
            assertThat(obtainedSecurityToken.getScheme()).isEqualTo("s3");
            assertThat(obtainedSecurityToken.getValidUntil()).isEmpty();
            assertThat(fileSystem).isInstanceOf(S3FileSystem.class);
            assertThat(((S3FileSystem) fileSystem).s3DelegationTokenProvider.getType())
                    .isEqualTo(S3DelegationTokenProvider.Type.NO_TOKEN);
        }

        @ParameterizedTest
        @ValueSource(strings = {"s3.", "s3a.", "fs.s3a."})
        void testWhiteListedOptions(String prefix) {
            org.apache.hadoop.conf.Configuration hadoopConfig;
            Configuration config = new Configuration();
            // First test with a single whitelisted option
            config.setString(prefix + "access-key", "fluss-s3-access-key");
            hadoopConfig = s3FileSystemPlugin.getHadoopConfiguration(config);
            assertThat(hadoopConfig.get("fs.s3a.access-key")).isEqualTo("fluss-s3-access-key");
            // Then add more whitelisted options
            config.set(
                    ConfigBuilder.key(prefix + "secret-key").stringType().noDefaultValue(),
                    "fluss-s3-secret-key");
            config.setString(prefix + "aws.credentials.provider", "fluss-credential-provider");
            hadoopConfig = s3FileSystemPlugin.getHadoopConfiguration(config);
            assertThat(hadoopConfig.get("fs.s3a.access-key")).isEqualTo("fluss-s3-access-key");
            assertThat(hadoopConfig.get("fs.s3a.secret-key")).isEqualTo("fluss-s3-secret-key");
            assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider"))
                    .isEqualTo("fluss-credential-provider");
            // Then add a non-whitelisted option
            config.setString(prefix + "non-white-listed-option", "fluss");
            hadoopConfig = s3FileSystemPlugin.getHadoopConfiguration(config);
            assertThat(hadoopConfig.get("fs.s3a.access-key")).isEqualTo("fluss-s3-access-key");
            assertThat(hadoopConfig.get("fs.s3a.secret-key")).isEqualTo("fluss-s3-secret-key");
            assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider"))
                    .isEqualTo("fluss-credential-provider");
            assertThat(hadoopConfig.get("non-white-listed-option")).isNotEqualTo("fluss");
        }
    }
}
