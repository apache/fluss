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

package org.apache.fluss.fs.azure;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.azure.token.AbfsDelegationTokenReceiver;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.fs.azure.AzureFileSystemOptions.ACCOUNT_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AzureFileSystemPlugin}. */
public class AzureFileSystemPluginTest {

    // Reserved TLD (RFC 2606) so the host is guaranteed to never resolve or be registered.
    private static final String TEST_FS_URI = "abfs://container@account.dfs.core.windows.invalid/";

    private static Configuration offlineConfig() {
        Configuration flussConfig = new Configuration();
        flussConfig.setString("fs.azure.account.hns.enabled", "false");
        flussConfig.setString("fs.azure.io.retry.max.retries", "0");
        return flussConfig;
    }

    @Test
    void testGetHadoopConfiguration() {
        AzureFileSystemPlugin plugin = new AbfsFileSystemPlugin();
        Configuration flussConfig = new Configuration();
        flussConfig.setString("fs.azure.some.prop", "some-value");
        flussConfig.setString("azure.another.prop", "another-value");
        flussConfig.setString("other.prop", "other-value");

        org.apache.hadoop.conf.Configuration hadoopConfig =
                plugin.getHadoopConfiguration(flussConfig);

        assertThat(hadoopConfig.get("fs.azure.some.prop")).isEqualTo("some-value");
        assertThat(hadoopConfig.get("fs.azure.another.prop")).isEqualTo("another-value");
        assertThat(hadoopConfig.get("other.prop")).isNull();
    }

    @Test
    void testGetHadoopConfigurationNull() {
        AzureFileSystemPlugin plugin = new AbfsFileSystemPlugin();
        org.apache.hadoop.conf.Configuration hadoopConfig = plugin.getHadoopConfiguration(null);
        assertThat(hadoopConfig).isNotNull();
    }

    @Test
    void testCreateWithAccountKey() throws Exception {
        AzureFileSystemPlugin plugin = new AbfsFileSystemPlugin();
        Configuration flussConfig = offlineConfig();
        // ABFS requires the account key to be base64 encoded.
        flussConfig.setString(
                ACCOUNT_KEY.key(),
                Base64.getEncoder().encodeToString("some-key".getBytes(StandardCharsets.UTF_8)));

        assertThat(plugin.create(new URI(TEST_FS_URI), flussConfig))
                .isInstanceOf(AzureFileSystem.class);
    }

    @Test
    void testCreateWithoutAccountKey() throws Exception {
        AzureFileSystemPlugin plugin = new AbfsFileSystemPlugin();
        Configuration flussConfig = offlineConfig();

        // Prepare credentials so updateHadoopConfig doesn't throw IllegalStateException
        Credentials credentials = new Credentials("id", "secret", "token");
        Map<String, String> additionInfos = new HashMap<>();
        additionInfos.put("some", "info");
        ObtainedSecurityToken token =
                new ObtainedSecurityToken(
                        "abfs", CredentialsJsonSerde.toJson(credentials), 100L, additionInfos);
        new AbfsDelegationTokenReceiver().onNewTokensObtained(token);

        assertThat(plugin.create(new URI(TEST_FS_URI), flussConfig))
                .isInstanceOf(AzureFileSystem.class);
    }

    @Test
    void testUnsupportedScheme() {
        AzureFileSystemPlugin plugin =
                new AzureFileSystemPlugin() {
                    @Override
                    public String getScheme() {
                        return "unsupported";
                    }
                };

        Configuration flussConfig = new Configuration();
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();

        // Accessing setCredentialProvider via reflection or by making it package-private.
        // In the code it is private. Let's see if we can trigger it via create.
        assertThatThrownBy(() -> plugin.create(new URI("unsupported://foo"), flussConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported scheme: unsupported");
    }
}
