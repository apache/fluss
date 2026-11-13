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

package org.apache.fluss.fs.cos.token;

import org.apache.fluss.fs.cos.COSFileSystemPlugin;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.CosNFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that Fluss restores the STS session token dropped by {@code hadoop-cos} 3.3.5
 * (HADOOP-19648).
 */
class COSSessionCredentialsTest {

    private static final String SESSION_TOKEN = "test-session-token";
    private static final URI TEST_URI = URI.create("cosn://test-bucket-1234567890");

    @AfterEach
    void tearDown() {
        COSSecurityTokenReceiver.credentials = null;
        COSSecurityTokenReceiver.additionInfos = null;
    }

    @Test
    void testHadoopCosDropsSessionToken() throws Exception {
        installSessionToken();

        try (CosNFileSystem fileSystem = new CosNFileSystem()) {
            fileSystem.initialize(TEST_URI, hadoopConfiguration());

            COSCredentials credentials =
                    CosNSessionCredentialsRestorer.getCosClientCredentials(fileSystem);
            assertThat(credentials).isNotInstanceOf(COSSessionCredentials.class);
        }
    }

    @Test
    void testPluginInitPreservesSessionToken() throws Exception {
        installSessionToken();

        TestingCOSFileSystemPlugin plugin = new TestingCOSFileSystemPlugin();
        try (CosNFileSystem fileSystem =
                (CosNFileSystem)
                        plugin.createInitializedFileSystem(TEST_URI, hadoopConfiguration())) {
            COSCredentials credentials =
                    CosNSessionCredentialsRestorer.getCosClientCredentials(fileSystem);
            assertThat(credentials).isInstanceOf(COSSessionCredentials.class);
            assertThat(((COSSessionCredentials) credentials).getSessionToken())
                    .isEqualTo(SESSION_TOKEN);
        }
    }

    private static void installSessionToken() {
        ObtainedSecurityToken token =
                new ObtainedSecurityToken(
                        "cosn",
                        CredentialsJsonSerde.toJson(
                                new Credentials(
                                        "test-access-key", "test-secret-key", SESSION_TOKEN)),
                        null,
                        additionInfos());
        new COSSecurityTokenReceiver().onNewTokensObtained(token);
    }

    private static Configuration hadoopConfiguration() {
        Configuration configuration = new Configuration(false);
        configuration.set(
                "fs.cosn.credentials.provider", DynamicTemporaryCOSCredentialsProvider.NAME);
        additionInfos().forEach(configuration::set);
        return configuration;
    }

    private static Map<String, String> additionInfos() {
        Map<String, String> additionInfos = new HashMap<>();
        additionInfos.put("fs.cosn.userinfo.region", "ap-guangzhou");
        additionInfos.put("fs.cosn.bucket.endpoint_suffix", "cos.ap-guangzhou.myqcloud.com");
        return additionInfos;
    }

    private static final class TestingCOSFileSystemPlugin extends COSFileSystemPlugin {
        private org.apache.hadoop.fs.FileSystem createInitializedFileSystem(
                URI uri, Configuration configuration) throws Exception {
            return initFileSystem(uri, configuration);
        }
    }
}
