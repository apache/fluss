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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.s3.token.DynamicTemporaryAWSCredentialsProvider;
import org.apache.fluss.fs.s3.token.S3DelegationTokenReceiver;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for server/client detection in {@link S3FileSystemPlugin}. */
class S3FileSystemPluginTest {

    private static final String PROVIDER_CONFIG = "fs.s3a.aws.credentials.provider";

    @Test
    void testServerModeWithStaticKeys() {
        Configuration flussConfig = new Configuration();
        flussConfig.setString("fs.s3a.access.key", "testAccessKey");
        flussConfig.setString("fs.s3a.secret.key", "testSecretKey");

        S3FileSystemPlugin plugin = new S3FileSystemPlugin();
        org.apache.hadoop.conf.Configuration hadoopConfig =
                plugin.buildHadoopConfiguration(flussConfig);

        String providers = hadoopConfig.get(PROVIDER_CONFIG, "");
        assertThat(providers).doesNotContain(DynamicTemporaryAWSCredentialsProvider.NAME);
    }

    @Test
    void testServerModeWithRoleArnOnly() {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                "fs.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/test-role");

        S3FileSystemPlugin plugin = new S3FileSystemPlugin();
        org.apache.hadoop.conf.Configuration hadoopConfig =
                plugin.buildHadoopConfiguration(flussConfig);

        String providers = hadoopConfig.get(PROVIDER_CONFIG, "");
        assertThat(providers).doesNotContain(DynamicTemporaryAWSCredentialsProvider.NAME);
    }

    @Test
    void testClientModeWithDelegatedCredentials() {
        // Pre-populate receiver so updateHadoopConfig does not throw.
        Credentials creds = new Credentials("testKey", "testSecret", "testToken");
        ObtainedSecurityToken token =
                new ObtainedSecurityToken(
                        "s3",
                        CredentialsJsonSerde.toJson(creds),
                        System.currentTimeMillis() + 3600000,
                        Collections.singletonMap("fs.s3a.region", "us-east-1"));
        S3DelegationTokenReceiver receiver = new S3DelegationTokenReceiver();
        receiver.onNewTokensObtained(token);

        Configuration flussConfig = new Configuration();

        S3FileSystemPlugin plugin = new S3FileSystemPlugin();
        org.apache.hadoop.conf.Configuration hadoopConfig =
                plugin.buildHadoopConfiguration(flussConfig);

        String providers = hadoopConfig.get(PROVIDER_CONFIG, "");
        assertThat(providers).contains(DynamicTemporaryAWSCredentialsProvider.NAME);
    }
}
