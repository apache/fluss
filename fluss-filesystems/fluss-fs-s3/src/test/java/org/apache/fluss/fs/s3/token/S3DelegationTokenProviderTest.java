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

package org.apache.fluss.fs.s3.token;

import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3DelegationTokenProvider}. */
class S3DelegationTokenProviderTest {

    @Test
    void testDefaultChainWithRoleArn() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/test-role");

        assertThatCode(() -> new S3DelegationTokenProvider("s3", conf)).doesNotThrowAnyException();
    }

    @Test
    void testDefaultChainWithoutRoleArnAllowed() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        // No AK/SK and no roleArn — should be allowed (IRSA / default chain)
        assertThatCode(() -> new S3DelegationTokenProvider("s3", conf)).doesNotThrowAnyException();
    }

    @Test
    void testPartialStaticCredentialsThrows() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.access.key", "testAccessKey");

        assertThatThrownBy(() -> new S3DelegationTokenProvider("s3", conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must both be set or both be unset");
    }

    @Test
    void testDelegationNamespaceOverridesStandard() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.access.key", "standardKey");
        conf.set("fs.s3a.secret.key", "standardSecret");
        conf.set("fs.delegation.s3a.access.key", "delegationKey");
        conf.set("fs.delegation.s3a.secret.key", "delegationSecret");

        Configuration delegationConf = S3DelegationTokenProvider.buildDelegationConfig(conf);

        assertThat(delegationConf.get("fs.s3a.access.key")).isEqualTo("delegationKey");
        assertThat(delegationConf.get("fs.s3a.secret.key")).isEqualTo("delegationSecret");
    }

    @Test
    void testDelegationFallsBackToStandard() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.access.key", "standardKey");
        conf.set("fs.s3a.secret.key", "standardSecret");
        // No delegation keys set

        Configuration delegationConf = S3DelegationTokenProvider.buildDelegationConfig(conf);

        assertThat(delegationConf.get("fs.s3a.access.key")).isEqualTo("standardKey");
        assertThat(delegationConf.get("fs.s3a.secret.key")).isEqualTo("standardSecret");
    }

    @Test
    void testDelegationArbitrarySubConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/server-role");
        conf.set(
                "fs.delegation.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/client-role");
        conf.set(
                "fs.delegation.s3a.assumed.role.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider");

        Configuration delegationConf = S3DelegationTokenProvider.buildDelegationConfig(conf);

        assertThat(delegationConf.get("fs.s3a.assumed.role.arn"))
                .isEqualTo("arn:aws:iam::123456789012:role/client-role");
        assertThat(delegationConf.get("fs.s3a.assumed.role.credentials.provider"))
                .isEqualTo("com.amazonaws.auth.EnvironmentVariableCredentialsProvider");
    }

    @Test
    void testNoDelegationProviderReturnsEmptyToken() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set(
                "fs.s3a.aws.credentials.provider",
                NoDelegationAWSCredentialsProvider.class.getName());

        S3DelegationTokenProvider provider = new S3DelegationTokenProvider("s3", conf);
        ObtainedSecurityToken token = provider.obtainSecurityToken();

        assertThat(token.isEmpty()).isTrue();
        assertThat(token.getScheme()).isEqualTo("s3");
    }

    @Test
    void testNoDelegationProviderViaDelegationNamespace() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.access.key", "serverKey");
        conf.set("fs.s3a.secret.key", "serverSecret");
        conf.set(
                "fs.delegation.s3a.aws.credentials.provider",
                NoDelegationAWSCredentialsProvider.class.getName());

        S3DelegationTokenProvider provider = new S3DelegationTokenProvider("s3", conf);
        ObtainedSecurityToken token = provider.obtainSecurityToken();

        assertThat(token.isEmpty()).isTrue();
        assertThat(token.getScheme()).isEqualTo("s3");
    }
}
