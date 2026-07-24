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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.io.IOException;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3DelegationTokenProvider} constructor validation. */
public class S3DelegationTokenProviderTest {

    private static final String PROVIDER_CONFIG = "fs.s3a.aws.credentials.provider";

    @Test
    void testDefaultChainWithRoleArn() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/test-role");

        assertThatCode(() -> new S3DelegationTokenProvider("s3", conf)).doesNotThrowAnyException();
    }

    @Test
    void testDefaultChainWithoutRoleArnThrows() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");

        assertThatThrownBy(() -> new S3DelegationTokenProvider("s3", conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Role ARN must be set");
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
    void testConfiguredProviderWithoutStaticCredentialsIsAccepted() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        setConfiguredProvider(conf, RefreshableCredentialsProvider.class);

        assertThatCode(() -> new S3DelegationTokenProvider("s3", conf)).doesNotThrowAnyException();
    }

    @Test
    void testDynamicTemporaryCredentialsProviderIsRejected() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        setConfiguredProvider(conf, DynamicTemporaryAWSCredentialsProvider.class);

        assertThatThrownBy(() -> new S3DelegationTokenProvider("s3", conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DynamicTemporaryAWSCredentialsProvider.NAME)
                .hasMessageContaining("server-side");
    }

    @Test
    void testConfiguredProviderRequiresRegion() {
        Configuration conf = new Configuration();
        setConfiguredProvider(conf, RefreshableCredentialsProvider.class);

        assertThatThrownBy(() -> new S3DelegationTokenProvider("s3", conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Region is not set");
    }

    @Test
    void testConfiguredProviderWithRoleArnThrows() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        setConfiguredProvider(conf, RefreshableCredentialsProvider.class);
        conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/test-role");

        assertThatThrownBy(() -> new S3DelegationTokenProvider("s3", conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AssumeRole")
                .hasMessageContaining("custom AWS credentials provider");
    }

    @Test
    void testConfiguredProviderCredentialsAreResolvedForEachUse() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        setConfiguredProvider(conf, RefreshableCredentialsProvider.class);
        RefreshableCredentialsProvider.setCredentials("firstAccessKey", "firstSecretKey");
        S3DelegationTokenProvider provider = new S3DelegationTokenProvider("s3", conf);

        AwsCredentials firstCredentials =
                provider.createStsCredentialsProvider().resolveCredentials();
        RefreshableCredentialsProvider.setCredentials("secondAccessKey", "secondSecretKey");
        AwsCredentials secondCredentials =
                provider.createStsCredentialsProvider().resolveCredentials();

        assertThat(firstCredentials.accessKeyId()).isEqualTo("firstAccessKey");
        assertThat(firstCredentials.secretAccessKey()).isEqualTo("firstSecretKey");
        assertThat(secondCredentials.accessKeyId()).isEqualTo("secondAccessKey");
        assertThat(secondCredentials.secretAccessKey()).isEqualTo("secondSecretKey");
    }

    @Test
    void testConfiguredProviderTakesPrecedenceOverStaticCredentials() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.access.key", "staticAccessKey");
        conf.set("fs.s3a.secret.key", "staticSecretKey");
        setConfiguredProvider(conf, RefreshableCredentialsProvider.class);
        RefreshableCredentialsProvider.setCredentials("providerAccessKey", "providerSecretKey");
        S3DelegationTokenProvider provider = new S3DelegationTokenProvider("s3", conf);

        AwsCredentials credentials = provider.createStsCredentialsProvider().resolveCredentials();

        assertThat(credentials.accessKeyId()).isEqualTo("providerAccessKey");
        assertThat(credentials.secretAccessKey()).isEqualTo("providerSecretKey");
    }

    @Test
    void testConfiguredProviderReturningSessionCredentialsThrowsBeforeSts() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        setConfiguredProvider(conf, SessionCredentialsProvider.class);
        S3DelegationTokenProvider provider = new S3DelegationTokenProvider("s3", conf);

        assertThatThrownBy(provider::createStsCredentialsProvider)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Session credentials")
                .hasMessageNotContaining("sessionSecretKey")
                .hasMessageNotContaining("sessionToken");
    }

    @Test
    void testConfiguredProviderWithUriConfigurationConstructorIsSupported() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("test.config.value", "configured-value");
        setConfiguredProvider(conf, UriConfigurationCredentialsProvider.class);

        new S3DelegationTokenProvider("s3", conf);

        assertThat(UriConfigurationCredentialsProvider.uri).isNull();
        assertThat(UriConfigurationCredentialsProvider.configuredValue)
                .isEqualTo("configured-value");
    }

    private static void setConfiguredProvider(
            Configuration conf, Class<? extends AwsCredentialsProvider> providerClass) {
        conf.set(PROVIDER_CONFIG, providerClass.getName());
        conf.setBoolean(S3DelegationTokenProvider.CREDENTIAL_PROVIDER_EXPLICITLY_CONFIGURED, true);
    }

    /** Refreshable credentials provider for tests. */
    public static class RefreshableCredentialsProvider implements AwsCredentialsProvider {
        private static volatile AwsCredentials credentials =
                AwsBasicCredentials.create("defaultAccessKey", "defaultSecretKey");

        static void setCredentials(String accessKey, String secretKey) {
            credentials = AwsBasicCredentials.create(accessKey, secretKey);
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return credentials;
        }
    }

    /** Session credentials provider for tests. */
    public static class SessionCredentialsProvider implements AwsCredentialsProvider {

        @Override
        public AwsSessionCredentials resolveCredentials() {
            return AwsSessionCredentials.create(
                    "sessionAccessKey", "sessionSecretKey", "sessionToken");
        }
    }

    /** Credentials provider with the Hadoop S3A URI/configuration constructor form. */
    public static class UriConfigurationCredentialsProvider implements AwsCredentialsProvider {
        private static volatile URI uri;
        private static volatile String configuredValue;

        public UriConfigurationCredentialsProvider(URI uri, Configuration conf) {
            UriConfigurationCredentialsProvider.uri = uri;
            UriConfigurationCredentialsProvider.configuredValue = conf.get("test.config.value");
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("uriAccessKey", "uriSecretKey");
        }
    }
}
