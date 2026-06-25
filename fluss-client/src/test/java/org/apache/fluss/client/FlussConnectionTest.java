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

package org.apache.fluss.client;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussConnection}. */
class FlussConnectionTest {

    @Test
    void testAllowNonCredentialClientFileSystemOptions() {
        Configuration conf = new Configuration();
        conf.setString("client.fs.s3a.endpoint", "http://localhost:9000");
        conf.setString("client.fs.s3a.region", "us-east-1");
        conf.setString("client.fs.s3a.path-style-access", "true");
        conf.setString("client.fs.oss.endpoint", "http://oss-local");
        conf.setString("client.test.key", "client-value");

        Map<String, String> clientFsOptions =
                FlussConnection.getClientFileSystemConfiguration(conf).toMap();

        assertThat(clientFsOptions)
                .containsEntry("s3a.endpoint", "http://localhost:9000")
                .containsEntry("s3a.region", "us-east-1")
                .containsEntry("s3a.path-style-access", "true")
                .containsEntry("oss.endpoint", "http://oss-local")
                .doesNotContainKey("client.test.key");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "client.fs.s3a.aws.credentials.provider",
                "client.fs.s3a.access.key",
                "client.fs.s3a.secret.key",
                "client.fs.s3a.access-key",
                "client.fs.s3a.secret-key",
                "client.fs.s3a.session.token",
                "client.fs.s3a.assumed.role.arn",
                "client.fs.s3a.assumed.role.sts.endpoint",
                "client.fs.s3.aws.credentials.provider",
                "client.fs.s3.access.key",
                "client.fs.s3.secret.key",
                "client.fs.s3.secret-key",
                "client.fs.fs.s3a.aws.credentials.provider",
                "client.fs.fs.s3a.access.key",
                "client.fs.fs.s3a.secret-key",
                "client.fs.fs.s3a.assumed.role.arn"
            })
    void testRejectCredentialBearingS3ClientFileSystemOptions(String key) {
        Configuration conf = new Configuration();
        conf.setString(key, "test-value");

        assertThatThrownBy(() -> FlussConnection.getClientFileSystemConfiguration(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(key)
                .hasMessageContaining("Client-side S3 credential configuration")
                .hasMessageContaining("Fluss servers");
    }
}
