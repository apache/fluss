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

import org.apache.fluss.exception.FlussRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3DelegationTokenReceiver}. */
public class S3DelegationTokenReceiverTest {

    @BeforeEach
    void beforeEach() {
        S3DelegationTokenReceiver.additionalInfos = null;
    }

    @Test
    void testUpdateCredentialProviders() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        // We start with an empty provider list and iteratively add providers
        hadoopConfig.set("fs.s3a.aws.credentials.provider", "");

        // On an empty list, no changes should be made
        S3DelegationTokenReceiver.updateHadoopConfigCredentialProviders(
                hadoopConfig, Collections.emptyList());
        assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider")).isEqualTo("");

        hadoopConfig.set(
                "fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider");
        S3DelegationTokenReceiver.updateHadoopConfigCredentialProviders(
                hadoopConfig, Collections.emptyList());
        assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider"))
                .isEqualTo(
                        "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider");

        // Credential providers should be prepended in the given order
        S3DelegationTokenReceiver.updateHadoopConfigCredentialProviders(
                hadoopConfig,
                Arrays.asList(
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                        "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"));
        assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider"))
                .isEqualTo(
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider,software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider");

        // Credential providers should only be added once
        S3ADelegationTokenReceiver.updateHadoopConfigCredentialProviders(
                hadoopConfig,
                Collections.singletonList(
                        "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"));
        assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider"))
                .isEqualTo(
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider,software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider");
    }

    @Test
    void testUpdateAdditionalInfosPresent() {
        Map<String, String> additionalInfos = new HashMap<>();
        additionalInfos.put("fs.s3a.region", "eu-central-1");
        additionalInfos.put("fs.s3a.endpoint", "http://localhost:9000");
        S3DelegationTokenReceiver.additionalInfos = additionalInfos;

        org.apache.hadoop.conf.Configuration conf;
        conf = new org.apache.hadoop.conf.Configuration();
        S3ADelegationTokenReceiver.updateHadoopConfigAdditionalInfos(conf);
        assertThat(conf.get("fs.s3a.region")).isEqualTo("eu-central-1");
        assertThat(conf.get("fs.s3a.endpoint")).isEqualTo("http://localhost:9000");
    }

    @Test
    void testUpdateAdditionalInfosNotPresent() {
        assertThatThrownBy(
                        () ->
                                S3ADelegationTokenReceiver.updateHadoopConfigAdditionalInfos(
                                        new org.apache.hadoop.conf.Configuration()))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Expected additionalInfos to be not null.");
    }
}
