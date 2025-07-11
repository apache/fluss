package com.alibaba.fluss.fs.s3.token;

import com.alibaba.fluss.exception.FlussRuntimeException;

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
        org.apache.hadoop.conf.Configuration hadoopConfig;

        // On an empty list, no changes should be made
        hadoopConfig = new org.apache.hadoop.conf.Configuration();
        hadoopConfig.set("fs.s3a.aws.credentials.provider", "");
        S3DelegationTokenReceiver.updateHadoopConfigCredentialProviders(
                hadoopConfig, Collections.emptyList());
        assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider")).isEqualTo("");

        hadoopConfig = new org.apache.hadoop.conf.Configuration();
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
