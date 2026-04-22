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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Delegation token provider for S3 Hadoop filesystems. */
public class S3DelegationTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(S3DelegationTokenProvider.class);

    private static final String ACCESS_KEY_ID = "fs.s3a.access.key";
    private static final String ACCESS_KEY_SECRET = "fs.s3a.secret.key";

    private static final String REGION_KEY = "fs.s3a.region";
    private static final String ENDPOINT_KEY = "fs.s3a.endpoint";

    private static final String ROLE_ARN_KEY = "fs.s3a.assumed.role.arn";
    private static final String STS_ENDPOINT_KEY = "fs.s3a.assumed.role.sts.endpoint";

    private static final String CREDENTIALS_PROVIDER_KEY = "fs.s3a.aws.credentials.provider";

    static final String DELEGATION_CONFIG_PREFIX = "fs.delegation.s3a.";
    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private final String scheme;
    private final String region;
    @Nullable private final String accessKey;
    @Nullable private final String secretKey;
    @Nullable private final String roleArn;
    @Nullable private final String stsEndpoint;
    @Nullable private final String credentialsProviderClass;
    private final Configuration delegationConfig;
    private final Map<String, String> additionInfos;

    public S3DelegationTokenProvider(String scheme, Configuration conf) {
        this.scheme = scheme;
        this.delegationConfig = buildDelegationConfig(conf);
        this.region = delegationConfig.get(REGION_KEY);
        checkArgument(region != null, "Region is not set.");
        this.accessKey = delegationConfig.get(ACCESS_KEY_ID);
        this.secretKey = delegationConfig.get(ACCESS_KEY_SECRET);
        this.roleArn = delegationConfig.get(ROLE_ARN_KEY);
        this.stsEndpoint = delegationConfig.get(STS_ENDPOINT_KEY);
        this.credentialsProviderClass = delegationConfig.get(CREDENTIALS_PROVIDER_KEY);

        checkArgument(
                (accessKey == null) == (secretKey == null),
                "S3 access key and secret key must both be set or both be unset.");

        this.additionInfos = new HashMap<>();
        for (String key : Arrays.asList(REGION_KEY, ENDPOINT_KEY)) {
            if (delegationConfig.get(key) != null) {
                additionInfos.put(key, delegationConfig.get(key));
            }
        }
    }

    /**
     * Builds a delegation-specific configuration by overlaying {@code fs.delegation.s3a.*} keys
     * (remapped to {@code fs.s3a.*}) onto the base Hadoop configuration. This allows delegation to
     * use different credentials than the server's own S3 operations.
     */
    @VisibleForTesting
    static Configuration buildDelegationConfig(Configuration baseConf) {
        Configuration delegationConf = new Configuration(baseConf);
        Iterator<Map.Entry<String, String>> iter = baseConf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String key = entry.getKey();
            if (key.startsWith(DELEGATION_CONFIG_PREFIX)) {
                String remappedKey =
                        HADOOP_CONFIG_PREFIX + key.substring(DELEGATION_CONFIG_PREFIX.length());
                delegationConf.set(remappedKey, entry.getValue());
                LOG.debug(
                        "Delegation config override: {} -> {} = {}",
                        key,
                        remappedKey,
                        entry.getValue());
            }
        }
        return delegationConf;
    }

    public ObtainedSecurityToken obtainSecurityToken() {
        if (credentialsProviderClass != null) {
            return obtainWithConfiguredProvider();
        }

        AWSCredentialsProvider stsCredentialsProvider;
        if (accessKey != null && secretKey != null) {
            stsCredentialsProvider =
                    new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        } else {
            stsCredentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        }

        AWSSecurityTokenService stsClient = buildStsClient(stsCredentialsProvider);
        try {
            Credentials credentials;

            if (roleArn != null) {
                LOG.info("Obtaining session credentials via AssumeRole, role: {}", roleArn);
                AssumeRoleRequest request =
                        new AssumeRoleRequest()
                                .withRoleArn(roleArn)
                                .withRoleSessionName("fluss-" + UUID.randomUUID());
                AssumeRoleResult result = stsClient.assumeRole(request);
                credentials = result.getCredentials();
            } else {
                LOG.info(
                        "Obtaining session credentials via GetSessionToken with access key: {}",
                        accessKey);
                GetSessionTokenResult result = stsClient.getSessionToken();
                credentials = result.getCredentials();
            }

            LOG.info(
                    "Session credentials obtained successfully with access key: {} expiration: {}",
                    credentials.getAccessKeyId(),
                    credentials.getExpiration());

            return new ObtainedSecurityToken(
                    scheme,
                    toJson(credentials),
                    credentials.getExpiration().getTime(),
                    additionInfos);
        } finally {
            stsClient.shutdown();
        }
    }

    private ObtainedSecurityToken obtainWithConfiguredProvider() {
        LOG.info(
                "Using configured credentials provider for delegation: {}",
                credentialsProviderClass);
        try {
            AWSCredentialsProvider provider = instantiateProvider(credentialsProviderClass);
            // Trigger credential resolution to detect NoDelegationAWSCredentialsProvider
            provider.getCredentials();

            // If we get here, the provider returned valid credentials — use them for STS
            AWSSecurityTokenService stsClient = buildStsClient(provider);
            try {
                Credentials credentials;
                if (roleArn != null) {
                    LOG.info("Obtaining session credentials via AssumeRole, role: {}", roleArn);
                    AssumeRoleRequest request =
                            new AssumeRoleRequest()
                                    .withRoleArn(roleArn)
                                    .withRoleSessionName("fluss-" + UUID.randomUUID());
                    AssumeRoleResult result = stsClient.assumeRole(request);
                    credentials = result.getCredentials();
                } else {
                    LOG.info("Obtaining session credentials via GetSessionToken");
                    GetSessionTokenResult result = stsClient.getSessionToken();
                    credentials = result.getCredentials();
                }

                LOG.info(
                        "Session credentials obtained successfully with access key: {} expiration: {}",
                        credentials.getAccessKeyId(),
                        credentials.getExpiration());

                return new ObtainedSecurityToken(
                        scheme,
                        toJson(credentials),
                        credentials.getExpiration().getTime(),
                        additionInfos);
            } finally {
                stsClient.shutdown();
            }
        } catch (NoAwsCredentialsException e) {
            LOG.info("Delegation is disabled via {}", credentialsProviderClass);
            return ObtainedSecurityToken.empty(scheme);
        }
    }

    private AWSCredentialsProvider instantiateProvider(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (AWSCredentialsProvider) clazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "Cannot instantiate credentials provider: " + className, e);
        }
    }

    private AWSSecurityTokenService buildStsClient(AWSCredentialsProvider credentialsProvider) {
        AWSSecurityTokenServiceClientBuilder builder =
                AWSSecurityTokenServiceClientBuilder.standard()
                        .withCredentials(credentialsProvider);

        if (stsEndpoint != null) {
            builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(stsEndpoint, region));
        } else {
            builder.withRegion(region);
        }

        return builder.build();
    }

    private byte[] toJson(Credentials credentials) {
        org.apache.fluss.fs.token.Credentials flussCredentials =
                new org.apache.fluss.fs.token.Credentials(
                        credentials.getAccessKeyId(),
                        credentials.getSecretAccessKey(),
                        credentials.getSessionToken());
        return CredentialsJsonSerde.toJson(flussCredentials);
    }
}
