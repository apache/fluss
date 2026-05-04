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

import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
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
    private static final String PATH_STYLE_ACCESS_KEY = "fs.s3a.path.style.access";

    private static final String ROLE_ARN_KEY = "fs.s3a.assumed.role.arn";
    private static final String STS_ENDPOINT_KEY = "fs.s3a.assumed.role.sts.endpoint";

    private final String scheme;
    private final String region;
    @Nullable private final String accessKey;
    @Nullable private final String secretKey;
    @Nullable private final String roleArn;
    @Nullable private final String stsEndpoint;
    private final Map<String, String> additionInfos;

    public S3DelegationTokenProvider(String scheme, Configuration conf) {
        this.scheme = scheme;
        this.region = conf.get(REGION_KEY);
        checkArgument(region != null, "Region is not set.");
        this.accessKey = conf.get(ACCESS_KEY_ID);
        this.secretKey = conf.get(ACCESS_KEY_SECRET);
        this.roleArn = conf.get(ROLE_ARN_KEY);
        this.stsEndpoint = conf.get(STS_ENDPOINT_KEY);

        checkArgument(
                (accessKey == null) == (secretKey == null),
                "S3 access key and secret key must both be set or both be unset.");
        if (accessKey == null) {
            checkArgument(
                    roleArn != null,
                    "Role ARN must be set when static credentials are not provided.");
        }

        this.additionInfos = new HashMap<>();
        for (String key : Arrays.asList(REGION_KEY, ENDPOINT_KEY, PATH_STYLE_ACCESS_KEY)) {
            if (conf.get(key) != null) {
                additionInfos.put(key, conf.get(key));
            }
        }
    }

    public ObtainedSecurityToken obtainSecurityToken() {
        AWSSecurityTokenService stsClient = buildStsClient();
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

    private AWSSecurityTokenService buildStsClient() {
        AWSSecurityTokenServiceClientBuilder builder =
                AWSSecurityTokenServiceClientBuilder.standard();

        if (accessKey != null && secretKey != null) {
            builder.withCredentials(
                    new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(accessKey, secretKey)));
        }

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
