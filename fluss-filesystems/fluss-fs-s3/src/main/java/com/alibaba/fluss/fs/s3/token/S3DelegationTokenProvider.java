/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.s3.token;

import com.alibaba.fluss.fs.token.CredentialsJsonSerde;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Delegation token provider for S3 Hadoop filesystems. */
public class S3DelegationTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(S3DelegationTokenProvider.class);

    private static final String ACCESS_KEY_ID = "fs.s3a.access.key";
    private static final String ACCESS_KEY_SECRET = "fs.s3a.secret.key";

    private static final String REGION_KEY = "fs.s3a.region";
    private static final String ENDPOINT_KEY = "fs.s3a.endpoint";

    private final String scheme;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final Map<String, String> additionInfos;

    public S3DelegationTokenProvider(String scheme, Configuration conf) {
        this.scheme = scheme;
        this.region = conf.get(REGION_KEY);
        checkNotNull(region, "Region is not set.");
        this.accessKey = conf.get(ACCESS_KEY_ID);
        this.secretKey = conf.get(ACCESS_KEY_SECRET);
        this.additionInfos = new HashMap<>();
        for (String key : Arrays.asList(REGION_KEY, ENDPOINT_KEY)) {
            if (conf.get(key) != null) {
                additionInfos.put(key, conf.get(key));
            }
        }
    }

    public ObtainedSecurityToken obtainSecurityToken() {
        LOG.info("Obtaining session credentials token with access key: {}", accessKey);

        AWSSecurityTokenService stsClient =
                AWSSecurityTokenServiceClientBuilder.standard()
                        .withRegion(region)
                        .withCredentials(
                                new AWSStaticCredentialsProvider(
                                        new BasicAWSCredentials(accessKey, secretKey)))
                        .build();
        GetSessionTokenResult sessionTokenResult = stsClient.getSessionToken();
        Credentials credentials = sessionTokenResult.getCredentials();

        LOG.info(
                "Session credentials obtained successfully with access key: {} expiration: {}",
                credentials.getAccessKeyId(),
                credentials.getExpiration());

        return new ObtainedSecurityToken(
                scheme, toJson(credentials), credentials.getExpiration().getTime(), additionInfos);
    }

    private byte[] toJson(Credentials credentials) {
        com.alibaba.fluss.fs.token.Credentials flussCredentials =
                new com.alibaba.fluss.fs.token.Credentials(
                        credentials.getAccessKeyId(),
                        credentials.getSecretAccessKey(),
                        credentials.getSessionToken());
        return CredentialsJsonSerde.toJson(flussCredentials);
    }
}
