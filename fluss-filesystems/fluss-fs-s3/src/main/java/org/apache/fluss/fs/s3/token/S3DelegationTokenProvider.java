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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.GetSessionTokenResponse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

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

        StsClient stsClient =
                StsClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(accessKey, secretKey)))
                        .build();
        GetSessionTokenResponse sessionTokenResponse = stsClient.getSessionToken();
        Credentials credentials = sessionTokenResponse.credentials();

        LOG.info(
                "Session credentials obtained successfully with access key: {} expiration: {}",
                credentials.accessKeyId(),
                credentials.expiration());

        return new ObtainedSecurityToken(
                scheme,
                toJson(credentials),
                credentials.expiration().toEpochMilli(),
                additionInfos);
    }

    private byte[] toJson(Credentials credentials) {
        org.apache.fluss.fs.token.Credentials flussCredentials =
                new org.apache.fluss.fs.token.Credentials(
                        credentials.accessKeyId(),
                        credentials.secretAccessKey(),
                        credentials.sessionToken());
        return CredentialsJsonSerde.toJson(flussCredentials);
    }
}
