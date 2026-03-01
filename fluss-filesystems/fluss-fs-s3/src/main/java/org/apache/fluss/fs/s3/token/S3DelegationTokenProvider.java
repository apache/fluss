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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

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

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Delegation token provider for S3 Hadoop filesystems. */
public class S3DelegationTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(S3DelegationTokenProvider.class);

    private static final String ACCESS_KEY_ID = "fs.s3a.access.key";
    private static final String ACCESS_KEY_SECRET = "fs.s3a.secret.key";

    private static final String REGION_KEY = "fs.s3a.region";
    private static final String ENDPOINT_KEY = "fs.s3a.endpoint";
    private static final String PATH_STYLE_ACCESS_KEY = "fs.s3a.path.style.access";

    private final String scheme;
    private final Type type;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final Map<String, String> additionalInfos;
    private final ObtainedSecurityToken defaultToken;

    /** Type of token that should be provided. */
    public enum Type {
        NO_TOKEN, // supported by S3-compatible object stores (e.g., MinIO)
        STS_SESSION_TOKEN // currently only supported by AWS
    }

    public S3DelegationTokenProvider(String scheme, Configuration conf, Type type) {
        this.scheme = scheme;
        this.type = type;
        this.region = conf.get(REGION_KEY);
        validateRegion(type, region);
        this.accessKey = conf.get(ACCESS_KEY_ID);
        this.secretKey = conf.get(ACCESS_KEY_SECRET);
        this.additionalInfos = new HashMap<>();
        for (String key : Arrays.asList(REGION_KEY, ENDPOINT_KEY, PATH_STYLE_ACCESS_KEY)) {
            if (conf.get(key) != null) {
                additionalInfos.put(key, conf.get(key));
            }
        }
        if (type == Type.NO_TOKEN) {
            defaultToken =
                    new ObtainedSecurityToken(this.scheme, new byte[0], null, additionalInfos);
        } else {
            defaultToken = null;
        }
    }

    private static void validateRegion(Type type, String region) {
        if (type == Type.STS_SESSION_TOKEN) {
            checkNotNull(region, "Region is not set.");
        }
    }

    public ObtainedSecurityToken obtainSecurityToken() {
        switch (type) {
            case NO_TOKEN:
                return defaultToken;
            case STS_SESSION_TOKEN:
                return obtainStsSessionToken();
            default:
                throw new FlussRuntimeException("Unknown token type: " + type);
        }
    }

    private ObtainedSecurityToken obtainStsSessionToken() {
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
                scheme,
                toJson(credentials),
                credentials.getExpiration().getTime(),
                additionalInfos);
    }

    private byte[] toJson(Credentials credentials) {
        org.apache.fluss.fs.token.Credentials flussCredentials =
                new org.apache.fluss.fs.token.Credentials(
                        credentials.getAccessKeyId(),
                        credentials.getSecretAccessKey(),
                        credentials.getSessionToken());
        return CredentialsJsonSerde.toJson(flussCredentials);
    }

    @VisibleForTesting
    public Type getType() {
        return type;
    }
}
