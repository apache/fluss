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

package com.alibaba.fluss.fs.cosn.token;

import com.alibaba.fluss.fs.token.Credentials;
import com.alibaba.fluss.fs.token.CredentialsJsonSerde;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import com.tencentcloudapi.sts.v20180813.StsClient;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleRequest;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.CosNConfigKeys;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_REGION_KEY;

/** A provider to provide cosn security token. */
public class COSNSecurityTokenProvider {

    private static final String ROLE_ARN_KEY = "fs.cosn.roleArn";
    // e.g., sts.tencentcloudapi.com
    private static final String STS_ENDPOINT_KEY = "fs.cosn.sts.endpoint";
    public static final String STS_REGION_KEY = "fs.cosn.sts.region";

    private final String region;
    private final StsClient stsClient;
    private final String roleArn;
    private final String roleSessionName;

    public COSNSecurityTokenProvider(Configuration conf) throws IOException {
        String secretId = conf.get(CosNConfigKeys.COSN_SECRET_ID_KEY);
        String secretKey = conf.get(CosNConfigKeys.COSN_SECRET_KEY_KEY);
        String stsEndpoint = conf.get(STS_ENDPOINT_KEY, "sts.tencentcloudapi.com");

        if (secretId == null || secretKey == null) {
            throw new IOException(
                    "Missing required configuration: "
                            + CosNConfigKeys.COSN_SECRET_ID_KEY
                            + " or "
                            + CosNConfigKeys.COSN_SECRET_KEY_KEY);
        }

        Credential cred = new Credential(secretId, secretKey);
        HttpProfile httpProfile = new HttpProfile();
        httpProfile.setEndpoint(stsEndpoint);
        ClientProfile clientProfile = new ClientProfile();
        clientProfile.setHttpProfile(httpProfile);

        this.roleArn = conf.get(ROLE_ARN_KEY);
        if (this.roleArn == null) {
            throw new IOException("Missing required configuration: " + ROLE_ARN_KEY);
        }
        this.region = conf.get(COSN_REGION_KEY);
        if (this.region == null) {
            throw new IOException("Missing required configuration: " + COSN_REGION_KEY);
        }

        String stsRegion = conf.get(STS_REGION_KEY);
        if (stsRegion == null) {
            throw new IOException("Missing required configuration: " + STS_REGION_KEY);
        }
        // session name is used for audit, in here, we just generate a unique session name
        this.roleSessionName = "fluss-" + UUID.randomUUID();

        this.stsClient = new StsClient(cred, stsRegion, clientProfile);
    }

    public ObtainedSecurityToken obtainSecurityToken(String scheme) throws Exception {
        final AssumeRoleRequest request = new AssumeRoleRequest();
        request.setRoleArn(roleArn);
        request.setRoleSessionName(roleSessionName);
        // todo: may consider make token duration time configurable, we don't set it now
        request.setDurationSeconds(3600L);

        final AssumeRoleResponse response = stsClient.AssumeRole(request);

        com.tencentcloudapi.sts.v20180813.models.Credentials credentials =
                response.getCredentials();

        // Convert to our internal Credentials format for serialization
        Credentials flussCredentials =
                new Credentials(
                        credentials.getTmpSecretId(),
                        credentials.getTmpSecretKey(),
                        credentials.getToken());

        Map<String, String> additionInfo = new HashMap<>();
        additionInfo.put(COSN_REGION_KEY, region);

        // Expiration is a timestamp string like "2023-08-08T12:00:00Z", convert to epoch millis
        // Note: Tencent STS SDK returns expiration as a string timestamp, needs parsing.
        // Assuming the response object `credentials` has getExpiration() returning a UTC string.
        long expirationEpochMilli = Instant.parse(response.getExpiration()).toEpochMilli();

        return new ObtainedSecurityToken(
                scheme, toJson(flussCredentials), expirationEpochMilli, additionInfo);
    }

    private byte[] toJson(Credentials credentials) {
        // Use the same serialization logic as the original project
        return CredentialsJsonSerde.toJson(credentials);
    }
}
