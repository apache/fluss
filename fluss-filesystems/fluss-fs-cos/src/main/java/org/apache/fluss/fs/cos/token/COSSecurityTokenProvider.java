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

package org.apache.fluss.fs.cos.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import com.tencentcloudapi.sts.v20180813.StsClient;
import com.tencentcloudapi.sts.v20180813.models.GetFederationTokenRequest;
import com.tencentcloudapi.sts.v20180813.models.GetFederationTokenResponse;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.fs.cos.COSFileSystemPlugin.ENDPOINT_KEY;
import static org.apache.fluss.fs.cos.COSFileSystemPlugin.REGION;
import static org.apache.fluss.fs.cos.COSFileSystemPlugin.SECRET_ID;
import static org.apache.fluss.fs.cos.COSFileSystemPlugin.SECRET_KEY;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A provider to provide Tencent Cloud COS security token by calling STS GetFederationToken API to
 * obtain temporary credentials.
 */
public class COSSecurityTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(COSSecurityTokenProvider.class);

    /** Default federation token name. */
    private static final String FEDERATION_TOKEN_NAME = "fluss-cos-federation";

    /** Default duration seconds for temporary credentials, 1800s = 30min. */
    private static final long DEFAULT_DURATION_SECONDS = 1800L;

    /**
     * Default COS full read-write policy that allows all COS operations. See <a
     * href="https://cloud.tencent.com/document/product/436/6884">Tencent Cloud COS doc</a>.
     */
    private static final String DEFAULT_POLICY =
            "{"
                    + "\"version\": \"2.0\","
                    + "\"statement\": [{"
                    + "\"action\": [\"name/cos:*\"],"
                    + "\"effect\": \"allow\","
                    + "\"resource\": [\"*\"]"
                    + "}]"
                    + "}";

    private final String region;
    private final String secretId;
    private final String secretKey;
    private final Map<String, String> additionInfos;

    public COSSecurityTokenProvider(Configuration conf) {
        this.region = conf.get(REGION);
        checkNotNull(region, "Region is not set. Please set " + REGION);
        this.secretId = conf.get(SECRET_ID);
        checkNotNull(secretId, "Secret ID is not set. Please set " + SECRET_ID);
        this.secretKey = conf.get(SECRET_KEY);
        checkNotNull(secretKey, "Secret Key is not set. Please set " + SECRET_KEY);

        this.additionInfos = new HashMap<>();
        for (String key : Arrays.asList(REGION, ENDPOINT_KEY)) {
            if (conf.get(key) != null) {
                additionInfos.put(key, conf.get(key));
            }
        }
    }

    public ObtainedSecurityToken obtainSecurityToken(String scheme)
            throws TencentCloudSDKException {
        LOG.info("Obtaining session credentials token with secret id: {}", secretId);

        Credential cred = new Credential(secretId, secretKey);
        StsClient stsClient = new StsClient(cred, region);

        GetFederationTokenRequest request = new GetFederationTokenRequest();
        request.setName(FEDERATION_TOKEN_NAME);
        request.setDurationSeconds(DEFAULT_DURATION_SECONDS);
        request.setPolicy(DEFAULT_POLICY);

        GetFederationTokenResponse response = stsClient.GetFederationToken(request);
        com.tencentcloudapi.sts.v20180813.models.Credentials stsCredentials =
                response.getCredentials();

        // ExpiredTime is a Unix timestamp in seconds, convert to milliseconds
        long expiredTimeMillis = response.getExpiredTime() * 1000L;

        LOG.info(
                "Session credentials obtained successfully with tmp secret id: {}, expiration: {}",
                stsCredentials.getTmpSecretId(),
                response.getExpiration());

        return new ObtainedSecurityToken(
                scheme, toJson(stsCredentials), expiredTimeMillis, additionInfos);
    }

    private byte[] toJson(com.tencentcloudapi.sts.v20180813.models.Credentials stsCredentials) {
        Credentials credentials =
                new Credentials(
                        stsCredentials.getTmpSecretId(),
                        stsCredentials.getTmpSecretKey(),
                        stsCredentials.getToken());
        return CredentialsJsonSerde.toJson(credentials);
    }
}
