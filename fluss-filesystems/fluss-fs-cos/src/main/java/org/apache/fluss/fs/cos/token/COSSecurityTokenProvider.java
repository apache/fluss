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

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.fs.cos.COSFileSystemPlugin.ENDPOINT_KEY;
import static org.apache.fluss.fs.cos.COSFileSystemPlugin.REGION;
import static org.apache.fluss.fs.cos.COSFileSystemPlugin.SECRET_ID;
import static org.apache.fluss.fs.cos.COSFileSystemPlugin.SECRET_KEY;
import static org.apache.fluss.fs.cos.COSFileSystemPlugin.SECURITY_TOKEN_POLICY;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A provider to provide Tencent Cloud COS security token by calling STS GetFederationToken API to
 * obtain temporary credentials.
 */
public class COSSecurityTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(COSSecurityTokenProvider.class);

    /** Default federation token name. */
    private static final String FEDERATION_TOKEN_NAME = "fluss-cos-federation";

    /** Default duration seconds for temporary credentials, 3600s = 1h. */
    private static final long DEFAULT_DURATION_SECONDS = 3600L;

    private final String region;
    private final String secretId;
    private final String secretKey;
    private final String policy;
    private final Map<String, String> additionInfos;

    public COSSecurityTokenProvider(URI fsUri, Configuration conf) {
        checkNotNull(fsUri, "fsUri must not be null");
        this.region = conf.get(REGION);
        checkNotNull(region, "Region is not set. Please set " + REGION);
        this.secretId = conf.get(SECRET_ID);
        checkNotNull(secretId, "Secret ID is not set. Please set " + SECRET_ID);
        this.secretKey = conf.get(SECRET_KEY);
        checkNotNull(secretKey, "Secret Key is not set. Please set " + SECRET_KEY);
        this.policy = resolvePolicy(conf, fsUri, region);

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
        request.setPolicy(policy);

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

    /**
     * Resolves the STS policy to apply when calling {@code GetFederationToken}.
     *
     * <p>If the user has explicitly configured {@link
     * org.apache.fluss.fs.cos.COSFileSystemPlugin#SECURITY_TOKEN_POLICY}, that value is used as-is.
     * Otherwise a default policy is built that scopes the temporary credential to the bucket and
     * (optional) prefix derived from the fsUri (i.e. {@code remote.data.dir}), so the token cannot
     * be used to access other COS resources.
     */
    private static String resolvePolicy(Configuration conf, URI fsUri, String region) {
        String userPolicy = conf.get(SECURITY_TOKEN_POLICY);
        if (userPolicy != null && !userPolicy.trim().isEmpty()) {
            LOG.info(
                    "Using user-provided STS policy from {} for COS federation token.",
                    SECURITY_TOKEN_POLICY);
            return userPolicy;
        }
        return buildBucketScopedPolicy(fsUri, region);
    }

    /**
     * Builds a default STS policy that grants {@code name/cos:*} only on the bucket (and optional
     * key prefix) referenced by the given fsUri.
     *
     * <p>COS resource format used here: {@code qcs::cos:<region>:uid/*:<bucket>/<prefix>*}. The
     * wildcard owner uid keeps the policy independent of the account uin while still restricting
     * access to a specific bucket.
     */
    private static String buildBucketScopedPolicy(URI fsUri, String region) {
        String bucket = fsUri.getAuthority();
        if (bucket == null || bucket.isEmpty()) {
            // Fall back to all-resources policy if we cannot derive the bucket from fsUri.
            LOG.warn(
                    "Unable to derive bucket from fsUri {}. Falling back to a wildcard COS resource"
                            + " policy. Consider explicitly configuring {} to a least-privilege"
                            + " policy.",
                    fsUri,
                    SECURITY_TOKEN_POLICY);
            return "{"
                    + "\"version\": \"2.0\","
                    + "\"statement\": [{"
                    + "\"action\": [\"name/cos:*\"],"
                    + "\"effect\": \"allow\","
                    + "\"resource\": [\"*\"]"
                    + "}]"
                    + "}";
        }

        String path = fsUri.getPath();
        String prefix;
        if (path == null || path.isEmpty() || "/".equals(path)) {
            prefix = "";
        } else {
            // Strip leading '/' so the resource pattern looks like "<bucket>/sub/dir/*".
            prefix = path.startsWith("/") ? path.substring(1) : path;
            if (!prefix.endsWith("/")) {
                prefix = prefix + "/";
            }
        }

        String resource = "qcs::cos:" + region + ":uid/*:" + bucket + "/" + prefix + "*";
        LOG.info("Using bucket-scoped STS policy with resource: {}", resource);
        return "{"
                + "\"version\": \"2.0\","
                + "\"statement\": [{"
                + "\"action\": [\"name/cos:*\"],"
                + "\"effect\": \"allow\","
                + "\"resource\": [\""
                + resource
                + "\"]"
                + "}]"
                + "}";
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
