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

package org.apache.fluss.fs.oss.token;

import org.apache.fluss.fs.FileSystem.FSKey;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.utils.StringUtils;

import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.fluss.fs.oss.OSSFileSystemPlugin.REGION_KEY;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_SECRET;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ENDPOINT_KEY;

/** A provider to provide oss security token. */
public class OSSSecurityTokenProvider {

    private static final String ROLE_ARN_KEY = "fs.oss.roleArn";
    private static final String STS_ENDPOINT_KEY = "fs.oss.sts.endpoint";
    /** Prefix for all OSS properties: {@value}. */
    private static final String FS_OSS_PREFIX = "fs.oss.";
    /** Prefix for OSS bucket-specific properties: {@value}. */
    private static final String FS_OSS_BUCKET_PREFIX = "fs.oss.bucket.";

    protected final Configuration conf;

    protected final Map<FSKey, OSSClientInfo> clientInfoMap = new HashMap<>();

    public OSSSecurityTokenProvider(Configuration conf) {
        this.conf = conf;
    }

    public ObtainedSecurityToken obtainSecurityToken(String scheme, String authority)
            throws Exception {
        FSKey fsKey = new FSKey(scheme, authority);
        if (!clientInfoMap.containsKey(fsKey)) {
            clientInfoMap.put(fsKey, initOSSClientInfo(authority));
        }

        OSSClientInfo info = clientInfoMap.get(fsKey);

        final AssumeRoleRequest request = new AssumeRoleRequest();
        request.setSysMethod(MethodType.POST);
        request.setRoleArn(info.roleArn);
        // session name is used for audit, in here, we just generate a unique session name
        // todo: may consider use meaningful session name
        request.setRoleSessionName("fluss-" + UUID.randomUUID());
        // todo: may consider make token duration time configurable, we don't set it now
        // token duration time is 1 hour by default
        final AssumeRoleResponse response = info.acsClient.getAcsResponse(request);

        AssumeRoleResponse.Credentials credentials = response.getCredentials();
        DefaultCredentials defaultCredentials =
                new DefaultCredentials(
                        response.getCredentials().getAccessKeyId(),
                        response.getCredentials().getAccessKeySecret(),
                        response.getCredentials().getSecurityToken());

        Map<String, String> additionInfo = new HashMap<>();
        // we need to put endpoint as addition info
        additionInfo.put(ENDPOINT_KEY, info.endpoint);
        additionInfo.put(REGION_KEY, info.region);

        return new ObtainedSecurityToken(
                scheme,
                authority,
                toJson(defaultCredentials),
                Instant.parse(credentials.getExpiration()).toEpochMilli(),
                additionInfo);
    }

    private OSSClientInfo initOSSClientInfo(String bucket) throws IOException {
        OSSClientInfo info = new OSSClientInfo();

        info.endpoint = getBucketOption(conf, bucket, ENDPOINT_KEY);
        String accessKeyId = getBucketOption(conf, bucket, ACCESS_KEY_ID);
        String accessKeySecret = getBucketOption(conf, bucket, ACCESS_KEY_SECRET);
        String endpoint = getBucketOption(conf, bucket, STS_ENDPOINT_KEY);
        info.region = getBucketOption(conf, bucket, REGION_KEY);
        // don't need set region id
        DefaultProfile.addEndpoint("", "Sts", endpoint);
        IClientProfile profile = DefaultProfile.getProfile("", accessKeyId, accessKeySecret);
        info.acsClient = new DefaultAcsClient(profile);
        info.roleArn = getBucketOption(conf, bucket, ROLE_ARN_KEY);

        return info;
    }

    private byte[] toJson(DefaultCredentials defaultCredentials) {
        Credentials credentials =
                new Credentials(
                        defaultCredentials.getAccessKeyId(),
                        defaultCredentials.getSecretAccessKey(),
                        defaultCredentials.getSecurityToken());
        return CredentialsJsonSerde.toJson(credentials);
    }

    /**
     * Get a bucket-specific property. If the generic key passed in has an {@code fs.oss.prefix},
     * that's stripped off.
     *
     * @param conf configuration to get
     * @param bucket bucket name
     * @param genericKey key; can start with "fs.oss."
     * @return the bucket option, null if there is none
     */
    public static String getBucketOption(Configuration conf, String bucket, String genericKey)
            throws IOException {
        final String baseKey =
                genericKey.startsWith(FS_OSS_PREFIX)
                        ? genericKey.substring(FS_OSS_PREFIX.length())
                        : genericKey;
        String value =
                AliyunOSSUtils.getValueWithKey(conf, FS_OSS_BUCKET_PREFIX + bucket + '.' + baseKey);
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            value = AliyunOSSUtils.getValueWithKey(conf, FS_OSS_PREFIX + baseKey);
        }
        return value;
    }

    protected static class OSSClientInfo {
        public String endpoint;
        public String region;
        public DefaultAcsClient acsClient;
        public String roleArn;
    }
}
