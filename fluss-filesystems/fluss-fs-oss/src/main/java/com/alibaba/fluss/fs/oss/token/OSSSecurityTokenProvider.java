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

package com.alibaba.fluss.fs.oss.token;

import com.alibaba.fluss.fs.token.Credentials;
import com.alibaba.fluss.fs.token.CredentialsJsonSerde;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

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

import static com.alibaba.fluss.fs.oss.OSSFileSystemPlugin.REGION_KEY;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_SECRET;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ENDPOINT_KEY;

/** A provider to provide oss security token. */
public class OSSSecurityTokenProvider {

    private static final String ROLE_ARN_KEY = "fs.oss.roleArn";
    private static final String STS_ENDPOINT_KEY = "fs.oss.sts.endpoint";

    private final String endpoint;
    private final String region;
    private final DefaultAcsClient acsClient;
    private final String roleArn;

    public OSSSecurityTokenProvider(Configuration conf) throws IOException {
        endpoint = AliyunOSSUtils.getValueWithKey(conf, ENDPOINT_KEY);
        String accessKeyId = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_ID);
        String accessKeySecret = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_SECRET);
        String endpoint = AliyunOSSUtils.getValueWithKey(conf, STS_ENDPOINT_KEY);
        region = AliyunOSSUtils.getValueWithKey(conf, REGION_KEY);
        // don't need set region id
        DefaultProfile.addEndpoint("", "Sts", endpoint);
        IClientProfile profile = DefaultProfile.getProfile("", accessKeyId, accessKeySecret);
        acsClient = new DefaultAcsClient(profile);
        roleArn = conf.get(ROLE_ARN_KEY);
    }

    public ObtainedSecurityToken obtainSecurityToken(String scheme) throws Exception {
        final AssumeRoleRequest request = new AssumeRoleRequest();
        request.setSysMethod(MethodType.POST);
        request.setRoleArn(roleArn);
        // session name is used for audit, in here, we just generate a unique session name
        // todo: may consider use meaningful session name
        request.setRoleSessionName("fluss-" + UUID.randomUUID());
        // todo: may consider make token duration time configurable, we don't set it now
        // token duration time is 1 hour by default
        final AssumeRoleResponse response = acsClient.getAcsResponse(request);

        AssumeRoleResponse.Credentials credentials = response.getCredentials();
        DefaultCredentials defaultCredentials =
                new DefaultCredentials(
                        response.getCredentials().getAccessKeyId(),
                        response.getCredentials().getAccessKeySecret(),
                        response.getCredentials().getSecurityToken());

        Map<String, String> additionInfo = new HashMap<>();
        // we need to put endpoint as addition info
        additionInfo.put(ENDPOINT_KEY, endpoint);
        additionInfo.put(REGION_KEY, region);

        return new ObtainedSecurityToken(
                scheme,
                toJson(defaultCredentials),
                Instant.parse(credentials.getExpiration()).toEpochMilli(),
                additionInfo);
    }

    private byte[] toJson(DefaultCredentials defaultCredentials) {
        Credentials credentials =
                new Credentials(
                        defaultCredentials.getAccessKeyId(),
                        defaultCredentials.getSecretAccessKey(),
                        defaultCredentials.getSecurityToken());
        return CredentialsJsonSerde.toJson(credentials);
    }
}
