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

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.fs.cos.COSFileSystemPlugin.ENDPOINT_KEY;

/** A provider to provide Tencent Cloud COS security token. */
public class COSSecurityTokenProvider {

    private static final String SECRET_ID = "fs.cosn.userinfo.secretId";
    private static final String SECRET_KEY = "fs.cosn.userinfo.secretKey";

    private final String endpoint;
    private final String secretId;
    private final String secretKey;

    public COSSecurityTokenProvider(Configuration conf) {
        endpoint = conf.get(ENDPOINT_KEY);
        secretId = conf.get(SECRET_ID);
        secretKey = conf.get(SECRET_KEY);
    }

    public ObtainedSecurityToken obtainSecurityToken(String scheme) {
        // For COS, we directly use the configured secret id and secret key as the token.
        // If STS temporary credentials are needed in the future, this can be extended
        // to call Tencent Cloud STS API to get temporary credentials.
        Map<String, String> additionInfo = new HashMap<>();
        // we need to put endpoint as addition info
        if (endpoint != null) {
            additionInfo.put(ENDPOINT_KEY, endpoint);
        }

        Credentials credentials = new Credentials(secretId, secretKey, null);
        byte[] tokenBytes = CredentialsJsonSerde.toJson(credentials);

        // token does not expire when using static credentials
        return new ObtainedSecurityToken(scheme, tokenBytes, Long.MAX_VALUE, additionInfo);
    }
}
