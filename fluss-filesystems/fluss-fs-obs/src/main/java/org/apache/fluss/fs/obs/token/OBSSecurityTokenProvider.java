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

package org.apache.fluss.fs.obs.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.huaweicloud.sdk.core.auth.GlobalCredentials;
import com.huaweicloud.sdk.core.auth.ICredential;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenRequest;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenRequestBody;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.huaweicloud.sdk.iam.v3.model.IdentityToken;
import com.huaweicloud.sdk.iam.v3.model.TokenAuth;
import com.huaweicloud.sdk.iam.v3.model.TokenAuthIdentity;
import com.huaweicloud.sdk.iam.v3.region.IamRegion;
import org.apache.hadoop.conf.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.fs.obs.OBSFileSystemPlugin.REGION_KEY;

/** A provider to provide HuaweiCloud obs security token. */
public class OBSSecurityTokenProvider {

    private static final String ACCESS_KEY_ID = "fs.obs.access.key";
    private static final String ACCESS_KEY_SECRET = "fs.obs.secret.key";
    private static final String ENDPOINT_KEY = "fs.obs.endpoint";

    private final String endpoint;
    private final String region;
    private final IamClient iamClient;

    public OBSSecurityTokenProvider(Configuration conf) {
        endpoint = conf.get(ENDPOINT_KEY);
        String accessKeyId = conf.get(ACCESS_KEY_ID);
        String accessKeySecret = conf.get(ACCESS_KEY_SECRET);
        region = conf.get(REGION_KEY);
        // Create IAM client
        ICredential credentials =
                new GlobalCredentials().withAk(accessKeyId).withSk(accessKeySecret);

        iamClient =
                IamClient.newBuilder()
                        .withCredential(credentials)
                        .withRegion(IamRegion.valueOf(region))
                        .build();
    }

    public ObtainedSecurityToken obtainSecurityToken(String scheme) {
        final CreateTemporaryAccessKeyByTokenRequest request =
                new CreateTemporaryAccessKeyByTokenRequest();
        CreateTemporaryAccessKeyByTokenRequestBody body =
                new CreateTemporaryAccessKeyByTokenRequestBody();
        // todo: may consider make token duration time configurable, we don't set it now
        // token duration time is 1 hour by default
        IdentityToken tokenIdentity = new IdentityToken();
        tokenIdentity.withDurationSeconds(3600);
        List<TokenAuthIdentity.MethodsEnum> listIdentityMethods = new ArrayList<>();
        listIdentityMethods.add(TokenAuthIdentity.MethodsEnum.fromValue("token"));
        TokenAuthIdentity identityAuth = new TokenAuthIdentity();
        identityAuth.withMethods(listIdentityMethods).withToken(tokenIdentity);
        TokenAuth authbody = new TokenAuth();
        authbody.withIdentity(identityAuth);
        body.withAuth(authbody);
        request.withBody(body);
        final CreateTemporaryAccessKeyByTokenResponse response =
                iamClient.createTemporaryAccessKeyByToken(request);
        Credential credential = response.getCredential();
        Map<String, String> additionInfo = new HashMap<>();
        // we need to put endpoint as addition info
        additionInfo.put(ENDPOINT_KEY, endpoint);
        additionInfo.put(REGION_KEY, region);
        return new ObtainedSecurityToken(
                scheme,
                toJson(credential),
                Instant.parse(credential.getExpiresAt()).toEpochMilli(),
                additionInfo);
    }

    private byte[] toJson(Credential credential) {
        Credentials credentials =
                new Credentials(
                        credential.getAccess(),
                        credential.getSecret(),
                        credential.getSecuritytoken());
        return CredentialsJsonSerde.toJson(credentials);
    }
}
