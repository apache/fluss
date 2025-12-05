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

package org.apache.fluss.fs.abfs.token;

import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Token provider for abfs Hadoop filesystems. */
public class AzureDelegationTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AzureDelegationTokenProvider.class);

    private static final String CLIENT_ID = "fs.azure.account.oauth2.client.id";
    private static final String CLIENT_SECRET = "fs.azure.account.oauth2.client.secret";

    private static final String ENDPOINT_KEY = "fs.azure.account.oauth2.client.endpoint";

    private final String scheme;
    private final String clientId;
    private final String clientSecret;

    private final String authEndpoint;
    private final Map<String, String> additionInfos;

    public AzureDelegationTokenProvider(String scheme, Configuration conf) {
        this.scheme = scheme;

        this.clientId = conf.get(CLIENT_ID);
        this.clientSecret = conf.get(CLIENT_SECRET);
        this.authEndpoint = conf.get(ENDPOINT_KEY);
        this.additionInfos = new HashMap<>();

        for (String key : Arrays.asList(ENDPOINT_KEY)) {
            if (conf.get(key) != null) {
                additionInfos.put(key, conf.get(key));
            }
        }
    }

    public ObtainedSecurityToken obtainSecurityToken() {
        LOG.info("Obtaining session credentials token with access key: {}", clientId);

        try {
            AzureADToken azureADToken =
                    AzureADAuthenticator.getTokenUsingClientCreds(
                            this.authEndpoint, this.clientId, this.clientSecret);

            LOG.info(
                    "Session credentials obtained successfully with expiration: {}",
                    azureADToken.getExpiry());

            return new ObtainedSecurityToken(
                    scheme,
                    toJson(azureADToken),
                    azureADToken.getExpiry().getTime(),
                    additionInfos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] toJson(AzureADToken accessToken) {
        org.apache.fluss.fs.token.Credentials flussCredentials =
                new org.apache.fluss.fs.token.Credentials(null, null, accessToken.getAccessToken());
        return CredentialsJsonSerde.toJson(flussCredentials);
    }
}
