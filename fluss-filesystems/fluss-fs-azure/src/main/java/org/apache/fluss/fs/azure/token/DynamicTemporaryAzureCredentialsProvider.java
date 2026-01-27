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

package org.apache.fluss.fs.azure.token;

import org.apache.fluss.fs.token.Credentials;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * Support dynamic token for authenticating with Azure. Please note that users may reference this
 * class name from configuration property fs.azure.account.oauth.provider.type. Therefore, changing
 * the class name would be a backward-incompatible change. This credential provider must not fail in
 * creation because that will break a chain of credential providers.
 */
public class DynamicTemporaryAzureCredentialsProvider extends AccessTokenProvider
        implements CustomTokenProviderAdaptee {

    public static final String NAME = DynamicTemporaryAzureCredentialsProvider.class.getName();

    public static final String COMPONENT = "Dynamic session credentials for Fluss";

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicTemporaryAzureCredentialsProvider.class);

    @Override
    public void initialize(Configuration configuration, String s) throws IOException {}

    @Override
    public String getAccessToken() throws IOException {
        return getToken().getAccessToken();
    }

    @Override
    public Date getExpiryTime() {
        try {
            return getToken().getExpiry();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        Credentials credentials = AzureDelegationTokenReceiver.getCredentials();
        Long validUntil = AzureDelegationTokenReceiver.validUntil;

        if (credentials == null) {
            throw new TokenAccessProviderException(COMPONENT);
        }

        LOG.debug("Providing session credentials");

        AzureADToken azureADToken = new AzureADToken();
        azureADToken.setAccessToken(credentials.getSecurityToken());
        azureADToken.setExpiry(new Date(validUntil));
        return azureADToken;
    }
}
