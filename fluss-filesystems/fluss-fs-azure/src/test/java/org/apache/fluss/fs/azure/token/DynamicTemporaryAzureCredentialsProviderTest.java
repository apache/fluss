/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.fs.azure.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DynamicTemporaryAzureCredentialsProvider}. */
class DynamicTemporaryAzureCredentialsProviderTest {

    private static final String CLIENT_ID = null;
    private static final String CLIENT_SECRET = null;

    private static final String SESSION_TOKEN = "sessionToken";

    @AfterEach
    void tearDown() {
        AzureDelegationTokenReceiver.credentials = null;
        AzureDelegationTokenReceiver.validUntil = null;
        AzureDelegationTokenReceiver.additionInfos = null;
    }

    @Test
    void getCredentialsShouldThrowExceptionWhenNoCredentials() {
        DynamicTemporaryAzureCredentialsProvider provider =
                new DynamicTemporaryAzureCredentialsProvider();

        assertThatThrownBy(provider::getToken).isInstanceOf(TokenAccessProviderException.class);
    }

    @Test
    void getCredentialsShouldStoreCredentialsWhenCredentialsProvided() throws Exception {
        DynamicTemporaryAzureCredentialsProvider provider =
                new DynamicTemporaryAzureCredentialsProvider();
        Credentials credentials = new Credentials(CLIENT_ID, CLIENT_SECRET, SESSION_TOKEN);

        AzureDelegationTokenReceiver receiver = new AbfsDelegationTokenReceiver();

        byte[] json = CredentialsJsonSerde.toJson(credentials);

        ObtainedSecurityToken obtainedSecurityToken =
                new ObtainedSecurityToken("abfs", json, 1L, new HashMap<>());
        receiver.onNewTokensObtained(obtainedSecurityToken);

        AzureADToken azureADToken = provider.getToken();
        assertThat(azureADToken.getAccessToken()).isEqualTo(credentials.getSecurityToken());
        assertThat(azureADToken.getExpiry()).isEqualTo(new Date(1L));
    }
}
