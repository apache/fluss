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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.apache.fluss.fs.azure.AzureFileSystemOptions.CLIENT_ID;
import static org.apache.fluss.fs.azure.AzureFileSystemOptions.CLIENT_SECRET;
import static org.apache.fluss.fs.azure.AzureFileSystemOptions.ENDPOINT_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AzureDelegationTokenProvider}. */
public class AzureDelegationTokenProviderTest {

    private static final String TEST_CLIENT_ID = "testClientId";
    private static final String TEST_CLIENT_SECRET = "testClientSecret";

    private static final String TEST_ENDPOINT_PREFIX = "http://localhost:";
    private static String TEST_ENDPOINT;

    private static MockAuthServer mockAuthServer;

    @BeforeAll
    static void setup() {
        mockAuthServer = MockAuthServer.create();
        TEST_ENDPOINT = TEST_ENDPOINT_PREFIX + mockAuthServer.getPort();
    }

    @Test
    void obtainSecurityTokenShouldReturnSecurityToken() {
        Configuration configuration = new Configuration();
        configuration.set(CLIENT_ID, TEST_CLIENT_ID);
        configuration.set(CLIENT_SECRET, TEST_CLIENT_SECRET);
        configuration.set(ENDPOINT_KEY, TEST_ENDPOINT);
        AzureDelegationTokenProvider azureDelegationTokenProvider =
                new AzureDelegationTokenProvider("abfs", configuration);
        ObtainedSecurityToken obtainedSecurityToken =
                azureDelegationTokenProvider.obtainSecurityToken();
        byte[] token = obtainedSecurityToken.getToken();
        Credentials credentials = CredentialsJsonSerde.fromJson(token);
        assertThat(credentials.getAccessKeyId()).isEqualTo("null");
        assertThat(credentials.getSecretAccessKey()).isEqualTo("null");
        assertThat(credentials.getSecurityToken()).isEqualTo("token");
    }

    @AfterAll
    static void tearDown() {
        mockAuthServer.close();
    }
}
