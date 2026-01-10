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

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AzureDelegationTokenProvider}. */
public class AzureDelegationTokenProviderTest {

    private static final String CONFIG_PREFIX = "fs.azure.account.oauth2.client";
    private static final String CLIENT_ID = "testClientId";
    private static final String CLIENT_SECRET = "testClientSecret";

    private static final String ENDPOINT_KEY = "http://localhost:8080";

    private static MockAuthServer mockGSServer;

    @BeforeAll
    static void setup() {
        mockGSServer = MockAuthServer.create();
    }

    @Test
    void obtainSecurityTokenShouldReturnSecurityToken() {
        Configuration configuration = new Configuration();
        configuration.set(CONFIG_PREFIX + ".id", CLIENT_ID);
        configuration.set(CONFIG_PREFIX + ".secret", CLIENT_SECRET);
        configuration.set(CONFIG_PREFIX + ".endpoint", ENDPOINT_KEY);
        AzureDelegationTokenProvider azureDelegationTokenProvider =
                new AzureDelegationTokenProvider("abfs", configuration);
        ObtainedSecurityToken obtainedSecurityToken =
                azureDelegationTokenProvider.obtainSecurityToken();
        byte[] token = obtainedSecurityToken.getToken();
        Credentials credentials = CredentialsJsonSerde.fromJson(token);
        assertThat(credentials.getSecurityToken()).isEqualTo("token");
    }

    @AfterAll
    static void tearDown() {
        mockGSServer.close();
    }
}
