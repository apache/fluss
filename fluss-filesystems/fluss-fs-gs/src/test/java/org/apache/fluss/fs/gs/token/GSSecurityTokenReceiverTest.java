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

package org.apache.fluss.fs.gs.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.HashMap;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for distributing a short-lived Google Cloud Storage access token to clients. */
@ResourceLock("gs-security-token")
class GSSecurityTokenReceiverTest {

    @BeforeEach
    void clearToken() {
        GSSecurityTokenReceiver.reset();
    }

    @AfterEach
    void resetToken() {
        GSSecurityTokenReceiver.reset();
    }

    @Test
    void serviceLoaderFindsReceiver() {
        boolean found = false;
        for (SecurityTokenReceiver receiver : ServiceLoader.load(SecurityTokenReceiver.class)) {
            if (receiver instanceof GSSecurityTokenReceiver) {
                found = true;
                assertThat(receiver.scheme()).isEqualTo("gs");
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void receivedTokenIsInstalledAsAccessTokenProvider() throws Exception {
        ObtainedSecurityToken obtained =
                new ObtainedSecurityToken(
                        "gs",
                        CredentialsJsonSerde.toJson(new Credentials("", "", "access-token")),
                        1_700_000_000_000L,
                        new HashMap<String, String>());

        GSSecurityTokenReceiver receiver = new GSSecurityTokenReceiver();
        receiver.onNewTokensObtained(obtained);

        assertThat(GSSecurityTokenReceiver.getAccessToken()).isNotNull();
        Configuration configuration = new Configuration(false);
        GSSecurityTokenReceiver.updateHadoopConfig(configuration);
        assertThat(configuration.get(GSSecurityTokenReceiver.PROVIDER_CONFIG_NAME))
                .isEqualTo(GSAccessTokenProvider.NAME);

        GSAccessTokenProvider provider = new GSAccessTokenProvider();
        assertThat(provider.getAccessToken().getToken()).isEqualTo("access-token");
        assertThat(provider.getAccessToken().getExpirationTimeMilliSeconds())
                .isEqualTo(1_700_000_000_000L);
    }

    @Test
    void updateHadoopConfigFailsBeforeATokenArrives() {
        assertThatThrownBy(
                        () -> GSSecurityTokenReceiver.updateHadoopConfig(new Configuration(false)))
                .isInstanceOf(org.apache.fluss.exception.FlussRuntimeException.class)
                .hasMessageContaining("not set");
    }
}
