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

package org.apache.fluss.kafka.security;

import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.kafka.TestingSaslAuthenticator;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests authentication admission, principal establishment, and authenticator cleanup. */
class KafkaSaslConnectionTest {

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testAuthenticationGatesRequestsAndReleasesAuthenticator(short version) {
        TestingSaslAuthenticator authenticator = new TestingSaslAuthenticator();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        assertThat(connection.isRequestAllowed(ApiKeys.API_VERSIONS)).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.PRODUCE)).isFalse();

        connection.beginAuthentication("PLAIN", "KAFKA", null, version);
        assertThat(connection.isAuthenticatingWithRawTokens()).isEqualTo(version == 0);
        assertThat(connection.isRequestAllowed(ApiKeys.PRODUCE)).isFalse();
        assertThat(connection.authenticate(new byte[0])).isEmpty();

        assertThat(connection.isReady()).isTrue();
        assertThat(connection.principal()).isEqualTo(new FlussPrincipal("alice", "User"));
        assertThat(connection.isRequestAllowed(ApiKeys.PRODUCE)).isTrue();
        assertThat(authenticator.closed).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFailedAuthenticationReleasesAuthenticator(boolean failInitialize) {
        TestingSaslAuthenticator authenticator =
                new TestingSaslAuthenticator(
                        FlussPrincipal.ANONYMOUS, failInitialize, !failInitialize);
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        assertThatThrownBy(
                        () -> {
                            connection.beginAuthentication("PLAIN", "KAFKA", null);
                            connection.authenticate(new byte[0]);
                        })
                .isInstanceOf(AuthenticationException.class);
        assertThat(connection.shouldClose()).isTrue();
        assertThat(connection.principal()).isEqualTo(FlussPrincipal.ANONYMOUS);
        assertThat(connection.isRequestAllowed(ApiKeys.PRODUCE)).isFalse();
        assertThat(authenticator.closed).isTrue();
    }

    @Test
    void testDisconnectReleasesPendingAuthenticator() {
        TestingSaslAuthenticator authenticator = new TestingSaslAuthenticator();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        connection.beginAuthentication("PLAIN", "KAFKA", null);
        connection.close();
        assertThat(authenticator.closed).isTrue();
        assertThat(connection.shouldClose()).isTrue();
        assertThat(connection.isRequestAllowed(ApiKeys.SASL_AUTHENTICATE)).isFalse();
    }
}
