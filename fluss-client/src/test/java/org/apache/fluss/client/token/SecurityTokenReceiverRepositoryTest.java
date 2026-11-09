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

package org.apache.fluss.client.token;

import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SecurityTokenReceiverRepository}. */
class SecurityTokenReceiverRepositoryTest {

    @Test
    void testFallbackWithoutPrimaryReceiver() {
        RecordingSecurityTokenReceiver fallback =
                new RecordingSecurityTokenReceiver("oss", Collections.singleton("ossj"));
        SecurityTokenReceiverRepository repository =
                new SecurityTokenReceiverRepository(Collections.singletonList(fallback));
        ObtainedSecurityToken token = token("ossj");

        repository.onNewTokensObtained(token);

        assertThat(fallback.receivedToken).isSameAs(token);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPrimaryReceiverTakesPrecedenceOverFallback(boolean primaryFirst) {
        RecordingSecurityTokenReceiver primary =
                new RecordingSecurityTokenReceiver("ossj", Collections.emptySet());
        RecordingSecurityTokenReceiver fallback =
                new RecordingSecurityTokenReceiver("oss", Collections.singleton("ossj"));
        SecurityTokenReceiverRepository repository =
                new SecurityTokenReceiverRepository(
                        primaryFirst
                                ? Arrays.asList(primary, fallback)
                                : Arrays.asList(fallback, primary));
        ObtainedSecurityToken token = token("ossj");

        repository.onNewTokensObtained(token);

        assertThat(primary.receivedToken).isSameAs(token);
        assertThat(fallback.receivedToken).isNull();
        ObtainedSecurityToken ossToken = token("oss");
        repository.onNewTokensObtained(ossToken);
        assertThat(fallback.receivedToken).isSameAs(ossToken);
    }

    @Test
    void testReceiverWithoutFallbackSchemes() {
        SecurityTokenReceiver receiver = new TestingSecurityTokenReceiver();
        assertThat(receiver.getFallbackSchemes()).isEmpty();
        SecurityTokenReceiverRepository repository =
                new SecurityTokenReceiverRepository(Collections.singletonList(receiver));

        assertThatCode(() -> repository.onNewTokensObtained(token("testing")))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> repository.onNewTokensObtained(token("ossj")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no receiver found");
    }

    @Test
    void testDuplicatePrimaryReceiversRejected() {
        assertThatThrownBy(
                        () ->
                                new SecurityTokenReceiverRepository(
                                        Arrays.asList(
                                                new TestingSecurityTokenReceiver(),
                                                new TestingSecurityTokenReceiver())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("testing has multiple implementations");
    }

    private static ObtainedSecurityToken token(String scheme) {
        return new ObtainedSecurityToken(scheme, new byte[] {1}, null, Collections.emptyMap());
    }

    private static class RecordingSecurityTokenReceiver extends TestingSecurityTokenReceiver {

        private final String scheme;
        private final Set<String> fallbackSchemes;
        private ObtainedSecurityToken receivedToken;

        private RecordingSecurityTokenReceiver(String scheme, Set<String> fallbackSchemes) {
            this.scheme = scheme;
            this.fallbackSchemes = fallbackSchemes;
        }

        @Override
        public String scheme() {
            return scheme;
        }

        @Override
        public Set<String> getFallbackSchemes() {
            return fallbackSchemes;
        }

        @Override
        public void onNewTokensObtained(ObtainedSecurityToken token) {
            receivedToken = token;
        }
    }
}
