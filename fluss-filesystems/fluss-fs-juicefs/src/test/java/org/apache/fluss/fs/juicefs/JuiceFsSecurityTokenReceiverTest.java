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

package org.apache.fluss.fs.juicefs;

import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link JuiceFsSecurityTokenReceiver}.
 *
 * <p>These tests protect the client-side flow against the regression described in {@link
 * JuiceFsSecurityTokenReceiver}: without a receiver registered for the {@code "jfs"} scheme, the
 * placeholder token returned by {@link JuiceFsFileSystem#obtainSecurityToken()} triggers an {@code
 * IllegalStateException} inside {@code SecurityTokenReceiverRepository}, which {@code
 * DefaultSecurityTokenManager} translates into a periodic retry loop.
 */
class JuiceFsSecurityTokenReceiverTest {

    @Test
    void schemeShouldBeJfs() {
        assertThat(new JuiceFsSecurityTokenReceiver().scheme()).isEqualTo(JuiceFsPlugin.SCHEME);
    }

    @Test
    void onNewTokensObtainedShouldBeANoOpForPlaceholderToken() {
        JuiceFsSecurityTokenReceiver receiver = new JuiceFsSecurityTokenReceiver();
        // Same shape as the token produced by JuiceFsFileSystem#obtainSecurityToken().
        ObtainedSecurityToken placeholder =
                new ObtainedSecurityToken(
                        JuiceFsPlugin.SCHEME, new byte[0], null, Collections.emptyMap());

        assertThatCode(() -> receiver.onNewTokensObtained(placeholder)).doesNotThrowAnyException();
    }

    @Test
    void serviceLoaderShouldDiscoverJfsReceiver() {
        // Mirrors the discovery contract used by SecurityTokenReceiverRepository#loadReceivers().
        List<SecurityTokenReceiver> discovered = new ArrayList<>();
        ServiceLoader.load(
                        SecurityTokenReceiver.class, SecurityTokenReceiver.class.getClassLoader())
                .forEach(discovered::add);

        assertThat(discovered)
                .extracting(SecurityTokenReceiver::scheme)
                .contains(JuiceFsPlugin.SCHEME);
    }
}
