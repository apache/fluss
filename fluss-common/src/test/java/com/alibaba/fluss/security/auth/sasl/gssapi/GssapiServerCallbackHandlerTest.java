/*
 *  Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.security.auth.sasl.gssapi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link GssapiServerCallbackHandler}. */
public class GssapiServerCallbackHandlerTest {

    private GssapiServerCallbackHandler handler;

    @BeforeEach
    public void setUp() {
        this.handler = new GssapiServerCallbackHandler();
    }

    @Test
    public void testSuccessfulAuthorization() throws IOException, UnsupportedCallbackException {
        String principal = "user-a@FLUSS.COM";
        AuthorizeCallback cb = new AuthorizeCallback(principal, principal);

        handler.handle(new Callback[] {cb});

        assertThat(cb.isAuthorized()).isTrue();
        assertThat(cb.getAuthorizedID()).isEqualTo(principal);
    }

    @Test
    public void testFailedAuthorizationOnIdMismatch()
            throws IOException, UnsupportedCallbackException {
        AuthorizeCallback cb = new AuthorizeCallback("user-a@FLUSS.COM", "user-b@FLUSS.COM");

        handler.handle(new Callback[] {cb});
        assertThat(cb.isAuthorized()).isFalse();
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void testInvalidAuthenticationIdThrowsException(String invalidAuthenticationId) {
        AuthorizeCallback cb = new AuthorizeCallback(invalidAuthenticationId, "user-b@FLUSS.COM");
        assertThatThrownBy(() -> handler.handle(new Callback[] {cb}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Authentication ID cannot be null or empty");
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void testInvalidAuthorizationIdThrowsException(String invalidAuthorizationId) {
        AuthorizeCallback cb = new AuthorizeCallback("user-a@FLUSS.com", invalidAuthorizationId);
        assertThatThrownBy(() -> handler.handle(new Callback[] {cb}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Authorization ID cannot be null or empty");
    }

    @Test
    public void testUnsupportedCallbackThrowsException() {
        Callback unsupportedCallback = new javax.security.auth.callback.NameCallback("unsupported");
        assertThatThrownBy(() -> handler.handle(new Callback[] {unsupportedCallback}))
                .isInstanceOf(UnsupportedCallbackException.class);
    }
}
