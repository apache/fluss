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

package org.apache.fluss.security.auth.sasl.gssapi;

import org.junit.jupiter.api.Test;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link GssapiServerCallbackHandler}. */
class GssapiServerCallbackHandlerTest {

    @Test
    void testAuthorization() throws Exception {
        GssapiServerCallbackHandler handler = new GssapiServerCallbackHandler();
        AuthorizeCallback callback = new AuthorizeCallback("client", "client");
        handler.handle(new Callback[] {callback});
        assertThat(callback.isAuthorized()).isTrue();
        assertThat(callback.getAuthorizedID()).isEqualTo("client");
    }

    @Test
    void testAuthorizationWithRealm() throws Exception {
        GssapiServerCallbackHandler handler = new GssapiServerCallbackHandler();
        AuthorizeCallback callback =
                new AuthorizeCallback("client@EXAMPLE.COM", "client@EXAMPLE.COM");
        handler.handle(new Callback[] {callback});
        assertThat(callback.isAuthorized()).isTrue();
        assertThat(callback.getAuthorizedID()).isEqualTo("client");
    }

    @Test
    void testAuthorizationIdMismatch() throws Exception {
        GssapiServerCallbackHandler handler = new GssapiServerCallbackHandler();
        AuthorizeCallback callback = new AuthorizeCallback("client", "other");
        handler.handle(new Callback[] {callback});
        assertThat(callback.isAuthorized()).isFalse();
    }

    @Test
    void testUnsupportedCallback() {
        GssapiServerCallbackHandler handler = new GssapiServerCallbackHandler();
        assertThatThrownBy(() -> handler.handle(new Callback[] {new RealmCallback("realm")}))
                .isInstanceOf(UnsupportedCallbackException.class);
    }
}
