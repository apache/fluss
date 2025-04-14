/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.security.auth;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.security.acl.FlussPrincipal;

import javax.annotation.Nullable;

/** A {@link AuthenticationPlugin} that is used to test authentication plugin identifier. */
public class TestIdentifierAuthenticationPlugin
        implements ClientAuthenticationPlugin, ServerAuthenticationPlugin {

    public String authProtocol() {
        return "SSL_TEST";
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        return new TestIdentifierServerAuthenticator();
    }

    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        return new TestIdentifierClientAuthenticator();
    }

    /** A {@link ClientAuthenticator} that is used to test authentication plugin identifier. */
    public static class TestIdentifierClientAuthenticator implements ClientAuthenticator {

        @Override
        public String protocol() {
            return "SSL_TEST";
        }

        @Nullable
        @Override
        public byte[] authenticate(byte[] data) throws AuthenticationException {
            return null;
        }

        @Override
        public boolean isCompleted() {
            return true;
        }
    }

    /** A {@link ServerAuthenticator} that is used to test authentication plugin identifier. */
    public static class TestIdentifierServerAuthenticator implements ServerAuthenticator {
        @Override
        public String protocol() {
            return "SSL_TEST";
        }

        @Override
        public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
            return null;
        }

        @Override
        public FlussPrincipal createPrincipal() {
            return null;
        }

        @Override
        public boolean isCompleted() {
            return true;
        }
    }
}
