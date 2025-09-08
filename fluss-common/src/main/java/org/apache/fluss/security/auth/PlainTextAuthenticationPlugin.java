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

package org.apache.fluss.security.auth;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;

/** Authentication Plugin for PLAINTEXT which not need to do authentication. */
public class PlainTextAuthenticationPlugin
        implements ServerAuthenticationPlugin, ClientAuthenticationPlugin {
    private static final String AUTH_PROTOCOL = "PLAINTEXT";

    @Override
    public String authProtocol() {
        return AUTH_PROTOCOL;
    }

    @Override
    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        return new PlainTextClientAuthenticator();
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        return new PlainTextServerAuthenticator();
    }

    /** Client Authenticator for PLAINTEXT which not need to do authentication. */
    public static class PlainTextClientAuthenticator implements ClientAuthenticator {
        @Override
        public String protocol() {
            return AUTH_PROTOCOL;
        }

        @Override
        public byte[] authenticate(byte[] data) throws AuthenticationException {
            return null;
        }

        @Override
        public boolean isCompleted() {
            return true;
        }
    }

    /** Server Authenticator for PLAINTEXT which not need to do authentication. */
    public static class PlainTextServerAuthenticator implements ServerAuthenticator {

        @Override
        public String protocol() {
            return AUTH_PROTOCOL;
        }

        @Override
        public byte[] evaluateResponse(byte[] token) {
            return new byte[0];
        }

        @Override
        public FlussPrincipal createPrincipal() {
            return FlussPrincipal.ANONYMOUS;
        }

        @Override
        public boolean isCompleted() {
            return true;
        }
    }
}
