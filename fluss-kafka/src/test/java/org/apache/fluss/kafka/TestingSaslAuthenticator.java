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

package org.apache.fluss.kafka;

import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;

/** Controllable authenticator for failure cleanup and request identity tests. */
public class TestingSaslAuthenticator implements ServerAuthenticator {
    private final FlussPrincipal principal;
    private final boolean failInitialize;
    private final boolean failAuthentication;
    public boolean closed;
    private boolean evaluated;

    /** Creates a successful authenticator for alice. */
    public TestingSaslAuthenticator() {
        this(new FlussPrincipal("alice", "User"), false, false);
    }

    /** Creates an authenticator with an explicit identity and configurable failure points. */
    public TestingSaslAuthenticator(
            FlussPrincipal principal, boolean failInitialize, boolean failAuthentication) {
        this.principal = principal;
        this.failInitialize = failInitialize;
        this.failAuthentication = failAuthentication;
    }

    @Override
    public String protocol() {
        return "sasl";
    }

    @Override
    public void initialize(AuthenticateContext context) {
        if (failInitialize) {
            throw failure();
        }
    }

    @Override
    public byte[] evaluateResponse(byte[] token) {
        evaluated = true;
        if (failAuthentication) {
            throw failure();
        }
        return new byte[0];
    }

    @Override
    public boolean isCompleted() {
        return evaluated && !failAuthentication;
    }

    @Override
    public FlussPrincipal createPrincipal() {
        return principal;
    }

    @Override
    public void close() {
        closed = true;
    }

    private static AuthenticationException failure() {
        return new AuthenticationException(
                "password-secret", new IllegalStateException("cause-secret"));
    }
}
