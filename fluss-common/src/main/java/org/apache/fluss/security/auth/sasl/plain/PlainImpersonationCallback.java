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

package org.apache.fluss.security.auth.sasl.plain;

import javax.security.auth.callback.Callback;

/**
 * A {@link Callback} used during SASL/PLAIN authentication to check whether an authenticated user
 * is allowed to impersonate the requested authorization id (RFC 4616 authzid).
 *
 * <p>It is issued by {@link PlainSaslServer} when the client requests an authorization id different
 * from the authenticated username, and answered by {@link PlainServerCallbackHandler} based on the
 * impersonation grants in the JAAS configuration.
 */
public class PlainImpersonationCallback implements Callback {

    private final String authenticatedUser;
    private final String requestedAuthorizationId;
    private boolean allowed;

    public PlainImpersonationCallback(String authenticatedUser, String requestedAuthorizationId) {
        this.authenticatedUser = authenticatedUser;
        this.requestedAuthorizationId = requestedAuthorizationId;
    }

    /** Returns the user that has been authenticated with its own credentials. */
    public String authenticatedUser() {
        return authenticatedUser;
    }

    /** Returns the authorization id the client requested to act as. */
    public String requestedAuthorizationId() {
        return requestedAuthorizationId;
    }

    /** Returns true if the impersonation is authorized, as set by the server callback handler. */
    public boolean allowed() {
        return allowed;
    }

    /** Sets whether the impersonation is authorized. */
    public void allowed(boolean allowed) {
        this.allowed = allowed;
    }
}
