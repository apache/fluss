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

import org.apache.fluss.security.auth.sasl.jaas.AuthenticateCallbackHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.AuthorizeCallback;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A server-side {@link javax.security.auth.callback.CallbackHandler} implementation for the
 * SASL/GSSAPI (Kerberos) mechanism.
 *
 * <p>This handler does not perform credential validation (e.g., checking passwords or kerberos
 * tickets) itself. That responsibility is handled by the underlying JAVA GSSAPI library and the
 * JAAS configuration, which use the server's keytab to validate the clients' service tickets.
 *
 * <p>The primary role of this handler is to process the {@link AuthorizeCallback} , which is
 * invoked after successful authentication to determine if the authenticated principal is permitted
 * to act as the requested authorization identity.
 */
public class GssapiServerCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(GssapiServerCallbackHandler.class);

    /**
     * Configures this callback handler. For GSSAPI, this is often a no-op because the necessary
     * principal and keytab information is already in the JAAS configuration entries and is used
     * directly by the Krb5LoginModule.
     */
    @Override
    public void configure(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        LOG.debug("Configuring GssapiServerCallbackHandler for mechanism: {}", saslMechanism);
    }

    /** Handles the callbacks provided by the SASL/GSSAPI mechanism. */
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof AuthorizeCallback) {
                AuthorizeCallback ac = (AuthorizeCallback) callback;
                String authenticationId = ac.getAuthenticationID();
                String authorizationId = ac.getAuthorizationID();

                if (authenticationId == null || authenticationId.isEmpty()) {
                    throw new IOException("Authentication ID cannot be null or empty");
                }
                if (authorizationId == null || authorizationId.isEmpty()) {
                    throw new IOException("Authorization ID cannot be null or empty");
                }

                LOG.info(
                        "Authorizing client: authenticationID='{}', authorizationID='{}'",
                        authenticationId,
                        authorizationId);

                if (isAuthorizedActAs(authenticationId, authorizationId)) {
                    ac.setAuthorized(true);
                    ac.setAuthorizedID(authorizationId);
                    LOG.info("Successfully authorized client: {}", authorizationId);
                } else {
                    ac.setAuthorized(false);
                    LOG.warn(
                            "Authorization failed. Authenticated user '{}' is not authorized to act as '{}'",
                            authenticationId,
                            authorizationId);
                }
            } else {
                throw new UnsupportedCallbackException(
                        callback, "Unsupported callback type: " + callback.getClass().getName());
            }
        }
    }

    /** Checks if the authenticated user has the permission to act as the authorization user. */
    private boolean isAuthorizedActAs(String authnId, String authzId) {
        // Default policy: allow the authenticated user to act as themselves.
        boolean isSameUser = Objects.equals(authnId, authzId);

        return isSameUser;
    }
}
