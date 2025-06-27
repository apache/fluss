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

import com.alibaba.fluss.security.auth.sasl.jaas.AuthenticateCallbackHandler;

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
 * <p>The primary role of this handler is to process the {@linke AuthorizeCallback} , which is
 * invoked after successful authentication to determine if the authenticated principal is permitted
 * to act s the requested authorization identity.
 */
public class GssapiServerCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(GssapiServerCallbackHandler.class);

    /**
     * Configures this callback handler. For GSSAPI, this is often a no-op because the necessary
     * principal and keytab information is already in the JASS configuration entries and is used
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
                // get authenticated principal name,
                // which is the user who has been authenticated (e.g., "user-a@REALM")
                String authnId = ac.getAuthenticationID();
                // get requested authzId,
                // which is the principal name the user wants to act as (e.g., "user-b@REALM")
                String authzId = ac.getAuthorizationID();

                if (authnId == null || authnId.isEmpty()) {
                    throw new IOException("Authentication ID cannot be null or empty");
                }
                if (authzId == null || authzId.isEmpty()) {
                    throw new IOException("Authorization ID cannot be null or empty");
                }

                LOG.info(
                        "Authorizing client: authenticationID='{}', authorizationID='{}'",
                        authnId,
                        authzId);

                if (isAuthorizedActAs(authnId, authzId)) {
                    ac.setAuthorized(true);
                    ac.setAuthorizedID(authzId);
                    LOG.info("Successfully authorized client: {}", authnId);
                } else {
                    ac.setAuthorized(false);
                    LOG.warn(
                            "Authorization failed. Authenticated user '{}' is not authorized to act as '{}'",
                            authnId,
                            authzId);
                }
            } else {
                throw new UnsupportedCallbackException(
                        callback, "Unsupported callback type: " + callback.getClass().getName());
            }
        }
    }

    /**
     * Checks if the authenticated user (authnId) has the permission to act as the authorization
     * user (authnzId).
     */
    private boolean isAuthorizedActAs(String authnId, String authzId) {
        // Default policy: allow the authenticated user to act as themselves.
        boolean isSameUser = Objects.equals(authnId, authzId);

        // Future extension for impersonation/proxy logic.
        // For example,
        // boolean hasProxyPermission = checkProxyImpersonation(authnId, authzId);
        // return isSameUser || hasProxyPermission;

        return isSameUser;
    }
}
