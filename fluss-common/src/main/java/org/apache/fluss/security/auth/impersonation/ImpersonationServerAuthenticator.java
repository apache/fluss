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

package org.apache.fluss.security.auth.impersonation;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.sasl.authenticator.SaslServerAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A SASL server authenticator that lets an authenticated user act as another user.
 *
 * <p>Authentication itself is left to the SASL mechanism. Whether the authenticated user may act as
 * the user it requested is decided from {@link ConfigOptions#SERVER_IMPERSONATION_PROXY_USERS}, so
 * that this right is server-side configuration instead of something shipped with credentials.
 * Acting as a user listed in {@link ConfigOptions#SUPER_USERS} is always rejected, including
 * through the {@code *} wildcard.
 */
public class ImpersonationServerAuthenticator extends SaslServerAuthenticator {

    private static final Logger LOG =
            LoggerFactory.getLogger(ImpersonationServerAuthenticator.class);

    private static final String WILDCARD = "*";

    /** Maps a user to the users it may act as, or to {@link #WILDCARD} for any user. */
    private final Map<String, Set<String>> allowedTargetUsers;

    private final Set<String> superUserNames;

    private FlussPrincipal principal;

    public ImpersonationServerAuthenticator(Configuration configuration) {
        super(enableImpersonation(configuration));
        this.allowedTargetUsers = parseProxyUsers(configuration);
        this.superUserNames = parseSuperUserNames(configuration);
    }

    /** Lets the SASL mechanism report a requested authorization id instead of rejecting it. */
    private static Configuration enableImpersonation(Configuration configuration) {
        Configuration withImpersonation = new Configuration(configuration);
        withImpersonation.setString(PlainSaslServer.IMPERSONATION_ENABLED_PROP, "true");
        return withImpersonation;
    }

    @Override
    public String protocol() {
        return ImpersonationAuthenticationPlugin.IMPERSONATION_AUTH_PROTOCOL;
    }

    @Override
    public void initialize(AuthenticateContext context) {
        principal = null;
        super.initialize(context);
    }

    @Override
    public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
        byte[] challenge = super.evaluateResponse(token);
        if (super.isCompleted() && principal == null) {
            // Resolved once, so that an unauthorized request fails the connection instead of
            // every request made on it.
            principal = resolvePrincipal();
        }
        return challenge;
    }

    @Override
    public FlussPrincipal createPrincipal() {
        return principal;
    }

    private FlussPrincipal resolvePrincipal() {
        FlussPrincipal authenticated = super.createPrincipal();
        Object requested = negotiatedProperty(PlainSaslServer.REQUESTED_AUTHORIZATION_ID_PROP);
        if (requested == null) {
            return authenticated;
        }

        String authenticatedUser = authenticated.getName();
        String requestedUser = requested.toString();
        if (superUserNames.contains(requestedUser)) {
            throw new AuthenticationException(
                    String.format(
                            "Authentication failed: user '%s' is not allowed to act as '%s' because it is a super user",
                            authenticatedUser, requestedUser));
        }
        if (!mayActAs(authenticatedUser, requestedUser)) {
            throw new AuthenticationException(
                    String.format(
                            "Authentication failed: user '%s' is not allowed to act as '%s'",
                            authenticatedUser, requestedUser));
        }

        LOG.debug(
                "User '{}' authenticated and is acting as '{}'", authenticatedUser, requestedUser);
        return new FlussPrincipal(requestedUser, authenticated.getType());
    }

    private boolean mayActAs(String authenticatedUser, String requestedUser) {
        Set<String> allowed = allowedTargetUsers.get(authenticatedUser);
        return allowed != null && (allowed.contains(WILDCARD) || allowed.contains(requestedUser));
    }

    private static Map<String, Set<String>> parseProxyUsers(Configuration configuration) {
        Map<String, Set<String>> allowedTargetUsers = new HashMap<>();
        configuration
                .getOptional(ConfigOptions.SERVER_IMPERSONATION_PROXY_USERS)
                .orElse(Collections.emptyMap())
                .forEach(
                        (proxyUser, targetUsers) ->
                                allowedTargetUsers.put(
                                        proxyUser.trim(), splitOnSemicolon(targetUsers)));
        return allowedTargetUsers;
    }

    /** Collects super user names regardless of type, so the type can not be varied to bypass. */
    private static Set<String> parseSuperUserNames(Configuration configuration) {
        Set<String> names = new HashSet<>();
        String superUsers = configuration.getOptional(ConfigOptions.SUPER_USERS).orElse("");
        for (String superUser : splitOnSemicolon(superUsers)) {
            String[] typeAndName = superUser.split(":");
            if (typeAndName.length == 2) {
                names.add(typeAndName[1].trim());
            }
        }
        return names;
    }

    private static Set<String> splitOnSemicolon(String value) {
        Set<String> values = new HashSet<>();
        for (String part : value.split(";")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return values;
    }
}
