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
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.UserPasswordVerifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Authenticates a proxy account and authorizes its requested target using the impersonation
 * protocol. Password verification uses the shared credential store; token handling and proxy
 * authorization are independent of SASL.
 */
public class ImpersonationServerAuthenticator implements ServerAuthenticator {

    private static final Logger LOG =
            LoggerFactory.getLogger(ImpersonationServerAuthenticator.class);

    private static final String WILDCARD = "*";

    /** Maps a user to the users it may act as, or to {@link #WILDCARD} for any user. */
    private final Map<String, Set<String>> allowedTargetUsers;

    private final Set<FlussPrincipal> superUsers;

    private final boolean principalIgnoreCase;

    private final Configuration configuration;
    private UserPasswordVerifier passwordVerifier;
    private FlussPrincipal principal;

    /** Creates an authenticator using the configured proxy grants and super users. */
    public ImpersonationServerAuthenticator(Configuration configuration) {
        this.configuration = new Configuration(configuration);
        this.allowedTargetUsers = parseProxyUsers(configuration);
        this.superUsers =
                configuration
                        .getOptional(ConfigOptions.SUPER_USERS)
                        .map(FlussPrincipal::parsePrincipals)
                        .orElse(Collections.emptySet());
        this.principalIgnoreCase =
                configuration.get(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE);
    }

    @Override
    public String protocol() {
        return ImpersonationAuthenticationPlugin.IMPERSONATION_AUTH_PROTOCOL;
    }

    @Override
    public void initialize(AuthenticateContext context) {
        close();
        matchProtocol(context.protocol());
        passwordVerifier = UserPasswordVerifier.forServer(configuration, context.listenerName());
    }

    @Override
    public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
        checkState(passwordVerifier != null, "Authenticator has not been initialized");
        checkState(!isCompleted(), "Authentication exchange has already completed");
        ImpersonationToken request = ImpersonationToken.decode(token);
        if (request.username().isEmpty() || request.password().isEmpty()) {
            throw new AuthenticationException(
                    "Authentication failed: username and password must not be empty");
        }
        char[] password = request.password().toCharArray();
        try {
            if (!passwordVerifier.authenticate(request.username(), password)) {
                throw new AuthenticationException(
                        "Authentication failed: Invalid username or password");
            }
        } finally {
            Arrays.fill(password, (char) 0);
        }
        principal = resolvePrincipal(request.username(), request.targetUser());
        return new byte[0];
    }

    @Override
    public boolean isCompleted() {
        return principal != null;
    }

    @Override
    public FlussPrincipal createPrincipal() {
        checkState(isCompleted(), "Authentication exchange has not completed");
        return principal;
    }

    @Override
    public void close() {
        principal = null;
        passwordVerifier = null;
    }

    private FlussPrincipal resolvePrincipal(String authenticatedUser, String requestedUser) {
        if (requestedUser.isEmpty() || requestedUser.equals(authenticatedUser)) {
            return new FlussPrincipal(authenticatedUser, "User");
        }
        if (superUsers.stream()
                .anyMatch(superUser -> superUser.matchesName(requestedUser, principalIgnoreCase))) {
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
        LOG.info("User '{}' authenticated and is acting as '{}'", authenticatedUser, requestedUser);
        return new FlussPrincipal(requestedUser, "User");
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
