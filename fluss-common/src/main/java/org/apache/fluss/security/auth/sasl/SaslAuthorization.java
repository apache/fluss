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

package org.apache.fluss.security.auth.sasl;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Immutable proxy grants for SASL authorization identities. */
@Internal
public final class SaslAuthorization {
    private static final Logger LOG = LoggerFactory.getLogger(SaslAuthorization.class);
    private static final String REGEX_PREFIX = "regex:";

    private final Map<String, List<Pattern>> allowedTargetUsers;
    private final Set<FlussPrincipal> superUsers;
    private final boolean principalIgnoreCase;

    /** Compiles and validates the configured proxy grants. */
    public SaslAuthorization(Configuration configuration) {
        this.allowedTargetUsers = parseProxyUsers(configuration);
        this.superUsers =
                configuration
                        .getOptional(ConfigOptions.SUPER_USERS)
                        .map(FlussPrincipal::parsePrincipals)
                        .orElse(Collections.emptySet());
        this.principalIgnoreCase =
                configuration.get(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE);
    }

    /** Returns the effective identity after checking the authenticated account's proxy grants. */
    public String authorize(String authenticatedUser, String requestedUser) {
        if (requestedUser.isEmpty() || requestedUser.equals(authenticatedUser)) {
            return authenticatedUser;
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
        LOG.info(
                "User '{}' authenticated and is acting as '{}'",
                escapeForLog(authenticatedUser),
                escapeForLog(requestedUser));
        return requestedUser;
    }

    private boolean mayActAs(String authenticatedUser, String requestedUser) {
        List<Pattern> allowed = allowedTargetUsers.get(authenticatedUser);
        return allowed != null
                && allowed.stream().anyMatch(pattern -> pattern.matcher(requestedUser).matches());
    }

    private static String escapeForLog(String value) {
        return value.replace("\r", "\\r").replace("\n", "\\n");
    }

    private static Map<String, List<Pattern>> parseProxyUsers(Configuration configuration) {
        Map<String, List<Pattern>> allowedTargetUsers = new HashMap<>();
        Map<String, String> proxyUsers =
                configuration
                        .getOptional(ConfigOptions.SERVER_SASL_PROXY_USERS)
                        .orElse(Collections.emptyMap());
        for (Map.Entry<String, String> entry : proxyUsers.entrySet()) {
            List<Pattern> patterns = new ArrayList<>();
            for (String target : splitOnSemicolon(entry.getValue())) {
                patterns.add(compileGrant(entry.getKey(), target));
            }
            allowedTargetUsers.put(entry.getKey().trim(), Collections.unmodifiableList(patterns));
        }
        return Collections.unmodifiableMap(allowedTargetUsers);
    }

    private static Pattern compileGrant(String proxyUser, String grant) {
        String regex;
        if ("*".equals(grant)) {
            regex = "(?s:.*)";
        } else if (grant.startsWith(REGEX_PREFIX)) {
            regex = grant.substring(REGEX_PREFIX.length());
        } else {
            regex = Pattern.quote(grant);
        }
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new ConfigException(
                    String.format(
                            "Invalid rule '%s' for proxy user '%s' in '%s': %s",
                            grant,
                            proxyUser,
                            ConfigOptions.SERVER_SASL_PROXY_USERS.key(),
                            e.getDescription()),
                    e);
        }
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
