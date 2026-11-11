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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.config.ConfigBuilder.key;

/** Immutable proxy authorization policy for a single SASL/PLAIN listener. */
@Internal
public final class PlainSaslProxyAuthorization {
    private static final Logger LOG = LoggerFactory.getLogger(PlainSaslProxyAuthorization.class);
    private static final String LISTENER_PREFIX = "security.sasl.listener.name.";
    private static final String PROXY_USERS_SUFFIX = ".plain.proxy-users";
    private static final PlainSaslProxyAuthorization DISABLED =
            new PlainSaslProxyAuthorization(
                    "", Collections.emptyMap(), Collections.emptySet(), false);

    private final String listenerName;
    private final Map<String, Set<String>> proxyUsers;
    private final Set<FlussPrincipal> superUsers;
    private final boolean principalIgnoreCase;

    private PlainSaslProxyAuthorization(
            String listenerName,
            Map<String, Set<String>> proxyUsers,
            Set<FlussPrincipal> superUsers,
            boolean principalIgnoreCase) {
        this.listenerName = listenerName;
        this.proxyUsers = proxyUsers;
        this.superUsers = superUsers;
        this.principalIgnoreCase = principalIgnoreCase;
    }

    /** Returns the proxy grants configuration option for a SASL/PLAIN listener. */
    public static ConfigOption<Map<String, String>> proxyUsersOption(String listenerName) {
        return proxyUsersOptionForKey(
                LISTENER_PREFIX + listenerName.toLowerCase(Locale.ROOT) + PROXY_USERS_SUFFIX);
    }

    /** Returns a policy that denies all proxy requests. */
    public static PlainSaslProxyAuthorization disabled() {
        return DISABLED;
    }

    /** Loads and validates the configured policies, keyed by the bound Fluss listener name. */
    public static Map<String, PlainSaslProxyAuthorization> loadForListeners(
            Configuration configuration, Collection<String> listeners) {
        Map<String, String> protocols =
                configuration.get(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP);
        Map<String, List<String>> listenersByKey = new HashMap<>();
        for (String listener : listeners) {
            if ("SASL".equalsIgnoreCase(protocols.get(listener))) {
                listenersByKey
                        .computeIfAbsent(
                                proxyUsersOption(listener).key(), ignored -> new ArrayList<>())
                        .add(listener);
            }
        }

        Set<FlussPrincipal> superUsers =
                configuration
                        .getOptional(ConfigOptions.SUPER_USERS)
                        .map(FlussPrincipal::parsePrincipals)
                        .map(Collections::unmodifiableSet)
                        .orElse(Collections.emptySet());
        boolean principalIgnoreCase =
                configuration.get(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE);
        Map<String, PlainSaslProxyAuthorization> policies = new HashMap<>();
        for (String configKey : configuration.keySet()) {
            if (!configKey.startsWith(LISTENER_PREFIX) || !configKey.endsWith(PROXY_USERS_SUFFIX)) {
                continue;
            }
            List<String> matchingListeners =
                    listenersByKey.getOrDefault(configKey, Collections.emptyList());
            if (matchingListeners.size() != 1) {
                throw new ConfigException(
                        String.format(
                                "Proxy grants in '%s' require exactly one matching SASL listener (found %d).",
                                configKey, matchingListeners.size()));
            }
            Map<String, String> configuredGrants =
                    readProxyGrants(configuration, proxyUsersOptionForKey(configKey));
            if (!configuredGrants.isEmpty()) {
                String listener = matchingListeners.get(0);
                policies.put(
                        listener,
                        new PlainSaslProxyAuthorization(
                                listener,
                                parseProxyGrants(configuredGrants, configKey),
                                superUsers,
                                principalIgnoreCase));
            }
        }
        return Collections.unmodifiableMap(policies);
    }

    void authorize(String authenticatedUser, String requestedUser) {
        Set<String> grants = proxyUsers.get(authenticatedUser);
        if (grants != null) {
            FlussPrincipal targetPrincipal = new FlussPrincipal(requestedUser, "User");
            for (FlussPrincipal superUser : superUsers) {
                if (superUser.matches(targetPrincipal, principalIgnoreCase)) {
                    throw new AuthenticationException(
                            denialMessage(authenticatedUser, requestedUser)
                                    + " because it is a super user");
                }
            }
            if (grants.contains("*") || grants.contains(requestedUser)) {
                LOG.info(
                        "User '{}' authenticated on listener '{}' and is acting as '{}'",
                        escapeForLog(authenticatedUser),
                        listenerName,
                        escapeForLog(requestedUser));
                return;
            }
        }
        throw new AuthenticationException(denialMessage(authenticatedUser, requestedUser));
    }

    private static Map<String, String> readProxyGrants(
            Configuration configuration, ConfigOption<Map<String, String>> option) {
        try {
            return configuration.get(option);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Failed to parse proxy grants in '" + option.key() + "'.", e);
        }
    }

    private static Map<String, Set<String>> parseProxyGrants(
            Map<String, String> configuredGrants, String configKey) {
        Map<String, Set<String>> grants = new HashMap<>();
        for (Map.Entry<String, String> entry : configuredGrants.entrySet()) {
            String source = entry.getKey();
            String targets = entry.getValue();
            if (source == null || source.trim().isEmpty() || targets == null || targets.isEmpty()) {
                throw new ConfigException(
                        "Proxy grants in '"
                                + configKey
                                + "' require a source and at least one target.");
            }
            Set<String> allowedTargets = new HashSet<>();
            for (String target : targets.split(";", -1)) {
                String allowedTarget = target.trim();
                if (allowedTarget.isEmpty()) {
                    throw new ConfigException(
                            "Empty proxy target for '" + source + "' in '" + configKey + "'.");
                }
                allowedTargets.add(allowedTarget);
            }
            grants.put(source, Collections.unmodifiableSet(allowedTargets));
        }
        return Collections.unmodifiableMap(grants);
    }

    private static ConfigOption<Map<String, String>> proxyUsersOptionForKey(String configKey) {
        return key(configKey).mapType().defaultValue(Collections.emptyMap());
    }

    private static String denialMessage(String authenticatedUser, String requestedUser) {
        return String.format(
                "Authentication failed: user '%s' is not allowed to act as '%s'",
                escapeForLog(authenticatedUser), escapeForLog(requestedUser));
    }

    private static String escapeForLog(String value) {
        return value.replace("\r", "\\r").replace("\n", "\\n");
    }
}
