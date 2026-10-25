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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.security.acl.FlussPrincipal;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Manages the effective server configuration for SASL/PLAIN authentication.
 *
 * <p>{@link ConfigOptions#SERVER_SASL_CREDENTIALS} is a convenient credential map, while the SASL
 * implementation consumes {@link ConfigOptions#SERVER_SASL_PLAIN_JAAS_CONFIG}. This manager
 * validates the credential map and converts it to a JAAS configuration. Credentials from the map
 * are merged with credentials in the initial JAAS configuration and take precedence when the same
 * username is present in both sources.
 *
 * <p>The managed {@link Configuration} has a stable identity so authenticator suppliers that
 * capture it during server startup see later credential updates. Callers must treat the returned
 * configuration as read-only and perform updates through this manager.
 */
@Internal
public final class PlainSaslServerConfigManager implements ServerReconfigurable {

    private static final String PLAIN_CREDENTIALS_CONFIG =
            ConfigOptions.SERVER_SASL_CREDENTIALS.key();

    /** Pattern to match {@code user_<username>="<password>"} entries in a JAAS config. */
    private static final Pattern JAAS_USER_PATTERN = Pattern.compile("user_(\\w+)=\"([^\"]*)\"");

    /** Usernames become JAAS option keys, so only word characters are accepted. */
    private static final Pattern VALID_USERNAME_PATTERN = Pattern.compile("\\w+");

    /** Characters that would break the credential-map syntax or generated JAAS statement. */
    private static final Pattern INVALID_PASSWORD_PATTERN =
            Pattern.compile("[,:\"\\\\;]|[\\x00-\\x1F\\x7F]");

    private final Map<String, String> initialPlainCredentialsFromJaasConfig;

    private final Configuration configuration;
    private final Set<FlussPrincipal> superUsers;
    private final boolean principalIgnoreCase;

    // Access is guarded by synchronized validate/reconfigure calls.
    @GuardedBy("this")
    private Map<String, String> currentPlainCredentials;

    /**
     * Creates a manager from the initial server configuration.
     *
     * @param configuration initial server configuration
     * @throws ConfigException if the configured credential map is invalid
     */
    public PlainSaslServerConfigManager(Configuration configuration) throws ConfigException {
        checkNotNull(configuration, "configuration must not be null");
        this.configuration = new Configuration(configuration);
        this.principalIgnoreCase =
                configuration.get(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE);
        this.superUsers = parseSuperUsers(configuration);
        this.initialPlainCredentialsFromJaasConfig = parseCredentialsFromJaasConfig(configuration);
        reconfigure(configuration);
    }

    /**
     * Returns the managed configuration containing the effective generated JAAS configuration.
     *
     * <p>The returned object has a stable identity and must be treated as read-only by callers.
     *
     * @return the managed effective configuration
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public synchronized void validate(Configuration newConfiguration) throws ConfigException {
        Map<String, String> newCredentials = readPlainCredentials(newConfiguration);
        if (Objects.equals(newCredentials, currentPlainCredentials)) {
            return;
        }

        if (newCredentials != null && !newCredentials.isEmpty()) {
            int index = 0;
            for (Map.Entry<String, String> credential : newCredentials.entrySet()) {
                validateUsername(credential.getKey());
                validatePassword(index, credential.getKey(), credential.getValue());
                index++;
            }
        }

        // Build the value during validation so reconfigure cannot fail after validation succeeds.
        generateMergedJaasConfig(newCredentials);
    }

    @Override
    public synchronized void validate(
            Configuration newConfiguration, @Nullable FlussPrincipal requester)
            throws ConfigException {
        authorizeSuperUserCredentialChanges(readPlainCredentials(newConfiguration), requester);
        validate(newConfiguration);
    }

    @Override
    public synchronized void reconfigure(Configuration newConfiguration) throws ConfigException {
        // DynamicServerConfig may continue to reconfigure other components after a validation
        // failure when it is applying a best-effort update. Defensively validate here as well so
        // malformed credentials can never be rendered into an effective JAAS statement.
        validate(newConfiguration);
        Map<String, String> newCredentials = readPlainCredentials(newConfiguration);
        if (Objects.equals(newCredentials, currentPlainCredentials)) {
            return;
        }

        configuration.setString(
                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG,
                generateMergedJaasConfig(newCredentials));
        currentPlainCredentials = copyCredentials(newCredentials);
    }

    private static Map<String, String> readPlainCredentials(Configuration configuration)
            throws ConfigException {
        try {
            return configuration.get(ConfigOptions.SERVER_SASL_CREDENTIALS);
        } catch (IllegalArgumentException | IllegalStateException e) {
            // Parsing errors, such as duplicate map keys, may include passwords in their
            // messages or causes. Do not retain them in an exception that can be logged.
            throw new ConfigException("Failed to parse " + PLAIN_CREDENTIALS_CONFIG + '.');
        }
    }

    private static Map<String, String> copyCredentials(Map<String, String> credentials) {
        return credentials == null ? null : new LinkedHashMap<>(credentials);
    }

    private static void validateUsername(String username) throws ConfigException {
        if (username == null || !VALID_USERNAME_PATTERN.matcher(username).matches()) {
            throw new ConfigException(
                    String.format(
                            "%s: username '%s' contains invalid characters. "
                                    + "Only letters, digits, and underscores are allowed.",
                            PLAIN_CREDENTIALS_CONFIG, username));
        }
    }

    private static void validatePassword(int index, String username, String password)
            throws ConfigException {
        if (password == null || password.isEmpty()) {
            throw new ConfigException(
                    String.format(
                            "%s[%d]: password for user '%s' must not be empty.",
                            PLAIN_CREDENTIALS_CONFIG, index, username));
        }
        if (INVALID_PASSWORD_PATTERN.matcher(password).find()) {
            throw new ConfigException(
                    String.format(
                            "%s[%d]: password for user '%s' contains invalid characters. "
                                    + "Commas, colons, quotes, semicolons, backslashes, and control characters are not allowed.",
                            PLAIN_CREDENTIALS_CONFIG, index, username));
        }
    }

    /**
     * Generates the merged JAAS config string by combining existing credentials from the current
     * {@code security.sasl.plain.jaas.config} with the given new credentials map. New credentials
     * take priority on username conflict.
     *
     * @param newCredentials map of username to password from SERVER_SASL_CREDENTIALS
     * @return the generated JAAS config string
     */
    private String generateMergedJaasConfig(Map<String, String> newCredentials) {
        Map<String, String> mergedCredentials = mergePlainCredentials(newCredentials);

        StringBuilder jaasConfig =
                new StringBuilder(PlainLoginModule.class.getName()).append(" required");
        for (Map.Entry<String, String> entry : mergedCredentials.entrySet()) {
            jaasConfig
                    .append(" user_")
                    .append(entry.getKey())
                    .append("=\"")
                    .append(entry.getValue())
                    .append('"');
        }
        return jaasConfig.append(';').toString();
    }

    private Map<String, String> mergePlainCredentials(Map<String, String> plainCredentials) {
        Map<String, String> mergedCredentials =
                new LinkedHashMap<>(initialPlainCredentialsFromJaasConfig);
        if (plainCredentials != null) {
            mergedCredentials.putAll(plainCredentials);
        }
        return mergedCredentials;
    }

    /**
     * Rejects the change if the requester is not a configured super user but the credentials of a
     * configured super user would be added, removed or modified.
     */
    private void authorizeSuperUserCredentialChanges(
            @Nullable Map<String, String> newCredentials, @Nullable FlussPrincipal requester) {
        if (requester == null || isSuperUser(requester)) {
            return;
        }

        if (!Objects.equals(
                superUserCredentials(currentPlainCredentials),
                superUserCredentials(newCredentials))) {
            throw new AuthorizationException(
                    String.format(
                            "Principal %s cannot modify credentials belonging to users in 'super.users', "
                                    + "the requester must itself be a super user.",
                            requester));
        }
    }

    /** Returns the merged credentials whose username matches the name of a configured superuser. */
    private Map<String, String> superUserCredentials(@Nullable Map<String, String> credentials) {
        Map<String, String> superUserCredentials = new HashMap<>();
        mergePlainCredentials(credentials)
                .forEach(
                        (user, password) -> {
                            if (isSuperUserName(user)) {
                                superUserCredentials.put(user, password);
                            }
                        });
        return superUserCredentials;
    }

    private boolean isSuperUser(FlussPrincipal principal) {
        return superUsers.stream()
                .anyMatch(superUser -> superUser.matches(principal, principalIgnoreCase));
    }

    private boolean isSuperUserName(String username) {
        return superUsers.stream()
                .anyMatch(
                        superUser ->
                                principalIgnoreCase
                                        ? username.equalsIgnoreCase(superUser.getName())
                                        : username.equals(superUser.getName()));
    }

    private static Set<FlussPrincipal> parseSuperUsers(Configuration configuration) {
        return configuration
                .getOptional(ConfigOptions.SUPER_USERS)
                .map(FlussPrincipal::parsePrincipals)
                .orElse(Collections.emptySet());
    }

    private static Map<String, String> parseCredentialsFromJaasConfig(Configuration configuration) {
        Map<String, String> credentials = new LinkedHashMap<>();
        String existingJaas = configuration.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG);
        if (existingJaas != null) {
            Matcher matcher = JAAS_USER_PATTERN.matcher(existingJaas);
            while (matcher.find()) {
                credentials.put(matcher.group(1), matcher.group(2));
            }
        }
        return credentials;
    }
}
