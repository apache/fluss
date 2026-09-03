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
import org.apache.fluss.exception.ConfigException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
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

    // Access is guarded by synchronized validate/reconfigure calls.
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
        this.initialPlainCredentialsFromJaasConfig = parseCredentialsFromJaasConfig(configuration);
        validate(configuration);
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
            return copyCredentials(configuration.get(ConfigOptions.SERVER_SASL_CREDENTIALS));
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new ConfigException(
                    String.format(
                            "Failed to parse %s: %s", PLAIN_CREDENTIALS_CONFIG, e.getMessage()),
                    e);
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

    private String generateMergedJaasConfig(Map<String, String> newCredentials) {
        Map<String, String> mergedCredentials =
                new LinkedHashMap<>(initialPlainCredentialsFromJaasConfig);
        if (newCredentials != null) {
            mergedCredentials.putAll(newCredentials);
        }

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
