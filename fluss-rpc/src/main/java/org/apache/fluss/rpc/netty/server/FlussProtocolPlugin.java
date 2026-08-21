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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.PlainTextAuthenticationPlugin;
import org.apache.fluss.security.auth.sasl.jaas.JaasConfig;
import org.apache.fluss.security.auth.sasl.plain.PlainLoginModule;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** Build-in protocol plugin for Fluss. */
public class FlussProtocolPlugin implements NetworkProtocolPlugin, ServerReconfigurable {

    private static final String PLAIN_CREDENTIALS_CONFIG =
            ConfigOptions.SERVER_SASL_CREDENTIALS.key();
    private static final String PLAIN_JAAS_CONFIG =
            ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG.key();
    private static final String JAAS_CONTEXT_NAME = "FlussServer";

    /**
     * Valid username pattern. Only letters, digits, and underscores are allowed because the
     * username is used as part of the JAAS option key {@code user_<username>}.
     */
    private static final Pattern VALID_USERNAME_PATTERN = Pattern.compile("\\w+");

    /**
     * Characters forbidden in passwords. These would break the map format or the generated JAAS
     * config string: comma (entry separator), colon (key-value separator), double-quote (JAAS value
     * delimiter), semicolon (JAAS statement terminator), backslash (escape char), and control
     * characters.
     */
    private static final Pattern INVALID_PASSWORD_PATTERN =
            Pattern.compile("[,:\"\\\\;]|[\\x00-\\x1F\\x7F]");

    private final ApiManager apiManager;
    private final List<String> listeners;
    private final RequestsMetrics requestsMetrics;
    private Configuration conf;

    /** Startup JAAS baseline whose complete options are preserved during credential updates. */
    private PlainJaasBaseline initialPlainJaasBaseline;

    /** Current config `security.sasl.plain.credentials`. */
    private Map<String, String> currentPlainCredentials;

    public FlussProtocolPlugin(
            ServerType serverType, List<String> listeners, RequestsMetrics requestsMetrics) {
        this.apiManager = new ApiManager(serverType);
        this.listeners = listeners;
        this.requestsMetrics = requestsMetrics;
    }

    @Override
    public String name() {
        return FLUSS_PROTOCOL_NAME;
    }

    @Override
    public void setup(Configuration conf) {
        this.conf = new Configuration(conf);
        this.initialPlainJaasBaseline =
                new PlainJaasBaseline(conf.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG));
        enrichWithJaasConfig(conf);
    }

    @Override
    public List<String> listenerNames() {
        return listeners;
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels, String listenerName) {
        return new ServerChannelInitializer(
                requestChannels,
                apiManager,
                listenerName,
                listenerName.equals(conf.get(ConfigOptions.INTERNAL_LISTENER_NAME)),
                requestsMetrics,
                conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds(),
                (int) conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes(),
                Optional.ofNullable(
                                AuthenticationFactory.loadServerAuthenticatorSuppliers(this.conf)
                                        .get(listenerName))
                        .orElse(PlainTextAuthenticationPlugin.PlainTextServerAuthenticator::new));
    }

    @Override
    public RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        return new FlussRequestHandler(service);
    }

    // --- ServerReconfigurable ---

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        Map<String, String> newCredentials = readPlainCredentials(newConfig);
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

        // Generate the merged JAAS config value to ensure it is valid.
        initialPlainJaasBaseline.materialize(newCredentials);
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        enrichWithJaasConfig(newConfig);
    }

    /**
     * Updates the generated JAAS config from the immutable startup baseline and the current
     * credentials overlay. The existing configuration update path is intentionally retained.
     */
    private void enrichWithJaasConfig(Configuration newConfig) throws ConfigException {
        Map<String, String> newCredentials = readPlainCredentials(newConfig);
        if (Objects.equals(newCredentials, currentPlainCredentials)) {
            return;
        }

        conf.setString(
                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG,
                initialPlainJaasBaseline.materialize(newCredentials));
        currentPlainCredentials = newCredentials;
    }

    @Nullable
    private static Map<String, String> readPlainCredentials(Configuration config)
            throws ConfigException {
        try {
            return config.get(ConfigOptions.SERVER_SASL_CREDENTIALS);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new ConfigException(
                    String.format(
                            "Failed to parse %s: %s", PLAIN_CREDENTIALS_CONFIG, e.getMessage()),
                    e);
        }
    }

    private static void validateUsername(String username) throws ConfigException {
        if (!VALID_USERNAME_PATTERN.matcher(username).matches()) {
            throw new ConfigException(
                    String.format(
                            "%s: username '%s' contains invalid characters. "
                                    + "Only letters, digits, and underscores are allowed.",
                            PLAIN_CREDENTIALS_CONFIG, username));
        }
    }

    private static void validatePassword(int index, String username, String password)
            throws ConfigException {
        if (password == null || INVALID_PASSWORD_PATTERN.matcher(password).find()) {
            throw new ConfigException(
                    String.format(
                            "%s[%d]: password for user '%s' contains invalid characters. "
                                    + "Commas, colons, quotes, semicolons, backslashes, and control characters are not allowed.",
                            PLAIN_CREDENTIALS_CONFIG, index, username));
        }
    }

    /** Materializes a JAAS config from a raw startup baseline for testing. */
    static String materializePlainJaasConfig(
            @Nullable String startupJaasConfig, @Nullable Map<String, String> newCredentials) {
        return new PlainJaasBaseline(startupJaasConfig).materialize(newCredentials);
    }

    /** Parses the single JAAS entry used as the materialization baseline. */
    private static AppConfigurationEntry parseSingleJaasEntry(String jaasConfig) {
        final AppConfigurationEntry[] entries;
        try {
            entries =
                    new JaasConfig(JAAS_CONTEXT_NAME, jaasConfig)
                            .getAppConfigurationEntry(JAAS_CONTEXT_NAME);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                    "Failed to parse " + PLAIN_JAAS_CONFIG + ": " + e.getMessage(), e);
        }
        if (entries.length != 1) {
            throw new ConfigException(
                    String.format(
                            "%s contains %d JAAS modules, should be 1 module",
                            PLAIN_JAAS_CONFIG, entries.length));
        }
        return entries[0];
    }

    /** Immutable startup source used to rebuild effective PLAIN JAAS configurations. */
    private static final class PlainJaasBaseline {
        @Nullable private final String rawConfig;

        private PlainJaasBaseline(@Nullable String rawConfig) {
            this.rawConfig = rawConfig;
        }

        /**
         * Copies every option from the startup entry and overlays credentials as {@code user_*}
         * options. Removing the overlay restores the exact startup config. Without a startup
         * config, an empty PLAIN entry prevents fallback to JVM-wide JAAS credentials. When an
         * overlay is present, the generated entry uses the legacy canonical {@code PlainLoginModule
         * required} form.
         */
        private String materialize(@Nullable Map<String, String> credentials) {
            if (credentials == null && rawConfig != null && !rawConfig.isEmpty()) {
                return rawConfig;
            }

            Map<String, Object> mergedOptions = new LinkedHashMap<>();
            if (rawConfig != null && !rawConfig.isEmpty()) {
                mergedOptions.putAll(parseSingleJaasEntry(rawConfig).getOptions());
            }
            if (credentials != null) {
                for (Map.Entry<String, String> credential : credentials.entrySet()) {
                    mergedOptions.put("user_" + credential.getKey(), credential.getValue());
                }
            }

            StringBuilder sb =
                    new StringBuilder(PlainLoginModule.class.getName()).append(" required");
            for (Map.Entry<String, Object> option : mergedOptions.entrySet()) {
                sb.append(' ')
                        .append(quoteJaasToken(option.getKey()))
                        .append('=')
                        .append(quoteJaasToken(String.valueOf(option.getValue())));
            }
            return sb.append(';').toString();
        }
    }

    /**
     * Quotes a token for the {@link java.io.StreamTokenizer} grammar used by {@link JaasConfig}.
     */
    private static String quoteJaasToken(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (character == '\\' || character == '"') {
                escaped.append('\\').append(character);
            } else if (character < 0x20 || character == 0x7f) {
                // StreamTokenizer decodes up to three octal digits in quoted strings. Always
                // emitting three keeps a following octal digit from becoming part of the escape.
                escaped.append('\\');
                escaped.append((char) ('0' + ((character >> 6) & 0x7)));
                escaped.append((char) ('0' + ((character >> 3) & 0x7)));
                escaped.append((char) ('0' + (character & 0x7)));
            } else {
                escaped.append(character);
            }
        }
        return escaped.append('"').toString();
    }
}
