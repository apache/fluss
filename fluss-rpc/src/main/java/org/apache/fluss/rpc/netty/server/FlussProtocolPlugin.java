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
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Build-in protocol plugin for Fluss. */
public class FlussProtocolPlugin implements NetworkProtocolPlugin, ServerReconfigurable {

    private static final String PLAIN_CREDENTIALS_CONFIG =
            ConfigOptions.SERVER_SASL_CREDENTIALS.key();

    /**
     * Pattern matching {@code <prefix><name>="<value>"} option entries in JAAS config strings,
     * scoped to the {@code user_} and {@code impersonate_} prefixes so only options consumed by the
     * SASL/PLAIN callback handler are extracted; the login module class and control flag are
     * ignored. These prefixes must cover PlainServerCallbackHandler.KNOWN_OPTION_PREFIXES, which a
     * unit test enforces.
     */
    private static final Pattern JAAS_OPTION_PATTERN =
            Pattern.compile("((?:user_|impersonate_)\\w+)=\"([^\"]*)\"");

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

    /**
     * All recognized JAAS options parsed from the initial {@code security.sasl.plain.jaas.config},
     * keyed by full option key. Preserved when the JAAS config is regenerated from the credentials
     * map so non-credential options such as {@code impersonate_*} are not dropped.
     */
    private Map<String, String> initialKnownOptionsFromJaasConfig;

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
        String initialJaasConfig = conf.getString(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG);
        this.initialKnownOptionsFromJaasConfig = parseKnownOptionsFromJaasConfig(initialJaasConfig);
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
        generateMergedJaasConfig(initialKnownOptionsFromJaasConfig, newCredentials);
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        enrichWithJaasConfig(newConfig);
    }

    /**
     * Enriches the plugin's configuration with a generated JAAS config string by merging:
     *
     * <ol>
     *   <li>All recognized options parsed from the initial {@code security.sasl.plain.jaas.config}
     *       (including non-credential options such as {@code impersonate_*})
     *   <li>New credentials from the {@code security.sasl.plain.credentials} map in {@code
     *       newConfig}
     * </ol>
     *
     * <p>New credentials take priority when a username exists in both sources. If the credentials
     * map is not present in {@code newConfig}, the configuration is returned unchanged.
     */
    private void enrichWithJaasConfig(Configuration newConfig) throws ConfigException {
        Map<String, String> newCredentials = readPlainCredentials(newConfig);
        if (Objects.equals(newCredentials, currentPlainCredentials)) {
            return;
        }

        conf.setString(
                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG,
                generateMergedJaasConfig(initialKnownOptionsFromJaasConfig, newCredentials));
        currentPlainCredentials = newCredentials;
    }

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

    /**
     * Generates the merged JAAS config string from the options parsed out of the initial {@code
     * security.sasl.plain.jaas.config} and the given credentials map. Credentials take priority on
     * username conflict; non-credential options such as {@code impersonate_*} are kept unchanged.
     */
    static String generateMergedJaasConfig(
            Map<String, String> initialKnownOptions, Map<String, String> newCredentials) {
        Map<String, String> mergedOptions = new LinkedHashMap<>(initialKnownOptions);
        if (newCredentials != null) {
            for (Map.Entry<String, String> credential : newCredentials.entrySet()) {
                mergedOptions.put("user_" + credential.getKey(), credential.getValue());
            }
        }

        StringBuilder sb =
                new StringBuilder(
                        "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required");
        for (Map.Entry<String, String> entry : mergedOptions.entrySet()) {
            sb.append(String.format(" %s=\"%s\"", entry.getKey(), entry.getValue()));
        }
        sb.append(";");
        return sb.toString();
    }

    /**
     * Parses the {@code user_*} and {@code impersonate_*} options from the given JAAS config
     * string, keyed by full option key. Only these options are retained across regeneration.
     */
    static Map<String, String> parseKnownOptionsFromJaasConfig(String jaasConfig) {
        Map<String, String> knownOptions = new LinkedHashMap<>();
        if (jaasConfig == null) {
            return knownOptions;
        }
        Matcher matcher = JAAS_OPTION_PATTERN.matcher(jaasConfig);
        while (matcher.find()) {
            knownOptions.put(matcher.group(1), matcher.group(2));
        }
        return knownOptions;
    }
}
