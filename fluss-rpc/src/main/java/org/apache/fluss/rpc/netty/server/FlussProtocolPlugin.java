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
import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.ConfigOption;
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

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Build-in protocol plugin for Fluss. */
public class FlussProtocolPlugin implements NetworkProtocolPlugin, ServerReconfigurable {

    private static final ConfigOption<String> PLAIN_JAAS_CONFIG =
            ConfigBuilder.key("security.sasl.plain.jaas.config").stringType().noDefaultValue();
    private static final String PLAIN_CREDENTIALS_CONFIG =
            ConfigOptions.SERVER_SASL_CREDENTIALS.key();

    /** Pattern to match {@code user_<username>="<password>"} entries in JAAS config strings. */
    private static final Pattern JAAS_USER_PATTERN = Pattern.compile("user_(\\w+)=\"([^\"]*)\"");

    /**
     * Valid username pattern. Only letters, digits, and underscores are allowed because the
     * username is used as part of the JAAS option key {@code user_<username>}.
     */
    private static final Pattern VALID_USERNAME_PATTERN = Pattern.compile("\\w+");

    /**
     * Characters forbidden in passwords. These would break the comma-separated list format or the
     * generated JAAS config string: comma (list separator), double-quote (JAAS value delimiter),
     * semicolon (JAAS statement terminator), backslash (escape char), and control characters.
     */
    private static final Pattern INVALID_PASSWORD_PATTERN =
            Pattern.compile("[,\"\\\\;]|[\\x00-\\x1F\\x7F]");

    private final ApiManager apiManager;
    private final List<String> listeners;
    private final RequestsMetrics requestsMetrics;
    private Configuration conf;
    /** Initial credentials from `security.sasl.plain.jaas.config`. */
    private Map<String, String> initialPlainCredentialsFromJaasConfig;

    /** Current config `security.sasl.plain.credentials`. */
    private List<String> currentPlainCredentialEntries;

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
        this.initialPlainCredentialsFromJaasConfig = parseCredentialsFromJaasConfig(conf);
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
        List<String> credentialEntries = newConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS);
        if (Objects.equals(credentialEntries, currentPlainCredentialEntries)) {
            return;
        }
        Map<String, String> newCredentials = new LinkedHashMap<>();
        if (credentialEntries != null && !credentialEntries.isEmpty()) {
            for (int i = 0; i < credentialEntries.size(); i++) {
                String entry = credentialEntries.get(i).trim();
                int colonIdx = entry.indexOf(':');
                if (colonIdx <= 0 || colonIdx == entry.length() - 1) {
                    throw new ConfigException(
                            String.format(
                                    "%s must be in 'username:password' format, but got '%s'.",
                                    PLAIN_CREDENTIALS_CONFIG, entry));
                }
                String username = entry.substring(0, colonIdx);
                String password = entry.substring(colonIdx + 1);
                if (!VALID_USERNAME_PATTERN.matcher(username).matches()) {
                    throw new ConfigException(
                            String.format(
                                    "%s: username '%s' contains invalid characters. "
                                            + "Only letters, digits, and underscores are allowed.",
                                    PLAIN_CREDENTIALS_CONFIG, username));
                }
                if (INVALID_PASSWORD_PATTERN.matcher(password).find()) {
                    throw new ConfigException(
                            String.format(
                                    "%s[%d]: password for user '%s' contains invalid characters. "
                                            + "Commas, quotes, semicolons, backslashes, and control characters are not allowed.",
                                    PLAIN_CREDENTIALS_CONFIG, i, username));
                }
                if (newCredentials.containsKey(username)) {
                    throw new ConfigException(
                            PLAIN_CREDENTIALS_CONFIG
                                    + " must not contain duplicate usernames: '"
                                    + username
                                    + "'.");
                }
                newCredentials.put(username, password);
            }
        }

        // Generate the merged JAAS config value to ensure it is valid.
        generateMergedJaasConfig(newCredentials);
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        enrichWithJaasConfig(newConfig);
    }

    /**
     * Enriches the plugin's configuration with a generated JAAS config string by merging:
     *
     * <ol>
     *   <li>Existing credentials parsed from the current {@code security.sasl.plain.jaas.config}
     *   <li>New credentials from the {@code security.sasl.plain.credentials} list in {@code
     *       newConfig}
     * </ol>
     *
     * <p>New credentials take priority when a username exists in both sources. If the credentials
     * list is not present in {@code newConfig}, the configuration is returned unchanged.
     */
    private void enrichWithJaasConfig(Configuration newConfig) {
        List<String> credentialEntries = newConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS);
        if (Objects.equals(credentialEntries, currentPlainCredentialEntries)) {
            return;
        }
        Map<String, String> newCredentials = new LinkedHashMap<>();
        if (credentialEntries != null && !credentialEntries.isEmpty()) {
            for (String entry : credentialEntries) {
                int colonIdx = entry.indexOf(':');
                checkArgument(colonIdx > 0, "Invalid user entry format: '%s'", entry);
                newCredentials.put(entry.substring(0, colonIdx), entry.substring(colonIdx + 1));
            }
        }

        conf.setString(PLAIN_JAAS_CONFIG, generateMergedJaasConfig(newCredentials));
        currentPlainCredentialEntries = credentialEntries;
    }

    /**
     * Generates the merged JAAS config string by combining existing credentials from the current
     * {@code security.sasl.plain.jaas.config} with the given new credentials map. New credentials
     * take priority on username conflict.
     *
     * @param newCredentials ordered map of username → password from SERVER_SASL_CREDENTIALS
     * @return the generated JAAS config string
     */
    private String generateMergedJaasConfig(Map<String, String> newCredentials) {
        Map<String, String> mergedCredentials =
                new LinkedHashMap<>(initialPlainCredentialsFromJaasConfig);
        mergedCredentials.putAll(newCredentials);

        StringBuilder sb =
                new StringBuilder(
                        "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required");
        for (Map.Entry<String, String> entry : mergedCredentials.entrySet()) {
            sb.append(String.format(" user_%s=\"%s\"", entry.getKey(), entry.getValue()));
        }
        sb.append(";");
        return sb.toString();
    }

    private static Map<String, String> parseCredentialsFromJaasConfig(Configuration configuration) {
        Map<String, String> credentials = new LinkedHashMap<>();
        String existingJaas = configuration.getString(PLAIN_JAAS_CONFIG);
        if (existingJaas != null) {
            Matcher matcher = JAAS_USER_PATTERN.matcher(existingJaas);
            while (matcher.find()) {
                credentials.put(matcher.group(1), matcher.group(2));
            }
        }
        return credentials;
    }
}
