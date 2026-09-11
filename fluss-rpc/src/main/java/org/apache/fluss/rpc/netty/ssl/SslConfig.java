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

package org.apache.fluss.rpc.netty.ssl;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.Password;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.IllegalConfigurationException;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The parsed and validated TLS configuration for one side (server or client) of an RPC connection.
 *
 * <p>This is a thin, immutable holder over the {@code security.ssl.*} (server) and {@code
 * client.security.ssl.*} (client) {@link ConfigOptions}. Whether TLS is enabled at all is also
 * encoded here: the {@link #fromServerConfig} / {@link #fromClientConfig} factory methods return
 * {@link Optional#empty()} when TLS is not enabled (no listener listed in {@code
 * security.ssl.enabled.listeners}, resp. {@code client.security.ssl.enabled} being false), so
 * callers never have to consult the raw configuration to make that decision. Validation that does
 * not depend on the actual key material (e.g. "a keystore must be configured for a TLS server",
 * "every configured TLS protocol and cipher suite is one this JVM supports") happens in the same
 * factory methods so misconfiguration fails fast with a clear message, instead of surfacing later
 * as an engine-level error on every connection.
 *
 * <p>The per-listener client-certificate requirement <b>is</b> held here: a TLS listener whose
 * {@code security.protocol.map} entry is {@code mTLS} requires a client certificate, which the
 * server can only validate against an explicitly configured truststore — so such a listener without
 * {@code security.ssl.truststore.path} is rejected here rather than silently falling back to the
 * JVM default truststore. The server pipeline reads the requirement per listener via {@link
 * #requiresClientAuth(String)} instead of re-deriving it from the raw configuration.
 */
@Internal
public final class SslConfig {

    /**
     * The {@code security.protocol.map} authentication protocol that requires a client certificate.
     * Matched case-insensitively, as {@code AuthenticationFactory} matches authentication protocol
     * names.
     */
    private static final String MUTUAL_TLS_AUTH_PROTOCOL = "mTLS";

    /** Server-only: the listener names for which TLS is enabled (empty for a client config). */
    private final List<String> enabledListeners;

    /**
     * Server-only: the subset of {@link #enabledListeners} that requires a client certificate
     * (empty for a client config).
     */
    private final Set<String> clientAuthListeners;

    private final List<String> enabledProtocols;
    private final List<String> cipherSuites;

    @Nullable private final String keystorePath;
    @Nullable private final String keystorePassword;
    private final String keystoreType;
    @Nullable private final String keyPassword;

    @Nullable private final String truststorePath;
    @Nullable private final String truststorePassword;
    private final String truststoreType;

    /**
     * Client-only: the endpoint identification algorithm (empty disables hostname verification).
     */
    private final String endpointIdentificationAlgorithm;

    private SslConfig(
            List<String> enabledListeners,
            Set<String> clientAuthListeners,
            List<String> enabledProtocols,
            List<String> cipherSuites,
            @Nullable String keystorePath,
            @Nullable String keystorePassword,
            String keystoreType,
            @Nullable String keyPassword,
            @Nullable String truststorePath,
            @Nullable String truststorePassword,
            String truststoreType,
            String endpointIdentificationAlgorithm) {
        this.enabledListeners = enabledListeners;
        this.clientAuthListeners = clientAuthListeners;
        this.enabledProtocols = enabledProtocols;
        this.cipherSuites = cipherSuites;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.keystoreType = keystoreType;
        this.keyPassword = keyPassword;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
        this.truststoreType = truststoreType;
        this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
    }

    /**
     * Build and validate the server-side TLS configuration, or {@link Optional#empty()} when TLS is
     * not enabled for any listener (i.e. {@code security.ssl.enabled.listeners} is unset or empty).
     */
    public static Optional<SslConfig> fromServerConfig(Configuration conf) {
        List<String> enabledListeners =
                orEmpty(conf.get(ConfigOptions.SERVER_SSL_ENABLED_LISTENERS));
        if (enabledListeners.isEmpty()) {
            return Optional.empty();
        }

        String keystorePath = conf.getString(ConfigOptions.SERVER_SSL_KEYSTORE_PATH);
        if (keystorePath == null) {
            throw new IllegalConfigurationException(
                    "'%s' must be configured when any listener enables TLS via '%s'.",
                    ConfigOptions.SERVER_SSL_KEYSTORE_PATH.key(),
                    ConfigOptions.SERVER_SSL_ENABLED_LISTENERS.key());
        }

        Map<String, String> protocolMap = conf.get(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP);
        Set<String> clientAuthListeners =
                enabledListeners.stream()
                        .filter(
                                listener ->
                                        MUTUAL_TLS_AUTH_PROTOCOL.equalsIgnoreCase(
                                                protocolMap.get(listener)))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        String truststorePath = conf.getString(ConfigOptions.SERVER_SSL_TRUSTSTORE_PATH);
        if (!clientAuthListeners.isEmpty() && truststorePath == null) {
            throw new IllegalConfigurationException(
                    "'%s' must be configured to validate client certificates for the %s listener(s) %s. "
                            + "Without it the server would validate client certificates against the JVM "
                            + "default truststore, accepting any certificate issued by a public CA.",
                    ConfigOptions.SERVER_SSL_TRUSTSTORE_PATH.key(),
                    MUTUAL_TLS_AUTH_PROTOCOL,
                    clientAuthListeners);
        }

        List<String> enabledProtocols =
                orEmpty(conf.get(ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS));
        List<String> cipherSuites = orEmpty(conf.get(ConfigOptions.SERVER_SSL_CIPHER_SUITES));
        validateProtocolsAndCipherSuites(
                enabledProtocols,
                ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS,
                cipherSuites,
                ConfigOptions.SERVER_SSL_CIPHER_SUITES);

        return Optional.of(
                new SslConfig(
                        enabledListeners,
                        clientAuthListeners,
                        enabledProtocols,
                        cipherSuites,
                        keystorePath,
                        password(conf.get(ConfigOptions.SERVER_SSL_KEYSTORE_PASSWORD)),
                        conf.getString(ConfigOptions.SERVER_SSL_KEYSTORE_TYPE),
                        password(conf.get(ConfigOptions.SERVER_SSL_KEY_PASSWORD)),
                        truststorePath,
                        password(conf.get(ConfigOptions.SERVER_SSL_TRUSTSTORE_PASSWORD)),
                        conf.getString(ConfigOptions.SERVER_SSL_TRUSTSTORE_TYPE),
                        ""));
    }

    /**
     * Build and validate the client-side TLS configuration, or {@link Optional#empty()} when TLS is
     * disabled (i.e. {@code client.security.ssl.enabled} is false).
     */
    public static Optional<SslConfig> fromClientConfig(Configuration conf) {
        if (!conf.get(ConfigOptions.CLIENT_SSL_ENABLED)) {
            return Optional.empty();
        }

        List<String> enabledProtocols =
                orEmpty(conf.get(ConfigOptions.CLIENT_SSL_ENABLED_PROTOCOLS));
        List<String> cipherSuites = orEmpty(conf.get(ConfigOptions.CLIENT_SSL_CIPHER_SUITES));
        validateProtocolsAndCipherSuites(
                enabledProtocols,
                ConfigOptions.CLIENT_SSL_ENABLED_PROTOCOLS,
                cipherSuites,
                ConfigOptions.CLIENT_SSL_CIPHER_SUITES);

        return Optional.of(
                new SslConfig(
                        Collections.emptyList(),
                        Collections.emptySet(),
                        enabledProtocols,
                        cipherSuites,
                        conf.getString(ConfigOptions.CLIENT_SSL_KEYSTORE_PATH),
                        password(conf.get(ConfigOptions.CLIENT_SSL_KEYSTORE_PASSWORD)),
                        conf.getString(ConfigOptions.CLIENT_SSL_KEYSTORE_TYPE),
                        password(conf.get(ConfigOptions.CLIENT_SSL_KEY_PASSWORD)),
                        conf.getString(ConfigOptions.CLIENT_SSL_TRUSTSTORE_PATH),
                        password(conf.get(ConfigOptions.CLIENT_SSL_TRUSTSTORE_PASSWORD)),
                        conf.getString(ConfigOptions.CLIENT_SSL_TRUSTSTORE_TYPE),
                        conf.getString(
                                ConfigOptions.CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM)));
    }

    /**
     * Reject protocol and cipher suite names this JVM does not support, and an empty protocol list.
     * Both are otherwise only caught when an {@link SSLEngine} is created, i.e. once per connection
     * and without naming the option at fault — and an empty protocol list is not caught at all: the
     * engine then comes up with no protocol enabled and every handshake fails.
     *
     * <p>Cipher suites are only checked against what the JVM supports, not against the enabled
     * protocols: which suites can actually be negotiated depends on the protocol version agreed
     * during the handshake, so pinning e.g. a TLS 1.3 suite alongside TLS 1.2 is a handshake
     * concern, not a configuration error. An empty cipher suite list is not an error either — it
     * selects the provider defaults.
     */
    private static void validateProtocolsAndCipherSuites(
            List<String> enabledProtocols,
            ConfigOption<List<String>> protocolsOption,
            List<String> cipherSuites,
            ConfigOption<List<String>> cipherSuitesOption) {
        if (enabledProtocols.isEmpty()) {
            throw new IllegalConfigurationException(
                    "'%s' must list at least one TLS protocol.", protocolsOption.key());
        }

        SSLEngine probe = supportedAlgorithmsProbe();

        List<String> unsupportedProtocols =
                unsupported(enabledProtocols, probe.getSupportedProtocols());
        if (!unsupportedProtocols.isEmpty()) {
            throw new IllegalConfigurationException(
                    "'%s' contains TLS protocol(s) not supported by this JVM: %s. Supported: %s.",
                    protocolsOption.key(),
                    unsupportedProtocols,
                    Arrays.asList(probe.getSupportedProtocols()));
        }

        List<String> unsupportedCipherSuites =
                unsupported(cipherSuites, probe.getSupportedCipherSuites());
        if (!unsupportedCipherSuites.isEmpty()) {
            throw new IllegalConfigurationException(
                    "'%s' contains cipher suite(s) not supported by this JVM: %s. Supported: %s.",
                    cipherSuitesOption.key(),
                    unsupportedCipherSuites,
                    Arrays.asList(probe.getSupportedCipherSuites()));
        }
    }

    private static List<String> unsupported(List<String> configured, String[] supported) {
        Set<String> supportedSet = new HashSet<>(Arrays.asList(supported));
        return configured.stream()
                .filter(value -> !supportedSet.contains(value))
                .collect(Collectors.toList());
    }

    /**
     * An engine off the default JSSE context, used only for the protocol and cipher suite names it
     * reports as supported. Those come from the security provider, so they do not depend on the
     * configured key material.
     */
    private static SSLEngine supportedAlgorithmsProbe() {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, null, null);
            return context.createSSLEngine();
        } catch (GeneralSecurityException e) {
            throw new FlussRuntimeException(
                    "Failed to determine the TLS protocols and cipher suites supported by this JVM.",
                    e);
        }
    }

    @Nullable
    private static String password(@Nullable Password password) {
        return password == null ? null : password.value();
    }

    private static List<String> orEmpty(@Nullable List<String> list) {
        return list == null ? Collections.emptyList() : list;
    }

    /**
     * Server-only: the listener names for which TLS is enabled, as configured via {@code
     * security.ssl.enabled.listeners}. Never empty for a server-side config (an empty list means
     * TLS is off and {@link #fromServerConfig} returns no config at all); always empty for a
     * client-side config.
     */
    public List<String> enabledListeners() {
        return enabledListeners;
    }

    /**
     * Server-only: the TLS listeners that require a client certificate, i.e. those whose {@code
     * security.protocol.map} entry is {@code mTLS}. Always empty for a client-side config.
     */
    public Set<String> clientAuthListeners() {
        return clientAuthListeners;
    }

    /**
     * Whether {@code listenerName} requires a client certificate during the TLS handshake. A
     * truststore is guaranteed to be configured when this returns true.
     */
    public boolean requiresClientAuth(String listenerName) {
        return clientAuthListeners.contains(listenerName);
    }

    public String[] enabledProtocols() {
        return enabledProtocols.toArray(new String[0]);
    }

    public List<String> cipherSuites() {
        return cipherSuites;
    }

    @Nullable
    public String keystorePath() {
        return keystorePath;
    }

    @Nullable
    public String keystorePassword() {
        return keystorePassword;
    }

    public String keystoreType() {
        return keystoreType;
    }

    /** The key password, falling back to the keystore password when not explicitly configured. */
    @Nullable
    public String keyPassword() {
        return keyPassword != null ? keyPassword : keystorePassword;
    }

    @Nullable
    public String truststorePath() {
        return truststorePath;
    }

    @Nullable
    public String truststorePassword() {
        return truststorePassword;
    }

    public String truststoreType() {
        return truststoreType;
    }

    public String endpointIdentificationAlgorithm() {
        return endpointIdentificationAlgorithm;
    }
}
