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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.Password;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The parsed and validated TLS configuration for one side (server or client) of an RPC connection.
 *
 * <p>This is a thin, immutable holder over the {@code security.ssl.*} (server) and {@code
 * client.security.ssl.*} (client) {@link ConfigOptions}. Whether TLS is enabled at all is also
 * encoded here: the {@link #fromServerConfig} / {@link #fromClientConfig} factory methods return
 * {@link Optional#empty()} when TLS is not enabled (no listener listed in {@code
 * security.ssl.enabled.listeners}, resp. {@code client.security.ssl.enabled} being false), so
 * callers never have to consult the raw configuration to make that decision. Validation that does
 * not depend on the actual key material (e.g. "a keystore must be configured for a TLS server")
 * happens in the same factory methods so misconfiguration fails fast with a clear message.
 *
 * <p>The per-listener client-certificate requirement is <b>not</b> held here: it is derived per
 * listener from the listener's authentication protocol ({@code mTLS} ⇒ require) when the server
 * pipeline is wired. The server context is always built with a trust manager when a truststore is
 * configured, so any listener can request client certs.
 */
@Internal
public final class SslConfig {

    /** Server-only: the listener names for which TLS is enabled (empty for a client config). */
    private final List<String> enabledListeners;

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

    /** How often to poll the key material for changes (0 disables periodic reload). */
    private final Duration reloadInterval;

    private SslConfig(
            List<String> enabledListeners,
            List<String> enabledProtocols,
            List<String> cipherSuites,
            @Nullable String keystorePath,
            @Nullable String keystorePassword,
            String keystoreType,
            @Nullable String keyPassword,
            @Nullable String truststorePath,
            @Nullable String truststorePassword,
            String truststoreType,
            String endpointIdentificationAlgorithm,
            Duration reloadInterval) {
        this.enabledListeners = enabledListeners;
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
        this.reloadInterval = reloadInterval;
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
        checkArgument(
                keystorePath != null,
                "'%s' must be configured when any listener enables TLS via '%s'.",
                ConfigOptions.SERVER_SSL_KEYSTORE_PATH.key(),
                ConfigOptions.SERVER_SSL_ENABLED_LISTENERS.key());

        return Optional.of(
                new SslConfig(
                        enabledListeners,
                        conf.get(ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS),
                        orEmpty(conf.get(ConfigOptions.SERVER_SSL_CIPHER_SUITES)),
                        keystorePath,
                        password(conf.get(ConfigOptions.SERVER_SSL_KEYSTORE_PASSWORD)),
                        conf.getString(ConfigOptions.SERVER_SSL_KEYSTORE_TYPE),
                        password(conf.get(ConfigOptions.SERVER_SSL_KEY_PASSWORD)),
                        conf.getString(ConfigOptions.SERVER_SSL_TRUSTSTORE_PATH),
                        password(conf.get(ConfigOptions.SERVER_SSL_TRUSTSTORE_PASSWORD)),
                        conf.getString(ConfigOptions.SERVER_SSL_TRUSTSTORE_TYPE),
                        "",
                        conf.get(ConfigOptions.SERVER_SSL_RELOAD_INTERVAL)));
    }

    /**
     * Build and validate the client-side TLS configuration, or {@link Optional#empty()} when TLS is
     * disabled (i.e. {@code client.security.ssl.enabled} is false).
     */
    public static Optional<SslConfig> fromClientConfig(Configuration conf) {
        if (!conf.get(ConfigOptions.CLIENT_SSL_ENABLED)) {
            return Optional.empty();
        }

        return Optional.of(
                new SslConfig(
                        Collections.emptyList(),
                        conf.get(ConfigOptions.CLIENT_SSL_ENABLED_PROTOCOLS),
                        orEmpty(conf.get(ConfigOptions.CLIENT_SSL_CIPHER_SUITES)),
                        conf.getString(ConfigOptions.CLIENT_SSL_KEYSTORE_PATH),
                        password(conf.get(ConfigOptions.CLIENT_SSL_KEYSTORE_PASSWORD)),
                        conf.getString(ConfigOptions.CLIENT_SSL_KEYSTORE_TYPE),
                        password(conf.get(ConfigOptions.CLIENT_SSL_KEY_PASSWORD)),
                        conf.getString(ConfigOptions.CLIENT_SSL_TRUSTSTORE_PATH),
                        password(conf.get(ConfigOptions.CLIENT_SSL_TRUSTSTORE_PASSWORD)),
                        conf.getString(ConfigOptions.CLIENT_SSL_TRUSTSTORE_TYPE),
                        conf.getString(ConfigOptions.CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM),
                        conf.get(ConfigOptions.CLIENT_SSL_RELOAD_INTERVAL)));
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

    public Duration reloadInterval() {
        return reloadInterval;
    }
}
