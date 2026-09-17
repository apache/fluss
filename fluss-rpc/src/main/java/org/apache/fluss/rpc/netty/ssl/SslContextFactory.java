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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslProvider;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.util.Optional;

/**
 * Builds Netty {@link SslContext}s and {@link SslHandler}s for the Fluss RPC layer from the {@code
 * security.ssl.*} (server) and {@code client.security.ssl.*} (client) configuration.
 *
 * <p>The JDK SSL provider is used for portability (it does not require a native OpenSSL binding to
 * be present on the host). Transport encryption is orthogonal to the application-level
 * authentication protocol: a TLS-enabled listener can run any authenticator. Mutual TLS is realized
 * by a listener whose auth protocol is {@code mTLS}; that per-listener client-certificate
 * requirement is parsed and validated by {@link SslConfig} and applied on the server handler
 * ({@link #createServerSslHandler}).
 */
@Internal
public final class SslContextFactory {

    private SslContextFactory() {}

    /**
     * Build a server {@link SslContext} from the {@code security.ssl.*} configuration, or {@link
     * Optional#empty()} when TLS is not enabled for any listener.
     */
    public static Optional<SslContext> createServerSslContext(Configuration conf) {
        return SslConfig.fromServerConfig(conf).map(SslContextFactory::createServerSslContext);
    }

    /** Build a server {@link SslContext} from a parsed {@link SslConfig}. */
    public static SslContext createServerSslContext(SslConfig config) {
        KeyManagerFactory kmf =
                keyManagerFactory(
                        SERVER_KEYSTORE,
                        config.keystorePath(),
                        config.keystoreType(),
                        config.keystorePassword(),
                        config.keyPassword());
        SslContextBuilder builder =
                SslContextBuilder.forServer(kmf)
                        .sslProvider(SslProvider.JDK)
                        .protocols(config.enabledProtocols());
        if (!config.cipherSuites().isEmpty()) {
            builder.ciphers(config.cipherSuites());
        }
        if (config.truststorePath() != null) {
            builder.trustManager(
                    trustManagerFactory(
                            SERVER_TRUSTSTORE,
                            config.truststorePath(),
                            config.truststoreType(),
                            config.truststorePassword()));
        }
        try {
            return builder.build();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to build the server SSL context.", e);
        }
    }

    /**
     * Build a client {@link SslContext} from the {@code client.security.ssl.*} configuration, or
     * {@link Optional#empty()} when TLS is disabled on the client.
     */
    public static Optional<SslContext> createClientSslContext(Configuration conf) {
        return SslConfig.fromClientConfig(conf).map(SslContextFactory::createClientSslContext);
    }

    /** Build a client {@link SslContext} from a parsed {@link SslConfig}. */
    public static SslContext createClientSslContext(SslConfig config) {
        SslContextBuilder builder =
                SslContextBuilder.forClient()
                        .sslProvider(SslProvider.JDK)
                        .protocols(config.enabledProtocols());
        if (!config.cipherSuites().isEmpty()) {
            builder.ciphers(config.cipherSuites());
        }
        if (config.truststorePath() != null) {
            builder.trustManager(
                    trustManagerFactory(
                            CLIENT_TRUSTSTORE,
                            config.truststorePath(),
                            config.truststoreType(),
                            config.truststorePassword()));
        }
        // Present a client certificate when a keystore is configured (required for mutual TLS).
        if (config.keystorePath() != null) {
            builder.keyManager(
                    keyManagerFactory(
                            CLIENT_KEYSTORE,
                            config.keystorePath(),
                            config.keystoreType(),
                            config.keystorePassword(),
                            config.keyPassword()));
        }
        try {
            return builder.build();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to build the client SSL context.", e);
        }
    }

    /**
     * Create a server-side {@link SslHandler} for a newly accepted channel. When {@code
     * requireClientAuth} is true the engine demands a client certificate during the handshake;
     * callers derive that per listener from {@link SslConfig#requiresClientAuth(String)}, which
     * only returns true when a truststore is configured to validate those certificates against.
     */
    public static SslHandler createServerSslHandler(
            SslContext sslContext, ByteBufAllocator alloc, boolean requireClientAuth) {
        SslHandler handler = sslContext.newHandler(alloc);
        if (requireClientAuth) {
            handler.engine().setNeedClientAuth(true);
        }
        return handler;
    }

    /**
     * Create a client-side {@link SslHandler} for a connection to {@code host:port}, configuring
     * SNI and (optionally) hostname verification via the endpoint identification algorithm.
     */
    public static SslHandler createClientSslHandler(
            SslContext sslContext,
            ByteBufAllocator alloc,
            String host,
            int port,
            String endpointIdentificationAlgorithm) {
        SslHandler handler = sslContext.newHandler(alloc, host, port);
        SSLEngine engine = handler.engine();
        SSLParameters parameters = engine.getSSLParameters();
        // An empty algorithm disables hostname verification (Kafka semantics).
        parameters.setEndpointIdentificationAlgorithm(
                endpointIdentificationAlgorithm == null
                                || endpointIdentificationAlgorithm.trim().isEmpty()
                        ? null
                        : endpointIdentificationAlgorithm);
        engine.setSSLParameters(parameters);
        return handler;
    }

    private static KeyManagerFactory keyManagerFactory(
            Store store,
            String path,
            String type,
            @Nullable String storePassword,
            @Nullable String keyPassword) {
        KeyStore keyStore = loadKeyStore(store, path, type, storePassword);
        try {
            KeyManagerFactory kmf =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, keyPassword == null ? null : keyPassword.toCharArray());
            return kmf;
        } catch (UnrecoverableKeyException e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Cannot recover the private key from the %s at '%s'. Check '%s', which "
                                    + "defaults to '%s' when not set.",
                            store.what, path, store.keyPasswordKey, store.passwordKey),
                    e);
        } catch (GeneralSecurityException e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to build a key manager from the %s at '%s'.", store.what, path),
                    e);
        }
    }

    private static TrustManagerFactory trustManagerFactory(
            Store store, String path, String type, @Nullable String storePassword) {
        KeyStore trustStore = loadKeyStore(store, path, type, storePassword);
        try {
            TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            return tmf;
        } catch (GeneralSecurityException e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to build a trust manager from the %s at '%s'.",
                            store.what, path),
                    e);
        }
    }

    /**
     * Load a keystore or truststore, reporting the failures an operator can actually cause - a path
     * that is not there, and a store password that does not open the file - with the file and the
     * option that configures it, rather than letting the bare JCA exception surface.
     */
    private static KeyStore loadKeyStore(
            Store store, String path, String type, @Nullable String password) {
        KeyStore keyStore;
        try {
            keyStore = KeyStore.getInstance(type);
        } catch (GeneralSecurityException e) {
            // unreachable for a config parsed by SslConfig, which validates the type up front.
            throw new FlussRuntimeException(
                    String.format(
                            "'%s' is set to the unsupported store type '%s'.", store.typeKey, type),
                    e);
        }
        try (InputStream in = Files.newInputStream(Paths.get(path))) {
            keyStore.load(in, password == null ? null : password.toCharArray());
            return keyStore;
        } catch (NoSuchFileException e) {
            throw new FlussRuntimeException(
                    String.format(
                            "No %s at '%s', configured by '%s'.", store.what, path, store.pathKey),
                    e);
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to read the %s at '%s' as a '%s' store. A wrong '%s' is the "
                                    + "usual cause; the file may also be unreadable or not a "
                                    + "keystore at all.",
                            store.what, path, type, store.passwordKey),
                    e);
        } catch (GeneralSecurityException e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to load the %s at '%s' as a '%s' store.",
                            store.what, path, type),
                    e);
        }
    }

    /** The options that configure one keystore or truststore, so failures can name them. */
    private static final class Store {
        private final String what;
        private final String pathKey;
        private final String passwordKey;
        private final String typeKey;
        @Nullable private final String keyPasswordKey;

        private Store(
                String what,
                String pathKey,
                String passwordKey,
                String typeKey,
                @Nullable String keyPasswordKey) {
            this.what = what;
            this.pathKey = pathKey;
            this.passwordKey = passwordKey;
            this.typeKey = typeKey;
            this.keyPasswordKey = keyPasswordKey;
        }
    }

    private static final Store SERVER_KEYSTORE =
            new Store(
                    "server keystore",
                    ConfigOptions.SERVER_SSL_KEYSTORE_PATH.key(),
                    ConfigOptions.SERVER_SSL_KEYSTORE_PASSWORD.key(),
                    ConfigOptions.SERVER_SSL_KEYSTORE_TYPE.key(),
                    ConfigOptions.SERVER_SSL_KEY_PASSWORD.key());

    private static final Store SERVER_TRUSTSTORE =
            new Store(
                    "server truststore",
                    ConfigOptions.SERVER_SSL_TRUSTSTORE_PATH.key(),
                    ConfigOptions.SERVER_SSL_TRUSTSTORE_PASSWORD.key(),
                    ConfigOptions.SERVER_SSL_TRUSTSTORE_TYPE.key(),
                    null);

    private static final Store CLIENT_KEYSTORE =
            new Store(
                    "client keystore",
                    ConfigOptions.CLIENT_SSL_KEYSTORE_PATH.key(),
                    ConfigOptions.CLIENT_SSL_KEYSTORE_PASSWORD.key(),
                    ConfigOptions.CLIENT_SSL_KEYSTORE_TYPE.key(),
                    ConfigOptions.CLIENT_SSL_KEY_PASSWORD.key());

    private static final Store CLIENT_TRUSTSTORE =
            new Store(
                    "client truststore",
                    ConfigOptions.CLIENT_SSL_TRUSTSTORE_PATH.key(),
                    ConfigOptions.CLIENT_SSL_TRUSTSTORE_PASSWORD.key(),
                    ConfigOptions.CLIENT_SSL_TRUSTSTORE_TYPE.key(),
                    null);
}
