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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
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
        try {
            KeyManagerFactory kmf =
                    keyManagerFactory(
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
                                config.truststorePath(),
                                config.truststoreType(),
                                config.truststorePassword()));
            }
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
        try {
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
                                config.truststorePath(),
                                config.truststoreType(),
                                config.truststorePassword()));
            }
            // Present a client certificate when a keystore is configured (required for mutual TLS).
            if (config.keystorePath() != null) {
                builder.keyManager(
                        keyManagerFactory(
                                config.keystorePath(),
                                config.keystoreType(),
                                config.keystorePassword(),
                                config.keyPassword()));
            }
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
            String path, String type, String storePassword, String keyPassword) throws Exception {
        KeyStore keyStore = loadKeyStore(path, type, storePassword);
        KeyManagerFactory kmf =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword == null ? null : keyPassword.toCharArray());
        return kmf;
    }

    private static TrustManagerFactory trustManagerFactory(
            String path, String type, String storePassword) throws Exception {
        KeyStore trustStore = loadKeyStore(path, type, storePassword);
        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        return tmf;
    }

    private static KeyStore loadKeyStore(String path, String type, String password)
            throws Exception {
        KeyStore keyStore = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(Paths.get(path))) {
            keyStore.load(in, password == null ? null : password.toCharArray());
        }
        return keyStore;
    }
}
