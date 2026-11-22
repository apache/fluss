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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.rpc.netty.ssl.SslConfig;
import org.apache.fluss.rpc.netty.ssl.SslContextFactory;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Validates at startup that other servers will accept this server on an mTLS internal listener.
 *
 * <p>With an mTLS internal listener, a server connects to other servers with the certificate in
 * {@code client.security.ssl.keystore.*}. If the internal listener rejects that certificate, every
 * request between servers fails at runtime. The server fails to start instead.
 */
final class InternalListenerConfigValidator {

    private InternalListenerConfigValidator() {}

    /**
     * Connects the server to itself in memory and fails if its mTLS internal listener rejects the
     * certificate of the server. Does nothing if the internal listener does not use mTLS.
     *
     * @param sslConfig the server TLS configuration, or null if no listener enables TLS
     * @param sslContext the server TLS context, or null if no listener enables TLS
     * @param authenticatorSuppliers the authenticator of each listener in {@code
     *     security.protocol.map}
     */
    static void checkOwnCertificate(
            Configuration conf,
            @Nullable SslConfig sslConfig,
            @Nullable SslContext sslContext,
            Map<String, Supplier<ServerAuthenticator>> authenticatorSuppliers) {
        String internalListener = conf.get(ConfigOptions.INTERNAL_LISTENER_NAME);
        if (sslConfig == null || !sslConfig.requiresClientAuth(internalListener)) {
            return;
        }
        // Not empty: FlussProtocolPlugin has verified that client TLS is enabled for this listener.
        SslContext clientContext = SslContextFactory.createClientSslContext(conf).get();
        ServerAuthenticator authenticator = authenticatorSuppliers.get(internalListener).get();
        try {
            SSLSession session = handshakeInMemory(clientContext, sslContext);
            authenticator.initialize(
                    new ServerAuthenticator.AuthenticateContext() {
                        @Override
                        public String ipAddress() {
                            // No connection exists: the handshake ran in memory.
                            return "127.0.0.1";
                        }

                        @Override
                        public String listenerName() {
                            return internalListener;
                        }

                        @Override
                        public String protocol() {
                            return authenticator.protocol();
                        }

                        @Override
                        public SSLSession sslSession() {
                            return session;
                        }
                    });
            authenticator.evaluateResponse(new byte[0]);
        } catch (SSLException e) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The TLS handshake of the 'client.security.ssl.*' options with the "
                                    + "mTLS internal listener '%s' fails: %s. Servers connect to "
                                    + "each other with these options, so every request between "
                                    + "servers would fail.",
                            internalListener, e.getMessage()),
                    e);
        } catch (AuthenticationException e) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The mTLS internal listener '%s' rejects the certificate in '%s', "
                                    + "with which this server connects to other servers: %s "
                                    + "Check '%s' and '%s'.",
                            internalListener,
                            ConfigOptions.CLIENT_SSL_KEYSTORE_PATH.key(),
                            e.getMessage(),
                            ConfigOptions.SERVER_SSL_PRINCIPAL_MAPPING_RULES.key(),
                            ConfigOptions.SUPER_USERS.key()),
                    e);
        } finally {
            IOUtils.closeQuietly(authenticator);
        }
    }

    /**
     * Runs a TLS handshake between a client engine and a server engine that requires a client
     * certificate, and returns the session of the server side.
     *
     * <p>The client engine has no peer host, so the handshake does not verify a hostname.
     *
     * @throws SSLException if either side rejects the handshake
     */
    private static SSLSession handshakeInMemory(SslContext clientContext, SslContext serverContext)
            throws SSLException {
        SSLEngine client = clientContext.newEngine(ByteBufAllocator.DEFAULT);
        SSLEngine server =
                SslContextFactory.createServerSslHandler(
                                serverContext, ByteBufAllocator.DEFAULT, true)
                        .engine();
        int packetSize = client.getSession().getPacketBufferSize();
        ByteBuffer clientToServer = ByteBuffer.allocate(packetSize);
        ByteBuffer serverToClient = ByteBuffer.allocate(packetSize);
        ByteBuffer empty = ByteBuffer.allocate(0);
        ByteBuffer discarded = ByteBuffer.allocate(client.getSession().getApplicationBufferSize());
        client.beginHandshake();
        server.beginHandshake();
        // Each round moves at most one TLS record per direction. A handshake needs about ten.
        for (int round = 0; round < 100; round++) {
            if (client.getHandshakeStatus() == HandshakeStatus.NOT_HANDSHAKING
                    && server.getHandshakeStatus() == HandshakeStatus.NOT_HANDSHAKING) {
                return server.getSession();
            }
            client.wrap(empty, clientToServer);
            runDelegatedTasks(client);
            server.wrap(empty, serverToClient);
            runDelegatedTasks(server);
            clientToServer.flip();
            serverToClient.flip();
            client.unwrap(serverToClient, discarded);
            runDelegatedTasks(client);
            server.unwrap(clientToServer, discarded);
            runDelegatedTasks(server);
            clientToServer.compact();
            serverToClient.compact();
        }
        throw new SSLException("The TLS handshake did not complete.");
    }

    private static void runDelegatedTasks(SSLEngine engine) {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }
}
