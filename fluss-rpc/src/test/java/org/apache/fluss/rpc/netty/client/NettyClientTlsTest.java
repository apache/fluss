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

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.rpc.netty.ssl.SslContextFactory;
import org.apache.fluss.rpc.netty.ssl.TestSslUtils;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLSession;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for a {@link NettyClient} with {@code client.security.ssl.enabled}. */
class NettyClientTlsTest {

    private static final String PLAINTEXT_LISTENER = "INTERNAL";

    @TempDir private Path tempDir;

    private NettyServer server;

    @AfterEach
    void teardown() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void testRequestOverTls() throws Exception {
        SelfSignedCertificate cert = startServer("localhost");
        try (NettyClient client = newClient(clientConf(cert))) {
            ApiVersionsResponse response =
                    sendApiVersions(client, serverNode(TestSslUtils.TLS_LISTENER)).get();
            assertThat(response.getApiVersionsList()).isNotEmpty();

            // a real protocol and cipher suite were negotiated, not a plaintext fallback.
            SSLSession session =
                    client.connections()
                            .iterator()
                            .next()
                            .channel()
                            .pipeline()
                            .get(SslHandler.class)
                            .engine()
                            .getSession();
            assertThat(session.getProtocol()).startsWith("TLS");
            assertThat(session.getCipherSuite()).doesNotContain("NULL");
        }
    }

    @Test
    void testPlaintextListenerFailsFastAsNonRetriable() throws Exception {
        SelfSignedCertificate cert = startServer("localhost");
        try (NettyClient client = newClient(clientConf(cert))) {
            assertThatThrownBy(() -> sendApiVersions(client, serverNode(PLAINTEXT_LISTENER)).get())
                    .hasCauseInstanceOf(AuthenticationException.class);
        }
    }

    @Test
    void testUntrustedCertificateFailsAsNonRetriable() throws Exception {
        startServer("localhost");
        SelfSignedCertificate otherCert = TestSslUtils.generateCertificate("localhost");
        ServerNode node = serverNode(TestSslUtils.TLS_LISTENER);
        try (NettyClient client = newClient(clientConf(otherCert))) {
            assertFailedConnectionIsKept(client, node);
        }
    }

    @Test
    void testRejectedSaslCredentialsDoNotRepeatTlsHandshake() throws Exception {
        Configuration serverConf = new Configuration();
        serverConf.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(),
                TestSslUtils.TLS_LISTENER + ":sasl");
        serverConf.setString("security.sasl.enabled.mechanisms", "plain");
        serverConf.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_admin=\"admin-secret\";");
        SelfSignedCertificate cert = startServer("localhost", serverConf);

        Configuration conf = clientConf(cert);
        conf.setString("client.security.protocol", "sasl");
        conf.setString("client.security.sasl.mechanism", "PLAIN");
        conf.setString("client.security.sasl.username", "admin");
        conf.setString("client.security.sasl.password", "wrong-secret");
        try (NettyClient client = newClient(conf)) {
            assertFailedConnectionIsKept(client, serverNode(TestSslUtils.TLS_LISTENER));
        }
    }

    @Test
    void testRejectedClientCertificateFailsAsNonRetriable() throws Exception {
        SelfSignedCertificate cert = TestSslUtils.generateCertificate("localhost");
        Configuration serverConf = new Configuration();
        TestSslUtils.setServerSslConfig(
                serverConf,
                TestSslUtils.createKeyStore(tempDir, "keystore.jks", cert),
                TestSslUtils.createTrustStore(tempDir, "server-truststore.jks", cert));
        SslContext sslContext = SslContextFactory.createServerSslContext(serverConf).get();
        // A plain Netty server that requires a client certificate. NettyServer cannot start such
        // a listener before the mTLS authenticator exists (#3793).
        EventLoopGroup group = new NioEventLoopGroup(1);
        // the client trusts the server, but presents no certificate of its own.
        try (NettyClient client = newClient(clientConf(cert))) {
            Channel serverChannel =
                    new ServerBootstrap()
                            .group(group)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(
                                    new ChannelInitializer<Channel>() {
                                        @Override
                                        protected void initChannel(Channel ch) {
                                            ch.pipeline()
                                                    .addLast(
                                                            SslContextFactory
                                                                    .createServerSslHandler(
                                                                            sslContext,
                                                                            ch.alloc(),
                                                                            true));
                                        }
                                    })
                            .bind("localhost", 0)
                            .sync()
                            .channel();
            ServerNode node =
                    new ServerNode(
                            1,
                            "localhost",
                            ((InetSocketAddress) serverChannel.localAddress()).getPort(),
                            ServerType.COORDINATOR);

            // With TLS 1.3 the server rejects the missing certificate only after the client
            // handshake has completed.
            assertThatThrownBy(() -> sendApiVersions(client, node).get())
                    .hasCauseInstanceOf(AuthenticationException.class);
        } finally {
            group.shutdownGracefully();
        }
    }

    @Test
    void testHandshakeTimeoutUsesConnectTimeout() throws Exception {
        SelfSignedCertificate cert = TestSslUtils.generateCertificate("localhost");
        Configuration conf = clientConf(cert);
        conf.set(ConfigOptions.CLIENT_CONNECT_TIMEOUT, Duration.ofSeconds(1));
        // the kernel completes the TCP connect into the backlog, but nothing ever answers the
        // ClientHello.
        try (ServerSocket silentServer = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
                NettyClient client = newClient(conf)) {
            ServerNode node =
                    new ServerNode(
                            1, "localhost", silentServer.getLocalPort(), ServerType.COORDINATOR);
            // Fails if the timeout is set after the SslHandler is added, since the handler then
            // schedules Netty's 10s default instead.
            assertThatThrownBy(() -> sendApiVersions(client, node).get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(RetriableException.class);
        }
    }

    @Test
    void testUnusableProtocolFailsFastAsNonRetriable() throws Exception {
        SelfSignedCertificate cert = startServer("localhost");
        // SSLv3 is supported by every JDK but disabled by its security policy, so it passes
        // configuration validation and the engine rejects it only when the handshake starts,
        // which happens synchronously while the SslHandler is added to the pipeline.
        Configuration conf = clientConf(cert);
        conf.setString(ConfigOptions.CLIENT_SSL_ENABLED_PROTOCOLS.key(), "SSLv3");
        try (NettyClient client = newClient(conf)) {
            assertThatThrownBy(
                            () ->
                                    sendApiVersions(client, serverNode(TestSslUtils.TLS_LISTENER))
                                            .get())
                    .hasCauseInstanceOf(AuthenticationException.class);
        }
    }

    @Test
    void testHostnameVerification() throws Exception {
        SelfSignedCertificate cert = startServer("other.host");

        try (NettyClient client = newClient(clientConf(cert))) {
            assertThatThrownBy(
                            () ->
                                    sendApiVersions(client, serverNode(TestSslUtils.TLS_LISTENER))
                                            .get())
                    .hasCauseInstanceOf(AuthenticationException.class);
        }

        Configuration noVerification = clientConf(cert);
        noVerification.setString(
                ConfigOptions.CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM.key(), "");
        try (NettyClient client = newClient(noVerification)) {
            assertThat(sendApiVersions(client, serverNode(TestSslUtils.TLS_LISTENER)).get())
                    .isNotNull();
        }
    }

    @Test
    void testInvalidTlsConfigFailsClientCreation() {
        Configuration conf = new Configuration();
        TestSslUtils.setClientSslConfig(conf, tempDir.resolve("missing.jks"), null);

        assertThatThrownBy(() -> newClient(conf))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("missing.jks");
    }

    /**
     * Start a server with a TLS and a plaintext listener, presenting a certificate for {@code
     * fqdn}.
     */
    private SelfSignedCertificate startServer(String fqdn) throws Exception {
        return startServer(fqdn, new Configuration());
    }

    private SelfSignedCertificate startServer(String fqdn, Configuration serverConf)
            throws Exception {
        SelfSignedCertificate cert = TestSslUtils.generateCertificate(fqdn);
        TestSslUtils.setServerSslConfig(
                serverConf, TestSslUtils.createKeyStore(tempDir, "keystore.jks", cert), null);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        server =
                new NettyServer(
                        serverConf,
                        Arrays.asList(
                                new Endpoint("localhost", 0, TestSslUtils.TLS_LISTENER),
                                new Endpoint("localhost", 0, PLAINTEXT_LISTENER)),
                        new TestingGatewayService(),
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
        server.start();
        return cert;
    }

    /** A TLS client configuration trusting {@code cert}. */
    private Configuration clientConf(SelfSignedCertificate cert) throws Exception {
        Configuration conf = new Configuration();
        TestSslUtils.setClientSslConfig(
                conf, TestSslUtils.createTrustStore(tempDir, "truststore.jks", cert), null);
        return conf;
    }

    /**
     * The first request fails as non-retriable. The failed connection then stays for a while: the
     * next request fails with the same cause without another TLS handshake.
     */
    private static void assertFailedConnectionIsKept(NettyClient client, ServerNode node) {
        assertThatThrownBy(() -> sendApiVersions(client, node).get())
                .hasCauseInstanceOf(AuthenticationException.class);
        ServerConnection failed = client.connections().iterator().next();

        assertThatThrownBy(() -> sendApiVersions(client, node).get())
                .hasCauseInstanceOf(AuthenticationException.class);
        assertThat(client.connections()).containsExactly(failed);

        retry(Duration.ofSeconds(20), () -> assertThat(client.connections()).isEmpty());
    }

    private static NettyClient newClient(Configuration conf) {
        return new NettyClient(conf, TestingClientMetricGroup.newInstance());
    }

    private ServerNode serverNode(String listenerName) {
        Endpoint endpoint =
                server.getBindEndpoints().stream()
                        .filter(e -> e.getListenerName().equals(listenerName))
                        .findFirst()
                        .get();
        // the advertised host name, which the certificate is issued for, not the bound address.
        return new ServerNode(1, "localhost", endpoint.getPort(), ServerType.COORDINATOR);
    }

    private static CompletableFuture<ApiVersionsResponse> sendApiVersions(
            NettyClient client, ServerNode node) {
        return client.sendRequest(
                        node,
                        ApiKeys.API_VERSIONS,
                        new ApiVersionsRequest()
                                .setClientSoftwareName("test")
                                .setClientSoftwareVersion("1.0"))
                .thenApply(ApiVersionsResponse.class::cast);
    }
}
