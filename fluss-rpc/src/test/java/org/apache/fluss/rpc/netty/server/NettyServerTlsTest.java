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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingGatewayService;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.NettyUtils;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.netty.ssl.SslContextFactory;
import org.apache.fluss.rpc.netty.ssl.TestSslUtils;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.MessageCodec;
import org.apache.fluss.rpc.protocol.ResponseType;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for a {@link NettyServer} serving a TLS listener next to a plaintext one. The Fluss client
 * cannot speak TLS yet, so the TLS requests come from a minimal Netty client in this test.
 */
class NettyServerTlsTest {

    private static final String PLAINTEXT_LISTENER = "INTERNAL";

    @TempDir private Path tempDir;

    private Configuration clientSslConf;
    private NettyServer server;

    @BeforeEach
    void setup() throws Exception {
        SelfSignedCertificate cert = TestSslUtils.generateCertificate("localhost");
        Path keyStore = TestSslUtils.createKeyStore(tempDir, "keystore.jks", cert);
        Path trustStore = TestSslUtils.createTrustStore(tempDir, "truststore.jks", cert);

        Configuration serverConf = new Configuration();
        TestSslUtils.setServerSslConfig(serverConf, keyStore, null);
        clientSslConf = new Configuration();
        TestSslUtils.setClientSslConfig(clientSslConf, trustStore, null);

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
    }

    @AfterEach
    void teardown() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void testTlsListenerServesRequestsOverTls() throws Exception {
        EventLoopGroup group = NettyUtils.newEventLoopGroup(1, "tls-test-client");
        try {
            CompletableFuture<SslHandler> sslHandler = new CompletableFuture<>();
            CompletableFuture<Byte> responseType = new CompletableFuture<>();
            SslContext sslContext = SslContextFactory.createClientSslContext(clientSslConf).get();
            Endpoint endpoint = bindEndpoint(TestSslUtils.TLS_LISTENER);
            Channel channel =
                    new Bootstrap()
                            .group(group)
                            .channel(NettyUtils.getClientSocketChannelClass(group))
                            .handler(
                                    new ChannelInitializer<SocketChannel>() {
                                        @Override
                                        protected void initChannel(SocketChannel ch) {
                                            SslHandler handler =
                                                    SslContextFactory.createClientSslHandler(
                                                            sslContext,
                                                            ch.alloc(),
                                                            endpoint.getHost(),
                                                            endpoint.getPort(),
                                                            "");
                                            sslHandler.complete(handler);
                                            ch.pipeline()
                                                    .addLast(handler)
                                                    .addLast(
                                                            new LengthFieldBasedFrameDecoder(
                                                                    Integer.MAX_VALUE, 0, 4, 0, 4))
                                                    .addLast(
                                                            new SimpleChannelInboundHandler<
                                                                    ByteBuf>() {
                                                                @Override
                                                                protected void channelRead0(
                                                                        ChannelHandlerContext ctx,
                                                                        ByteBuf frame) {
                                                                    responseType.complete(
                                                                            frame.readByte());
                                                                }
                                                            });
                                        }
                                    })
                            .connect(endpoint.getHost(), endpoint.getPort())
                            .sync()
                            .channel();
            try {
                channel.writeAndFlush(
                        MessageCodec.encodeRequest(
                                channel.alloc(),
                                ApiKeys.API_VERSIONS.id,
                                ApiKeys.API_VERSIONS.highestSupportedVersion,
                                0,
                                apiVersionsRequest()));

                assertThat(responseType.get()).isEqualTo(ResponseType.SUCCESS_RESPONSE.id);
                // a real protocol and cipher were negotiated, not a plaintext fallback.
                SslHandler handler = sslHandler.get();
                assertThat(handler.engine().getSession().getProtocol()).startsWith("TLS");
                assertThat(handler.engine().getSession().getCipherSuite()).doesNotContain("NULL");
            } finally {
                channel.close().sync();
            }
        } finally {
            NettyUtils.shutdownGroup(group).get();
        }
    }

    @Test
    void testTlsListenerRejectsPlaintextClient() throws Exception {
        // The server reads a plaintext frame as the start of an incomplete TLS record and closes
        // the connection at the handshake timeout (10s by default), never serving the request.
        try (NettyClient client =
                new NettyClient(new Configuration(), TestingClientMetricGroup.newInstance())) {
            assertThatThrownBy(
                            () ->
                                    client.sendRequest(
                                                    serverNode(TestSslUtils.TLS_LISTENER),
                                                    ApiKeys.API_VERSIONS,
                                                    apiVersionsRequest())
                                            .get())
                    .hasMessageContaining("Disconnected");
        }
    }

    @Test
    void testPlaintextListenerIsUnaffected() throws Exception {
        try (NettyClient client =
                new NettyClient(new Configuration(), TestingClientMetricGroup.newInstance())) {
            ApiVersionsResponse response =
                    (ApiVersionsResponse)
                            client.sendRequest(
                                            serverNode(PLAINTEXT_LISTENER),
                                            ApiKeys.API_VERSIONS,
                                            apiVersionsRequest())
                                    .get();
            assertThat(response.getApiVersionsList()).isNotEmpty();
        }
    }

    private Endpoint bindEndpoint(String listenerName) {
        return server.getBindEndpoints().stream()
                .filter(endpoint -> endpoint.getListenerName().equals(listenerName))
                .findFirst()
                .get();
    }

    private ServerNode serverNode(String listenerName) {
        Endpoint endpoint = bindEndpoint(listenerName);
        return new ServerNode(1, endpoint.getHost(), endpoint.getPort(), ServerType.COORDINATOR);
    }

    private static ApiVersionsRequest apiVersionsRequest() {
        return new ApiVersionsRequest()
                .setClientSoftwareName("test")
                .setClientSoftwareVersion("1.0");
    }
}
