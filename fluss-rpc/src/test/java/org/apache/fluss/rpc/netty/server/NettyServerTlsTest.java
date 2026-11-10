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
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.netty.ssl.TestSslUtils;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for a {@link NettyServer} serving a TLS listener next to a plaintext one. */
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
        try (NettyClient client =
                new NettyClient(clientSslConf, TestingClientMetricGroup.newInstance())) {
            ApiVersionsResponse response =
                    (ApiVersionsResponse)
                            client.sendRequest(
                                            serverNode(TestSslUtils.TLS_LISTENER),
                                            ApiKeys.API_VERSIONS,
                                            apiVersionsRequest())
                                    .get();
            assertThat(response.getApiVersionsList()).isNotEmpty();
        }
    }

    @Test
    void testTlsListenerRejectsPlaintextClient() throws Exception {
        // The server reads a plaintext frame as the start of an incomplete TLS record and closes
        // the connection at the handshake timeout (10s by default), never serving the request.
        // Nothing reaches the client before that, so this mismatch fails only as a retriable
        // disconnect.
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
        // the host name the certificate is issued for, not the bound address.
        return new ServerNode(1, "localhost", endpoint.getPort(), ServerType.COORDINATOR);
    }

    private static ApiVersionsRequest apiVersionsRequest() {
        return new ApiVersionsRequest()
                .setClientSoftwareName("test")
                .setClientSoftwareVersion("1.0");
    }
}
