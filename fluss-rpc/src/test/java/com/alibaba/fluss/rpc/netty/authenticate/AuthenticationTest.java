/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.rpc.netty.authenticate;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.NOPMetricsGroup;
import com.alibaba.fluss.rpc.TestingGatewayService;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.rpc.netty.client.NettyClient;
import com.alibaba.fluss.rpc.netty.server.NettyServer;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.RPC;
import com.alibaba.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for authentication. */
public class AuthenticationTest {
    private NettyServer nettyServer;
    private ServerNode serverNode;

    @BeforeEach
    public void setup() throws Exception {
        buildNettyServer();
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (nettyServer != null) {
            nettyServer.close();
        }
    }

    @Test
    void testNormalAuthenticate() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "username_password");
        clientConfig.setString("client.security.username_password.username", "root");
        clientConfig.setString("client.security.username_password.password", "password");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance())) {
            ListDatabasesRequest request = new ListDatabasesRequest();
            ListDatabasesResponse listDatabasesResponse =
                    (ListDatabasesResponse)
                            nettyClient
                                    .sendRequest(serverNode, ApiKeys.LIST_DATABASES, request)
                                    .get();

            assertThat(listDatabasesResponse.getDatabaseNamesList())
                    .isEqualTo(Collections.singletonList("test-database"));
        }
    }

    @Test
    void testClientLackAuthenticateProtocol() throws Exception {
        Configuration clientConfig = new Configuration();
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance())) {
            ListDatabasesRequest request = new ListDatabasesRequest();
            assertThatThrownBy(
                            () ->
                                    nettyClient
                                            .sendRequest(
                                                    serverNode, ApiKeys.LIST_DATABASES, request)
                                            .get())
                    .cause()
                    .isExactlyInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("The connection has not completed authentication yet.");
        }
    }

    @Test
    void testWrongPassword() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "username_password");
        clientConfig.setString("client.security.username_password.username", "root");
        clientConfig.setString("client.security.username_password.password", "password2");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance())) {
            ListDatabasesRequest request = new ListDatabasesRequest();
            assertThatThrownBy(
                            () ->
                                    nettyClient
                                            .sendRequest(
                                                    serverNode, ApiKeys.LIST_DATABASES, request)
                                            .get())
                    .cause()
                    .isExactlyInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("username or password is incorrect");
        }
    }

    @Test
    void testMultiClientsWithSameProtocol() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "username_password");
        clientConfig.setString("client.security.username_password.username", "root");
        clientConfig.setString("client.security.username_password.password", "password");

        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance())) {
            ListDatabasesRequest request = new ListDatabasesRequest();
            ListDatabasesResponse listDatabasesResponse =
                    (ListDatabasesResponse)
                            nettyClient
                                    .sendRequest(serverNode, ApiKeys.LIST_DATABASES, request)
                                    .get();
            assertThat(listDatabasesResponse.getDatabaseNamesList())
                    .isEqualTo(Collections.singletonList("test-database"));
            // client2 with wrong password after client1 successes to authenticate.
            clientConfig.setString("client.security.username_password.password", "password2");
            try (NettyClient nettyClient2 =
                    new NettyClient(clientConfig, TestingClientMetricGroup.newInstance())) {
                assertThatThrownBy(
                                () ->
                                        nettyClient2
                                                .sendRequest(
                                                        serverNode, ApiKeys.LIST_DATABASES, request)
                                                .get())
                        .cause()
                        .isExactlyInstanceOf(AuthenticationException.class)
                        .hasMessageContaining("username or password is incorrect");
            }
        }
    }

    private void buildNettyServer() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:username_password");
        configuration.setString("security.username_password.username", "root");
        configuration.setString("security.username_password.password", "password");
        // 3 worker threads is enough for this test
        configuration.setString(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS.key(), "3");
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        TestingAuthenticateGatewayService service = new TestingAuthenticateGatewayService();
        try (NetUtils.Port availablePort1 = getAvailablePort();
                NetUtils.Port availablePort2 = getAvailablePort()) {
            this.nettyServer =
                    new NettyServer(
                            configuration,
                            Arrays.asList(
                                    new Endpoint("localhost", availablePort1.getPort(), "INTERNAL"),
                                    new Endpoint("localhost", availablePort2.getPort(), "CLIENT")),
                            service,
                            metricGroup,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
            // override method of LIST_DATABASES for test request required authentication before,
            nettyServer
                    .getApiManager()
                    .registerApiMethod(
                            ApiKeys.LIST_DATABASES,
                            TestingAuthenticateGatewayService.class.getMethod(
                                    "listDatabases", ListDatabasesRequest.class),
                            ServerType.COORDINATOR);
            nettyServer.start();

            // use client listener to connect to server
            serverNode =
                    new ServerNode(
                            2, "localhost", availablePort2.getPort(), ServerType.COORDINATOR);
        }
    }

    /**
     * A testing gateway service which apply a non API_VERSIONS request which requires
     * authentication.
     */
    public static class TestingAuthenticateGatewayService extends TestingGatewayService {

        @RPC(api = ApiKeys.LIST_DATABASES)
        public CompletableFuture<ListDatabasesResponse> listDatabases(
                ListDatabasesRequest request) {
            return CompletableFuture.completedFuture(
                    new ListDatabasesResponse()
                            .addAllDatabaseNames(Collections.singleton("test-database")));
        }
    }
}
