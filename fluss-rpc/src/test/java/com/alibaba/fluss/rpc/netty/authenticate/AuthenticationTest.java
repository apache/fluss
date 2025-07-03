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

package com.alibaba.fluss.rpc.netty.authenticate;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.util.NOPMetricsGroup;
import com.alibaba.fluss.rpc.TestingTabletGatewayService;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.rpc.netty.client.NettyClient;
import com.alibaba.fluss.rpc.netty.server.NettyServer;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
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
    private ServerNode usernamePasswordServerNode;
    private ServerNode mutualAuthServerNode;

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
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "sasl");
        clientConfig.set(ConfigOptions.CLIENT_SASL_MECHANISM, "plain");
        clientConfig.setString("client.security.sasl.username", "root");
        clientConfig.setString("client.security.sasl.password", "password");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            verifyGetTableNamesList(nettyClient, usernamePasswordServerNode);
        }
    }

    @Test
    void testMutualAuthenticate() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "mutual");

        // test normal mutual auth
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            verifyGetTableNamesList(nettyClient, mutualAuthServerNode);
        }

        // test invalid challenge from server
        clientConfig.setString("client.security.mutual.error-type", "SERVER_ERROR_CHALLENGE");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            assertThatThrownBy(() -> verifyGetTableNamesList(nettyClient, mutualAuthServerNode))
                    .hasRootCauseExactlyInstanceOf(AuthenticationException.class)
                    .rootCause()
                    .hasMessageContaining("Invalid challenge value");
        }

        // test invalid token from client
        clientConfig.setString("client.security.mutual.error-type", "CLIENT_ERROR_SECOND_TOKEN");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            assertThatThrownBy(() -> verifyGetTableNamesList(nettyClient, mutualAuthServerNode))
                    .rootCause()
                    .hasMessageContaining("Invalid token value");
        }
    }

    @Test
    void testNoChallengeBeforeClientComplete() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "mutual");
        clientConfig.setString("client.security.mutual.error-type", "SERVER_NO_CHALLENGE");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {

            assertThatThrownBy(() -> verifyGetTableNamesList(nettyClient, mutualAuthServerNode))
                    .hasRootCauseExactlyInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "client authenticator is not completed while server generate no challenge");
        }
    }

    @Test
    void testRetirableAuthenticateException() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "mutual");
        clientConfig.setString("client.security.mutual.error-type", "RETRIABLE_EXCEPTION");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            verifyGetTableNamesList(nettyClient, mutualAuthServerNode);
        }
    }

    @Test
    void testClientLackAuthenticateProtocol() throws Exception {
        Configuration clientConfig = new Configuration();
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            assertThatThrownBy(
                            () -> verifyGetTableNamesList(nettyClient, usernamePasswordServerNode))
                    .cause()
                    .isExactlyInstanceOf(AuthenticationException.class)
                    .hasMessageContaining(
                            "The connection has not completed authentication yet. This may be caused by a missing or incorrect configuration of 'client.security.protocol' on the client side.");
        }
    }

    @Test
    void testAuthenticateProtocolNotMatch() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "mutual");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            assertThatThrownBy(
                            () -> verifyGetTableNamesList(nettyClient, usernamePasswordServerNode))
                    .cause()
                    .isExactlyInstanceOf(AuthenticationException.class)
                    .hasMessageContaining(
                            "SASL server enables [PLAIN] while protocol of client is 'mutual'");
        }
    }

    @Test
    void testWrongPassword() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "sasl");
        clientConfig.set(ConfigOptions.CLIENT_SASL_MECHANISM, "plain");
        clientConfig.setString("client.security.sasl.username", "root");
        clientConfig.setString("client.security.sasl.password", "password2");
        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            assertThatThrownBy(
                            () -> verifyGetTableNamesList(nettyClient, usernamePasswordServerNode))
                    .cause()
                    .isExactlyInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("Invalid username or password");
        }
    }

    @Test
    void testMultiClientsWithSameProtocol() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "sasl");
        clientConfig.set(ConfigOptions.CLIENT_SASL_MECHANISM, "plain");
        clientConfig.setString("client.security.sasl.username", "root");
        clientConfig.setString("client.security.sasl.password", "password");

        try (NettyClient nettyClient =
                new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
            verifyGetTableNamesList(nettyClient, usernamePasswordServerNode);
            // client2 with wrong password after client1 successes to authenticate.
            clientConfig.setString("client.security.sasl.password", "password2");
            try (NettyClient nettyClient2 =
                    new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
                assertThatThrownBy(
                                () ->
                                        verifyGetTableNamesList(
                                                nettyClient2, usernamePasswordServerNode))
                        .cause()
                        .isExactlyInstanceOf(AuthenticationException.class)
                        .hasMessageContaining("Invalid username or password");
            }
        }
    }

    private void verifyGetTableNamesList(NettyClient nettyClient, ServerNode serverNode)
            throws Exception {
        ListTablesRequest request = new ListTablesRequest().setDatabaseName("test-database");
        ListTablesResponse listTablesResponse =
                (ListTablesResponse)
                        nettyClient.sendRequest(serverNode, ApiKeys.LIST_TABLES, request).get();

        assertThat(listTablesResponse.getTableNamesList())
                .isEqualTo(Collections.singletonList("test-table"));
    }

    private void buildNettyServer() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT1:mutual,CLIENT2:sasl");
        configuration.setString("security.sasl.enabled.mechanisms", "plain");
        configuration.setString(
                "security.sasl.plain.jaas.config",
                "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_root=\"password\" "
                        + "    user_guest=\"password2\";");
        configuration.set(ConfigOptions.SUPER_USERS, "User:root");
        configuration.set(ConfigOptions.AUTHORIZER_ENABLED, true);
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
                                    new Endpoint("localhost", availablePort1.getPort(), "CLIENT1"),
                                    new Endpoint("localhost", availablePort2.getPort(), "CLIENT2")),
                            service,
                            metricGroup,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
            nettyServer.start();

            // use client listener to connect to server
            mutualAuthServerNode =
                    new ServerNode(
                            1, "localhost", availablePort1.getPort(), ServerType.COORDINATOR);
            usernamePasswordServerNode =
                    new ServerNode(
                            2, "localhost", availablePort2.getPort(), ServerType.COORDINATOR);
        }
    }

    /**
     * A testing gateway service which apply a non API_VERSIONS request which requires
     * authentication.
     */
    public static class TestingAuthenticateGatewayService extends TestingTabletGatewayService {
        @Override
        public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
            return CompletableFuture.completedFuture(
                    new ListTablesResponse().addAllTableNames(Collections.singleton("test-table")));
        }
    }
}
