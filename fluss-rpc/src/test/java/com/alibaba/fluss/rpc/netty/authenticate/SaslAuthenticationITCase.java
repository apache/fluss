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
import com.alibaba.fluss.security.auth.sasl.jaas.TestJaasConfig;
import com.alibaba.fluss.utils.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for sasl authentication. */
public class SaslAuthenticationITCase {
    private static final String CLIENT_JAAS_INFO =
            "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";";
    private static final String SERVER_JAAS_INFO =
            "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required "
                    + "    user_admin=\"admin-secret\" "
                    + "    user_alice=\"alice-secret\";";

    @AfterEach
    void cleanup() {
        javax.security.auth.login.Configuration.setConfiguration(new TestJaasConfig());
    }

    @Test
    void testNormalAuthenticate() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.mechanism", "plain");
        clientConfig.setString("client.security.sasl.jaas.config", CLIENT_JAAS_INFO);
        testAuthentication(clientConfig);
    }

    @Test
    void testClientWrongPassword() {
        String jaasClientInfo =
                "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"admin\" password=\"wrong-secret\";";
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.mechanism", "PLAIN");
        clientConfig.setString("client.security.sasl.jaas.config", jaasClientInfo);
        assertThatThrownBy(() -> testAuthentication(clientConfig))
                .cause()
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("Authentication failed: Invalid username or password");
    }

    @Test
    void testClientLackMechanism() {
        String jaasClientInfo =
                "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"admin\" password=\"wrong-secret\";";
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.mechanism", "FAKE");
        clientConfig.setString("client.security.sasl.jaas.config", jaasClientInfo);
        assertThatThrownBy(() -> testAuthentication(clientConfig))
                .cause()
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("Unable to find a matching SASL mechanism for FAKE");
    }

    @Test
    void testClientLackLoginModule() {
        String jaasClientInfo =
                "com.alibaba.fluss.security.auth.sasl.jaas.FakeLoginModule required username=\"admin\" password=\"wrong-secret\";";
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.mechanism", "FAKE");
        clientConfig.setString("client.security.sasl.jaas.config", jaasClientInfo);
        assertThatThrownBy(() -> testAuthentication(clientConfig))
                .cause()
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("Failed to load login manager");
    }

    @Test
    void testClientMechanismNotMatchServer() {
        String jaasClientInfo =
                " com.alibaba.fluss.security.auth.sasl.jaas.DigestLoginModule required username=\"admin\" password=\"wrong-secret\";";
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.mechanism", "DIGEST-MD5");
        clientConfig.setString("client.security.sasl.jaas.config", jaasClientInfo);
        assertThatThrownBy(() -> testAuthentication(clientConfig))
                .cause()
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("SASL server enables [PLAIN] while protocol of client is 'DIGEST-MD5'");
    }

    @Test
    void testServerMechanismWithListenerAndMechanism() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.mechanism", "PLAIN");
        clientConfig.setString("client.security.sasl.jaas.config", CLIENT_JAAS_INFO);
        Configuration serverConfig = getDefaultServerConfig();
        String jaasServerInfo =
                "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_bob=\"bob-secret\";";
        serverConfig.setString(
                "security.sasl.listener.name.client.plain.jaas.config", jaasServerInfo);
        assertThatThrownBy(() -> testAuthentication(clientConfig, serverConfig))
                .cause()
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("Authentication failed: Invalid username or password");
        clientConfig.setString(
                "client.security.sasl.jaas.config",
                "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"bob\" password=\"bob-secret\";");
        testAuthentication(clientConfig, serverConfig);
    }

    @Test
    void testLoadJassConfigFallBackToJvmOptions() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.mechanism", "PLAIN");
        Configuration serverConfig = getDefaultServerConfig();
        serverConfig.removeKey("security.sasl.plain.jaas.config");
        assertThatThrownBy(() -> testAuthentication(clientConfig, serverConfig))
                .cause()
                .hasMessage(
                        "Could not find a 'FlussClient' entry in the JAAS configuration. System property 'java.security.auth.login.config' is not set");
        TestJaasConfig.createConfiguration("PLAIN", Collections.singletonList("PLAIN"));
        testAuthentication(clientConfig, serverConfig);
    }

    @Test
    void testSimplifyUsernameAndPassword() throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.username", "alice");
        assertThatThrownBy(() -> testAuthentication(clientConfig))
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage(
                        "Configuration 'client.security.sasl.username' and 'client.security.sasl.password' must be set together for SASL JAAS authentication");
        clientConfig.setString("client.security.sasl.password", "wrong-secret");
        assertThatThrownBy(() -> testAuthentication(clientConfig))
                .cause()
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("Authentication failed: Invalid username or password");
        clientConfig.setString("client.security.sasl.password", "alice-secret");
        testAuthentication(clientConfig);
    }

    private void testAuthentication(Configuration clientConfig) throws Exception {
        testAuthentication(clientConfig, getDefaultServerConfig());
    }

    private void testAuthentication(Configuration clientConfig, Configuration serverConfig)
            throws Exception {
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        TestingAuthenticateGatewayService service = new TestingAuthenticateGatewayService();
        try (NetUtils.Port availablePort1 = getAvailablePort();
                NettyServer nettyServer =
                        new NettyServer(
                                serverConfig,
                                Collections.singletonList(
                                        new Endpoint(
                                                "localhost", availablePort1.getPort(), "CLIENT")),
                                service,
                                metricGroup,
                                RequestsMetrics.createCoordinatorServerRequestMetrics(
                                        metricGroup))) {
            nettyServer.start();

            // use client listener to connect to server
            ServerNode serverNode =
                    new ServerNode(
                            1, "localhost", availablePort1.getPort(), ServerType.COORDINATOR);
            try (NettyClient nettyClient =
                    new NettyClient(clientConfig, TestingClientMetricGroup.newInstance())) {
                ListTablesRequest request =
                        new ListTablesRequest().setDatabaseName("test-database");
                ListTablesResponse listTablesResponse =
                        (ListTablesResponse)
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.LIST_TABLES, request)
                                        .get();

                assertThat(listTablesResponse.getTableNamesList())
                        .isEqualTo(Collections.singletonList("test-table"));
            }
        }
    }

    private Configuration getDefaultServerConfig() {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
        configuration.setString("security.sasl.enabled.mechanisms", "plain");
        configuration.setString("security.sasl.plain.jaas.config", SERVER_JAAS_INFO);
        // 3 worker threads is enough for this test
        configuration.setString(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS.key(), "3");
        return configuration;
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
