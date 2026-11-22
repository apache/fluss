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

package org.apache.fluss.rpc.netty.authenticate;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.rpc.netty.ssl.TestSslUtils;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests mutual TLS (mTLS) authentication between a {@link NettyClient} and a {@link NettyServer}.
 */
class MutualTlsAuthenticationITCase {

    private static final String CN_RULE = "RULE:^CN=([^,]+)$/$1/";

    @TempDir private Path tempDir;

    private Path serverKeyStore;
    private Path serverTrustStore;
    private Path clientTrustStore;
    private Path aliceKeyStore;
    private Path internalKeyStore;

    private NettyServer server;

    @BeforeEach
    void setup() throws Exception {
        SelfSignedCertificate serverCert = TestSslUtils.generateCertificate("localhost");
        SelfSignedCertificate aliceCert = TestSslUtils.generateCertificate("alice");
        serverKeyStore = TestSslUtils.createKeyStore(tempDir, "server.jks", serverCert);
        aliceKeyStore = TestSslUtils.createKeyStore(tempDir, "alice.jks", aliceCert);
        SelfSignedCertificate internalCert = TestSslUtils.generateCertificate("fluss-server");
        internalKeyStore = TestSslUtils.createKeyStore(tempDir, "internal.jks", internalCert);
        serverTrustStore =
                TestSslUtils.createTrustStore(
                        tempDir,
                        "server-truststore.jks",
                        "JKS",
                        aliceCert.cert(),
                        internalCert.cert());
        clientTrustStore =
                TestSslUtils.createTrustStore(tempDir, "client-truststore.jks", serverCert);
    }

    @AfterEach
    void teardown() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void testPrincipalFromCommonNameRule() throws Exception {
        ServerNode node = startServer(CN_RULE + ",DEFAULT");
        try (NettyClient client = newClient(mutualTlsClientConf(aliceKeyStore))) {
            assertThat(principalOf(client, node)).isEqualTo(new FlussPrincipal("alice", "User"));
        }
    }

    @Test
    void testDefaultRuleKeepsDistinguishedName() throws Exception {
        ServerNode node = startServer("DEFAULT");
        try (NettyClient client = newClient(mutualTlsClientConf(aliceKeyStore))) {
            assertThat(principalOf(client, node)).isEqualTo(new FlussPrincipal("CN=alice", "User"));
        }
    }

    @Test
    void testNoMatchingRuleRejectsClientWithoutRevealingRules() throws Exception {
        ServerNode node = startServer("RULE:^CN=admin$/admin/");
        try (NettyClient client = newClient(mutualTlsClientConf(aliceKeyStore))) {
            assertThatThrownBy(() -> principalOf(client, node))
                    .cause()
                    .isExactlyInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("'CN=alice'")
                    .hasMessageNotContaining("RULE:");
        }
    }

    @Test
    void testClientWithoutCertificateIsRejected() throws Exception {
        ServerNode node = startServer("DEFAULT");
        // client.security.protocol=mTLS requires a keystore, so this client uses PLAINTEXT.
        Configuration conf = new Configuration();
        TestSslUtils.setClientSslConfig(conf, clientTrustStore, null);
        try (NettyClient client = newClient(conf)) {
            assertThatThrownBy(() -> principalOf(client, node))
                    .hasCauseInstanceOf(AuthenticationException.class);
        }
    }

    @Test
    void testUntrustedClientCertificateIsRejected() throws Exception {
        ServerNode node = startServer("DEFAULT");
        Path untrustedKeyStore =
                TestSslUtils.createKeyStore(
                        tempDir, "untrusted.jks", TestSslUtils.generateCertificate("alice"));
        try (NettyClient client = newClient(mutualTlsClientConf(untrustedKeyStore))) {
            assertThatThrownBy(() -> principalOf(client, node))
                    .hasCauseInstanceOf(AuthenticationException.class);
        }
    }

    @Test
    void testClientWithCertificateButPlaintextProtocolIsRejected() throws Exception {
        ServerNode node = startServer("DEFAULT");
        Configuration conf = mutualTlsClientConf(aliceKeyStore);
        conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "PLAINTEXT");
        try (NettyClient client = newClient(conf)) {
            // the TLS handshake succeeds, but the client never authenticates.
            assertThatThrownBy(() -> principalOf(client, node))
                    .cause()
                    .isExactlyInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("has not completed authentication");
        }
    }

    @Test
    void testInternalListenerAcceptsSuperUser() throws Exception {
        ServerNode node = startInternalServer("User:fluss-server;User:alice");
        try (NettyClient client = newClient(mutualTlsClientConf(aliceKeyStore))) {
            assertThat(principalOf(client, node)).isEqualTo(new FlussPrincipal("alice", "User"));
        }
    }

    @Test
    void testInternalListenerRejectsOtherPrincipals() throws Exception {
        ServerNode node = startInternalServer("User:fluss-server");
        try (NettyClient client = newClient(mutualTlsClientConf(aliceKeyStore))) {
            assertThatThrownBy(() -> principalOf(client, node))
                    .cause()
                    .isExactlyInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("'User:alice'")
                    .hasMessageContaining("internal listener '" + TestSslUtils.TLS_LISTENER + "'")
                    .hasMessageNotContaining("fluss-server");
        }
    }

    private ServerNode startServer(String mappingRules) throws Exception {
        return startServer(mappingRules, new Configuration());
    }

    /** Starts a server whose mTLS listener is also its internal listener. */
    private ServerNode startInternalServer(String superUsers) throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.INTERNAL_LISTENER_NAME, TestSslUtils.TLS_LISTENER);
        // The server connects to other servers with this certificate and checks it at startup.
        TestSslUtils.setClientSslConfig(conf, clientTrustStore, internalKeyStore);
        conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "mTLS");
        conf.set(ConfigOptions.SUPER_USERS, superUsers);
        return startServer(CN_RULE, conf);
    }

    private ServerNode startServer(String mappingRules, Configuration conf) throws Exception {
        TestSslUtils.setServerSslConfig(conf, serverKeyStore, serverTrustStore);
        TestSslUtils.setMutualTlsProtocolMap(conf);
        conf.set(ConfigOptions.SERVER_SSL_PRINCIPAL_MAPPING_RULES, mappingRules);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        server =
                new NettyServer(
                        conf,
                        Collections.singletonList(
                                new Endpoint("localhost", 0, TestSslUtils.TLS_LISTENER)),
                        new PrincipalGatewayService(),
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
        server.start();
        return new ServerNode(
                1,
                "localhost",
                server.getBindEndpoints().get(0).getPort(),
                ServerType.TABLET_SERVER);
    }

    private Configuration mutualTlsClientConf(Path keyStore) {
        Configuration conf = new Configuration();
        TestSslUtils.setClientSslConfig(conf, clientTrustStore, keyStore);
        conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "mTLS");
        return conf;
    }

    private static NettyClient newClient(Configuration conf) {
        return new NettyClient(conf, TestingClientMetricGroup.newInstance());
    }

    /** The principal the server derived for the connection of {@code client}. */
    private static FlussPrincipal principalOf(NettyClient client, ServerNode node)
            throws Exception {
        ListTablesResponse response =
                (ListTablesResponse)
                        client.sendRequest(
                                        node,
                                        ApiKeys.LIST_TABLES,
                                        new ListTablesRequest().setDatabaseName("db"))
                                .get();
        List<String> nameAndType = response.getTableNamesList();
        return new FlussPrincipal(nameAndType.get(0), nameAndType.get(1));
    }

    /** Returns the name and the type of the session principal as the two table names. */
    private static class PrincipalGatewayService extends TestingTabletGatewayService {
        @Override
        public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
            FlussPrincipal principal = currentSession().getPrincipal();
            return CompletableFuture.completedFuture(
                    new ListTablesResponse()
                            .addAllTableNames(
                                    Arrays.asList(principal.getName(), principal.getType())));
        }
    }
}
