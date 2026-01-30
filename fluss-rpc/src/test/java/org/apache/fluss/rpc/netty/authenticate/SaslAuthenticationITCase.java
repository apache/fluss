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
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.security.auth.sasl.gssapi.FlussMiniKdc;
import org.apache.fluss.security.auth.sasl.jaas.TestJaasConfig;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.NetUtils;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_JAAS_CONFIG;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_MECHANISM;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for sasl authentication. */
public class SaslAuthenticationITCase {
    private static final String CLIENT_JAAS_INFO =
            "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";";
    private static final String SERVER_JAAS_INFO =
            "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                    + "    user_admin=\"admin-secret\" "
                    + "    user_alice=\"alice-secret\";";

    @AfterEach
    void cleanup() {
        javax.security.auth.login.Configuration.setConfiguration(new TestJaasConfig());
        System.clearProperty("java.security.krb5.conf");
    }

    /** Test the Kerberos authentication mechanism between server and client. */
    @Test
    void testGssapiAuthenticate() throws Exception {
        // Initialize and start a MiniKDC
        Properties conf = MiniKdc.createConf();
        FlussMiniKdc kdc = new FlussMiniKdc(conf);
        kdc.start();

        // Prepare temporary workspace for keytab and krb5.conf
        Path tempDir = Files.createTempDirectory("fluss-gssapi-test-" + UUID.randomUUID());
        File workDir = tempDir.toFile();
        File keytab = new File(workDir, "fluss.keytab");
        File krb5Conf = kdc.getKrb5Conf();

        try {
            // Create principal for server and client
            // Format
            // - server: service/hostname
            // - client: username
            kdc.createPrincipal(keytab, "fluss/127.0.0.1", "client");

            // Customize krb5.conf to enforce TCP and correct realm settings if necessary.
            if (krb5Conf.exists()) {
                String krb5Content =
                        "[libdefaults]\n"
                                + "    default_realm = "
                                + kdc.getRealm()
                                + "\n"
                                + "    udp_preference_limit = 1\n" // use TCP
                                + "    kdc_tcp_port = "
                                + kdc.getPort()
                                + "\n"
                                + "\n"
                                + "[realms]\n"
                                + "    "
                                + kdc.getRealm()
                                + " = {\n"
                                + "        kdc = 127.0.0.1:"
                                + kdc.getPort()
                                + "\n"
                                + "        admin_server = 127.0.0.1:"
                                + kdc.getPort()
                                + "\n"
                                + "    }\n";

                File customKrb5Conf =
                        new File(workDir, "krb5-custom-" + UUID.randomUUID() + ".conf");
                Files.write(customKrb5Conf.toPath(), krb5Content.getBytes());
                // Set the system property to point to our custom krb5.conf
                System.setProperty("java.security.krb5.conf", customKrb5Conf.getAbsolutePath());
            }

            String realm = kdc.getRealm();
            String serverPrincipal = String.format("fluss/127.0.0.1@%s", realm);
            String clientPrincipal = String.format("client@%s", realm);

            // Configure Fluss Server with GSSAPI enabled.
            Configuration serverConfig = new Configuration();
            // set client listener to use sasl
            serverConfig.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
            // set mechanism to GSSAPI
            serverConfig.setString(SERVER_SASL_ENABLED_MECHANISMS_CONFIG.key(), "GSSAPI");
            serverConfig.setString(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS.key(), "3");

            // Define the JAAS configuration for the server using the Krb5LoginModule.
            String serverJaas =
                    String.format(
                            "com.sun.security.auth.module.Krb5LoginModule required "
                                    + "useKeyTab=true storeKey=true useTicketCache=false "
                                    + "keyTab=\"%s\" principal=\"%s\";",
                            keytab.getAbsolutePath(), serverPrincipal);
            serverConfig.setString("security.sasl.gssapi.jaas.config", serverJaas);

            // Configure Fluss Client with GSSAPI enabled.
            Configuration clientConfig = new Configuration();
            clientConfig.setString("client.security.protocol", "sasl");
            clientConfig.setString(CLIENT_SASL_MECHANISM, "GSSAPI");

            // Define the JAAS configuration for the client.
            String clientJaas =
                    String.format(
                            "com.sun.security.auth.module.Krb5LoginModule required "
                                    + "useKeyTab=true storeKey=true useTicketCache=false "
                                    + "keyTab=\"%s\" principal=\"%s\";",
                            keytab.getAbsolutePath(), clientPrincipal);

            clientConfig.setString(CLIENT_SASL_JAAS_CONFIG, clientJaas);

            // Run authentication test
            testAuthentication(clientConfig, serverConfig);
        } finally {
            kdc.stop();
            FileUtils.deleteDirectory(workDir);
        }
    }

    @Test
    void testGssapiAuthenticationWrongPrincipal() throws Exception {
        // Initialize and start a MiniKDC
        Properties kdcConf = MiniKdc.createConf();
        FlussMiniKdc kdc = new FlussMiniKdc(kdcConf);
        kdc.start();

        // Prepare workspace
        Path tempDir = Files.createTempDirectory("fluss-gssapi-test-" + UUID.randomUUID());
        File workDir = tempDir.toFile();
        File keytab = new File(workDir, "fluss.keytab");
        File krb5Conf = kdc.getKrb5Conf();

        try {
            kdc.createPrincipal(keytab, "fluss/127.0.0.1", "client", "wrongclient");

            if (krb5Conf.exists()) {
                String realm = kdc.getRealm();
                String krb5Content =
                        "[libdefaults]\n"
                                + "    default_realm = "
                                + realm
                                + "\n"
                                + "    udp_preference_limit = 1\n"
                                + "    kdc_tcp_port = "
                                + kdc.getPort()
                                + "\n"
                                + "\n"
                                + "[realms]\n"
                                + "    "
                                + realm
                                + " = {\n"
                                + "        kdc = 127.0.0.1:"
                                + kdc.getPort()
                                + "\n"
                                + "        admin_server = 127.0.0.1:"
                                + kdc.getPort()
                                + "\n"
                                + "    }\n";

                File customKrb5Conf =
                        new File(workDir, "krb5-custom-" + UUID.randomUUID() + ".conf");
                Files.write(customKrb5Conf.toPath(), krb5Content.getBytes());
                System.setProperty("java.security.krb5.conf", customKrb5Conf.getAbsolutePath());
            }

            String realm = kdc.getRealm();
            String serverPrincipal = String.format("fluss/127.0.0.1@%s", realm);
            // Use a different client principal that doesn't exist in keytab for wrong principal
            String wrongClientPrincipal = String.format("nonexistent@%s", realm);

            Configuration serverConfig = new Configuration();
            serverConfig.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
            serverConfig.setString(
                    ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG.key(), "GSSAPI");
            String serverJaas =
                    String.format(
                            "com.sun.security.auth.module.Krb5LoginModule required "
                                    + "useKeyTab=true storeKey=true useTicketCache=false "
                                    + "keyTab=\"%s\" principal=\"%s\";",
                            keytab.getAbsolutePath(), serverPrincipal);
            serverConfig.setString("security.sasl.gssapi.jaas.config", serverJaas);
            serverConfig.setString(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS.key(), "3");

            // Configure client with wrong principal.
            Configuration clientConfig = new Configuration();
            clientConfig.setString("client.security.protocol", "sasl");
            clientConfig.setString("client.security.sasl.mechanism", "GSSAPI");
            String clientJaas =
                    String.format(
                            "com.sun.security.auth.module.Krb5LoginModule required "
                                    + "useKeyTab=true storeKey=true useTicketCache=false "
                                    + "keyTab=\"%s\" principal=\"%s\";",
                            keytab.getAbsolutePath(), wrongClientPrincipal);
            clientConfig.setString("client.security.sasl.jaas.config", clientJaas);

            // Authentication test fails with wrong principal.
            assertThatThrownBy(() -> testAuthentication(clientConfig, serverConfig))
                    .cause()
                    .isInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("Failed to load login manager");
        } finally {
            kdc.stop();
            FileUtils.deleteDirectory(workDir);
        }
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
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"admin\" password=\"wrong-secret\";";
        Configuration clientConfig = new Configuration();
        clientConfig.setString("client.security.protocol", "sasl");
        clientConfig.setString("client.security.sasl.mechanism", "PLAIN");
        clientConfig.setString("client.security.sasl.jaas.config", jaasClientInfo);
        assertThatThrownBy(() -> testAuthentication(clientConfig))
                .cause()
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessageContaining("Authentication failed: Invalid username or password");
    }

    @Test
    void testClientLackMechanism() {
        String jaasClientInfo =
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"admin\" password=\"wrong-secret\";";
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
                "org.apache.fluss.security.auth.sasl.jaas.FakeLoginModule required username=\"admin\" password=\"wrong-secret\";";
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
                " org.apache.fluss.security.auth.sasl.jaas.DigestLoginModule required username=\"admin\" password=\"wrong-secret\";";
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
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_bob=\"bob-secret\";";
        serverConfig.setString(
                "security.sasl.listener.name.client.plain.jaas.config", jaasServerInfo);
        assertThatThrownBy(() -> testAuthentication(clientConfig, serverConfig))
                .cause()
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessageContaining("Authentication failed: Invalid username or password");
        clientConfig.setString(
                "client.security.sasl.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"bob\" password=\"bob-secret\";");
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
                .hasMessageContaining("Authentication failed: Invalid username or password");
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
                                                "127.0.0.1", availablePort1.getPort(), "CLIENT")),
                                service,
                                metricGroup,
                                RequestsMetrics.createCoordinatorServerRequestMetrics(
                                        metricGroup))) {
            nettyServer.start();

            // use client listener to connect to server
            ServerNode serverNode =
                    new ServerNode(
                            1, "127.0.0.1", availablePort1.getPort(), ServerType.COORDINATOR);
            try (NettyClient nettyClient =
                    new NettyClient(clientConfig, TestingClientMetricGroup.newInstance(), false)) {
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
