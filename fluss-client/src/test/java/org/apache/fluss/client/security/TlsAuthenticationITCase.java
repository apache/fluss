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

package org.apache.fluss.client.security;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.netty.ssl.TestSslUtils;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.fluss.testutils.TestTableNames;
import org.apache.fluss.utils.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Authenticates clients and servers with mutual TLS (mTLS) on a cluster with the authorizer
 * enabled.
 *
 * <p>Both listeners are mTLS listeners. The client certificates of {@code admin} and {@code guest}
 * map to the principals {@code User:admin} and {@code User:guest}.
 *
 * <p>Servers connect to each other over the internal listener with a client certificate of their
 * own, which maps to the super user {@code User:fluss-server}.
 */
class TlsAuthenticationITCase {

    private static final String INTERNAL_LISTENER = "FLUSS";
    private static final String CLIENT_LISTENER = "CLIENT";
    private static final String LISTENERS =
            INTERNAL_LISTENER + "://localhost:0, " + CLIENT_LISTENER + "://localhost:0";

    private static final Path TEMP_DIR;
    private static final Path ADMIN_KEYSTORE;
    private static final Path GUEST_KEYSTORE;

    @RegisterExtension static final FlussClusterExtension CLUSTER;

    static {
        try {
            TEMP_DIR = Files.createTempDirectory("fluss-mtls-itcase");
            // Without advertised listeners the servers register their bound IP address, and
            // hostname verification matches an IP address only against an IP SAN.
            KeyStore.PrivateKeyEntry serverCert =
                    TestSslUtils.generateCertificateWithSans("localhost", "127.0.0.1");
            SelfSignedCertificate internalCert = TestSslUtils.generateCertificate("fluss-server");
            SelfSignedCertificate adminCert = TestSslUtils.generateCertificate("admin");
            SelfSignedCertificate guestCert = TestSslUtils.generateCertificate("guest");
            Path serverKeyStore =
                    TestSslUtils.createKeyStore(
                            TEMP_DIR,
                            "server.jks",
                            "JKS",
                            serverCert.getPrivateKey(),
                            serverCert.getCertificate());
            Path internalKeyStore =
                    TestSslUtils.createKeyStore(TEMP_DIR, "internal.jks", internalCert);
            ADMIN_KEYSTORE = TestSslUtils.createKeyStore(TEMP_DIR, "admin.jks", adminCert);
            GUEST_KEYSTORE = TestSslUtils.createKeyStore(TEMP_DIR, "guest.jks", guestCert);
            // One truststore for servers and clients.
            Path trustStore =
                    TestSslUtils.createTrustStore(
                            TEMP_DIR,
                            "truststore.jks",
                            "JKS",
                            serverCert.getCertificate(),
                            internalCert.cert(),
                            adminCert.cert(),
                            guestCert.cert());

            Configuration conf = new Configuration();
            TestSslUtils.setServerSslConfig(conf, serverKeyStore, trustStore);
            conf.setString(
                    ConfigOptions.SERVER_SSL_ENABLED_LISTENERS.key(),
                    INTERNAL_LISTENER + "," + CLIENT_LISTENER);
            conf.setString(
                    ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(),
                    INTERNAL_LISTENER + ":mTLS," + CLIENT_LISTENER + ":mTLS");
            conf.set(ConfigOptions.SERVER_SSL_PRINCIPAL_MAPPING_RULES, "RULE:^CN=([^,]+)$/$1/");
            // the servers' own client, for requests between servers.
            TestSslUtils.setClientSslConfig(conf, trustStore, internalKeyStore);
            conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "mTLS");
            conf.set(ConfigOptions.AUTHORIZER_ENABLED, true);
            conf.set(ConfigOptions.SUPER_USERS, "User:admin;User:fluss-server");
            conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
            CLUSTER =
                    FlussClusterExtension.builder()
                            .setNumOfTabletServers(3)
                            .setCoordinatorServerListeners(LISTENERS)
                            .setTabletServerListeners(LISTENERS)
                            .setClusterConf(conf)
                            .build();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final TestTableNames tableNames = new TestTableNames();

    @AfterAll
    static void afterAll() {
        FileUtils.deleteDirectoryQuietly(TEMP_DIR.toFile());
    }

    @Test
    void testSuperUserWritesAndServersReplicate() throws Exception {
        TablePath path = tableNames.tablePath("mtls", "log_table");
        try (Connection connection = connect(ADMIN_KEYSTORE);
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            createTable(admin, path);
            try (Table table = connection.getTable(path)) {
                TableBucket bucket = new TableBucket(table.getTableInfo().getTableId(), 0);
                CLUSTER.waitUntilAllReplicaReady(bucket);
                AppendWriter writer = table.newAppend().createWriter();
                for (int i = 0; i < 10; i++) {
                    writer.append(row(i, "value-" + i));
                }
                writer.flush();
                // Followers fetch the records from the leader over the mTLS internal listener.
                int leader = CLUSTER.waitAndGetLeader(bucket);
                for (int serverId = 0; serverId < 3; serverId++) {
                    if (serverId != leader) {
                        Replica follower = CLUSTER.waitAndGetFollowerReplica(bucket, serverId);
                        retry(
                                Duration.ofMinutes(1),
                                () -> assertThat(follower.getLocalLogEndOffset()).isEqualTo(10));
                    }
                }
            }
        }
    }

    @Test
    void testGuestNeedsAclGrant() throws Exception {
        TablePath path = tableNames.tablePath("mtls", "guest_table");
        try (Connection adminConnection = connect(ADMIN_KEYSTORE);
                Admin admin = adminConnection.getAdmin();
                Connection guestConnection = connect(GUEST_KEYSTORE);
                Admin guest = guestConnection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();

            assertThatThrownBy(() -> createTable(guest, path))
                    .cause()
                    .isInstanceOf(AuthorizationException.class)
                    .hasMessageContaining("name='guest'");

            admin.createAcls(
                            Collections.singletonList(
                                    new AclBinding(
                                            Resource.database(path.getDatabaseName()),
                                            new AccessControlEntry(
                                                    new FlussPrincipal("guest", "User"),
                                                    "*",
                                                    OperationType.CREATE,
                                                    PermissionType.ALLOW))))
                    .all()
                    .get();
            createTable(guest, path);
            assertThat(admin.tableExists(path).get()).isTrue();
        }
    }

    @Test
    void testInternalListenerRejectsRegularUser() {
        // guest has a trusted certificate, and the server skips ACL checks on this listener.
        assertThatThrownBy(
                        () -> {
                            try (Connection connection =
                                            connect(GUEST_KEYSTORE, INTERNAL_LISTENER);
                                    Admin admin = connection.getAdmin()) {
                                admin.listDatabases().get();
                            }
                        })
                .rootCause()
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("'User:guest'")
                .hasMessageContaining("internal listener '" + INTERNAL_LISTENER + "'");
    }

    private static Connection connect(Path keyStore) {
        return connect(keyStore, CLIENT_LISTENER);
    }

    /** A client connection to the given listener that presents the certificate in keyStore. */
    private static Connection connect(Path keyStore, String listener) {
        Configuration conf = CLUSTER.getClientConfig(listener);
        conf.setString(ConfigOptions.CLIENT_SSL_KEYSTORE_PATH.key(), keyStore.toString());
        return ConnectionFactory.createConnection(conf);
    }

    private static void createTable(Admin admin, TablePath path) throws Exception {
        admin.createTable(
                        path,
                        TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build(),
                        false)
                .get();
    }
}
