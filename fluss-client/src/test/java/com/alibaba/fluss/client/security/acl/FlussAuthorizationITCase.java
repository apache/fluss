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

package com.alibaba.fluss.client.security.acl;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.FlussConnection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.utils.ClientRpcMessageUtils;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.exception.AuthorizationException;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.security.acl.AccessControlEntry;
import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceFilter;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.fluss.shaded.guava32.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.security.acl.AccessControlEntry.WILD_CARD_HOST;
import static com.alibaba.fluss.security.acl.FlussPrincipal.WILD_CARD_PRINCIPAL;
import static com.alibaba.fluss.security.acl.OperationType.READ;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** It case to test authorization of admin operation、read and write operation. */
public class FlussAuthorizationITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(initConfig())
                    .build();

    private Connection rootConn;
    private Admin rootAdmin;
    private Connection guestConn;
    private Admin guestAdmin;
    private FlussPrincipal guestPrincipal;
    private Configuration guestConf;

    @BeforeEach
    protected void setup() throws Exception {
        Configuration conf = FLUSS_CLUSTER_EXTENSION.getClientConfig("CLIENT");
        conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "sasl");
        conf.set(ConfigOptions.CLIENT_SASL_MECHANISM, "plain");
        Configuration rootConf = new Configuration(conf);
        rootConf.setString("client.security.sasl.username", "root");
        rootConf.setString("client.security.sasl.password", "password");
        rootConn = ConnectionFactory.createConnection(rootConf);
        rootAdmin = rootConn.getAdmin();

        guestConf = new Configuration(conf);
        guestConf.setString("client.security.sasl.username", "guest");
        guestConf.setString("client.security.sasl.password", "password2");
        guestConn = ConnectionFactory.createConnection(guestConf);
        guestAdmin = guestConn.getAdmin();
        guestPrincipal = new FlussPrincipal("guest", "User");

        // prepare default database and table
        rootAdmin
                .createDatabase(
                        DATA1_TABLE_PATH_PK.getDatabaseName(), DatabaseDescriptor.EMPTY, true)
                .get();
        rootAdmin.createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK, true).get();
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (rootAdmin != null) {
            rootAdmin.dropAcls(Collections.singletonList(AclBindingFilter.ANY)).all().get();
            rootAdmin.close();
            rootAdmin = null;
        }

        if (rootConn != null) {
            rootConn.close();
            rootConn = null;
        }

        if (guestAdmin != null) {
            guestAdmin.close();
            guestAdmin = null;
        }

        if (guestConn != null) {
            guestConn.close();
            guestConn = null;
        }
    }

    @Test
    void testNoAuthorizer() throws Exception {
        Configuration configuration = initConfig();
        configuration.removeConfig(ConfigOptions.AUTHORIZER_ENABLED);

        FlussClusterExtension flussClusterExtension =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(1)
                        .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                        .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                        .setClusterConf(configuration)
                        .build();

        try {
            flussClusterExtension.start();
            Configuration conf = new Configuration(flussClusterExtension.getClientConfig("CLIENT"));
            conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "sasl");
            conf.set(ConfigOptions.CLIENT_SASL_MECHANISM, "plain");
            conf.setString("client.security.sasl.username", "root");
            conf.setString("client.security.sasl.password", "password");
            try (Connection connection = ConnectionFactory.createConnection(conf);
                    Admin admin = connection.getAdmin()) {
                assertThatThrownBy(() -> admin.listAcls(AclBindingFilter.ANY).get())
                        .hasMessageContaining("No Authorizer is configured.");
                assertThatThrownBy(
                                () ->
                                        admin.createAcls(
                                                        Collections.singletonList(
                                                                new AclBinding(
                                                                        Resource.cluster(),
                                                                        new AccessControlEntry(
                                                                                WILD_CARD_PRINCIPAL,
                                                                                WILD_CARD_HOST,
                                                                                OperationType
                                                                                        .CREATE,
                                                                                PermissionType
                                                                                        .ALLOW))))
                                                .all()
                                                .get())
                        .hasMessageContaining("No Authorizer is configured.");
                assertThatThrownBy(
                                () ->
                                        admin.dropAcls(
                                                        Collections.singletonList(
                                                                AclBindingFilter.ANY))
                                                .all()
                                                .get())
                        .hasMessageContaining("No Authorizer is configured.");

                // test initWriter without authorizer and empty table paths
                FlussConnection flussConnection = (FlussConnection) connection;
                TabletServerGateway tabletServerGateway =
                        flussConnection.getMetadataUpdater().newTabletServerClientForNode(0);
                InitWriterResponse response =
                        tabletServerGateway.initWriter(new InitWriterRequest()).get();
                assertThat(response.getWriterId()).isGreaterThanOrEqualTo(0);
            }

        } finally {
            flussClusterExtension.close();
        }
    }

    @Test
    void testAclOperation() throws Exception {
        // Test whether the user has authorization to perform the "list ACLs" operation.
        assertThat(guestAdmin.listAcls(AclBindingFilter.ANY).get()).isEmpty();
        List<AclBinding> aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.cluster(),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        WILD_CARD_HOST,
                                        OperationType.DESCRIBE,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        assertThat(guestAdmin.listAcls(AclBindingFilter.ANY).get()).hasSize(1);

        // test whether the user have authorization to operate create and drop acls.
        FlussPrincipal user1 = new FlussPrincipal("user1", "User");
        AclBinding user1AclBinding =
                new AclBinding(
                        Resource.table("test_db", "test_table"),
                        new AccessControlEntry(
                                user1, "*", OperationType.CREATE, PermissionType.ALLOW));
        List<AclBinding> noAuthorizationAclBinding =
                Arrays.asList(
                        user1AclBinding,
                        new AclBinding(
                                Resource.database("test_db2"),
                                new AccessControlEntry(
                                        new FlussPrincipal("ROLE", "test_role"),
                                        "127.0.0.1",
                                        OperationType.DROP,
                                        PermissionType.ANY)),
                        new AclBinding(
                                Resource.cluster(),
                                new AccessControlEntry(
                                        new FlussPrincipal("ROLE", "test_role"),
                                        "127.0.0.1",
                                        OperationType.DROP,
                                        PermissionType.ALLOW)));
        assertThatThrownBy(() -> guestAdmin.createAcls(noAuthorizationAclBinding).all().get())
                .hasMessageContaining(
                        "Principal %s have no authorization to operate ALTER on resource",
                        guestPrincipal);

        aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.cluster(),
                                new AccessControlEntry(
                                        WILD_CARD_PRINCIPAL,
                                        WILD_CARD_HOST,
                                        OperationType.ALTER,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        guestAdmin.createAcls(noAuthorizationAclBinding).all().get();

        assertThat(
                        guestAdmin
                                .listAcls(
                                        new AclBindingFilter(
                                                ResourceFilter.ANY,
                                                new AccessControlEntryFilter(
                                                        user1,
                                                        null,
                                                        OperationType.ANY,
                                                        PermissionType.ALLOW)))
                                .get())
                .containsExactlyInAnyOrderElementsOf(Collections.singleton(user1AclBinding));

        Collection<AclBinding> allAclBinds = rootAdmin.listAcls(AclBindingFilter.ANY).get();
        assertThat(guestAdmin.dropAcls(Collections.singletonList(AclBindingFilter.ANY)).all().get())
                .containsExactlyInAnyOrderElementsOf(allAclBinds);
        assertThat(rootAdmin.listAcls(AclBindingFilter.ANY).get()).isEmpty();
    }

    @Test
    void testAlterDatabase() throws Exception {
        assertThatThrownBy(
                        () ->
                                guestAdmin
                                        .createDatabase(
                                                "test-database1", DatabaseDescriptor.EMPTY, false)
                                        .get())
                .hasMessageContaining(
                        String.format(
                                "Principal %s have no authorization to operate CREATE on resource Resource{type=CLUSTER, name='fluss-cluster'}",
                                guestPrincipal));
        List<AclBinding> aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.cluster(),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        "*",
                                        OperationType.CREATE,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        guestAdmin.createDatabase("test-database2", DatabaseDescriptor.EMPTY, false).get();
        assertThat(rootAdmin.databaseExists("test-database1").get()).isFalse();
        assertThat(rootAdmin.databaseExists("test-database2").get()).isTrue();
    }

    @Test
    void testListDatabases() throws ExecutionException, InterruptedException {
        assertThat(guestAdmin.listDatabases().get())
                .containsExactlyInAnyOrderElementsOf(Collections.emptyList());
        assertThat(rootAdmin.listDatabases().get())
                .containsExactlyInAnyOrderElementsOf(
                        Lists.newArrayList("fluss", DATA1_TABLE_PATH_PK.getDatabaseName()));

        List<AclBinding> aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("fluss"),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        "*",
                                        OperationType.DESCRIBE,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(aclBindings, true);
        assertThat(guestAdmin.listDatabases().get()).isEqualTo(Collections.singletonList("fluss"));

        aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.cluster(),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        "*",
                                        OperationType.ALL,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(aclBindings, true);
        assertThat(guestAdmin.listDatabases().get())
                .containsExactlyInAnyOrderElementsOf(
                        Lists.newArrayList("fluss", DATA1_TABLE_PATH_PK.getDatabaseName()));
    }

    @Test
    void testAlterTable() throws Exception {
        assertThatThrownBy(
                        () ->
                                guestAdmin
                                        .createTable(
                                                DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false)
                                        .get())
                .hasMessageContaining(
                        String.format(
                                "Principal %s have no authorization to operate CREATE on resource Resource{type=DATABASE, name='test_db_1'}",
                                guestPrincipal));
        assertThat(rootAdmin.tableExists(DATA1_TABLE_PATH).get()).isFalse();

        List<AclBinding> aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.database(DATA1_TABLE_PATH.getDatabaseName()),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        "*",
                                        OperationType.CREATE,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        guestAdmin.createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false).get();
        assertThat(rootAdmin.tableExists(DATA1_TABLE_PATH).get()).isTrue();
    }

    @Test
    void testListTables() throws Exception {
        assertThat(guestAdmin.listTables(DATA1_TABLE_PATH_PK.getDatabaseName()).get())
                .isEqualTo(Collections.emptyList());

        List<AclBinding> aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.database(DATA1_TABLE_PATH_PK.getDatabaseName()),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        "*",
                                        OperationType.DESCRIBE,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(aclBindings, true);
        assertThat(guestAdmin.listTables(DATA1_TABLE_PATH_PK.getDatabaseName()).get())
                .isEqualTo(Collections.singletonList(DATA1_TABLE_PATH_PK.getTableName()));
    }

    @Test
    void testGetMetaInfo() throws Exception {
        MetadataRequest metadataRequest =
                ClientRpcMessageUtils.makeMetadataRequest(
                        Collections.singleton(DATA1_TABLE_PATH_PK), null, null);

        try (RpcClient rpcClient =
                RpcClient.create(guestConf, TestingClientMetricGroup.newInstance(), false)) {
            AdminGateway guestGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode("CLIENT"),
                            rpcClient,
                            AdminGateway.class);

            assertThat(guestGateway.metadata(metadataRequest).get().getTableMetadatasList())
                    .isEmpty();

            // if add acl to allow guest read any resource, it will allow to get metadata.
            List<AclBinding> aclBindings =
                    Collections.singletonList(
                            new AclBinding(
                                    Resource.table(DATA1_TABLE_PATH_PK),
                                    new AccessControlEntry(
                                            guestPrincipal,
                                            "*",
                                            OperationType.DESCRIBE,
                                            PermissionType.ALLOW)));
            rootAdmin.createAcls(aclBindings).all().get();
            FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(aclBindings, true);
            assertThat(guestGateway.metadata(metadataRequest).get().getTableMetadatasList())
                    .hasSize(1);
        }
    }

    @Test
    void testInitWriter() throws Exception {
        TablePath writeAclTable = TablePath.of("test_db_1", "write_acl_table");
        TablePath noWriteAclTable = TablePath.of("test_db_1", "no_write_acl_table");

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build();
        rootAdmin.createTable(writeAclTable, descriptor, false).get();
        TableInfo tableInfo = rootAdmin.getTableInfo(writeAclTable).get();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableInfo.getTableId());
        // create acl to allow guest write.
        List<AclBinding> aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.table(writeAclTable),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        "*",
                                        OperationType.WRITE,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(aclBindings, true);

        FlussConnection flussConnection = (FlussConnection) guestConn;
        TabletServerGateway tabletServerGateway =
                flussConnection.getMetadataUpdater().newTabletServerClientForNode(0);

        // test 1: empty table paths
        assertThatThrownBy(() -> tabletServerGateway.initWriter(new InitWriterRequest()).get())
                .cause()
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining(
                        "The request of InitWriter requires non empty table paths for authorization.");

        // request contains a table path without permission
        InitWriterRequest noAclRequest = new InitWriterRequest();
        noAclRequest
                .addTablePath()
                .setDatabaseName(noWriteAclTable.getDatabaseName())
                .setTableName(noWriteAclTable.getTableName());

        // test 2: no table has write permission
        assertThatThrownBy(() -> tabletServerGateway.initWriter(noAclRequest).get())
                .cause()
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining(
                        "No WRITE permission among all the tables: [test_db_1.no_write_acl_table]");

        // request contains both a table path with/without permission
        InitWriterRequest request = new InitWriterRequest();
        request.addTablePath()
                .setTableName(writeAclTable.getTableName())
                .setDatabaseName(writeAclTable.getDatabaseName());
        request.addTablePath()
                .setTableName(noWriteAclTable.getTableName())
                .setDatabaseName(noWriteAclTable.getDatabaseName());

        // test 3: one table has write permission, the other doesn't have permission
        InitWriterResponse response = tabletServerGateway.initWriter(request).get();
        assertThat(response.getWriterId()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testProduceWithNoWriteAuthorization() throws Exception {
        TablePath writeAclTable = TablePath.of("test_db_1", "write_acl_table_1");
        TablePath noWriteAclTable = TablePath.of("test_db_1", "no_write_acl_table_1");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build();
        rootAdmin.createTable(writeAclTable, descriptor, false).get();
        rootAdmin.createTable(noWriteAclTable, descriptor, false).get();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(
                rootAdmin.getTableInfo(writeAclTable).get().getTableId());
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(
                rootAdmin.getTableInfo(noWriteAclTable).get().getTableId());

        // create acl to allow guest write for writeAclTable.
        List<AclBinding> aclBindingOfWriteAclTables =
                Collections.singletonList(
                        new AclBinding(
                                Resource.table(writeAclTable),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        "*",
                                        OperationType.WRITE,
                                        PermissionType.ALLOW)));
        List<AclBinding> aclBindingOfNoWriteAclTables =
                Collections.singletonList(
                        new AclBinding(
                                Resource.table(noWriteAclTable),
                                new AccessControlEntry(
                                        guestPrincipal, "*", READ, PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindingOfWriteAclTables).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(aclBindingOfWriteAclTables, true);
        rootAdmin.createAcls(aclBindingOfNoWriteAclTables).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(aclBindingOfNoWriteAclTables, true);

        // 1. Try to write data to noWriteAclTable. It should throw AuthorizationException because
        // of request writeId failed.
        try (Table table = guestConn.getTable(noWriteAclTable)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            assertThatThrownBy(() -> appendWriter.append(row(1, "a")).get())
                    .hasRootCauseInstanceOf(AuthorizationException.class)
                    .rootCause()
                    .hasMessageContaining(
                            String.format(
                                    "No WRITE permission among all the tables: %s",
                                    Collections.singletonList(noWriteAclTable)));
        }

        // 2. Try to write data to writeAclTable. It will success and writeId will be set.
        try (Table table = guestConn.getTable(writeAclTable)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row(1, "a")).get();
        }

        // 3. Try to write data to writeAclTable again. It will throw AuthorizationException because
        // of no write permission.
        // Note: If guestUser have permission for table lists: [writeAclTable, noWriteAclTable].
        // When we give WRITE permission to writeAclTable for guestUser, guestUser will have
        // INIT_WRITER permission for both writeAclTable and noWriteAclTable.
        // In this case, when guestUser try to write noWriteAclTable, Fluss client can get writerId
        // but can not to write to noWriteAclTable because of no WRITE permission.
        try (Table table = guestConn.getTable(noWriteAclTable)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            assertThatThrownBy(() -> appendWriter.append(row(1, "a")).get())
                    .hasRootCauseInstanceOf(AuthorizationException.class)
                    .rootCause()
                    .hasMessageContaining(
                            String.format(
                                    "No permission to WRITE table %s in database %s",
                                    noWriteAclTable.getTableName(),
                                    noWriteAclTable.getDatabaseName()));
        }
    }

    @Test
    void testProduceAndConsumer() throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build();
        rootAdmin.createTable(DATA1_TABLE_PATH, descriptor, false).get();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(
                rootAdmin.getTableInfo(DATA1_TABLE_PATH).get().getTableId());
        // create acl to allow guest write.
        List<AclBinding> aclBindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.table(DATA1_TABLE_PATH),
                                new AccessControlEntry(
                                        guestPrincipal,
                                        "*",
                                        OperationType.WRITE,
                                        PermissionType.ALLOW)));
        rootAdmin.createAcls(aclBindings).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(aclBindings, true);
        try (Table table = guestConn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row(1, "a")).get();

            try (BatchScanner batchScanner =
                    table.newScan()
                            .limit(1)
                            .createBatchScanner(
                                    new TableBucket(table.getTableInfo().getTableId(), 0))) {
                assertThatThrownBy(() -> batchScanner.pollBatch(Duration.ofMinutes(1)))
                        .hasMessageContaining(
                                String.format(
                                        "No permission to %s table %s in database %s",
                                        READ,
                                        DATA1_TABLE_PATH.getTableName(),
                                        DATA1_TABLE_PATH.getDatabaseName()));
            }
            rootAdmin
                    .createAcls(
                            Collections.singletonList(
                                    new AclBinding(
                                            Resource.table(DATA1_TABLE_PATH),
                                            new AccessControlEntry(
                                                    guestPrincipal,
                                                    "*",
                                                    READ,
                                                    PermissionType.ALLOW))))
                    .all()
                    .get();

            // wait for acl notify to tablet server.
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            catchThrowable(
                                                    (() -> {
                                                        try (BatchScanner batchScanner =
                                                                table.newScan()
                                                                        .limit(1)
                                                                        .createBatchScanner(
                                                                                new TableBucket(
                                                                                        table.getTableInfo()
                                                                                                .getTableId(),
                                                                                        0))) {
                                                            CloseableIterator<InternalRow>
                                                                    internalRowCloseableIterator =
                                                                            batchScanner.pollBatch(
                                                                                    Duration
                                                                                            .ofMinutes(
                                                                                                    1));
                                                            assertThat(internalRowCloseableIterator)
                                                                    .hasNext();
                                                            assertThat(
                                                                            internalRowCloseableIterator
                                                                                    .next())
                                                                    .isEqualTo(row(1, "a"));
                                                        }
                                                    })))
                                    .doesNotThrowAnyException());
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter max lag time to make tests in FlussFailServerTableITCase faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        // set security information.
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
        conf.setString("security.sasl.enabled.mechanisms", "plain");
        conf.setString(
                "security.sasl.plain.jaas.config",
                "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_root=\"password\" "
                        + "    user_guest=\"password2\";");
        conf.set(ConfigOptions.SUPER_USERS, "User:root");
        conf.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        return conf;
    }
}
