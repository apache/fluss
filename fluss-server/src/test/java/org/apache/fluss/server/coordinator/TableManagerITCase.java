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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.GetTableSchemaRequest;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbAlterConfig;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateDatabaseRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDatabaseExistsRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropDatabaseRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newListTablesRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newMetadataRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newTableExistsRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toServerNode;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTablePath;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.apache.fluss.utils.PartitionUtils.generateAutoPartition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link TableManager}. */
class TableManagerITCase {
    public static final String CLIENT_LISTENER = "CLIENT";

    private ZooKeeperClient zkClient;
    private Configuration clientConf;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setCoordinatorServerListeners(
                            String.format(
                                    "%s://localhost:0, %s://localhost:0",
                                    DEFAULT_LISTENER_NAME, CLIENT_LISTENER))
                    .setTabletServerListeners(
                            String.format(
                                    "%s://localhost:0, %s://localhost:0",
                                    DEFAULT_LISTENER_NAME, CLIENT_LISTENER))
                    .setClusterConf(initConf())
                    .build();

    private static Configuration initConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL, Duration.ofSeconds(1));
        return conf;
    }

    @BeforeEach
    void setup() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
    }

    @Test
    void testCreateInvalidDatabaseAndTable() {
        AdminGateway adminGateway = getAdminGateway();
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createDatabase(
                                                newCreateDatabaseRequest("*invalid_db*", true))
                                        .get())
                .cause()
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining(
                        "Database name *invalid_db* is invalid: '*invalid_db*' contains one or more characters other than");
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createTable(
                                                newCreateTableRequest(
                                                        new TablePath("db", "=invalid_table!"),
                                                        newPkTable(),
                                                        true))
                                        .get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Table name =invalid_table! is invalid: '=invalid_table!' contains one or more characters other than");
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createTable(
                                                newCreateTableRequest(
                                                        new TablePath("", "=invalid_table!"),
                                                        newPkTable(),
                                                        true))
                                        .get())
                .cause()
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining("Database name  is invalid: the empty string is not allowed");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDatabaseManagement(boolean isCoordinatorServer) throws Exception {
        AdminReadOnlyGateway gateway = getAdminOnlyGateway(isCoordinatorServer);
        String db1 = "db1";
        assertThat(gateway.databaseExists(newDatabaseExistsRequest(db1)).get().isExists())
                .isFalse();

        AdminGateway adminGateway = getAdminGateway();
        // create the database, should success

        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        // check it again
        assertThat(gateway.databaseExists(newDatabaseExistsRequest(db1)).get().isExists()).isTrue();

        // now, should throw exception when create it again
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createDatabase(newCreateDatabaseRequest(db1, false))
                                        .get())
                .cause()
                .isInstanceOf(DatabaseAlreadyExistException.class)
                .hasMessageContaining("Database db1 already exists.");

        // with ignore if exists, shouldn't throw exception again
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, true)).get();

        // create another database
        String db2 = "db2";
        adminGateway.createDatabase(newCreateDatabaseRequest(db2, false)).get();

        // list database
        assertThat(gateway.listDatabases(new ListDatabasesRequest()).get().getDatabaseNamesList())
                .containsExactlyInAnyOrderElementsOf(Arrays.asList(db1, db2, "fluss"));

        // list the table, should be empty
        assertThat(gateway.listTables(newListTablesRequest(db1)).get().getTableNamesList())
                .isEmpty();

        // list a not exist database, should throw exception
        assertThatThrownBy(() -> gateway.listTables(newListTablesRequest("not_exist")).get())
                .cause()
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessageContaining("Database not_exist does not exist.");

        // now drop one database, should success
        adminGateway.dropDatabase(newDropDatabaseRequest(db1, false, true)).get();

        assertThat(gateway.listDatabases(new ListDatabasesRequest()).get().getDatabaseNamesList())
                .isEqualTo(Arrays.asList(db2, "fluss"));

        // drop a not exist database without ignore if not exists, should throw exception
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .dropDatabase(newDropDatabaseRequest(db1, false, true))
                                        .get())
                .cause()
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessageContaining("Database db1 does not exist.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTableManagement(boolean isCoordinatorServer) throws Exception {
        AdminReadOnlyGateway gateway = getAdminOnlyGateway(isCoordinatorServer);
        AdminGateway adminGateway = getAdminGateway();

        String db1 = "db1";
        String tb1 = "tb1";
        TablePath tablePath = TablePath.of(db1, tb1);
        // first create a database
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        // check no table exist
        assertThat(gateway.listTables(newListTablesRequest(db1)).get().getTableNamesList())
                .isEmpty();

        // check the table is not exist
        assertThat(gateway.tableExists(newTableExistsRequest(tablePath)).get().isExists())
                .isFalse();

        // drop a not exist table without ignore if not exists should throw exception
        assertThatThrownBy(() -> adminGateway.dropTable(newDropTableRequest(db1, tb1, false)).get())
                .cause()
                .isInstanceOf(TableNotExistException.class)
                .hasMessageContaining(String.format("Table %s does not exist.", tablePath));

        // drop a not exist table with ignore if not exists shouldn't throw exception
        adminGateway.dropTable(newDropTableRequest(db1, tb1, true)).get();

        // then create a table
        TableDescriptor tableDescriptor = newPkTable();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        // the table should exist then
        assertThat(gateway.tableExists(newTableExistsRequest(tablePath)).get().isExists()).isTrue();

        // get the table and check it
        GetTableInfoResponse response =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTable = TableDescriptor.fromJsonBytes(response.getTableJson());
        assertThat(gottenTable).isEqualTo(tableDescriptor.withReplicationFactor(1));

        // alter table
        Map<String, String> setProperties = new HashMap<>();
        setProperties.put("client.connect-timeout", "240s");

        List<String> resetProperties = new ArrayList<>();

        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                alterTableProperties(setProperties, resetProperties),
                                false))
                .get();
        // get the table and check it
        GetTableInfoResponse responseAfterAlter =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTableAfterAlter =
                TableDescriptor.fromJsonBytes(responseAfterAlter.getTableJson());

        String valueAfterAlter =
                gottenTableAfterAlter.getCustomProperties().get("client.connect-timeout");
        assertThat(valueAfterAlter).isEqualTo("240s");

        // check assignment, just check replica numbers, don't care about actual assignment
        checkAssignmentWithReplicaFactor(
                zkClient.getTableAssignment(response.getTableId()).get(),
                clientConf.getInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR));

        // then get the schema and check it
        GetTableSchemaRequest getSchemaRequest = new GetTableSchemaRequest();
        getSchemaRequest
                .setSchemaId(response.getSchemaId())
                .setTablePath()
                .setDatabaseName(db1)
                .setTableName(tb1);
        Schema gottenSchema =
                Schema.fromJsonBytes(
                        gateway.getTableSchema(getSchemaRequest).get().getSchemaJson());
        assertThat(gottenSchema).isEqualTo(tableDescriptor.getSchema());

        // then create the table with same name again, should throw exception
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createTable(
                                                newCreateTableRequest(
                                                        tablePath, tableDescriptor, false))
                                        .get())
                .cause()
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining(String.format("Table %s already exists.", tablePath));

        // create the table with same name again with ignoreIfExists = true,
        // should success
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, true)).get();

        // create another table without setting distribution
        String tb2 = "tb2";
        TableDescriptor tableDescriptor1 = newTableWithoutSettingDistribution();
        adminGateway
                .createTable(
                        newCreateTableRequest(new TablePath(db1, tb2), tableDescriptor1, false))
                .get();

        // check assignment, just check bucket number, it should be equal to the default bucket
        // number
        // configured in cluster-level
        response = gateway.getTableInfo(newGetTableInfoRequest(new TablePath(db1, tb2))).get();
        TableAssignment tableAssignment = zkClient.getTableAssignment(response.getTableId()).get();
        assertThat(tableAssignment.getBucketAssignments().size())
                .isEqualTo(clientConf.getInt(ConfigOptions.DEFAULT_BUCKET_NUMBER));

        // check drop database with should fail
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .dropDatabase(newDropDatabaseRequest(db1, false, false))
                                        .get())
                .cause()
                .isInstanceOf(DatabaseNotEmptyException.class)
                .hasMessageContaining(String.format("Database %s is not empty.", db1));

        // then drop the table, should success
        adminGateway.dropTable(newDropTableRequest(db1, tb1, false)).get();

        // check the schema is deleted, should throw schema not existed exception
        assertThatThrownBy(() -> gateway.getTableSchema(getSchemaRequest).get())
                .cause()
                .isInstanceOf(SchemaNotExistException.class)
                .hasMessageContaining(
                        String.format(
                                "Schema for table %s with schema id %s does not exist.",
                                tablePath, response.getSchemaId()));
    }

    @ParameterizedTest
    @EnumSource(AutoPartitionTimeUnit.class)
    void testPartitionedTableManagement(AutoPartitionTimeUnit timeUnit) throws Exception {
        AdminGateway adminGateway = getAdminGateway();
        String db1 = "db1";
        String tb1 = "tb1_" + timeUnit.name();
        TablePath tablePath = TablePath.of(db1, tb1);
        // first create a database
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        Instant now = Instant.now();
        // then create a partitioned table
        Map<String, String> options = new HashMap<>();
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), timeUnit.name());
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(), "1");
        TableDescriptor tableDescriptor = newPartitionedTable().withProperties(options);
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        // wait until partition is created
        Map<String, Long> partitions =
                waitValue(
                        () -> {
                            Map<String, Long> gotPartitions =
                                    zkClient.getPartitionNameAndIds(tablePath);
                            if (!gotPartitions.isEmpty()) {
                                return Optional.of(gotPartitions);
                            } else {
                                return Optional.empty();
                            }
                        },
                        Duration.ofMinutes(1),
                        "partition is not created");
        // check the created partitions
        List<String> expectAddedPartitions =
                getExpectAddedPartitions(Collections.singletonList("dt"), now, timeUnit, 1);
        assertThat(partitions).containsOnlyKeys(expectAddedPartitions);

        // let's drop the table
        adminGateway.dropTable(newDropTableRequest(db1, tb1, false)).get();
        assertThat(zkClient.getPartitions(tablePath)).isEmpty();

        // create a non-auto-partitioned table
        adminGateway
                .createTable(
                        newCreateTableRequest(
                                tablePath,
                                newPartitionedTable().withProperties(new HashMap<>()),
                                false))
                .get();

        // verify the partition assignment is deleted
        for (Long partitionId : partitions.values()) {
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(zkClient.getPartitionAssignment(partitionId)).isEmpty());
        }

        // make sure the auto partition manager won't create partitions for the new table
        assertThat(zkClient.getPartitions(tablePath)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMetadata(boolean isCoordinatorServer) throws Exception {
        AdminReadOnlyGateway gateway = getAdminOnlyGateway(isCoordinatorServer);
        AdminGateway adminGateway = getAdminGateway();

        String db1 = "db1";
        String tb1 = "tb1";
        TablePath tablePath = TablePath.of(db1, tb1);
        // first create a database
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();
        TableDescriptor tableDescriptor = newPkTable();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
        GetTableInfoResponse response =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        long tableId = response.getTableId();

        // retry until all replica ready.
        int expectBucketCount = tableDescriptor.getTableDistribution().get().getBucketCount().get();
        for (int i = 0; i < expectBucketCount; i++) {
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(new TableBucket(tableId, i));
        }

        // retry to check metadata.
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        MetadataResponse metadataResponse =
                gateway.metadata(newMetadataRequest(Collections.singletonList(tablePath))).get();
        // should be no tablet server as we only create tablet service.
        assertThat(metadataResponse.getTabletServersCount()).isEqualTo(3);

        assertThat(metadataResponse.getTableMetadatasCount()).isEqualTo(1);
        PbTableMetadata tableMetadata = metadataResponse.getTableMetadataAt(0);
        assertThat(toTablePath(tableMetadata.getTablePath())).isEqualTo(tablePath);
        assertThat(TableDescriptor.fromJsonBytes(tableMetadata.getTableJson()))
                .isEqualTo(tableDescriptor.withReplicationFactor(1));

        // now, check the table buckets metadata
        assertThat(tableMetadata.getBucketMetadatasCount()).isEqualTo(expectBucketCount);

        List<ServerInfo> tabletServerInfos = FLUSS_CLUSTER_EXTENSION.getTabletServerInfos();
        ServerInfo coordinatorServerInfo = FLUSS_CLUSTER_EXTENSION.getCoordinatorServerInfo();

        checkBucketMetadata(expectBucketCount, tableMetadata.getBucketMetadatasList());

        // now, assuming we send update metadata request to the server,
        // we should get the same response
        if (!isCoordinatorServer) {
            ((TabletServerGateway) gateway)
                    .updateMetadata(
                            makeUpdateMetadataRequest(
                                    coordinatorServerInfo,
                                    null,
                                    new HashSet<>(tabletServerInfos),
                                    Collections.emptyList(),
                                    Collections.emptyList()))
                    .get();
        }

        // test lookup metadata from internal view

        metadataResponse =
                gateway.metadata(newMetadataRequest(Collections.singletonList(tablePath))).get();
        // check coordinator server
        assertThat(toServerNode(metadataResponse.getCoordinatorServer(), ServerType.COORDINATOR))
                .isEqualTo(coordinatorServerInfo.node(DEFAULT_LISTENER_NAME));
        assertThat(metadataResponse.getTabletServersCount()).isEqualTo(3);
        List<ServerNode> tsNodes =
                metadataResponse.getTabletServersList().stream()
                        .map(n -> toServerNode(n, ServerType.TABLET_SERVER))
                        .collect(Collectors.toList());
        assertThat(tsNodes)
                .containsExactlyInAnyOrderElementsOf(
                        FLUSS_CLUSTER_EXTENSION.getTabletServerNodes());

        // test lookup metadata from client view with another client(because same uid will reuse
        // same connection)
        Configuration configuration = new Configuration();
        try (RpcClient rpcClient =
                RpcClient.create(
                        configuration,
                        new ClientMetricGroup(
                                MetricRegistry.create(configuration, null),
                                "fluss-cluster-extension"),
                        false)) {
            ServerNode serverNode =
                    FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode(CLIENT_LISTENER);
            AdminGateway adminGatewayForClient =
                    GatewayClientProxy.createGatewayProxy(
                            () -> serverNode, rpcClient, CoordinatorGateway.class);
            metadataResponse =
                    adminGatewayForClient
                            .metadata(newMetadataRequest(Collections.singletonList(tablePath)))
                            .get();
            // check coordinator server
            assertThat(
                            toServerNode(
                                    metadataResponse.getCoordinatorServer(),
                                    ServerType.COORDINATOR))
                    .isEqualTo(coordinatorServerInfo.node(CLIENT_LISTENER));
            assertThat(metadataResponse.getTabletServersCount()).isEqualTo(3);
            tsNodes =
                    metadataResponse.getTabletServersList().stream()
                            .map(n -> toServerNode(n, ServerType.TABLET_SERVER))
                            .collect(Collectors.toList());
            assertThat(tsNodes)
                    .containsExactlyInAnyOrderElementsOf(
                            FLUSS_CLUSTER_EXTENSION.getTabletServerNodes(CLIENT_LISTENER));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMetadataWithPartition(boolean isCoordinatorServer) throws Exception {
        AdminReadOnlyGateway gateway = getAdminOnlyGateway(isCoordinatorServer);
        AdminGateway adminGateway = getAdminGateway();
        String db1 = "db1";
        String tb1 = "tb1";
        // create a partitioned table, and request a not exist partition, should throw partition not
        // exist exception
        TablePath tablePath = TablePath.of(db1, tb1);
        TableDescriptor tableDescriptor = newPartitionedTable();
        // first create a database
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();
        int expectBucketCount = tableDescriptor.getTableDistribution().get().getBucketCount().get();

        Map<String, Long> partitionById =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(tablePath, 1);

        for (long partitionId : partitionById.values()) {
            for (int i = 0; i < expectBucketCount; i++) {
                FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                        new TableBucket(tableId, partitionId, i));
            }
        }

        MetadataRequest metadataRequest = new MetadataRequest();
        for (String partition : partitionById.keySet()) {
            metadataRequest
                    .addPartitionsPath()
                    .setDatabaseName(db1)
                    .setTableName(tb1)
                    .setPartitionName(partition);
        }
        MetadataResponse metadataResponse = gateway.metadata(metadataRequest).get();

        List<PbPartitionMetadata> partitionMetadata = metadataResponse.getPartitionMetadatasList();
        assertThat(partitionMetadata.size()).isEqualTo(partitionById.size());

        for (PbPartitionMetadata partition : partitionMetadata) {
            assertThat(partition.getPartitionName()).isIn(partitionById.keySet());
            assertThat(partition.getPartitionId())
                    .isEqualTo(partitionById.get(partition.getPartitionName()));
            assertThat(partition.getTableId()).isEqualTo(tableId);
            checkBucketMetadata(expectBucketCount, partition.getBucketMetadatasList());
        }

        assertThatThrownBy(
                        () -> {
                            MetadataRequest partitionMetadataRequest = new MetadataRequest();
                            partitionMetadataRequest
                                    .addPartitionsPath()
                                    .setDatabaseName(db1)
                                    .setTableName("partitioned_tb")
                                    .setPartitionName("not_exist_partition");
                            gateway.metadata(partitionMetadataRequest).get();
                        })
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        "Table partition 'db1.partitioned_tb(p=not_exist_partition)' does not exist.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMetadataCompatibility(boolean isCoordinatorServer) throws Exception {
        AdminReadOnlyGateway gateway = getAdminOnlyGateway(isCoordinatorServer);

        List<ServerInfo> tabletServerInfos = FLUSS_CLUSTER_EXTENSION.getTabletServerInfos();
        ServerInfo coordinatorServerInfo = FLUSS_CLUSTER_EXTENSION.getCoordinatorServerInfo();

        // now, assuming we send update metadata request to the server,
        // we should get the same response
        if (!isCoordinatorServer) {
            ((TabletServerGateway) gateway)
                    .updateMetadata(
                            makeLegacyUpdateMetadataRequest(
                                    Optional.of(coordinatorServerInfo),
                                    new HashSet<>(tabletServerInfos)))
                    .get();
        }

        // test lookup metadata
        AdminGateway adminGatewayForClient = getAdminGateway();
        MetadataResponse metadataResponse =
                adminGatewayForClient.metadata(newMetadataRequest(Collections.emptyList())).get();
        // check coordinator server
        assertThat(toServerNode(metadataResponse.getCoordinatorServer(), ServerType.COORDINATOR))
                .isEqualTo(coordinatorServerInfo.node(DEFAULT_LISTENER_NAME));
        assertThat(metadataResponse.getTabletServersCount()).isEqualTo(3);
        List<ServerNode> tsNodes =
                metadataResponse.getTabletServersList().stream()
                        .map(n -> toServerNode(n, ServerType.TABLET_SERVER))
                        .collect(Collectors.toList());
        assertThat(tsNodes)
                .containsExactlyInAnyOrderElementsOf(
                        FLUSS_CLUSTER_EXTENSION.getTabletServerNodes());
    }

    private void checkBucketMetadata(int expectBucketCount, List<PbBucketMetadata> bucketMetadata) {
        Set<Integer> liveServers =
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().stream()
                        .map(ServerNode::id)
                        .collect(Collectors.toSet());
        for (int i = 0; i < expectBucketCount; i++) {
            PbBucketMetadata tableBucketMetadata = bucketMetadata.get(i);
            assertThat(tableBucketMetadata.getBucketId()).isEqualTo(i);
            assertThat(tableBucketMetadata.hasLeaderId()).isTrue();

            // assert replicas
            Set<Integer> allReplicas = new HashSet<>();
            for (int replicaIdx = 0;
                    replicaIdx < tableBucketMetadata.getReplicaIdsCount();
                    replicaIdx++) {
                allReplicas.add(tableBucketMetadata.getReplicaIdAt(replicaIdx));
            }
            // assert replica count
            assertThat(allReplicas.size())
                    .isEqualTo(clientConf.getInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR));
            // assert replica is in the live servers
            for (int replica : allReplicas) {
                assertThat(liveServers.contains(replica)).isTrue();
            }
        }
    }

    private AdminReadOnlyGateway getAdminOnlyGateway(boolean isCoordinatorServer) {
        if (isCoordinatorServer) {
            return getAdminGateway();
        } else {
            return FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(0);
        }
    }

    private AdminGateway getAdminGateway() {
        return FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
    }

    public static List<String> getExpectAddedPartitions(
            List<String> partitionKeys,
            Instant addInstant,
            AutoPartitionTimeUnit timeUnit,
            int newPartitions) {
        ZonedDateTime addDateTime = ZonedDateTime.ofInstant(addInstant, ZoneId.systemDefault());
        List<String> partitions = new ArrayList<>();
        for (int i = 0; i < newPartitions; i++) {
            partitions.add(
                    generateAutoPartition(partitionKeys, addDateTime, i, timeUnit)
                            .getPartitionName());
        }
        return partitions;
    }

    private static void checkAssignmentWithReplicaFactor(
            TableAssignment tableAssignment, int expectedReplicaFactor) {
        for (BucketAssignment bucketAssignment : tableAssignment.getBucketAssignments().values()) {
            assertThat(bucketAssignment.getReplicas().size()).isEqualTo(expectedReplicaFactor);
        }
    }

    private static TableDescriptor newTableWithoutSettingDistribution() {
        return TableDescriptor.builder().schema(newPkSchema()).comment("first table").build();
    }

    private static TableDescriptor newPartitionedTable() {
        return newPartitionedTableBuilder(null).build();
    }

    private static TableDescriptor.Builder newPartitionedTableBuilder(
            @Nullable Schema.Column extraColumn) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .withComment("id comment")
                        .column("dt", DataTypes.STRING())
                        .column("a", DataTypes.BIGINT())
                        .column("ts", DataTypes.TIMESTAMP());
        if (extraColumn != null) {
            builder.column(extraColumn.getName(), extraColumn.getDataType());
            builder.primaryKey("id", "dt", extraColumn.getName());
        } else {
            builder.primaryKey("id", "dt");
        }
        return TableDescriptor.builder()
                .schema(builder.build())
                .comment("partitioned table")
                .distributedBy(3)
                .partitionedBy("dt")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
                .property(
                        ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(),
                        AutoPartitionTimeUnit.DAY.name())
                .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 1);
    }

    private static TableDescriptor newPkTable() {
        return TableDescriptor.builder()
                .schema(newPkSchema())
                .comment("first table")
                .distributedBy(3, "a")
                .build();
    }

    private static List<PbAlterConfig> alterTableProperties(
            Map<String, String> setProperties, List<String> resetProperties) {
        List<PbAlterConfig> res = new ArrayList<>();

        for (Map.Entry<String, String> entry : setProperties.entrySet()) {
            PbAlterConfig info = new PbAlterConfig();
            info.setConfigKey(entry.getKey());
            info.setConfigValue(entry.getValue());
            info.setOpType(AlterConfigOpType.SET.value());
            res.add(info);
        }

        for (String resetProperty : resetProperties) {
            PbAlterConfig info = new PbAlterConfig();
            info.setConfigKey(resetProperty);
            info.setOpType(AlterConfigOpType.DELETE.value());
            res.add(info);
        }

        return res;
    }

    private static Schema newPkSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .withComment("a comment")
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .build();
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private UpdateMetadataRequest makeLegacyUpdateMetadataRequest(
            Optional<ServerInfo> coordinatorServer, Set<ServerInfo> aliveTableServers) {
        UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest();
        Set<PbServerNode> aliveTableServerNodes = new HashSet<>();
        for (ServerInfo serverInfo : aliveTableServers) {
            // Legacy only support one endpoint
            Endpoint endpoint = serverInfo.endpoints().get(0);
            PbServerNode pbServerNode =
                    new PbServerNode()
                            .setNodeId(serverInfo.id())
                            .setHost(endpoint.getHost())
                            .setPort(endpoint.getPort());
            if (serverInfo.rack() != null) {
                pbServerNode.setRack(serverInfo.rack());
            }
            aliveTableServerNodes.add(pbServerNode);
        }
        updateMetadataRequest.addAllTabletServers(aliveTableServerNodes);
        // Legacy only support one endpoint
        coordinatorServer.map(
                node -> {
                    Endpoint endpoint = node.endpoints().get(0);
                    updateMetadataRequest
                            .setCoordinatorServer()
                            .setNodeId(node.id())
                            .setHost(endpoint.getHost())
                            .setPort(endpoint.getPort());
                    return null;
                });
        return updateMetadataRequest;
    }

    // Test methods for table creation restrictions

    @Test
    void testLogTableCreationRestriction() throws Exception {
        // Test with cluster that disallows log table creation
        FlussClusterExtension kvCluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(3)
                        .setCoordinatorServerListeners(
                                String.format(
                                        "%s://localhost:0, %s://localhost:0",
                                        DEFAULT_LISTENER_NAME, CLIENT_LISTENER))
                        .setTabletServerListeners(
                                String.format(
                                        "%s://localhost:0, %s://localhost:0",
                                        DEFAULT_LISTENER_NAME, CLIENT_LISTENER))
                        .setClusterConf(initLogRestrictedConf())
                        .build();

        try {
            kvCluster.start();

            AdminGateway kvClusterGateway = kvCluster.newCoordinatorClient();

            String tb1 = "log_table";
            TablePath tablePath = TablePath.of("fluss", tb1);

            // Try to create a log table (table without primary key), should fail
            TableDescriptor logTableDescriptor = newLogTable();
            assertThatThrownBy(
                            () ->
                                    kvClusterGateway
                                            .createTable(
                                                    newCreateTableRequest(
                                                            tablePath, logTableDescriptor, false))
                                            .get())
                    .cause()
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessageContaining("Creation of Log Tables is disallowed in the cluster.");

            // Try to create a kv table (table with primary key), should succeed
            String tb2 = "kv_table";
            TablePath kvTablePath = TablePath.of("fluss", tb2);
            TableDescriptor kvTableDescriptor = newPkTable();
            kvClusterGateway
                    .createTable(newCreateTableRequest(kvTablePath, kvTableDescriptor, false))
                    .get();

            // Verify the kv table was created successfully
            assertThat(
                            kvClusterGateway
                                    .tableExists(newTableExistsRequest(kvTablePath))
                                    .get()
                                    .isExists())
                    .isTrue();
        } finally {
            kvCluster.close();
        }
    }

    @Test
    void testKvTableCreationRestriction() throws Exception {
        // Test with cluster that disallows kv table creation
        FlussClusterExtension logCluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(3)
                        .setCoordinatorServerListeners(
                                String.format(
                                        "%s://localhost:0, %s://localhost:0",
                                        DEFAULT_LISTENER_NAME, CLIENT_LISTENER))
                        .setTabletServerListeners(
                                String.format(
                                        "%s://localhost:0, %s://localhost:0",
                                        DEFAULT_LISTENER_NAME, CLIENT_LISTENER))
                        .setClusterConf(initKvRestrictedConf())
                        .build();

        try {
            logCluster.start();
            AdminGateway logClusterGateway = logCluster.newCoordinatorClient();

            String tb1 = "kv_table";
            TablePath tablePath = TablePath.of("fluss", tb1);

            // Try to create a kv table (table with primary key), should fail
            TableDescriptor kvTableDescriptor = newPkTable();
            assertThatThrownBy(
                            () ->
                                    logClusterGateway
                                            .createTable(
                                                    newCreateTableRequest(
                                                            tablePath, kvTableDescriptor, false))
                                            .get())
                    .cause()
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessageContaining(
                            "Creation of Primary Key Tables is disallowed in the cluster.");

            // Try to create a log table (table without primary key), should succeed
            String tb2 = "log_table";
            TablePath logTablePath = TablePath.of("fluss", tb2);
            TableDescriptor logTableDescriptor = newLogTable();
            logClusterGateway
                    .createTable(newCreateTableRequest(logTablePath, logTableDescriptor, false))
                    .get();

            // Verify the log table was created successfully
            assertThat(
                            logClusterGateway
                                    .tableExists(newTableExistsRequest(logTablePath))
                                    .get()
                                    .isExists())
                    .isTrue();
        } finally {
            logCluster.close();
        }
    }

    private static Configuration initLogRestrictedConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_TABLE_ALLOW_CREATION, false);
        return conf;
    }

    private static Configuration initKvRestrictedConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL, Duration.ofSeconds(1));
        conf.set(ConfigOptions.KV_TABLE_ALLOW_CREATION, false);
        return conf;
    }

    // Helper methods for creating different table types
    private static TableDescriptor newLogTable() {
        return TableDescriptor.builder()
                .schema(newLogSchema())
                .comment("log table without primary key")
                .distributedBy(3, "a")
                .build();
    }

    private static Schema newLogSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .withComment("a comment")
                .column("b", DataTypes.STRING())
                .build();
    }
}
