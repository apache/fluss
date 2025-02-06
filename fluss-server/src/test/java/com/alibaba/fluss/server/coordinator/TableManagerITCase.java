/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.DatabaseAlreadyExistException;
import com.alibaba.fluss.exception.DatabaseNotEmptyException;
import com.alibaba.fluss.exception.DatabaseNotExistException;
import com.alibaba.fluss.exception.InvalidDatabaseException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.SchemaNotExistException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.messages.GetTableInfoResponse;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbBucketMetadata;
import com.alibaba.fluss.rpc.messages.PbPartitionMetadata;
import com.alibaba.fluss.rpc.messages.PbTableMetadata;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.types.DataTypes;

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

import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newCreateDatabaseRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDatabaseExistsRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropDatabaseRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newListTablesRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newMetadataRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newTableExistsRequest;
import static com.alibaba.fluss.server.utils.RpcMessageUtils.makeUpdateMetadataRequest;
import static com.alibaba.fluss.server.utils.RpcMessageUtils.toServerNode;
import static com.alibaba.fluss.server.utils.RpcMessageUtils.toTablePath;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static com.alibaba.fluss.utils.PartitionUtils.generateAutoPartition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link TableManager}. */
class TableManagerITCase {

    private ZooKeeperClient zkClient;
    private Configuration clientConf;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
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
                                                        newTable(),
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
                                                        newTable(),
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
        TableDescriptor tableDescriptor = newTable();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        // the table should exist then
        assertThat(gateway.tableExists(newTableExistsRequest(tablePath)).get().isExists()).isTrue();

        // get the table and check it
        GetTableInfoResponse response =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTable = TableDescriptor.fromJsonBytes(response.getTableJson());
        assertThat(gottenTable).isEqualTo(tableDescriptor.withReplicationFactor(1));

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

        // wait util partition is created
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

        // verify the partition assignment is deleted
        for (Long partitionId : partitions.values()) {
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(zkClient.getPartitionAssignment(partitionId)).isEmpty());
        }
    }

    @Test
    void testCreateInvalidPartitionedTable() throws Exception {
        AdminGateway adminGateway = getAdminGateway();
        String db1 = "db1";
        String tb1 = "tb1";
        TablePath tablePath = TablePath.of(db1, tb1);
        // first create a database
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        TableDescriptor tableWithMultiPartKey =
                newPartitionedTableBuilder(new Schema.Column("tttt", DataTypes.INT()))
                        .partitionedBy("id", "dt")
                        .build();
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createTable(
                                                newCreateTableRequest(
                                                        tablePath, tableWithMultiPartKey, false))
                                        .get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Currently, partitioned table only supports one partition key, but got partition keys [id, dt].");

        TableDescriptor tableWithIntPartKey =
                newPartitionedTableBuilder(null).partitionedBy("id").build();
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createTable(
                                                newCreateTableRequest(
                                                        tablePath, tableWithIntPartKey, false))
                                        .get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Currently, partitioned table supported partition key type are [STRING], but got partition key 'id' with data type INT NOT NULL.");
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
        TableDescriptor tableDescriptor = newTable();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
        GetTableInfoResponse response =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        long tableId = response.getTableId();

        // retry until all replica ready.
        int expectBucketCount = tableDescriptor.getTableDistribution().get().getBucketCount().get();
        for (int i = 0; i < expectBucketCount; i++) {
            FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(new TableBucket(tableId, i));
        }

        // retry to check metadata.
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();
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

        List<ServerNode> tabletServerNodes = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes();
        ServerNode coordinatorServerNode = FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode();

        checkBucketMetadata(expectBucketCount, tableMetadata.getBucketMetadatasList());

        // now, assuming we send update metadata request to the server,
        // we should get the same response
        gateway.updateMetadata(
                        makeUpdateMetadataRequest(
                                Optional.of(coordinatorServerNode),
                                new HashSet<>(tabletServerNodes)))
                .get();

        metadataResponse =
                gateway.metadata(newMetadataRequest(Collections.singletonList(tablePath))).get();
        // check coordinator server
        assertThat(toServerNode(metadataResponse.getCoordinatorServer(), ServerType.COORDINATOR))
                .isEqualTo(coordinatorServerNode);
        assertThat(metadataResponse.getTabletServersCount()).isEqualTo(3);
        List<ServerNode> tsNodes =
                metadataResponse.getTabletServersList().stream()
                        .map(n -> toServerNode(n, ServerType.TABLET_SERVER))
                        .collect(Collectors.toList());
        assertThat(tsNodes)
                .containsExactlyInAnyOrderElementsOf(
                        FLUSS_CLUSTER_EXTENSION.getTabletServerNodes());
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
                FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(
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
        return TableDescriptor.builder().schema(newSchema()).comment("first table").build();
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

    private static TableDescriptor newTable() {
        return TableDescriptor.builder()
                .schema(newSchema())
                .comment("first table")
                .distributedBy(3, "a")
                .build();
    }

    private static Schema newSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .withComment("a comment")
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .build();
    }
}
