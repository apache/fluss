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

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.client.metadata.KvSnapshotMetadata;
import com.alibaba.fluss.client.metadata.KvSnapshots;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.DatabaseAlreadyExistException;
import com.alibaba.fluss.exception.DatabaseNotEmptyException;
import com.alibaba.fluss.exception.DatabaseNotExistException;
import com.alibaba.fluss.exception.InvalidConfigException;
import com.alibaba.fluss.exception.InvalidDatabaseException;
import com.alibaba.fluss.exception.InvalidPartitionException;
import com.alibaba.fluss.exception.InvalidReplicationFactorException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.exception.PartitionAlreadyExistsException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.SchemaNotExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.KvSnapshotHandle;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlussAdmin}. */
class FlussAdminITCase extends ClientToServerITCaseBase {

    protected static final TablePath DEFAULT_TABLE_PATH = TablePath.of("test_db", "person");
    protected static final Schema DEFAULT_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .withComment("person id")
                    .column("name", DataTypes.STRING())
                    .withComment("person name")
                    .column("age", DataTypes.INT())
                    .withComment("person age")
                    .build();
    protected static final TableDescriptor DEFAULT_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_SCHEMA)
                    .comment("test table")
                    .distributedBy(3, "id")
                    .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofDays(1))
                    .customProperty("connector", "fluss")
                    .build();

    @BeforeEach
    protected void setup() throws Exception {
        super.setup();
        // create a default table in fluss.
        createTable(DEFAULT_TABLE_PATH, DEFAULT_TABLE_DESCRIPTOR, false);
    }

    @Test
    void testMultiClient() throws Exception {
        Admin admin1 = conn.getAdmin();
        Admin admin2 = conn.getAdmin();
        assertThat(admin1).isNotSameAs(admin2);

        TableInfo t1 = admin1.getTableInfo(DEFAULT_TABLE_PATH).get();
        TableInfo t2 = admin2.getTableInfo(DEFAULT_TABLE_PATH).get();
        assertThat(t1).isEqualTo(t2);

        admin1.close();
        admin2.close();
    }

    @Test
    void testGetDatabaseInfo() throws Exception {
        long timestampBeforeCreate = System.currentTimeMillis();
        admin.createDatabase(
                "test_db_2",
                DatabaseDescriptor.builder()
                        .comment("test comment")
                        .customProperty("key1", "value1")
                        .build(),
                false);
        DatabaseInfo databaseInfo = admin.getDatabaseInfo("test_db_2").get();
        long timestampAfterCreate = System.currentTimeMillis();
        assertThat(databaseInfo.getCreatedTime()).isEqualTo(databaseInfo.getModifiedTime());
        assertThat(databaseInfo.getDatabaseName()).isEqualTo("test_db_2");
        assertThat(databaseInfo.getDatabaseDescriptor().getComment().get())
                .isEqualTo("test comment");
        assertThat(databaseInfo.getDatabaseDescriptor().getCustomProperties())
                .containsEntry("key1", "value1");
        assertThat(databaseInfo.getDatabaseDescriptor().getCustomProperties()).hasSize(1);
        assertThat(databaseInfo.getCreatedTime())
                .isBetween(timestampBeforeCreate, timestampAfterCreate);
    }

    @Test
    void testGetTableInfoAndSchema() throws Exception {
        SchemaInfo schemaInfo = admin.getTableSchema(DEFAULT_TABLE_PATH).get();
        assertThat(schemaInfo.getSchema()).isEqualTo(DEFAULT_SCHEMA);
        assertThat(schemaInfo.getSchemaId()).isEqualTo(1);
        SchemaInfo schemaInfo2 = admin.getTableSchema(DEFAULT_TABLE_PATH, 1).get();

        // get default table.
        long timestampAfterCreate = System.currentTimeMillis();
        TableInfo tableInfo = admin.getTableInfo(DEFAULT_TABLE_PATH).get();
        assertThat(tableInfo.getSchemaId()).isEqualTo(schemaInfo.getSchemaId());
        assertThat(tableInfo.toTableDescriptor())
                .isEqualTo(
                        DEFAULT_TABLE_DESCRIPTOR
                                .withReplicationFactor(3)
                                .withDataLakeFormat(DataLakeFormat.PAIMON));
        assertThat(schemaInfo2).isEqualTo(schemaInfo);
        assertThat(tableInfo.getCreatedTime()).isEqualTo(tableInfo.getModifiedTime());
        assertThat(tableInfo.getCreatedTime()).isLessThan(timestampAfterCreate);

        // unknown table
        assertThatThrownBy(() -> admin.getTableInfo(TablePath.of("test_db", "unknown_table")).get())
                .cause()
                .isInstanceOf(TableNotExistException.class);
        assertThatThrownBy(
                        () -> admin.getTableSchema(TablePath.of("test_db", "unknown_table")).get())
                .cause()
                .isInstanceOf(SchemaNotExistException.class);

        // create and get a new table
        long timestampBeforeCreate = System.currentTimeMillis();
        TablePath tablePath = TablePath.of("test_db", "table_2");
        admin.createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, false);
        tableInfo = admin.getTableInfo(tablePath).get();
        timestampAfterCreate = System.currentTimeMillis();
        assertThat(tableInfo.getSchemaId()).isEqualTo(schemaInfo.getSchemaId());
        assertThat(tableInfo.toTableDescriptor())
                .isEqualTo(
                        DEFAULT_TABLE_DESCRIPTOR
                                .withReplicationFactor(3)
                                .withDataLakeFormat(DataLakeFormat.PAIMON));
        assertThat(schemaInfo2).isEqualTo(schemaInfo);
        // assert created time
        assertThat(tableInfo.getCreatedTime())
                .isBetween(timestampBeforeCreate, timestampAfterCreate);
    }

    @Test
    void testCreateInvalidDatabaseAndTable() {
        assertThatThrownBy(
                        () ->
                                admin.createDatabase(
                                                "*invalid_db*", DatabaseDescriptor.EMPTY, false)
                                        .get())
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining(
                        "Database name *invalid_db* is invalid: '*invalid_db*' contains one or more characters other than");
        assertThatThrownBy(
                        () ->
                                admin.createTable(
                                                TablePath.of("db", "=invalid_table!"),
                                                DEFAULT_TABLE_DESCRIPTOR,
                                                false)
                                        .get())
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Table name =invalid_table! is invalid: '=invalid_table!' contains one or more characters other than");
        assertThatThrownBy(
                        () ->
                                admin.createTable(
                                                TablePath.of(null, "table"),
                                                DEFAULT_TABLE_DESCRIPTOR,
                                                false)
                                        .get())
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining("Database name null is invalid: null string is not allowed");
    }

    @Test
    void testCreateTableWithInvalidProperty() {
        TablePath tablePath = TablePath.of(DEFAULT_TABLE_PATH.getDatabaseName(), "test_property");
        TableDescriptor t1 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("test table")
                        // unknown fluss table property
                        .property("connector", "fluss")
                        .build();
        // should throw exception
        assertThatThrownBy(() -> admin.createTable(tablePath, t1, false).get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("'connector' is not a Fluss table property.");

        TableDescriptor t2 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("test table")
                        // invalid property value
                        .property("table.log.ttl", "unknown")
                        .build();
        // should throw exception
        assertThatThrownBy(() -> admin.createTable(tablePath, t2, false).get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Invalid value for config 'table.log.ttl'. "
                                + "Reason: Could not parse value 'unknown' for key 'table.log.ttl'.");

        TableDescriptor t3 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("test table")
                        .property(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key(), "0")
                        .build();
        // should throw exception
        assertThatThrownBy(() -> admin.createTable(tablePath, t3, false).get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessage("'table.log.tiered.local-segments' must be greater than 0.");

        TableDescriptor t4 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA) // no pk
                        .comment("test table")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE.key(), "versioned")
                        .build();
        // should throw exception
        assertThatThrownBy(() -> admin.createTable(tablePath, t4, false).get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessage(
                        "'%s' must be set for versioned merge engine.",
                        ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key());

        TableDescriptor t5 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA) // no pk
                        .comment("test table")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE.key(), "versioned")
                        .property(
                                ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(),
                                "non-existed")
                        .build();
        // should throw exception
        assertThatThrownBy(() -> admin.createTable(tablePath, t5, false).get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessage(
                        "The version column 'non-existed' for versioned merge engine doesn't exist in schema.");

        TableDescriptor t6 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA) // no pk
                        .comment("test table")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE.key(), "versioned")
                        .property(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(), "name")
                        .build();
        // should throw exception
        assertThatThrownBy(() -> admin.createTable(tablePath, t6, false).get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessage(
                        "The version column 'name' for versioned merge engine must be one type of "
                                + "[INT, BIGINT, TIMESTAMP, TIMESTAMP_LTZ], but got STRING.");

        TableDescriptor t7 =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .primaryKey("a")
                                        .build())
                        .kvFormat(KvFormat.COMPACTED)
                        .logFormat(LogFormat.INDEXED)
                        .build();
        assertThatThrownBy(() -> admin.createTable(tablePath, t7, false).get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Currently, Primary Key Table only supports ARROW log format if kv format is COMPACTED.");
    }

    @Test
    void testCreateTableWithInvalidReplicationFactor() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_TABLE_PATH.getDatabaseName(), "t1");
        // set replica factor to a non positive number, should also throw exception
        TableDescriptor nonPositiveReplicaFactorTable =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("test table")
                        .customProperty("connector", "fluss")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "-1")
                        .build();
        // should throw exception
        assertThatThrownBy(
                        () ->
                                admin.createTable(tablePath, nonPositiveReplicaFactorTable, false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidReplicationFactorException.class)
                .hasMessageContaining("Replication factor must be larger than 0.");

        // let's kill one tablet server
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(0);

        // assert the cluster should have tablet server number to be 2
        assertHasTabletServerNumber(2);

        // let's set the table's replica.factor to 3, should also throw exception
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("test table")
                        .customProperty("connector", "fluss")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "3")
                        .build();
        assertThatThrownBy(() -> admin.createTable(tablePath, tableDescriptor, false).get())
                .cause()
                .isInstanceOf(InvalidReplicationFactorException.class)
                .hasMessageContaining(
                        "Replication factor: %s larger than available tablet servers: %s.", 3, 2);

        // now, let start the tablet server again
        FLUSS_CLUSTER_EXTENSION.startTabletServer(0);

        // assert the cluster should have tablet server number to be 3
        assertHasTabletServerNumber(3);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();

        // we can create the table now
        admin.createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, false).get();
        TableInfo tableInfo = admin.getTableInfo(DEFAULT_TABLE_PATH).get();
        assertThat(tableInfo.toTableDescriptor())
                .isEqualTo(
                        DEFAULT_TABLE_DESCRIPTOR
                                .withReplicationFactor(3)
                                .withDataLakeFormat(DataLakeFormat.PAIMON));
    }

    @Test
    void testCreateExistedTable() throws Exception {
        assertThatThrownBy(() -> createTable(DEFAULT_TABLE_PATH, DEFAULT_TABLE_DESCRIPTOR, false))
                .cause()
                .isInstanceOf(DatabaseAlreadyExistException.class);
        // no exception
        createTable(DEFAULT_TABLE_PATH, DEFAULT_TABLE_DESCRIPTOR, true);

        // database not exists, throw exception
        assertThatThrownBy(
                        () ->
                                admin.createTable(
                                                TablePath.of("unknown_db", "test"),
                                                DEFAULT_TABLE_DESCRIPTOR,
                                                true)
                                        .get())
                .cause()
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void testDropDatabaseAndTable() throws Exception {
        // drop not existed database with ignoreIfNotExists false.
        assertThatThrownBy(() -> admin.dropDatabase("unknown_db", false, true).get())
                .cause()
                .isInstanceOf(DatabaseNotExistException.class);

        // drop not existed database with ignoreIfNotExists true.
        admin.dropDatabase("unknown_db", true, true).get();

        // drop existed database with table exist in db.
        assertThatThrownBy(() -> admin.dropDatabase("test_db", false, false).get())
                .cause()
                .isInstanceOf(DatabaseNotEmptyException.class);

        // drop existed database with table exist in db.
        assertThat(admin.databaseExists("test_db").get()).isTrue();
        admin.dropDatabase("test_db", true, true).get();
        assertThat(admin.databaseExists("test_db").get()).isFalse();

        // re-create.
        createTable(DEFAULT_TABLE_PATH, DEFAULT_TABLE_DESCRIPTOR, false);

        // drop not existed table with ignoreIfNotExists false.
        assertThatThrownBy(
                        () ->
                                admin.dropTable(TablePath.of("test_db", "unknown_table"), false)
                                        .get())
                .cause()
                .isInstanceOf(TableNotExistException.class);

        // drop not existed table with ignoreIfNotExists true.
        admin.dropTable(TablePath.of("test_db", "unknown_table"), true).get();

        // drop existed table.
        assertThat(admin.tableExists(DEFAULT_TABLE_PATH).get()).isTrue();
        admin.dropTable(DEFAULT_TABLE_PATH, true).get();
        assertThat(admin.tableExists(DEFAULT_TABLE_PATH).get()).isFalse();
    }

    @Test
    void testListDatabasesAndTables() throws Exception {
        admin.createDatabase("db1", DatabaseDescriptor.EMPTY, true).get();
        admin.createDatabase("db2", DatabaseDescriptor.EMPTY, true).get();
        admin.createDatabase("db3", DatabaseDescriptor.EMPTY, true).get();
        assertThat(admin.listDatabases().get())
                .containsExactlyInAnyOrder("test_db", "db1", "db2", "db3", "fluss");

        admin.createTable(TablePath.of("db1", "table1"), DEFAULT_TABLE_DESCRIPTOR, true).get();
        admin.createTable(TablePath.of("db1", "table2"), DEFAULT_TABLE_DESCRIPTOR, true).get();
        assertThat(admin.listTables("db1").get()).containsExactlyInAnyOrder("table1", "table2");
        assertThat(admin.listTables("db2").get()).isEmpty();

        assertThatThrownBy(() -> admin.listTables("unknown_db").get())
                .cause()
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void testListPartitionInfos() throws Exception {
        String dbName = DEFAULT_TABLE_PATH.getDatabaseName();
        TablePath nonPartitionedTablePath = TablePath.of(dbName, "test_non_partitioned_table");
        admin.createTable(nonPartitionedTablePath, DEFAULT_TABLE_DESCRIPTOR, true).get();
        assertThatThrownBy(() -> admin.listPartitionInfos(nonPartitionedTablePath).get())
                .cause()
                .isInstanceOf(TableNotPartitionedException.class)
                .hasMessage("Table '%s' is not a partitioned table.", nonPartitionedTablePath);

        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("pt", DataTypes.STRING())
                                        .build())
                        .comment("test table")
                        .distributedBy(3, "id")
                        .partitionedBy("pt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        TablePath partitionedTablePath = TablePath.of(dbName, "test_partitioned_table");
        admin.createTable(partitionedTablePath, partitionedTable, true).get();
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(partitionedTablePath);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(partitionedTablePath).get();
        assertThat(partitionInfos).hasSize(partitionIdByNames.size());
        for (PartitionInfo partitionInfo : partitionInfos) {
            assertThat(partitionIdByNames.get(partitionInfo.getPartitionName()))
                    .isEqualTo(partitionInfo.getPartitionId());
        }
    }

    @Test
    void testGetKvSnapshot() throws Exception {
        TablePath tablePath1 =
                TablePath.of(DEFAULT_TABLE_PATH.getDatabaseName(), "test-table-snapshot");
        int bucketNum = 3;
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .distributedBy(bucketNum, "id")
                        .build();

        admin.createTable(tablePath1, tableDescriptor, true).get();

        // no any data, should no any bucket snapshot
        KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath1).get();
        assertNoBucketSnapshot(snapshots, bucketNum);

        long tableId = snapshots.getTableId();

        // write data
        try (Table table = conn.getTable(tablePath1)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 0; i < 10; i++) {
                upsertWriter.upsert(row(i, "v" + i, i + 1));
            }
            upsertWriter.flush();

            Map<Integer, CompletedSnapshot> expectedSnapshots = new HashMap<>();
            for (int bucket = 0; bucket < bucketNum; bucket++) {
                CompletedSnapshot completedSnapshot =
                        FLUSS_CLUSTER_EXTENSION.waitUtilSnapshotFinished(
                                new TableBucket(tableId, bucket), 0);
                expectedSnapshots.put(bucket, completedSnapshot);
            }

            // now, get the table snapshot info
            snapshots = admin.getLatestKvSnapshots(tablePath1).get();
            // check table snapshot info
            assertTableSnapshot(snapshots, bucketNum, expectedSnapshots);

            // write data again, should fall into bucket 0
            upsertWriter.upsert(row(0, "v000", 1));
            upsertWriter.flush();

            TableBucket tb = new TableBucket(snapshots.getTableId(), 0);
            // wait util the snapshot finish
            expectedSnapshots.put(
                    tb.getBucket(), FLUSS_CLUSTER_EXTENSION.waitUtilSnapshotFinished(tb, 1));

            // check snapshot
            snapshots = admin.getLatestKvSnapshots(tablePath1).get();
            assertTableSnapshot(snapshots, bucketNum, expectedSnapshots);
        }
    }

    @Test
    void testGetServerNodes() throws Exception {
        List<ServerNode> serverNodes = admin.getServerNodes().get();
        List<ServerNode> expectedNodes = new ArrayList<>();
        expectedNodes.add(FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode());
        expectedNodes.addAll(FLUSS_CLUSTER_EXTENSION.getTabletServerNodes());
        assertThat(serverNodes).containsExactlyInAnyOrderElementsOf(expectedNodes);
    }

    @Test
    void testCreateIllegalPartitionTable() {
        String dbName = DEFAULT_TABLE_PATH.getDatabaseName();
        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("age", DataTypes.STRING())
                                        .build())
                        .distributedBy(3, "id")
                        .partitionedBy("name", "age")
                        .build();
        TablePath tablePath = TablePath.of(dbName, "test_create_illegal_partitioned_table_1");
        assertThatThrownBy(() -> admin.createTable(tablePath, partitionedTable, true).get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Currently, partitioned table only supports one partition key, "
                                + "but got partition keys [name, age].");
    }

    @Test
    void testAddAndDropPartitions() throws Exception {
        String dbName = DEFAULT_TABLE_PATH.getDatabaseName();
        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("age", DataTypes.STRING())
                                        .build())
                        .distributedBy(3, "id")
                        .partitionedBy("age")
                        .build();
        TablePath tablePath = TablePath.of(dbName, "test_add_and_drop_partitioned_table");
        admin.createTable(tablePath, partitionedTable, true).get();
        assertPartitionInfo(admin.listPartitionInfos(tablePath).get(), Collections.emptyList());

        // add two partitions.
        admin.createPartition(tablePath, newPartitionSpec("age", "10"), false).get();
        admin.createPartition(tablePath, newPartitionSpec("age", "11"), false).get();
        assertPartitionInfo(admin.listPartitionInfos(tablePath).get(), Arrays.asList("10", "11"));

        // drop one partition.
        admin.dropPartition(tablePath, newPartitionSpec("age", "10"), false).get();
        assertPartitionInfo(
                admin.listPartitionInfos(tablePath).get(), Collections.singletonList("11"));

        // test create partition for a not existed table.
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                TablePath.of("unknown_db", "unknown_table"),
                                                newPartitionSpec("age", "10"),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(TableNotExistException.class)
                .hasMessageContaining("Table 'unknown_db.unknown_table' does not exist.");

        // test drop partition for a not existed table.
        assertThatThrownBy(
                        () ->
                                admin.dropPartition(
                                                TablePath.of("unknown_db", "unknown_table"),
                                                newPartitionSpec("age", "10"),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(TableNotExistException.class)
                .hasMessageContaining("Table 'unknown_db.unknown_table' does not exist.");

        // test create partition already exists with ignoreIfNotExists = false.
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath, newPartitionSpec("age", "11"), false)
                                        .get())
                .cause()
                .isInstanceOf(PartitionAlreadyExistsException.class)
                .hasMessageContaining(
                        "Partition 'age=11' already exists for table test_db.test_add_and_drop_partitioned_table");

        // test drop partition not-exists with ignoreIfNotExists = false.
        assertThatThrownBy(
                        () ->
                                admin.dropPartition(tablePath, newPartitionSpec("age", "13"), false)
                                        .get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Partition 'age=13' does not exist for table test_db.test_add_and_drop_partitioned_table");

        // test create partition with illegal value.
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath, newPartitionSpec("age", "$10"), false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining(
                        "The partition value $10 is invalid: '$10' contains one or more "
                                + "characters other than ASCII alphanumerics, '_' and '-'");

        // test create partition with wrong partition key.
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath, newPartitionSpec("name", "10"), false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining(
                        "PartitionSpec PartitionSpec{{name=10}} does not contain "
                                + "partition key 'age' for partitioned table test_db.test_add_and_drop_partitioned_table.");
    }

    @Test
    void testAddAndDropPartitionsForAutoPartitionedTable() throws Exception {
        String dbName = DEFAULT_TABLE_PATH.getDatabaseName();
        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("pt", DataTypes.STRING())
                                        .build())
                        .distributedBy(3, "id")
                        .partitionedBy("pt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        TablePath tablePath = TablePath.of(dbName, "test_add_and_drop_partitioned_table_1");
        admin.createTable(tablePath, partitionedTable, true).get();
        // wait all auto partitions created.
        FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(tablePath);

        // there are four auto created partitions.
        int currentYear = LocalDate.now().getYear();
        assertPartitionInfo(
                admin.listPartitionInfos(tablePath).get(),
                Arrays.asList(String.valueOf(currentYear), String.valueOf(currentYear + 1)));

        // add two older partitions (currentYear - 2, currentYear - 1).
        admin.createPartition(
                        tablePath, newPartitionSpec("pt", String.valueOf(currentYear - 2)), false)
                .get();
        admin.createPartition(
                        tablePath, newPartitionSpec("pt", String.valueOf(currentYear - 1)), false)
                .get();
        assertPartitionInfo(
                admin.listPartitionInfos(tablePath).get(),
                Arrays.asList(
                        String.valueOf(currentYear - 2),
                        String.valueOf(currentYear - 1),
                        String.valueOf(currentYear),
                        String.valueOf(currentYear + 1)));

        // drop one auto created partition.
        admin.dropPartition(
                        tablePath, newPartitionSpec("pt", String.valueOf(currentYear + 1)), false)
                .get();
        assertPartitionInfo(
                admin.listPartitionInfos(tablePath).get(),
                Arrays.asList(
                        String.valueOf(currentYear - 2),
                        String.valueOf(currentYear - 1),
                        String.valueOf(currentYear)));
    }

    private void assertHasTabletServerNumber(int tabletServerNumber) {
        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        retry(
                Duration.ofMinutes(2),
                () -> {
                    assertThat(
                                    coordinatorGateway
                                            .metadata(new MetadataRequest())
                                            .get()
                                            .getTabletServersCount())
                            .as("Tablet server number should be " + tabletServerNumber)
                            .isEqualTo(tabletServerNumber);
                });
    }

    private void assertNoBucketSnapshot(KvSnapshots snapshots, int expectBucketNum) {
        assertThat(snapshots.getBucketIds()).hasSize(expectBucketNum);
        for (int i = 0; i < expectBucketNum; i++) {
            assertThat(snapshots.getSnapshotId(i)).isEmpty();
        }
    }

    private void assertTableSnapshot(
            KvSnapshots snapshots,
            int expectBucketNum,
            Map<Integer, CompletedSnapshot> expectedSnapshots)
            throws ExecutionException, InterruptedException {
        // check bucket numbers
        assertThat(snapshots.getBucketIds()).hasSize(expectBucketNum);
        for (Map.Entry<Integer, CompletedSnapshot> expectedSnapshot :
                expectedSnapshots.entrySet()) {
            int bucketId = expectedSnapshot.getKey();
            OptionalLong snapshotId = snapshots.getSnapshotId(bucketId);
            assertThat(snapshotId).isPresent();

            TableBucket tableBucket =
                    new TableBucket(snapshots.getTableId(), snapshots.getPartitionId(), bucketId);
            KvSnapshotMetadata snapshotMetadata =
                    admin.getKvSnapshotMetadata(tableBucket, snapshotId.getAsLong()).get();
            // check bucket snapshot
            assertBucketSnapshot(snapshotMetadata, expectedSnapshot.getValue());
        }
    }

    private void assertBucketSnapshot(
            KvSnapshotMetadata snapshotMetadata, CompletedSnapshot expectedSnapshot) {
        // check snapshot files
        assertThat(snapshotMetadata.getSnapshotFiles())
                .containsExactlyInAnyOrderElementsOf(
                        toFsPathAndFileNames(expectedSnapshot.getKvSnapshotHandle()));

        // check offset
        assertThat(snapshotMetadata.getLogOffset()).isEqualTo(expectedSnapshot.getLogOffset());
    }

    private void assertPartitionInfo(
            List<PartitionInfo> partitionInfos, List<String> expectedPartitionNames) {
        assertThat(
                        partitionInfos.stream()
                                .map(PartitionInfo::getPartitionName)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expectedPartitionNames);
    }

    private List<FsPathAndFileName> toFsPathAndFileNames(KvSnapshotHandle kvSnapshotHandle) {
        return Stream.concat(
                        kvSnapshotHandle.getSharedKvFileHandles().stream(),
                        kvSnapshotHandle.getPrivateFileHandles().stream())
                .map(
                        kvFileHandleAndLocalPath ->
                                new FsPathAndFileName(
                                        kvFileHandleAndLocalPath.getKvFileHandle().getFilePath(),
                                        kvFileHandleAndLocalPath.getLocalPath()))
                .collect(Collectors.toList());
    }
}
