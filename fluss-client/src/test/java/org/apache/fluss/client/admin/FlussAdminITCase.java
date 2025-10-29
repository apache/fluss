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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.InvalidReplicationFactorException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.PartitionAlreadyExistsException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.exception.TooManyPartitionsException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.FsPathAndFileName;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
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
                        false)
                .get();
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
                                .withDataLakeFormat(PAIMON));
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
        admin.createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, false).get();
        tableInfo = admin.getTableInfo(tablePath).get();
        timestampAfterCreate = System.currentTimeMillis();
        assertThat(tableInfo.getSchemaId()).isEqualTo(schemaInfo.getSchemaId());
        assertThat(tableInfo.toTableDescriptor())
                .isEqualTo(
                        DEFAULT_TABLE_DESCRIPTOR
                                .withReplicationFactor(3)
                                .withDataLakeFormat(PAIMON));
        assertThat(schemaInfo2).isEqualTo(schemaInfo);
        // assert created time
        assertThat(tableInfo.getCreatedTime())
                .isBetween(timestampBeforeCreate, timestampAfterCreate);
    }

    @Test
    void testAlterTable() throws Exception {
        // create table
        TablePath tablePath = TablePath.of("test_db", "alter_table_1");
        admin.createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, false).get();

        TableInfo tableInfo = admin.getTableInfo(tablePath).get();

        TableDescriptor existingTableDescriptor = tableInfo.toTableDescriptor();
        Map<String, String> updateProperties =
                new HashMap<>(existingTableDescriptor.getProperties());
        Map<String, String> updateCustomProperties =
                new HashMap<>(existingTableDescriptor.getCustomProperties());
        updateCustomProperties.put("client.connect-timeout", "240s");

        TableDescriptor newTableDescriptor =
                TableDescriptor.builder()
                        .schema(existingTableDescriptor.getSchema())
                        .comment(existingTableDescriptor.getComment().orElse("test table"))
                        .partitionedBy(existingTableDescriptor.getPartitionKeys())
                        .distributedBy(
                                existingTableDescriptor
                                        .getTableDistribution()
                                        .get()
                                        .getBucketCount()
                                        .orElse(3),
                                existingTableDescriptor.getBucketKeys())
                        .properties(updateProperties)
                        .customProperties(updateCustomProperties)
                        .build();

        List<TableChange> tableChanges = new ArrayList<>();
        TableChange tableChange = TableChange.set("client.connect-timeout", "240s");
        tableChanges.add(tableChange);
        // alter table
        admin.alterTable(tablePath, tableChanges, false).get();

        TableInfo alteredTableInfo = admin.getTableInfo(tablePath).get();
        TableDescriptor alteredTableDescriptor = alteredTableInfo.toTableDescriptor();
        assertThat(alteredTableDescriptor).isEqualTo(newTableDescriptor);

        // throw exception if table not exist
        assertThatThrownBy(
                        () ->
                                admin.alterTable(
                                                TablePath.of("test_db", "alter_table_not_exist"),
                                                tableChanges,
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(TableNotExistException.class);
        // should success if ignore not exist
        admin.alterTable(TablePath.of("test_db", "alter_table_not_exist"), tableChanges, true)
                .get();

        // throw exception if database not exist
        assertThatThrownBy(
                        () ->
                                admin.alterTable(
                                                TablePath.of(
                                                        "test_db_not_exist",
                                                        "alter_table_not_exist"),
                                                tableChanges,
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(TableNotExistException.class);
        // should success if ignore not exist
        admin.alterTable(
                        TablePath.of("test_db_not_exist", "alter_table_not_exist"),
                        tableChanges,
                        true)
                .get();
    }

    @Test
    void testCreateInvalidDatabaseAndTable() throws Exception {
        assertThatThrownBy(
                        () ->
                                admin.createDatabase(
                                                "*invalid_db*", DatabaseDescriptor.EMPTY, false)
                                        .get())
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining(
                        "Database name *invalid_db* is invalid: '*invalid_db*' contains one or more characters other than");
        //  test internal database with '__' prefix is not allowed
        assertThatThrownBy(
                        () ->
                                admin.createDatabase(
                                                "__internal_db", DatabaseDescriptor.EMPTY, false)
                                        .get())
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining(
                        "Database name __internal_db is invalid: '__' is not allowed as prefix, since it is reserved for internal databases/internal tables/internal partitions in Fluss server");
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
        //  test internal table with '__' prefix is not allowed
        assertThatThrownBy(
                        () ->
                                admin.createTable(
                                                TablePath.of("db", "__internal_table"),
                                                DEFAULT_TABLE_DESCRIPTOR,
                                                false)
                                        .get())
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Table name __internal_table is invalid: '__' is not allowed as prefix, since it is reserved for internal databases/internal tables/internal partitions in Fluss server");
    }

    @Test
    void testCreateTableWithDeleteBehavior() {
        // Test 1: FIRST_ROW merge engine - should set delete behavior to IGNORE
        TablePath tablePath1 = TablePath.of("fluss", "test_ignore_delete_for_first_row");
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(ConfigOptions.TABLE_MERGE_ENGINE.key(), "first_row");

        TableDescriptor tableDescriptor1 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("first row merge engine table")
                        .properties(properties1)
                        .build();
        admin.createTable(tablePath1, tableDescriptor1, false).join();

        // Get the table and verify delete behavior is changed to IGNORE
        TableInfo tableInfo1 = admin.getTableInfo(tablePath1).join();
        assertThat(tableInfo1.getTableConfig().getDeleteBehavior()).hasValue(DeleteBehavior.IGNORE);

        // Test 2: VERSIONED merge engine - should set delete behavior to IGNORE
        TablePath tablePath2 = TablePath.of("fluss", "test_ignore_delete_for_versioned");
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(ConfigOptions.TABLE_MERGE_ENGINE.key(), "versioned");
        properties2.put(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(), "age");
        TableDescriptor tableDescriptor2 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("versioned merge engine table")
                        .properties(properties2)
                        .build();
        admin.createTable(tablePath2, tableDescriptor2, false).join();
        // Get the table and verify delete behavior is changed to IGNORE
        TableInfo tableInfo2 = admin.getTableInfo(tablePath2).join();
        assertThat(tableInfo2.getTableConfig().getDeleteBehavior()).hasValue(DeleteBehavior.IGNORE);

        // Test 3: FIRST_ROW merge engine with delete behavior explicitly set to ALLOW
        TablePath tablePath3 = TablePath.of("fluss", "test_allow_delete_for_first_row");
        Map<String, String> properties3 = new HashMap<>();
        properties3.put(ConfigOptions.TABLE_MERGE_ENGINE.key(), "first_row");
        properties3.put(ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), "ALLOW");
        TableDescriptor tableDescriptor3 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("first row merge engine table")
                        .properties(properties3)
                        .build();
        assertThatThrownBy(() -> admin.createTable(tablePath3, tableDescriptor3, false).join())
                .hasRootCauseInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Table with 'FIRST_ROW' merge engine does not support delete operations. "
                                + "The 'table.delete.behavior' config must be set to 'ignore' or 'disable', but got 'allow'.");

        // Test 4: VERSIONED merge engine with delete behavior explicitly set to ALLOW
        TablePath tablePath4 = TablePath.of("fluss", "test_allow_delete_for_versioned");
        Map<String, String> properties4 = new HashMap<>();
        properties4.put(ConfigOptions.TABLE_MERGE_ENGINE.key(), "versioned");
        properties4.put(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(), "age");
        properties4.put(ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), "ALLOW");
        TableDescriptor tableDescriptor4 =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("versioned merge engine table")
                        .properties(properties4)
                        .build();
        assertThatThrownBy(() -> admin.createTable(tablePath4, tableDescriptor4, false).join())
                .hasRootCauseInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Table with 'VERSIONED' merge engine does not support delete operations. "
                                + "The 'table.delete.behavior' config must be set to 'ignore' or 'disable', but got 'allow'.");

        // Test 5: Log table - not allow to set delete behavior
        TablePath tablePath5 = TablePath.of("fluss", "test_set_delete_behavior_for_log_table");
        Map<String, String> properties5 = new HashMap<>();
        properties5.put(ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), "IGNORE");
        TableDescriptor tableDescriptor5 =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .comment("log table")
                        .properties(properties5)
                        .build();
        assertThatThrownBy(() -> admin.createTable(tablePath5, tableDescriptor5, false).join())
                .hasRootCauseInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "The 'table.delete.behavior' configuration is only supported for primary key tables.");
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
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);

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
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        // we can create the table now
        admin.createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, false).get();
        // recreate the connection because the metadata of tablet server has changed
        try (Connection conn = ConnectionFactory.createConnection(clientConf);
                Admin admin = conn.getAdmin()) {
            TableInfo tableInfo = admin.getTableInfo(DEFAULT_TABLE_PATH).get();
            assertThat(tableInfo.toTableDescriptor())
                    .isEqualTo(
                            DEFAULT_TABLE_DESCRIPTOR
                                    .withReplicationFactor(3)
                                    .withDataLakeFormat(PAIMON));
        }
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
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(partitionedTablePath);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(partitionedTablePath).get();
        assertThat(partitionInfos).hasSize(partitionIdByNames.size());
        for (PartitionInfo partitionInfo : partitionInfos) {
            assertThat(partitionIdByNames.get(partitionInfo.getPartitionName()))
                    .isEqualTo(partitionInfo.getPartitionId());
        }
    }

    @Test
    void testListPartitionInfosByPartitionSpec() throws Exception {
        String dbName = DEFAULT_TABLE_PATH.getDatabaseName();

        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("pt", DataTypes.STRING())
                                        .column("secondary_partition", DataTypes.STRING())
                                        .build())
                        .comment("test table")
                        .distributedBy(3, "id")
                        .partitionedBy("pt", "secondary_partition")
                        .build();
        TablePath partitionedTablePath = TablePath.of(dbName, "test_partitioned_table");
        // create table
        admin.createTable(partitionedTablePath, partitionedTable, true).get();
        // add three partitions.
        admin.createPartition(
                        partitionedTablePath,
                        newPartitionSpec(
                                Arrays.asList("pt", "secondary_partition"),
                                Arrays.asList("2025", "10")),
                        false)
                .get();
        admin.createPartition(
                        partitionedTablePath,
                        newPartitionSpec(
                                Arrays.asList("pt", "secondary_partition"),
                                Arrays.asList("2025", "11")),
                        false)
                .get();
        admin.createPartition(
                        partitionedTablePath,
                        newPartitionSpec(
                                Arrays.asList("pt", "secondary_partition"),
                                Arrays.asList("2026", "12")),
                        false)
                .get();

        // run listPartitionInfos by partition spec with valid partition name.
        PartitionSpec partitionSpec = newPartitionSpec("pt", "2025");

        List<PartitionInfo> partitionInfos =
                admin.listPartitionInfos(partitionedTablePath, partitionSpec).get();

        List<String> actualPartitionNames =
                partitionInfos.stream()
                        .map(PartitionInfo::getPartitionName)
                        .collect(Collectors.toList());
        assertThat(actualPartitionNames).containsExactlyInAnyOrder("2025$10", "2025$11");

        // run listPartitionInfos by partition spec with invalid partition name.
        PartitionSpec invalidNamePartitionSpec = newPartitionSpec("pt", "2024");
        List<PartitionInfo> invalidNamePartitionInfos =
                admin.listPartitionInfos(partitionedTablePath, invalidNamePartitionSpec).get();
        assertThat(invalidNamePartitionInfos).hasSize(0);

        // run listPartitionInfos by invalid partition spec.
        PartitionSpec invalidPartitionSpec = newPartitionSpec("pt1", "2025");
        assertThatThrownBy(
                        () ->
                                admin.listPartitionInfos(partitionedTablePath, invalidPartitionSpec)
                                        .get())
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("table don't contains this partitionKey: pt1");
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
                        FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(
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
            // wait until the snapshot finish
            expectedSnapshots.put(
                    tb.getBucket(), FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(tb, 1));

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

        // test internal partition with '__' prefix is not allowed
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath,
                                                newPartitionSpec(
                                                        Arrays.asList("age"),
                                                        Arrays.asList("__18")),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining(
                        "The partition value __18 is invalid: '__' is not allowed as prefix, since it is reserved for internal databases/internal tables/internal partitions in Fluss server");

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
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);

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

    @Test
    void testBootstrapServerConfigAsTabletServer() throws Exception {
        Configuration newConf = clientConf;
        ServerNode ts0 = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0);
        newConf.set(
                ConfigOptions.BOOTSTRAP_SERVERS,
                Collections.singletonList(String.format("%s:%d", ts0.host(), ts0.port())));
        try (Connection conn = ConnectionFactory.createConnection(clientConf)) {
            Admin newAdmin = conn.getAdmin();
            String dbName = "test_bootstrap_server_t1";
            newAdmin.createDatabase(
                            dbName,
                            DatabaseDescriptor.builder().comment("test comment").build(),
                            false)
                    .get();
            newAdmin.createTable(
                            TablePath.of(dbName, "test_table_1"),
                            TableDescriptor.builder().schema(Schema.newBuilder().build()).build(),
                            false)
                    .get();
            assertThat(newAdmin.getDatabaseInfo(dbName).get().getDatabaseName()).isEqualTo(dbName);
            assertThat(
                            newAdmin.getTableInfo(TablePath.of(dbName, "test_table_1"))
                                    .get()
                                    .getTablePath()
                                    .getTableName())
                    .isEqualTo("test_table_1");
        }
    }

    @Test
    void testAddTooManyPartitions() throws Exception {
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
        TablePath tablePath = TablePath.of(dbName, "test_add_too_many_partitioned_table");
        admin.createTable(tablePath, partitionedTable, true).get();

        // add 10 partitions.
        for (int i = 0; i < 10; i++) {
            admin.createPartition(tablePath, newPartitionSpec("age", String.valueOf(i)), false)
                    .get();
        }
        // add out of limit partition
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath, newPartitionSpec("age", "11"), false)
                                        .get())
                .cause()
                .isInstanceOf(TooManyPartitionsException.class);
    }

    @Test
    void testDynamicConfigs() throws Exception {
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getCoordinatorServer()
                                .getCoordinatorService()
                                .getDataLakeFormat())
                .isEqualTo(PAIMON);

        admin.alterClusterConfigs(
                        Collections.singletonList(
                                new AlterConfig(
                                        DATALAKE_FORMAT.key(), null, AlterConfigOpType.SET)))
                .get();
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getCoordinatorServer()
                                .getCoordinatorService()
                                .getDataLakeFormat())
                .isNull();
        assertConfigEntry(
                DATALAKE_FORMAT.key(), null, ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG);

        admin.alterClusterConfigs(
                        Arrays.asList(
                                new AlterConfig(
                                        DATALAKE_FORMAT.key(), "paimon", AlterConfigOpType.SET),
                                new AlterConfig(
                                        "datalake.paimon.warehouse",
                                        "test-warehouse",
                                        AlterConfigOpType.SET)))
                .get();
        TablePath tablePath = TablePath.of("test_db", "test_table");
        createTable(
                tablePath,
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .property(TABLE_DATALAKE_ENABLED, true)
                        .build(),
                true);

        waitUntil(
                () -> {
                    TableInfo tableInfo = admin.getTableInfo(tablePath).get();
                    Map<String, String> tableProperties = tableInfo.getProperties().toMap();
                    return tableProperties.containsKey(TABLE_DATALAKE_FORMAT.key())
                            && tableProperties.containsKey("table.datalake.paimon.warehouse")
                            && PAIMON.toString()
                                    .equals(tableProperties.get(TABLE_DATALAKE_FORMAT.key()))
                            && "test-warehouse"
                                    .equals(tableProperties.get("table.datalake.paimon.warehouse"));
                },
                Duration.ofMinutes(1),
                "Get lakehouse info");
    }

    private void assertConfigEntry(
            String key, @Nullable String value, ConfigEntry.ConfigSource source)
            throws ExecutionException, InterruptedException {
        Collection<ConfigEntry> configEntries = admin.describeClusterConfigs().get();
        List<String> configKeys =
                configEntries.stream().map(ConfigEntry::key).collect(Collectors.toList());
        assertThat(configKeys).doesNotHaveDuplicates();
        assertThat(configEntries).contains(new ConfigEntry(key, value, source));
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
                                        new FsPath(
                                                kvFileHandleAndLocalPath
                                                        .getKvFileHandle()
                                                        .getFilePath()),
                                        kvFileHandleAndLocalPath.getLocalPath()))
                .collect(Collectors.toList());
    }

    /**
     * Test that creating a partitioned table with bucket count exceeding the maximum throws
     * TooManyBucketsException.
     */
    @Test
    public void testAddTooManyBuckets() throws Exception {
        // Already set low maximum bucket limit to 30 for this test in ClientToServerITCaseBase
        String dbName = DEFAULT_TABLE_PATH.getDatabaseName();
        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("age", DataTypes.STRING())
                                        .build())
                        .distributedBy(10, "id") // 10 buckets per partition
                        .partitionedBy("age")
                        .build();
        TablePath tablePath = TablePath.of(dbName, "test_add_too_many_buckets_table");
        admin.createTable(tablePath, partitionedTable, true).get();

        // Add 3 partitions (3 * 10 = 30 buckets, which is the limit)
        for (int i = 0; i < 3; i++) {
            admin.createPartition(tablePath, newPartitionSpec("age", String.valueOf(i)), false)
                    .get();
        }

        // Try to add one more partition, exceeding the bucket limit (4 * 10 > 30)
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath, newPartitionSpec("age", "4"), false)
                                        .get())
                .cause()
                .isInstanceOf(TooManyBucketsException.class)
                .hasMessageContaining("exceeding the maximum of 30 buckets");
    }

    /**
     * Test that creating a non-partitioned table with bucket count exceeding the maximum throws
     * TooManyBucketsException.
     */
    @Test
    public void testBucketLimitForNonPartitionedTable() throws Exception {
        // Set a low maximum bucket limit for this test
        // (Assuming the configuration is already set to 30 in ClientToServerITCaseBase)
        String dbName = DEFAULT_TABLE_PATH.getDatabaseName();

        // Create a non-partitioned table with 40 buckets (exceeding limit of 30)
        TableDescriptor nonPartitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("value", DataTypes.STRING())
                                        .build())
                        .distributedBy(40, "id") // 40 buckets exceeds the limit of 30
                        .build(); // No partitionedBy call makes this non-partitioned

        TablePath tablePath = TablePath.of(dbName, "test_too_many_buckets_non_partitioned");

        // Creating this table should throw TooManyBucketsException
        assertThatThrownBy(() -> admin.createTable(tablePath, nonPartitionedTable, false).get())
                .cause()
                .isInstanceOf(TooManyBucketsException.class)
                .hasMessageContaining("exceeds the maximum limit");
    }

    /** Test that creating a table with system columns throws InvalidTableException. */
    @Test
    public void testSystemsColumns() throws Exception {
        String dbName = DEFAULT_TABLE_PATH.getDatabaseName();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .column("__offset", DataTypes.STRING())
                                        .column("__timestamp", DataTypes.STRING())
                                        .column("__bucket", DataTypes.STRING())
                                        .build())
                        .build();

        TablePath tablePath = TablePath.of(dbName, "test_system_columns");

        // Creating this table should throw InvalidTableException
        assertThatThrownBy(() -> admin.createTable(tablePath, tableDescriptor, false).get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "__offset, __timestamp, __bucket cannot be used as column names, "
                                + "because they are reserved system columns in Fluss. "
                                + "Please use other names for these columns. "
                                + "The reserved system columns are: __offset, __timestamp, __bucket");
    }
}
