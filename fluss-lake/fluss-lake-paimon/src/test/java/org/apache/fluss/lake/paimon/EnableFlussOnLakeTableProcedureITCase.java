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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for enabling Fluss on existing Paimon tables through the Flink procedure. */
class EnableFlussOnLakeTableProcedureITCase extends FlinkPaimonTieringTestBase {

    private static final String CATALOG_NAME = "fluss_catalog";
    private static final String DATABASE = "fluss";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(createClusterConfiguration())
                    .build();

    private TableEnvironment tableEnvironment;

    @BeforeAll
    static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @BeforeEach
    void setUp() throws Exception {
        paimonCatalog.createDatabase(DATABASE, true);
        tableEnvironment = createTableEnvironment(EnvironmentSettings.inStreamingMode());
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    private static TableEnvironment createTableEnvironment(EnvironmentSettings settings)
            throws Exception {
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        tableEnvironment
                .executeSql(
                        String.format(
                                "CREATE CATALOG %s WITH ("
                                        + "'type' = 'fluss', "
                                        + "'bootstrap.servers' = '%s', "
                                        + "'paimon.metastore' = 'filesystem', "
                                        + "'paimon.warehouse' = '%s', "
                                        + "'paimon.cache-enabled' = 'false')",
                                CATALOG_NAME,
                                FLUSS_CLUSTER_EXTENSION.getBootstrapServers(),
                                warehousePath))
                .await();
        tableEnvironment.executeSql("USE CATALOG " + CATALOG_NAME).await();
        return tableEnvironment;
    }

    @Test
    void testEnableFlussWithTieringAndUnionRead() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_existing_table");
        FileStoreTable paimonTable =
                createPaimonTable(
                        tablePath,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .option(CoreOptions.BUCKET.key(), "1")
                                .option(CoreOptions.BUCKET_KEY.key(), "id")
                                .option("partition.legacy-name", "false")
                                .build());
        long snapshotId = writeSingleRow(paimonTable);

        assertProcedureSuccess(
                tablePath,
                "'bucket.num=1,table.log.format=compacted,table.datalake.freshness=500ms'");

        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getTableConfig().isDataLakeEnabled()).isTrue();
        assertThat(tableInfo.getTableConfig().getDataLakeFormat()).contains(DataLakeFormat.PAIMON);
        assertThat(tableInfo.getNumBuckets()).isEqualTo(1);

        LakeTableSnapshot registeredSnapshot =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getLakeTableSnapshot(tableInfo.getTableId(), null)
                        .get();
        assertThat(registeredSnapshot.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(registeredSnapshot.getBucketLogEndOffset()).isEmpty();

        assertProcedureSuccess(tablePath, null);
        LakeTableSnapshot snapshotAfterRetry =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getLakeTableSnapshot(tableInfo.getTableId(), null)
                        .get();
        assertThat(snapshotAfterRetry).isEqualTo(registeredSnapshot);

        writeRows(tablePath, Collections.singletonList(row(2L, "tiered")), true);
        JobClient tieringJob = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(new TableBucket(tableInfo.getTableId(), 0), 1L);
            assertThat(readPaimonRows(tablePath))
                    .containsExactlyInAnyOrder(Row.of(1L, "paimon"), Row.of(2L, "tiered"));
        } finally {
            tieringJob.cancel().get();
        }

        writeRows(tablePath, Collections.singletonList(row(3L, "fluss")), true);
        TableEnvironment batchTableEnvironment =
                createTableEnvironment(EnvironmentSettings.inBatchMode());
        List<Row> unionRows;
        try (CloseableIterator<Row> rows =
                batchTableEnvironment
                        .executeSql("SELECT * FROM " + tablePath.getTableName())
                        .collect()) {
            unionRows = CollectionUtil.iteratorToList(rows);
        }
        assertThat(unionRows)
                .containsExactlyInAnyOrder(
                        Row.of(1L, "paimon"), Row.of(2L, "tiered"), Row.of(3L, "fluss"));
    }

    @Test
    void testRetryAfterTableCreationBeforeSnapshotRegistration() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_retry_before_snapshot");
        FileStoreTable paimonTable =
                createPaimonTable(
                        tablePath,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .option(CoreOptions.BUCKET.key(), "1")
                                .option(CoreOptions.BUCKET_KEY.key(), "id")
                                .build());
        long snapshotId = writeSingleRow(paimonTable);

        TableInfo created =
                admin.createTableFromLake(tablePath, Collections.<String, String>emptyMap()).get();
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getLakeTableSnapshot(created.getTableId(), null))
                .isEmpty();

        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(created.getTableId());
        writeRows(tablePath, Collections.singletonList(row(2L, "fluss")), true);
        assertProcedureSuccess(tablePath, null);

        TableInfo enabled = admin.getTableInfo(tablePath).get();
        assertThat(enabled.getTableId()).isEqualTo(created.getTableId());
        assertThat(enabled.getTableConfig().isDataLakeEnabled()).isTrue();
        LakeTableSnapshot registered =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getLakeTableSnapshot(enabled.getTableId(), null)
                        .get();
        assertThat(registered.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(registered.getBucketLogEndOffset()).isEmpty();

        TableEnvironment batchTableEnvironment =
                createTableEnvironment(EnvironmentSettings.inBatchMode());
        try (CloseableIterator<Row> rows =
                batchTableEnvironment
                        .executeSql("SELECT * FROM " + tablePath.getTableName())
                        .collect()) {
            assertThat(CollectionUtil.iteratorToList(rows))
                    .containsExactlyInAnyOrder(Row.of(1L, "paimon"), Row.of(2L, "fluss"));
        }
    }

    @Test
    void testRejectIncompatibleExistingTable() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_incompatible_existing");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option("partition.legacy-name", "false")
                        .build());

        assertProcedureSuccess(tablePath, "'bucket.num=1,table.datalake.freshness=500ms'");
        assertProcedureSuccess(tablePath, "'bucket.num=1,table.datalake.freshness=500ms'");

        assertThatThrownBy(
                        () ->
                                tableEnvironment
                                        .executeSql(
                                                procedureSql(
                                                        tablePath, "'table.datalake.freshness=1s'"))
                                        .await())
                .rootCause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("definition does not match");
        assertThatThrownBy(
                        () ->
                                tableEnvironment
                                        .executeSql(procedureSql(tablePath, "'bucket.num=2'"))
                                        .await())
                .rootCause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("uses 1 buckets");

        paimonCatalog.alterTable(
                Identifier.create(DATABASE, tablePath.getTableName()),
                Collections.singletonList(
                        SchemaChange.addColumn("extra", org.apache.paimon.types.DataTypes.INT())),
                false);
        assertThatThrownBy(() -> tableEnvironment.executeSql(procedureSql(tablePath, null)).await())
                .rootCause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("definition does not match");
    }

    @Test
    void testReenableExistingTableWithRegisteredSnapshot() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_reenable_existing");
        FileStoreTable paimonTable =
                createPaimonTable(
                        tablePath,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .option(CoreOptions.BUCKET.key(), "1")
                                .option(CoreOptions.BUCKET_KEY.key(), "id")
                                .option("partition.legacy-name", "false")
                                .build());
        long snapshotId = writeSingleRow(paimonTable);

        assertProcedureSuccess(tablePath, "'table.datalake.freshness=500ms'");
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(TABLE_DATALAKE_ENABLED.key(), "false")),
                        false)
                .get();

        assertThatThrownBy(
                        () ->
                                tableEnvironment
                                        .executeSql(
                                                procedureSql(
                                                        tablePath, "'table.datalake.freshness=1s'"))
                                        .await())
                .rootCause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("definition does not match");
        assertThat(admin.getTableInfo(tablePath).get().getTableConfig().isDataLakeEnabled())
                .isFalse();

        assertProcedureSuccess(tablePath, null);

        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getTableConfig().isDataLakeEnabled()).isTrue();
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getLakeTableSnapshot(tableInfo.getTableId(), null)
                                .get()
                                .getSnapshotId())
                .isEqualTo(snapshotId);

        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "extra",
                                        org.apache.fluss.types.DataTypes.INT(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        assertProcedureSuccess(tablePath, null);
    }

    @Test
    void testRejectPrimaryKeyTableBeforeCreatingFlussMetadata() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_primary_key_table");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option("partition.legacy-name", "false")
                        .build());

        assertThatThrownBy(
                        () ->
                                tableEnvironment
                                        .executeSql(
                                                String.format(
                                                        "CALL %s.sys.enable_fluss_on_lake_table('%s')",
                                                        CATALOG_NAME, tablePath))
                                        .await())
                .rootCause()
                .hasMessageContaining("primary-key table")
                .hasMessageContaining("to a Fluss table is not supported yet");
        assertThat(admin.tableExists(tablePath).get()).isFalse();
    }

    private void assertProcedureSuccess(TablePath tablePath, String options) throws Exception {
        try (CloseableIterator<Row> rows =
                tableEnvironment.executeSql(procedureSql(tablePath, options)).collect()) {
            assertThat(rows).hasNext();
            assertThat(rows.next())
                    .isEqualTo(
                            Row.of(
                                    String.format(
                                            "Successfully enabled Fluss on paimon table '%s'.",
                                            tablePath)));
            assertThat(rows).isExhausted();
        }
    }

    private static String procedureSql(TablePath tablePath, String options) {
        return options == null
                ? String.format(
                        "CALL %s.sys.enable_fluss_on_lake_table('%s')",
                        CATALOG_NAME, tablePath.getTableName())
                : String.format(
                        "CALL %s.sys.enable_fluss_on_lake_table('%s', %s)",
                        CATALOG_NAME, tablePath.getTableName(), options);
    }

    private static FileStoreTable createPaimonTable(TablePath tablePath, Schema schema)
            throws Exception {
        Identifier identifier =
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
        paimonCatalog.createTable(identifier, schema, false);
        return (FileStoreTable) paimonCatalog.getTable(identifier);
    }

    private static long writeSingleRow(FileStoreTable table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1L, BinaryString.fromString("paimon")));
            commit.commit(write.prepareCommit());
        }
        return table.latestSnapshot().get().id();
    }

    private List<Row> readPaimonRows(TablePath tablePath) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (org.apache.paimon.utils.CloseableIterator<org.apache.paimon.data.InternalRow>
                iterator = getPaimonRowCloseableIterator(tablePath)) {
            while (iterator.hasNext()) {
                org.apache.paimon.data.InternalRow paimonRow = iterator.next();
                rows.add(Row.of(paimonRow.getLong(0), paimonRow.getString(1).toString()));
            }
        }
        return rows;
    }

    private static Configuration createClusterConfiguration() {
        Configuration configuration = FlinkPaimonTieringTestBase.initConfig();
        configuration.setString("datalake.paimon.cache-enabled", "false");
        return configuration;
    }
}
