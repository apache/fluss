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

import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.metadata.BulkLoadBuildContext;
import org.apache.fluss.client.metadata.BulkLoadHandle;
import org.apache.fluss.client.metadata.BulkLoadInfo;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.BucketCalculator;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.BulkLoad;
import org.apache.fluss.client.table.writer.BulkLoadBucketWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.action.ActionLoader;
import org.apache.fluss.flink.action.FlussActionEntrypoint;
import org.apache.fluss.flink.action.bulkload.LoadLakeDataToFlussAction;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.TestFileSystem;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
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
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStream;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
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

    private final Map<TablePath, Table> bulkLoadTables = new HashMap<>();

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

    @AfterEach
    void closeBulkLoadTables() throws Exception {
        IOUtils.closeAll(bulkLoadTables.values());
    }

    private BulkLoad bulkLoad(TablePath path) {
        return bulkLoadTables.computeIfAbsent(path, conn::getTable).newBulkLoad();
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

        TableInfo tableInfo =
                assertEnableFlussProcedureSucceeds(
                        tablePath,
                        "'bucket.num=1,table.log.format=compacted,table.datalake.freshness=500ms'");
        assertThat(tableInfo.getTableConfig().getDataLakeFormat()).contains(DataLakeFormat.PAIMON);
        assertThat(tableInfo.getNumBuckets()).isEqualTo(1);

        LakeTableSnapshot registeredSnapshot =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getLakeTableSnapshot(tableInfo.getTableId(), null)
                        .get();
        assertThat(registeredSnapshot.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(registeredSnapshot.getBucketLogEndOffset()).isEmpty();

        // Repeating the procedure must leave the registered snapshot unchanged.
        assertEnableFlussProcedureSucceeds(tablePath, null);
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
    void testEnableFlussWithVarCharAndVarBinary() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_bounded_types");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("name", org.apache.paimon.types.DataTypes.VARCHAR(20).notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.VARBINARY(128))
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .build());

        TableInfo tableInfo = assertEnableFlussProcedureSucceeds(tablePath, null);
        assertThat(tableInfo.getSchema().getRowType().getField("name").getType())
                .isEqualTo(org.apache.fluss.types.DataTypes.STRING().copy(false));
        assertThat(tableInfo.getSchema().getRowType().getField("payload").getType())
                .isEqualTo(org.apache.fluss.types.DataTypes.BYTES());
    }

    @Test
    void testEnableFlussWithNestedVarCharAndVarBinary() throws Exception {
        TablePath compatibleTablePath = TablePath.of(DATABASE, "procedure_nested_bounded_types");
        createPaimonTable(
                compatibleTablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column(
                                "names",
                                org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.VARCHAR(20).notNull()))
                        .column(
                                "payloads",
                                org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.VARBINARY(128)))
                        .column(
                                "attributes",
                                org.apache.paimon.types.DataTypes.MAP(
                                        org.apache.paimon.types.DataTypes.VARCHAR(20),
                                        org.apache.paimon.types.DataTypes.VARBINARY(128)))
                        .column(
                                "person",
                                org.apache.paimon.types.DataTypes.ROW(
                                        org.apache.paimon.types.DataTypes.FIELD(
                                                0,
                                                "name",
                                                org.apache.paimon.types.DataTypes.VARCHAR(20))))
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .build());

        TableInfo tableInfo = assertEnableFlussProcedureSucceeds(compatibleTablePath, null);
        assertThat(tableInfo.getSchema().getRowType().getField("names").getType())
                .isEqualTo(
                        org.apache.fluss.types.DataTypes.ARRAY(
                                org.apache.fluss.types.DataTypes.STRING().copy(false)));
        assertThat(tableInfo.getSchema().getRowType().getField("payloads").getType())
                .isEqualTo(
                        org.apache.fluss.types.DataTypes.ARRAY(
                                org.apache.fluss.types.DataTypes.BYTES()));
        assertThat(tableInfo.getSchema().getRowType().getField("attributes").getType())
                .isEqualTo(
                        org.apache.fluss.types.DataTypes.MAP(
                                org.apache.fluss.types.DataTypes.STRING(),
                                org.apache.fluss.types.DataTypes.BYTES()));
        assertThat(tableInfo.getSchema().getRowType().getField("person").getType())
                .isEqualTo(
                        org.apache.fluss.types.DataTypes.ROW(
                                org.apache.fluss.types.DataTypes.FIELD(
                                        "name", org.apache.fluss.types.DataTypes.STRING())));

        TablePath mismatchedTablePath =
                TablePath.of(DATABASE, "procedure_nested_nullability_mismatch");
        createPaimonTable(
                mismatchedTablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column(
                                "names",
                                org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.VARCHAR(20).notNull()))
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .build());

        // Recreate the empty Paimon table after Fluss metadata is recorded with a non-nullable
        // element. The procedure must reject the changed lake schema before enabling reads.
        admin.createTableFromLake(mismatchedTablePath, Collections.<String, String>emptyMap())
                .get();
        paimonCatalog.dropTable(
                Identifier.create(DATABASE, mismatchedTablePath.getTableName()), false);
        createPaimonTable(
                mismatchedTablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column(
                                "names",
                                org.apache.paimon.types.DataTypes.ARRAY(
                                        org.apache.paimon.types.DataTypes.VARCHAR(20)))
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .build());

        assertThatThrownBy(
                        () ->
                                tableEnvironment
                                        .executeSql(procedureSql(mismatchedTablePath, null))
                                        .await())
                .rootCause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("definition does not match");
        assertThat(
                        admin.getTableInfo(mismatchedTablePath)
                                .get()
                                .getTableConfig()
                                .isDataLakeEnabled())
                .isFalse();
    }

    @Test
    void testEnableFlussSucceedsWhenEffectivePaimonOptionsMatch() throws Exception {
        // The existing Paimon table may omit bucket or explicitly set bucket=-1. Both resolve
        // to -1, while the proposed Paimon schema built from the Fluss descriptor explicitly
        // sets bucket=-1. Enablement should succeed for either existing form.
        for (boolean explicitBucket : new boolean[] {false, true}) {
            TablePath tablePath =
                    TablePath.of(
                            DATABASE,
                            explicitBucket
                                    ? "procedure_explicit_bucket_unaware"
                                    : "procedure_implicit_bucket_unaware");
            Schema.Builder schemaBuilder =
                    Schema.newBuilder()
                            .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                            .column("payload", org.apache.paimon.types.DataTypes.STRING());
            if (explicitBucket) {
                schemaBuilder.option(CoreOptions.BUCKET.key(), "-1");
            }
            createPaimonTable(tablePath, schemaBuilder.build());

            assertEnableFlussProcedureSucceeds(tablePath, null);
        }

        // The existing fixed-bucket table may omit bucket-function.type or set it to "default".
        // Both resolve to the default bucket function, while the proposed Paimon schema built
        // from the Fluss descriptor omits this option. Enablement should succeed for both.
        for (boolean explicitFunction : new boolean[] {false, true}) {
            TablePath tablePath =
                    TablePath.of(
                            DATABASE,
                            explicitFunction
                                    ? "procedure_explicit_default_bucket_function"
                                    : "procedure_implicit_default_bucket_function");
            Schema.Builder schemaBuilder =
                    Schema.newBuilder()
                            .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                            .column("payload", org.apache.paimon.types.DataTypes.STRING())
                            .option(CoreOptions.BUCKET.key(), "2")
                            .option(CoreOptions.BUCKET_KEY.key(), "id");
            if (explicitFunction) {
                schemaBuilder.option(CoreOptions.BUCKET_FUNCTION_TYPE.key(), "default");
            }
            createPaimonTable(tablePath, schemaBuilder.build());

            TableInfo tableInfo = assertEnableFlussProcedureSucceeds(tablePath, null);
            assertThat(tableInfo.getNumBuckets()).isEqualTo(2);
        }
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
        TableInfo enabled = assertEnableFlussProcedureSucceeds(tablePath, null);
        assertThat(enabled.getTableId()).isEqualTo(created.getTableId());
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

        assertEnableFlussProcedureSucceeds(
                tablePath, "'bucket.num=1,table.datalake.freshness=500ms'");

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

        assertEnableFlussProcedureSucceeds(tablePath, "'table.datalake.freshness=500ms'");
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

        // Re-enabling must reuse the snapshot registered before lake reads were disabled.
        TableInfo tableInfo = assertEnableFlussProcedureSucceeds(tablePath, null);
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
        // Repeating the procedure after a schema change should still succeed.
        assertEnableFlussProcedureSucceeds(tablePath, null);
    }

    @Test
    void testLoadPrimaryKeySnapshotAndContinueTiering() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_primary_key_table");
        FileStoreTable paimonTable =
                createPaimonTable(
                        tablePath,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .primaryKey("id")
                                .option(CoreOptions.BUCKET.key(), "2")
                                .option("partition.legacy-name", "false")
                                .build());
        // Keep the source's implicit bucket key through import and the server's enable check.
        assertThat(paimonTable.options()).doesNotContainKey(CoreOptions.BUCKET_KEY.key());
        writePaimonRows(
                paimonTable,
                GenericRow.of(1L, BinaryString.fromString("old")),
                GenericRow.of(2L, BinaryString.fromString("deleted")));
        writePaimonRows(
                paimonTable,
                GenericRow.of(1L, BinaryString.fromString("paimon")),
                GenericRow.ofKind(
                        org.apache.paimon.types.RowKind.DELETE,
                        2L,
                        BinaryString.fromString("deleted")));
        long snapshotId = paimonTable.latestSnapshot().get().id();
        tableEnvironment.getConfig().set("table.dml-sync", "true");
        assertEnableFlussProcedureSucceeds(tablePath, "'table.datalake.freshness=500ms'");
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getBucketKeys()).containsExactly("id");
        Identifier identifier = Identifier.create(DATABASE, tablePath.getTableName());
        paimonCatalog.invalidateTable(identifier);
        assertThat(paimonCatalog.getTable(identifier).options())
                .doesNotContainKey(CoreOptions.BUCKET_KEY.key());
        assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId())
                .isEqualTo(snapshotId);
        waitUntilImportedBucketsReady(tablePath);
        assertThat(admin.getLatestLakeSnapshot(tablePath).get().getTableBucketsOffset())
                .containsEntry(new TableBucket(tableInfo.getTableId(), 0), 0L)
                .containsEntry(new TableBucket(tableInfo.getTableId(), 1), 0L)
                .hasSize(2);
        try (Table table = conn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            assertThatRow(lookuper.lookup(row(1L)).get().getSingletonRow())
                    .withSchema(tableInfo.getRowType())
                    .isEqualTo(row(1L, "paimon"));
            assertThat(lookuper.lookup(row(2L)).get().getSingletonRow()).isNull();
        }
        assertEnableFlussProcedureSucceeds(tablePath, null);
        writeRows(tablePath, Collections.singletonList(row(1L, "fluss")), false);
        triggerAndWaitSnapshot(tableInfo.getTableId(), tableInfo.getNumBuckets());
        JobClient tieringJob = buildTieringJob(execEnv);
        try {
            waitUntil(
                    () ->
                            readPaimonRows(tablePath)
                                    .equals(Collections.singletonList(Row.of(1L, "fluss"))),
                    Duration.ofMinutes(2),
                    Duration.ofMillis(100),
                    "First Fluss update was not tiered to the promoted Paimon table.");
        } finally {
            tieringJob.cancel().get();
        }
    }

    @Test
    void testLoadEmptyTable() throws Exception {
        TablePath path = TablePath.of(DATABASE, "action_empty_table");
        FileStoreTable source =
                createPaimonTable(
                        path,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .primaryKey("id")
                                .option(CoreOptions.BUCKET.key(), "2")
                                .option("partition.legacy-name", "false")
                                .build());
        assertThat(source.latestSnapshot()).isEmpty();
        TableInfo created = admin.createTableFromLake(path, Collections.emptyMap()).get();
        LoadLakeDataToFlussAction action =
                (LoadLakeDataToFlussAction)
                        ActionLoader.createAction(loadActionArguments(path).toArray(new String[0]))
                                .get();

        // An empty source still submits a job whose finalizer enables the existing Fluss table.
        action.submit(execEnv).getJobExecutionResult().get(2, TimeUnit.MINUTES);
        TableInfo enabled = admin.getTableInfo(path).get();
        assertThat(enabled.getTableId()).isEqualTo(created.getTableId());
        assertThat(enabled.getTableConfig().isDataLakeEnabled()).isTrue();
        assertThat(source.latestSnapshot()).isEmpty();
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(path).get())
                .cause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class);

        action.submit(execEnv).getJobExecutionResult().get(2, TimeUnit.MINUTES);
    }

    @Test
    void testRejectRescaledSourcePartitions() throws Exception {
        TablePath path = TablePath.of(DATABASE, "rescaled_source_4_8");
        FileStoreTable source =
                createPaimonTable(
                        path,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .column("dt", org.apache.paimon.types.DataTypes.STRING().notNull())
                                .primaryKey("id", "dt")
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "4")
                                .option(CoreOptions.BUCKET_KEY.key(), "id")
                                .option("partition.legacy-name", "false")
                                .build());
        long snapshot =
                writePaimonRows(
                        source,
                        GenericRow.of(
                                1L,
                                BinaryString.fromString("value"),
                                BinaryString.fromString("old")));
        Identifier identifier = Identifier.create(DATABASE, path.getTableName());
        paimonCatalog.alterTable(
                identifier,
                Collections.singletonList(SchemaChange.setOption(CoreOptions.BUCKET.key(), "8")),
                false);
        paimonCatalog.invalidateTable(identifier);
        source = (FileStoreTable) paimonCatalog.getTable(identifier);
        // Changing the table option leaves the existing partition's physical layout unchanged.
        assertThat(source.newScan().plan().splits())
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(((DataSplit) split).totalBuckets()).isEqualTo(4);
                            assertThat(((DataSplit) split).bucket()).isEqualTo(2);
                        });

        TableInfo table = admin.createTableFromLake(path, Collections.emptyMap()).get();
        assertThat(table.getNumBuckets()).isEqualTo(8);
        BucketCalculator calculator = BucketCalculator.forBucketKeyRow(table);
        byte[] key = calculator.encodeKey(row(1L));
        // The old bucket is in range, but lookup routes this key to bucket 6 after expansion.
        assertThat(calculator.getBucketId(key, 4)).isEqualTo(2).isLessThan(table.getNumBuckets());
        assertThat(calculator.getBucketId(key, 8)).isEqualTo(6);
        String loadId = UUID.randomUUID().toString();
        List<String> arguments = loadActionArguments(path);
        Collections.addAll(
                arguments,
                "--source-snapshot-id",
                Long.toString(snapshot),
                "--bulk-load-id",
                loadId);
        LoadLakeDataToFlussAction action =
                (LoadLakeDataToFlussAction)
                        ActionLoader.createAction(arguments.toArray(new String[0])).get();
        assertThatThrownBy(
                        () -> {
                            // Cancel a job if validation incorrectly accepts the layout.
                            JobClient job = action.submit(execEnv);
                            job.cancel().get();
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source bucket count 4")
                .hasMessageContaining("target bucket count 8");
        assertThat(admin.getTableInfo(path).get().getTableConfig().isDataLakeEnabled()).isFalse();
        assertThat(
                        bulkLoad(path)
                                .getInfo(new BulkLoadHandle(table.getTableId(), loadId))
                                .get()
                                .getTargets())
                .allSatisfy(target -> assertThat(target.getCommittedBucketCount()).isZero());
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(path).get())
                .cause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "true, true"})
    void testLoadPartitions(boolean autoPartitioned, boolean expiredOnly) throws Exception {
        TablePath path =
                TablePath.of(DATABASE, "load_partitions_" + autoPartitioned + "_" + expiredOnly);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .column("region", org.apache.paimon.types.DataTypes.STRING().notNull())
                        .column("dt", org.apache.paimon.types.DataTypes.STRING().notNull())
                        .primaryKey("id", "region", "dt")
                        .partitionKeys("region", "dt")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option("partition.legacy-name", "false");
        if (autoPartitioned) {
            // Retention must use the time column even when it follows another partition key.
            schema.option(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$dt")
                    .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd")
                    .option("sink.process-time-zone", "UTC");
        }
        FileStoreTable source = createPaimonTable(path, schema.build());
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        String expired = today.minusDays(8).toString();
        String current = today.toString();
        String future = today.plusDays(3).toString();
        List<String> dates =
                expiredOnly
                        ? Collections.singletonList(expired)
                        : Arrays.asList(expired, current, future);
        List<GenericRow> rows = new ArrayList<>();
        for (String region : Arrays.asList("east", "west")) {
            for (String date : dates) {
                rows.add(
                        GenericRow.of(
                                1L,
                                BinaryString.fromString(region + "-" + date),
                                BinaryString.fromString(region),
                                BinaryString.fromString(date)));
            }
        }
        long snapshotId = writePaimonRows(source, rows.toArray(new GenericRow[0]));

        if (autoPartitioned) {
            tableEnvironment.getConfig().set("table.dml-sync", "false");
            try (CloseableIterator<Row> result =
                    tableEnvironment
                            .executeSql(
                                    procedureSql(path, "'table.auto-partition.num-retention=7'"))
                            .collect()) {
                assertThat(result.next().getField(0).toString()).matches("JobID=[0-9a-f]{32}");
            }
            waitUntil(
                    () -> admin.getTableInfo(path).get().getTableConfig().isDataLakeEnabled(),
                    Duration.ofMinutes(2),
                    "Asynchronous partition load did not enable the table.");
        } else {
            TableInfo created = admin.createTableFromLake(path, Collections.emptyMap()).get();
            org.apache.flink.configuration.Configuration jobConfig =
                    new org.apache.flink.configuration.Configuration();
            jobConfig.set(DeploymentOptions.TARGET, "local");
            jobConfig.set(DeploymentOptions.ATTACHED, true);
            jobConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);
            jobConfig.set(DEFAULT_PARALLELISM, 1);
            StreamContextEnvironment.setAsContext(
                    new DefaultExecutorServiceLoader(),
                    jobConfig,
                    getClass().getClassLoader(),
                    false,
                    true);
            try {
                // No IDs are supplied; all two-bucket partitions must load using one slot.
                List<String> arguments = loadActionArguments(path);
                Collections.addAll(arguments, "--fluss.client.fs.test.write-token", "writable");
                FlussActionEntrypoint.main(arguments.toArray(new String[0]));
            } finally {
                StreamContextEnvironment.unsetAsContext();
            }
            assertThat(admin.getTableInfo(path).get().getTableId()).isEqualTo(created.getTableId());
            assertThat(
                            ((TestFileSystem) new FsPath("test:///").getFileSystem())
                                    .getConfiguration()
                                    .toMap())
                    .containsEntry("test.write-token", "writable");
        }

        TableInfo tableInfo = admin.getTableInfo(path).get();
        assertThat(tableInfo.getTableConfig().isDataLakeEnabled()).isTrue();
        assertThat(tableInfo.getTableConfig().isHistoricalPartitionEnabled()).isFalse();
        assertThat(tableInfo.isAutoPartitioned()).isEqualTo(autoPartitioned);
        assertThat(tableInfo.getPartitionKeys()).containsExactly("region", "dt");
        List<String> expectedPartitions = new ArrayList<>();
        if (!expiredOnly) {
            expectedPartitions.addAll(
                    Arrays.asList(
                            "east$" + current,
                            "west$" + current,
                            "east$" + future,
                            "west$" + future));
        }
        if (!autoPartitioned) {
            expectedPartitions.addAll(Arrays.asList("east$" + expired, "west$" + expired));
        }
        List<PartitionInfo> loadedPartitions = admin.listPartitionInfos(path).get();
        assertThat(loadedPartitions)
                .extracting(PartitionInfo::getPartitionName)
                .containsExactlyInAnyOrderElementsOf(expectedPartitions);
        Map<TableBucket, Long> expectedOffsets = new HashMap<>();
        for (PartitionInfo partition : loadedPartitions) {
            for (int bucket = 0; bucket < 2; bucket++) {
                expectedOffsets.put(
                        new TableBucket(tableInfo.getTableId(), partition.getPartitionId(), bucket),
                        0L);
            }
        }
        LakeSnapshot snapshot = admin.getLatestLakeSnapshot(path).get();
        assertThat(snapshot.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(snapshot.getTableBucketsOffset()).isEqualTo(expectedOffsets);
        waitUntilImportedBucketsReady(path);
        try (Table table = conn.getTable(path)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            for (GenericRow sourceRow : rows) {
                String region = sourceRow.getString(2).toString();
                String date = sourceRow.getString(3).toString();
                if (autoPartitioned && expired.equals(date)) {
                    continue;
                }
                // Reusing the same ID verifies that both partition columns identify the row.
                assertThatRow(lookuper.lookup(row(1L, region, date)).get().getSingletonRow())
                        .withSchema(tableInfo.getRowType())
                        .isEqualTo(row(1L, sourceRow.getString(1).toString(), region, date));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEnableWithoutLoad(boolean primaryKey) throws Exception {
        TablePath path = TablePath.of(DATABASE, "procedure_no_load_" + primaryKey);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option("partition.legacy-name", "false");
        if (primaryKey) {
            schema.primaryKey("id");
        }
        FileStoreTable source = createPaimonTable(path, schema.build());
        // Neither a log table with data nor an empty primary-key table needs KV loading.
        if (!primaryKey) {
            writeSingleRow(source);
        }
        try (CloseableIterator<Row> rows =
                tableEnvironment.executeSql(procedureSql(path, null)).collect()) {
            assertThat(rows.next().getField(0).toString())
                    .startsWith("Successfully enabled Fluss on paimon table");
            assertThat(rows).isExhausted();
        }
        assertThat(admin.getTableInfo(path).get().getTableConfig().isDataLakeEnabled()).isTrue();
        if (primaryKey) {
            assertThatThrownBy(() -> admin.getLatestLakeSnapshot(path).get())
                    .cause()
                    .isInstanceOf(LakeTableSnapshotNotExistException.class);
        } else {
            assertThat(admin.getLatestLakeSnapshot(path).get().getSnapshotId())
                    .isEqualTo(source.latestSnapshot().get().id());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEnablePrimaryKeyTableWithoutSnapshot(boolean metadataExists) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_empty_" + metadataExists);
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option("partition.legacy-name", "false")
                        .build());
        TableInfo created = null;
        if (metadataExists) {
            // Resume after metadata creation stopped before the lake was enabled.
            created = admin.createTableFromLake(tablePath, Collections.emptyMap()).get();
            assertThat(created.getTableConfig().isDataLakeEnabled()).isFalse();
        }

        TableInfo enabled = assertEnableFlussProcedureSucceeds(tablePath, null);
        assertThat(enabled.hasPrimaryKey()).isTrue();
        assertThat(enabled.getTableConfig().getMergeEngineType()).isEmpty();
        if (metadataExists) {
            assertThat(enabled.getTableId()).isEqualTo(created.getTableId());
        }
        assertThat(assertEnableFlussProcedureSucceeds(tablePath, null).getTableId())
                .isEqualTo(enabled.getTableId());
        FileStoreTable paimonTable =
                (FileStoreTable)
                        paimonCatalog.getTable(
                                Identifier.create(DATABASE, tablePath.getTableName()));
        assertThat(paimonTable.options()).doesNotContainKey(CoreOptions.BUCKET_KEY.key());
        assertThat(paimonTable.latestSnapshot()).isEmpty();
        assertThat(new CoreOptions(paimonTable.options()).mergeEngine())
                .isEqualTo(CoreOptions.MergeEngine.DEDUPLICATE);
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .cause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    @ParameterizedTest
    @CsvSource({
        "first-row,true,false,BulkLoad",
        "partial-update,true,false,BulkLoad",
        "partial-update,false,true,BulkLoad",
        "aggregation,true,false,BulkLoad",
        "aggregation,false,true,aggregation.remove-record-on-delete",
        "aggregation,true,true,aggregation.remove-record-on-delete"
    })
    void testRejectNativePrimaryKeyMergeEngine(
            String engine, boolean ignoreDelete, boolean removeOnDelete, String expectedError)
            throws Exception {
        TablePath tablePath =
                TablePath.of(
                        DATABASE,
                        "native_"
                                + engine.replace('-', '_')
                                + "_"
                                + ignoreDelete
                                + "_"
                                + removeOnDelete);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("amount", org.apache.paimon.types.DataTypes.BIGINT())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option(CoreOptions.MERGE_ENGINE.key(), engine)
                        .option(CoreOptions.IGNORE_DELETE.key(), Boolean.toString(ignoreDelete))
                        .option(CoreOptions.IGNORE_UPDATE_BEFORE.key(), "true");
        if ("aggregation".equals(engine)) {
            schema.option("fields.amount.aggregate-function", "sum")
                    .option(
                            CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE.key(),
                            Boolean.toString(removeOnDelete));
        } else if ("partial-update".equals(engine)) {
            schema.option(
                    CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key(),
                    Boolean.toString(removeOnDelete));
        }
        Map<String, String> nativeOptions = createPaimonTable(tablePath, schema.build()).options();

        assertThatThrownBy(() -> tableEnvironment.executeSql(procedureSql(tablePath, null)).await())
                .rootCause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(expectedError);
        assertThat(admin.tableExists(tablePath).get()).isFalse();
        Identifier identifier = Identifier.create(DATABASE, tablePath.getTableName());
        paimonCatalog.invalidateTable(identifier);
        assertThat(paimonCatalog.getTable(identifier).options()).isEqualTo(nativeOptions);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testResumeAfterPartitionDeletion(boolean restartJob) throws Exception {
        TablePath path = TablePath.of(DATABASE, "resume_deleted_" + restartJob);
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        String old = today.minusDays(1).toString();
        String current = today.toString();
        FileStoreTable source =
                createPaimonTable(
                        path,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .column("dt", org.apache.paimon.types.DataTypes.STRING().notNull())
                                .primaryKey("id", "dt")
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "1")
                                .option(CoreOptions.BUCKET_KEY.key(), "id")
                                .option(
                                        CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(),
                                        "yyyy-MM-dd")
                                .option("sink.process-time-zone", "UTC")
                                .option("partition.legacy-name", "false")
                                .build());
        long snapshot =
                writePaimonRows(
                        source,
                        GenericRow.of(
                                1L, BinaryString.fromString("old"), BinaryString.fromString(old)),
                        GenericRow.of(
                                2L,
                                BinaryString.fromString("current"),
                                BinaryString.fromString(current)));
        TableInfo table = admin.createTableFromLake(path, Collections.emptyMap()).get();
        assertThat(table.isAutoPartitioned()).isTrue();
        List<String> partitions = Arrays.asList(old, current);
        for (String partition : partitions) {
            admin.createPartition(
                            path,
                            new PartitionSpec(Collections.singletonMap("dt", partition)),
                            true)
                    .get();
        }
        for (PartitionInfo partition : admin.listPartitionInfos(path).get()) {
            FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(
                    table.getTableId(), partition.getPartitionId());
        }
        String loadId = UUID.randomUUID().toString();
        BulkLoadBuildContext context = bulkLoad(path).begin(loadId, partitions).get();
        BulkLoadBuildContext.Target oldTarget = context.getTarget(PhysicalTablePath.of(path, old));
        BulkLoadBuildContext.Target currentTarget =
                context.getTarget(PhysicalTablePath.of(path, current));
        if (!restartJob) {
            try (BulkLoadBucketWriter writer =
                    bulkLoad(path).tryOpenBucketWriter(currentTarget, 0).get()) {
                writer.write(row(2L, "current", current));
                writer.commit();
            }
        }

        // Model retention deleting an original target while the job is stopped and lake is
        // disabled.
        assertThat(admin.getTableInfo(path).get().getTableConfig().isDataLakeEnabled()).isFalse();
        admin.dropPartition(path, new PartitionSpec(Collections.singletonMap("dt", old)), false)
                .get();
        // Drop acknowledges the registration removal before the coordinator processes deletion.
        // Wait for the old physical partition to be removed before committing or recreating it.
        waitUntil(
                () ->
                        !FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getPartitionAssignment(oldTarget.getPartitionId())
                                .isPresent(),
                Duration.ofMinutes(1),
                "The original load partition was not deleted.");
        if (!restartJob) {
            // The deleted target has no bucket commit record, but the transaction may still finish.
            bulkLoad(path).commit(context.getHandle()).get();
        }
        admin.createPartition(path, new PartitionSpec(Collections.singletonMap("dt", old)), false)
                .get();
        assertThat(admin.listPartitionInfos(path).get())
                .filteredOn(partition -> old.equals(partition.getPartitionName()))
                .singleElement()
                .satisfies(
                        partition -> {
                            FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(
                                    table.getTableId(), partition.getPartitionId());
                            assertThat(partition.getPartitionId())
                                    .isNotEqualTo(oldTarget.getPartitionId());
                        });
        List<String> arguments = loadActionArguments(path);
        Collections.addAll(
                arguments,
                "--source-snapshot-id",
                Long.toString(snapshot),
                "--bulk-load-id",
                loadId);
        LoadLakeDataToFlussAction action =
                (LoadLakeDataToFlussAction)
                        ActionLoader.createAction(arguments.toArray(new String[0])).get();
        assertThat(bulkLoad(path).getInfo(context.getHandle()).get().getTargets())
                .extracting(BulkLoadInfo.TargetProgress::getPartitionName)
                .containsExactlyInAnyOrder(old, current);
        FsPath metadata =
                new FsPath(
                        FlussPaths.remoteLakeTableSnapshotDir(
                                FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(),
                                path,
                                table.getTableId()),
                        "metadata");
        FileSystem fs = metadata.getFileSystem();
        try {
            if (restartJob) {
                fs.mkdirs(metadata.getParent());
                try (OutputStream out = fs.create(metadata, FileSystem.WriteMode.NO_OVERWRITE)) {
                    out.write(0);
                }
                org.apache.flink.configuration.Configuration jobConfig =
                        new org.apache.flink.configuration.Configuration();
                jobConfig.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");
                jobConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
                jobConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
                jobConfig.set(
                        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                        Duration.ofSeconds(3));
                execEnv.configure(jobConfig);
            }
            JobClient job = action.submit(execEnv);
            if (restartJob) {
                waitUntil(
                        () -> job.getJobStatus().get() == JobStatus.RESTARTING,
                        Duration.ofMinutes(1),
                        "The load did not restart after snapshot registration failed.");
                assertThat(bulkLoad(path).getInfo(context.getHandle()).get().getState())
                        .isEqualTo(BulkLoadInfo.State.COMMITTED);
                fs.delete(metadata, false);
            }
            job.getJobExecutionResult().get(2, TimeUnit.MINUTES);
        } finally {
            if (restartJob && fs.exists(metadata) && !fs.getFileStatus(metadata).isDir()) {
                fs.delete(metadata, false);
            }
        }

        BulkLoadInfo completed = bulkLoad(path).getInfo(context.getHandle()).get();
        assertThat(completed.getState()).isEqualTo(BulkLoadInfo.State.COMMITTED);
        assertThat(completed.getTargets())
                .filteredOn(target -> oldTarget.getPartitionId().equals(target.getPartitionId()))
                .singleElement()
                .satisfies(target -> assertThat(target.getCommittedBucketCount()).isZero());
        assertThat(admin.getTableInfo(path).get().getTableConfig().isDataLakeEnabled()).isTrue();
        LakeSnapshot registered = admin.getLatestLakeSnapshot(path).get();
        assertThat(registered.getSnapshotId()).isEqualTo(snapshot);
        // Neither the dropped ID nor a replacement ID belongs to the resumed snapshot's offsets.
        assertThat(registered.getTableBucketsOffset())
                .containsExactlyEntriesOf(
                        Collections.singletonMap(
                                new TableBucket(
                                        table.getTableId(), currentTarget.getPartitionId(), 0),
                                0L));
        waitUntilImportedBucketsReady(path);
        try (Table loaded = conn.getTable(path)) {
            Lookuper lookuper = loaded.newLookup().createLookuper();
            assertThatRow(lookuper.lookup(row(2L, current)).get().getSingletonRow())
                    .withSchema(table.getRowType())
                    .isEqualTo(row(2L, "current", current));
            assertThat(lookuper.lookup(row(1L, old)).get().getSingletonRow()).isNull();
            assertThat(admin.getLatestKvSnapshots(path, old).get().getSnapshotId(0)).isNotPresent();
        }
    }

    @Test
    void testResumePartialLoad() throws Exception {
        TablePath path = TablePath.of(DATABASE, "resume_partial_load");
        FileStoreTable source =
                createPaimonTable(
                        path,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .column("dt", org.apache.paimon.types.DataTypes.STRING().notNull())
                                .primaryKey("id", "dt")
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "2")
                                .option(CoreOptions.BUCKET_KEY.key(), "id")
                                .option("partition.legacy-name", "false")
                                .build());
        long snapshot =
                writePaimonRows(
                        source,
                        GenericRow.of(
                                1L, BinaryString.fromString("one"), BinaryString.fromString("p1")),
                        GenericRow.of(
                                3L, BinaryString.fromString("two"), BinaryString.fromString("p1")),
                        GenericRow.of(
                                2L,
                                BinaryString.fromString("outside"),
                                BinaryString.fromString("p2")));
        TableInfo table = admin.createTableFromLake(path, Collections.emptyMap()).get();
        List<String> partitions = Arrays.asList("p1", "empty");
        for (String partition : partitions) {
            admin.createPartition(
                            path,
                            new PartitionSpec(Collections.singletonMap("dt", partition)),
                            false)
                    .get();
        }
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(path);
        String loadId = UUID.randomUUID().toString();
        BulkLoadBuildContext context = bulkLoad(path).begin(loadId, partitions).get();
        BulkLoadBuildContext.Target target = context.getTarget(PhysicalTablePath.of(path, "p1"));
        InternalRow firstRow = row(1L, "one", "p1");
        InternalRow secondRow = row(3L, "two", "p1");
        int committedBucket = target.bucketOf(firstRow);
        int remainingBucket = target.bucketOf(secondRow);
        assertThat(remainingBucket).isNotEqualTo(committedBucket);
        try (BulkLoadBucketWriter writer =
                bulkLoad(path).tryOpenBucketWriter(target, committedBucket).get()) {
            writer.write(firstRow);
            writer.commit();
        }
        long importedSnapshot =
                admin.getLatestKvSnapshots(path, "p1")
                        .get()
                        .getSnapshotId(committedBucket)
                        .getAsLong();
        assertThat(admin.getLatestKvSnapshots(path, "p1").get().getSnapshotId(remainingBucket))
                .isNotPresent();
        List<String> arguments = loadActionArguments(path);
        Collections.addAll(
                arguments,
                "--bulk-load-id",
                loadId,
                "--source-snapshot-id",
                Long.toString(snapshot));
        LoadLakeDataToFlussAction action =
                (LoadLakeDataToFlussAction)
                        ActionLoader.createAction(arguments.toArray(new String[0])).get();
        action.submit(execEnv).getJobExecutionResult().get(2, TimeUnit.MINUTES);

        assertThat(bulkLoad(path).getInfo(context.getHandle()).get().getState())
                .isEqualTo(BulkLoadInfo.State.COMMITTED);
        assertThat(admin.getLatestKvSnapshots(path, "p1").get().getSnapshotId(committedBucket))
                .hasValue(importedSnapshot);
        assertThat(admin.getLatestKvSnapshots(path, "p1").get().getSnapshotId(remainingBucket))
                .isPresent();
        assertThat(admin.listPartitionInfos(path).get())
                .extracting(PartitionInfo::getPartitionName)
                .containsExactlyInAnyOrderElementsOf(partitions);
        Map<TableBucket, Long> offsets = new HashMap<>();
        for (String partition : partitions) {
            Long partitionId =
                    context.getTarget(PhysicalTablePath.of(path, partition)).getPartitionId();
            for (int bucket = 0; bucket < 2; bucket++) {
                offsets.put(new TableBucket(table.getTableId(), partitionId, bucket), 0L);
            }
        }
        LakeSnapshot registered = admin.getLatestLakeSnapshot(path).get();
        assertThat(registered.getSnapshotId()).isEqualTo(snapshot);
        assertThat(registered.getTableBucketsOffset()).isEqualTo(offsets);
        waitUntilImportedBucketsReady(path);
        try (Table loaded = conn.getTable(path)) {
            Lookuper lookuper = loaded.newLookup().createLookuper();
            assertThatRow(lookuper.lookup(row(1L, "p1")).get().getSingletonRow())
                    .withSchema(table.getRowType())
                    .isEqualTo(firstRow);
            assertThatRow(lookuper.lookup(row(3L, "p1")).get().getSingletonRow())
                    .withSchema(table.getRowType())
                    .isEqualTo(secondRow);
            assertThat(lookuper.lookup(row(1L, "empty")).get().getSingletonRow()).isNull();
        }
        // Repeating the invocation on an enabled table preserves the registered snapshot.
        action.submit(execEnv).getJobExecutionResult().get(2, TimeUnit.MINUTES);
        assertThat(admin.getLatestLakeSnapshot(path).get().getSnapshotId()).isEqualTo(snapshot);
    }

    private void waitUntilImportedBucketsReady(TablePath path) throws Exception {
        for (TableBucket bucket :
                admin.getLatestLakeSnapshot(path).get().getTableBucketsOffset().keySet()) {
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        }
    }

    private static List<String> loadActionArguments(TablePath path) {
        List<String> arguments = new ArrayList<>();
        arguments.add("load_lake_data_to_fluss");
        arguments.add("--table");
        arguments.add(path.toString());
        arguments.add("--fluss.bootstrap.servers");
        arguments.add(FLUSS_CLUSTER_EXTENSION.getBootstrapServers());
        arguments.add("--datalake.format");
        arguments.add("paimon");
        arguments.add("--datalake.paimon.metastore");
        arguments.add("filesystem");
        arguments.add("--datalake.paimon.warehouse");
        arguments.add(warehousePath);
        return arguments;
    }

    private TableInfo assertEnableFlussProcedureSucceeds(TablePath tablePath, String options)
            throws Exception {
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
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getTableConfig().isDataLakeEnabled()).isTrue();
        return tableInfo;
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
        return writePaimonRows(table, GenericRow.of(1L, BinaryString.fromString("paimon")));
    }

    private static long writePaimonRows(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
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
