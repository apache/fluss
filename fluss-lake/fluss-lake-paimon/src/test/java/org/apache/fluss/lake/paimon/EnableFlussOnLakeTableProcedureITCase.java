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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.MergeEngineType;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    void testRejectPrimaryKeyTableWithSnapshotBeforeRegisteringOrEnabling() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "procedure_primary_key_table");
        FileStoreTable paimonTable =
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
        writeSingleRow(paimonTable);

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
                .hasMessageContaining("requires Bulk Load");
        assertThat(admin.getTableInfo(tablePath).get().getTableConfig().isDataLakeEnabled())
                .isFalse();
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .cause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    @ParameterizedTest
    @CsvSource({
        ", false",
        ", true",
        "first_row, false",
        "first_row, true",
        "versioned, false",
        "versioned, true",
        "aggregation, false",
        "aggregation, true"
    })
    void testEnablePrimaryKeyTableWithoutSnapshot(String mergeEngine, boolean metadataExists)
            throws Exception {
        TablePath tablePath =
                TablePath.of(DATABASE, "procedure_empty_" + mergeEngine + "_" + metadataExists);
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .column("version", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option("partition.legacy-name", "false")
                        .build());
        // Paimon stores the materialized Fluss results with deduplicate for every Fluss engine.
        Map<String, String> options = new HashMap<>();
        if (mergeEngine != null) {
            options.put(ConfigOptions.TABLE_MERGE_ENGINE.key(), mergeEngine);
        }
        if ("versioned".equals(mergeEngine)) {
            options.put(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(), "version");
        }
        String procedureOptions =
                options.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining(",", "'", "'"));
        TableInfo created = null;
        if (metadataExists) {
            // Resume after metadata creation stopped before the lake was enabled.
            created = admin.createTableFromLake(tablePath, options).get();
            assertThat(created.getTableConfig().isDataLakeEnabled()).isFalse();
        }

        TableInfo enabled = assertEnableFlussProcedureSucceeds(tablePath, procedureOptions);
        assertThat(enabled.hasPrimaryKey()).isTrue();
        if (mergeEngine == null) {
            assertThat(enabled.getTableConfig().getMergeEngineType()).isEmpty();
        } else {
            assertThat(enabled.getTableConfig().getMergeEngineType())
                    .contains(MergeEngineType.fromString(mergeEngine));
        }
        if (metadataExists) {
            assertThat(enabled.getTableId()).isEqualTo(created.getTableId());
        }
        assertThat(assertEnableFlussProcedureSucceeds(tablePath, procedureOptions).getTableId())
                .isEqualTo(enabled.getTableId());
        FileStoreTable paimonTable =
                (FileStoreTable)
                        paimonCatalog.getTable(
                                Identifier.create(DATABASE, tablePath.getTableName()));
        assertThat(paimonTable.latestSnapshot()).isEmpty();
        assertThat(new CoreOptions(paimonTable.options()).mergeEngine())
                .isEqualTo(CoreOptions.MergeEngine.DEDUPLICATE);
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .cause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    @ParameterizedTest
    @CsvSource({
        "first-row,true,false,first_row",
        "partial-update,true,false,aggregation",
        "partial-update,false,true,aggregation",
        "aggregation,true,false,aggregation",
        "aggregation,false,true,aggregation"
    })
    void testEnableNativePrimaryKeyMergeEngine(
            String engine, boolean ignoreDelete, boolean removeOnDelete, String flussEngine)
            throws Exception {
        TablePath tablePath =
                TablePath.of(DATABASE, "native_" + engine.replace('-', '_') + "_" + ignoreDelete);
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

        TableInfo enabled = assertEnableFlussProcedureSucceeds(tablePath, null);
        assertThat(enabled.getTableConfig().getMergeEngineType())
                .contains(MergeEngineType.fromString(flussEngine));

        // Re-enable and subsequent schema changes must also retain the native merge settings.
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false")),
                        false)
                .get();
        assertThat(assertEnableFlussProcedureSucceeds(tablePath, null).getTableId())
                .isEqualTo(enabled.getTableId());
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
        Identifier identifier = Identifier.create(DATABASE, tablePath.getTableName());
        paimonCatalog.invalidateTable(identifier);
        FileStoreTable paimonTable = (FileStoreTable) paimonCatalog.getTable(identifier);
        assertThat(paimonTable.options()).containsAllEntriesOf(nativeOptions);
        assertThat(paimonTable.rowType().getFieldNames()).containsExactly("id", "amount", "extra");
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
