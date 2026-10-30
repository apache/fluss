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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.LAKESTREAM_ENABLED_OPTION_KEY;
import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for creating Fluss metadata from existing Paimon tables. */
class CreateTableFromLakeITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static final String DATABASE = "fluss";
    private static final int DEFAULT_BUCKET_COUNT = 5;

    private static Catalog paimonCatalog;

    private Connection connection;
    private Admin admin;

    @BeforeEach
    void setUp() throws Exception {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = connection.getAdmin();
        paimonCatalog.createDatabase(DATABASE, true);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testCreatePrimaryKeyTableWithExistingData() throws Exception {
        TablePath tablePath = TablePath.of("promote_primary_key_database", "existing_data");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column(
                                "id",
                                org.apache.paimon.types.DataTypes.BIGINT().notNull(),
                                "identifier")
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "4")
                        .comment("existing primary-key table")
                        .build());
        FileStoreTable sourceTable = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        BatchWriteBuilder writeBuilder = sourceTable.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1L, BinaryString.fromString("existing")));
            commit.commit(write.prepareCommit());
        }
        Long snapshotId = sourceTable.snapshotManager().latestSnapshotId();
        assertThat(snapshotId).isNotNull();
        assertThat(admin.databaseExists(tablePath.getDatabaseName()).get()).isFalse();

        Map<String, String> options = Collections.emptyMap();
        TableInfo created = admin.createTableFromLake(tablePath, options).get();
        assertThat(created.getSchema().getPrimaryKeyColumnNames()).containsExactly("id");
        assertThat(created.getTableConfig().isDataLakeEnabled()).isFalse();
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .cause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class);

        FileStoreTable unchanged = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        assertThat(unchanged.schema()).isEqualTo(sourceTable.schema());
        assertThat(unchanged.snapshotManager().latestSnapshotId()).isEqualTo(snapshotId);

        assertThatThrownBy(() -> admin.createTableFromLake(tablePath, options).get())
                .cause()
                .isInstanceOf(TableAlreadyExistException.class);
        assertThat(admin.getTableInfo(tablePath).get()).isEqualTo(created);
    }

    @ParameterizedTest
    @CsvSource({
        "partial-update,true",
        "partial-update,false",
        "aggregation,true",
        "aggregation,false"
    })
    void testCreatePrimaryKeyTableWithMergeSemantics(String engine, boolean ignoreDelete)
            throws Exception {
        TablePath tablePath =
                TablePath.of(DATABASE, "promote_" + engine.replace('-', '_') + "_" + ignoreDelete);
        boolean aggregation = "aggregation".equals(engine);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("amount", org.apache.paimon.types.DataTypes.BIGINT())
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "4")
                        .option(CoreOptions.MERGE_ENGINE.key(), engine)
                        .option(CoreOptions.IGNORE_DELETE.key(), Boolean.toString(ignoreDelete))
                        .option(CoreOptions.IGNORE_UPDATE_BEFORE.key(), "true")
                        .option(
                                CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key(),
                                Boolean.toString(!ignoreDelete))
                        .option(
                                CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE.key(),
                                Boolean.toString(!ignoreDelete));
        if (aggregation) {
            schema.option("fields.amount.aggregate-function", "sum");
        }
        createPaimonTable(tablePath, schema.build());

        TableInfo created =
                admin.createTableFromLake(tablePath, Collections.<String, String>emptyMap()).get();
        assertThat(created.getTableConfig().getMergeEngineType())
                .contains(MergeEngineType.AGGREGATION);
        assertThat(created.getTableConfig().getDeleteBehavior())
                .contains(ignoreDelete ? DeleteBehavior.IGNORE : DeleteBehavior.ALLOW);
        assertThat(created.getSchema().getAggFunction("amount"))
                .contains(
                        aggregation ? AggFunctions.SUM() : AggFunctions.LAST_VALUE_IGNORE_NULLS());
        assertThat(created.getSchema().getAggFunction("payload"))
                .contains(AggFunctions.LAST_VALUE_IGNORE_NULLS());
        assertThat(created.getTableConfig().isDataLakeEnabled()).isFalse();
        assertThatThrownBy(
                        () ->
                                admin.createTableFromLake(
                                                tablePath, Collections.<String, String>emptyMap())
                                        .get())
                .cause()
                .isInstanceOf(TableAlreadyExistException.class);
        // Request the opposite DELETE behavior when reusing the existing Fluss table.
        // IGNORE and ALLOW are both explicitly derived, so neither can be overridden.
        assertCreateTableFromLakeFails(
                tablePath,
                ConfigOptions.TABLE_DELETE_BEHAVIOR.key(),
                ignoreDelete ? "allow" : "ignore",
                "cannot be set");
    }

    @ParameterizedTest
    @CsvSource({
        // engine, ignoreDelete, removeOnDelete, function, ignoreRetract, expectedMessage
        // Partial-update cannot accept tiering retractions with both options disabled.
        "partial-update, false, false, , false, partial-update.remove-record-on-delete",
        // Aggregation's default DELETE retracts field contributions, which Fluss cannot map.
        "aggregation, false, false, sum, false, aggregation.remove-record-on-delete",
        // SUM must retract the previous aggregate result before accumulating the next one.
        "aggregation, true, false, sum, true, fields.amount.ignore-retract",
        // MAX must ignore retractions, including when whole-row DELETE is enabled.
        "aggregation, true, false, max, false, fields.amount.ignore-retract",
        "aggregation, false, true, max, false, fields.amount.ignore-retract"
    })
    void testRejectUnsafePrimaryKeyMergeSemantics(
            String engine,
            boolean ignoreDelete,
            boolean removeOnDelete,
            @Nullable String function,
            boolean ignoreRetract,
            String expectedMessage)
            throws Exception {
        TablePath tablePath =
                TablePath.of(
                        DATABASE,
                        String.format(
                                "unsafe_merge_%s_%s_%s_%s",
                                engine.replace('-', '_'), ignoreDelete, removeOnDelete, function));
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("amount", org.apache.paimon.types.DataTypes.BIGINT())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "4")
                        .option(CoreOptions.MERGE_ENGINE.key(), engine)
                        .option(CoreOptions.IGNORE_DELETE.key(), Boolean.toString(ignoreDelete))
                        .option(CoreOptions.IGNORE_UPDATE_BEFORE.key(), "true")
                        .option(
                                CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key(),
                                Boolean.toString(removeOnDelete))
                        .option(
                                CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE.key(),
                                Boolean.toString(removeOnDelete));
        if (function != null) {
            schema.option("fields.amount.aggregate-function", function)
                    .option("fields.amount.ignore-retract", Boolean.toString(ignoreRetract));
        }
        createPaimonTable(tablePath, schema.build());

        assertThatThrownBy(
                        () ->
                                admin.createTableFromLake(
                                                tablePath, Collections.<String, String>emptyMap())
                                        .get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(expectedMessage);
        assertThat(admin.tableExists(tablePath).get()).isFalse();
    }

    @Test
    void testPrimaryKeyDerivedOptionsCannotBeOverridden() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "promote_versioned");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("version", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column(
                                "event_time",
                                org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "4")
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "version")
                        .option(CoreOptions.IGNORE_DELETE.key(), "true")
                        .option(CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(), "7 d")
                        .option(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "event_time")
                        .build());

        assertCreateTableFromLakeFails(
                tablePath,
                ConfigOptions.TABLE_MERGE_ENGINE.key(),
                "first_row",
                "is derived as '" + MergeEngineType.VERSIONED + "'");
        assertCreateTableFromLakeFails(
                tablePath,
                ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(),
                "id",
                "is derived as 'version'");
        assertCreateTableFromLakeFails(
                tablePath, ConfigOptions.TABLE_KV_TTL.key(), "1 d", "is derived as");
        assertCreateTableFromLakeFails(
                tablePath, ConfigOptions.TABLE_KV_TTL.key(), "invalid", "is derived as");
        assertCreateTableFromLakeFails(
                tablePath, ConfigOptions.TABLE_MERGE_ENGINE.key(), "invalid", "is derived as");
        assertCreateTableFromLakeFails(tablePath, "bucket.num", "8", "uses 4 buckets");
        assertThat(admin.tableExists(tablePath).get()).isFalse();

        TableInfo created =
                admin.createTableFromLake(
                                tablePath,
                                Collections.singletonMap(
                                        ConfigOptions.TABLE_KV_TTL.key(), "604800000 ms"))
                        .get();
        assertThat(created.getTableConfig().getMergeEngineType())
                .contains(MergeEngineType.VERSIONED);
        assertThat(created.getProperties().get(ConfigOptions.TABLE_KV_TTL))
                .isEqualTo(Duration.ofDays(7));
    }

    @ParameterizedTest
    @CsvSource({", allow", ", ignore", "first_row, ignore", "versioned, ignore"})
    void testSetPrimaryKeyOptionsNotDerivedFromLake(
            @Nullable String mergeEngine, String deleteBehavior) throws Exception {
        TablePath tablePath =
                TablePath.of(DATABASE, "primary_key_options_" + mergeEngine + "_" + deleteBehavior);
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("version", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "4")
                        .build());

        Map<String, String> options = new HashMap<>();
        options.put(ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), deleteBehavior);
        if (mergeEngine != null) {
            options.put(ConfigOptions.TABLE_MERGE_ENGINE.key(), mergeEngine);
        }
        if ("versioned".equals(mergeEngine)) {
            options.put(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(), "version");
        }
        TableInfo created = admin.createTableFromLake(tablePath, options).get();
        assertThat(created.getProperties().toMap()).containsAllEntriesOf(options);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCreatePartitionedPrimaryKeyTable(boolean inferAutoPartition) throws Exception {
        TablePath tablePath =
                TablePath.of(DATABASE, "promote_partitioned_primary_key_" + inferAutoPartition);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("dt", org.apache.paimon.types.DataTypes.STRING().notNull())
                        .column("region", org.apache.paimon.types.DataTypes.STRING().notNull())
                        .primaryKey("region", "id", "dt")
                        .partitionKeys("dt", "region")
                        .option(CoreOptions.BUCKET.key(), "4");
        Map<String, String> options = new HashMap<>();
        if (inferAutoPartition) {
            schema.option(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$dt")
                    .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd");
        } else {
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true");
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key(), "dt");
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "DAY");
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(), "0");
        }
        createPaimonTable(tablePath, schema.build());

        TableInfo created = admin.createTableFromLake(tablePath, options).get();
        assertThat(created.getSchema().getPrimaryKeyColumnNames())
                .containsExactly("region", "id", "dt");
        assertThat(created.getPartitionKeys()).containsExactly("dt", "region");
        assertThat(created.getBucketKeys()).containsExactly("id");
        assertThat(created.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED))
                .isTrue();
        assertThat(created.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_KEY))
                .isEqualTo("dt");
        assertThat(created.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE))
                .isZero();
        assertThat(created.getTableConfig().isDataLakeEnabled()).isFalse();
    }

    @Test
    void testCreateHashFixedTableOnLake() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "promote_hash_fixed");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("payload", org.apache.paimon.types.DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "4")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .build());

        TableInfo created =
                admin.createTableFromLake(
                                tablePath,
                                Collections.singletonMap(
                                        ConfigOptions.TABLE_LOG_FORMAT.key(), "compacted"))
                        .get();
        TableInfo stored = admin.getTableInfo(tablePath).get();

        assertThat(created.getTableId()).isEqualTo(stored.getTableId());
        assertThat(created.toTableDescriptor()).isEqualTo(stored.toTableDescriptor());
        assertThat(created.getSchema().getColumnNames()).containsExactly("id", "payload");
        assertThat(created.getBucketKeys()).containsExactly("id");
        assertThat(created.getNumBuckets()).isEqualTo(4);
        assertThat(created.getTableConfig().isDataLakeEnabled()).isFalse();
        assertThat(created.getTableConfig().getDataLakeFormat()).contains(DataLakeFormat.PAIMON);
        assertThat(created.getProperties().toMap())
                .containsEntry(ConfigOptions.TABLE_LOG_FORMAT.key(), "compacted")
                .doesNotContainKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key());

        Table paimonTable = paimonCatalog.getTable(toPaimon(tablePath));
        assertThat(paimonTable.options())
                .containsEntry(CoreOptions.BUCKET.key(), "4")
                .containsEntry(CoreOptions.BUCKET_KEY.key(), "id")
                .doesNotContainKey(LAKESTREAM_ENABLED_OPTION_KEY);
    }

    @Test
    void testCreateBucketUnawarePartitionedTableOnLake() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "promote_partitioned_bucket_unaware");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .column("region", org.apache.paimon.types.DataTypes.STRING())
                        .column("dt", org.apache.paimon.types.DataTypes.STRING())
                        .partitionKeys("region", "dt")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$dt")
                        .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd")
                        .option("sink.process-time-zone", "Asia/Shanghai")
                        .build());

        Map<String, String> options = new HashMap<>();
        options.put("bucket.num", "6");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy-MM-dd");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(), "30");
        TableInfo tableInfo = admin.createTableFromLake(tablePath, options).get();

        assertThat(tableInfo.getPartitionKeys()).containsExactly("region", "dt");
        assertThat(tableInfo.getBucketKeys()).isEmpty();
        assertThat(tableInfo.getNumBuckets()).isEqualTo(6);
        assertThat(tableInfo.getProperties().toMap())
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key(), "dt")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy-MM-dd")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "DAY")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(), "Asia/Shanghai")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(), "0")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(), "30")
                .doesNotContainKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
        assertThat(tableInfo.getTableConfig().isDataLakeEnabled()).isFalse();

        assertThat(paimonCatalog.getTable(toPaimon(tablePath)).options())
                .containsEntry(CoreOptions.BUCKET.key(), "-1");
    }

    @Test
    void testBucketUnawareTableUsesClusterDefault() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "promote_bucket_unaware_default");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build());

        TableInfo tableInfo =
                admin.createTableFromLake(tablePath, Collections.<String, String>emptyMap()).get();

        assertThat(tableInfo.getNumBuckets()).isEqualTo(DEFAULT_BUCKET_COUNT);
        assertThat(tableInfo.getProperties().toMap())
                .doesNotContainKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
    }

    @Test
    void testSpecifyAutoPartitionPropertiesWhenNotDerived() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "promote_manual_auto_partition");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .column("dt", org.apache.paimon.types.DataTypes.STRING())
                        .partitionKeys("dt")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build());

        Map<String, String> options = new HashMap<>();
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key(), "dt");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy-MM-dd");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "DAY");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(), "Asia/Shanghai");

        TableInfo tableInfo = admin.createTableFromLake(tablePath, options).get();

        assertThat(tableInfo.getProperties().toMap())
                .containsAllEntriesOf(options)
                .doesNotContainKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
    }

    @Test
    void testCreateTableFromLakeCreatesMissingDatabase() throws Exception {
        TablePath tablePath = TablePath.of("promote_new_database", "promote_table");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build());

        assertThat(admin.databaseExists(tablePath.getDatabaseName()).get()).isFalse();

        TableInfo tableInfo =
                admin.createTableFromLake(tablePath, Collections.<String, String>emptyMap()).get();

        assertThat(admin.databaseExists(tablePath.getDatabaseName()).get()).isTrue();
        assertThat(admin.tableExists(tablePath).get()).isTrue();
        assertThat(tableInfo.getTablePath()).isEqualTo(tablePath);
        assertThat(tableInfo.getProperties().toMap())
                .doesNotContainKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
    }

    @Test
    void testRejectUnsupportedProperties() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "promote_invalid_properties");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "4")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .build());

        assertCreateTableFromLakeFails(
                tablePath,
                ConfigOptions.TABLE_DATALAKE_ENABLED.key(),
                "false",
                "managed by Fluss when creating a table from Paimon");
        assertCreateTableFromLakeFails(
                tablePath,
                ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                "iceberg",
                "must match cluster");
        assertCreateTableFromLakeFails(
                tablePath,
                ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED.key(),
                "true",
                ConfigOptions.TABLE_DATALAKE_ENABLED.key());
        assertCreateTableFromLakeFails(
                tablePath,
                ConfigOptions.TABLE_KV_TTL.key(),
                "1 d",
                "only supported for primary key tables");
        assertCreateTableFromLakeFails(
                tablePath, "paimon.file.format", "parquet", "not a Fluss table property");
        assertCreateTableFromLakeFails(
                tablePath, "table.future-option", "value", "not a recognized Fluss table property");
        assertCreateTableFromLakeFails(tablePath, "bucket.num", "8", "uses 4 buckets");

        assertThat(admin.tableExists(tablePath).get()).isFalse();
    }

    @Test
    void testRejectConflictingDerivedPartitionProperty() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "promote_conflicting_partition_property");
        createPaimonTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .column("dt", org.apache.paimon.types.DataTypes.STRING())
                        .partitionKeys("dt")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd")
                        .build());

        assertCreateTableFromLakeFails(
                tablePath,
                ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(),
                "yyyy-MM-dd",
                "is derived as 'yyyyMMdd'");
        assertThat(admin.tableExists(tablePath).get()).isFalse();
    }

    private void assertCreateTableFromLakeFails(
            TablePath tablePath, String key, String value, String expectedMessage) {
        assertThatThrownBy(
                        () ->
                                admin.createTableFromLake(
                                                tablePath, Collections.singletonMap(key, value))
                                        .get())
                .cause()
                .hasMessageContaining(expectedMessage);
    }

    private static void createPaimonTable(TablePath tablePath, Schema schema) throws Exception {
        paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
        paimonCatalog.createTable(toPaimon(tablePath), schema, false);
    }

    private static Identifier toPaimon(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private static Configuration initConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        configuration.set(ConfigOptions.DEFAULT_BUCKET_NUMBER, DEFAULT_BUCKET_COUNT);
        configuration.setString("datalake.paimon.metastore", "filesystem");
        try {
            configuration.setString(
                    "datalake.paimon.warehouse",
                    Files.createTempDirectory("fluss-create-table-from-lake")
                            .resolve("warehouse")
                            .toString());
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create Paimon warehouse.", e);
        }
        configuration.setString("datalake.paimon.cache-enabled", "false");
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                Options.fromMap(extractLakeProperties(configuration))));
        return configuration;
    }
}
