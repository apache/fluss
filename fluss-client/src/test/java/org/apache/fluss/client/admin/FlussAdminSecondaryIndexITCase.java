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

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScanner;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for secondary index DDL operations via FlussAdmin. Migrated from V1
 * FlussAdminITCase and adapted to V2 index table naming and schema.
 */
class FlussAdminSecondaryIndexITCase extends ClientToServerITCaseBase {

    private static final String DB = "test_db_admin_idx";

    @Test
    void testCreateAndDropTableWithGlobalSecondaryIndexes() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_table_with_index");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("city", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "name_idx",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "age_city_idx",
                                IndexType.SECONDARY,
                                Arrays.asList("age", "city"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .comment("test table with global secondary index")
                        .property(ConfigOptions.TABLE_MIN_IN_SYNC_REPLICAS, 1)
                        .distributedBy(3, "id")
                        .build();

        createTable(tablePath, descriptor, true);

        // Verify main table
        TableInfo mainTableInfo = admin.getTableInfo(tablePath).get();
        assertThat(mainTableInfo.getSchemaId()).isEqualTo(1);
        assertThat(mainTableInfo.getTableConfig().getKvFormatVersion())
                .hasValue(ConfigOptions.KV_FORMAT_VERSION_2);
        assertThat(mainTableInfo.toTableDescriptor().getSchema().getIndexes()).hasSize(2);

        TablePath nameIndexTablePath = IndexTableUtils.indexTablePath(tablePath, "name_idx");
        TablePath ageCityIndexTablePath = IndexTableUtils.indexTablePath(tablePath, "age_city_idx");

        assertThat(admin.tableExists(nameIndexTablePath).get()).isTrue();
        TableInfo nameIndexInfo = admin.getTableInfo(nameIndexTablePath).get();
        assertThat(nameIndexInfo.getTableConfig().getMinInSyncReplicas()).hasValue(1);
        assertThat(nameIndexInfo.getTableConfig().getKvFormatVersion())
                .hasValue(ConfigOptions.KV_FORMAT_VERSION_2);
        assertThat(nameIndexInfo.toTableDescriptor().getSchema().getPrimaryKeyColumnNames())
                .containsExactly(
                        IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN,
                        IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN,
                        IndexTableUtils.ROW_KEY_SYSTEM_COLUMN);
        assertThat(
                        nameIndexInfo
                                .toTableDescriptor()
                                .getTableDistribution()
                                .get()
                                .getBucketCount()
                                .get())
                .isEqualTo(3);

        try (Table indexTable = conn.getTable(nameIndexTablePath)) {
            assertThatThrownBy(() -> indexTable.newScan().createLogScanner())
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Changelog subscription is not supported");
        }
        try (MultiTableLogScanner scanner =
                conn.getMultiTable().newMultiTableScan().createLogScanner()) {
            assertThatThrownBy(() -> scanner.subscribe(nameIndexTablePath, 0, 0L))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Changelog subscription is not supported");
        }

        assertThat(admin.tableExists(ageCityIndexTablePath).get()).isTrue();
        TableInfo ageCityIndexInfo = admin.getTableInfo(ageCityIndexTablePath).get();
        assertThat(ageCityIndexInfo.toTableDescriptor().getSchema().getPrimaryKeyColumnNames())
                .containsExactly(
                        IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN,
                        IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN,
                        IndexTableUtils.ROW_KEY_SYSTEM_COLUMN);

        admin.dropTable(tablePath, false).get();
        assertThat(admin.tableExists(tablePath).get()).isFalse();
        assertThat(admin.tableExists(nameIndexTablePath).get()).isFalse();
        assertThat(admin.tableExists(ageCityIndexTablePath).get()).isFalse();
    }

    @Test
    void testInternalIndexColumnNamesOnlyConflictInTheDerivedSchema() throws Exception {
        String column = IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN;
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(column, DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TablePath ordinary = TablePath.of(DB, "ordinary_index_column_name");
        createTable(
                ordinary, TableDescriptor.builder().schema(schema).distributedBy(1).build(), true);

        String indexName = "idx";
        TablePath indexed = TablePath.of(DB, "conflicting_index_column_name");
        Schema indexedSchema =
                Schema.newBuilder()
                        .fromSchema(schema)
                        .index(indexName, IndexType.SECONDARY, Collections.singletonList(column))
                        .build();
        assertThatThrownBy(
                        () ->
                                admin.createTable(
                                                indexed,
                                                TableDescriptor.builder()
                                                        .schema(indexedSchema)
                                                        .distributedBy(1)
                                                        .build(),
                                                false)
                                        .get())
                .hasRootCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining(column);
        assertThat(admin.tableExists(indexed).get()).isFalse();
        assertThat(admin.tableExists(IndexTableUtils.indexTablePath(indexed, indexName)).get())
                .isFalse();
    }

    @Test
    void testRejectsDerivedIndexBucketCountAboveClusterLimit() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_index_bucket_limit");
        admin.createDatabase(DB, DatabaseDescriptor.EMPTY, true).get();
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                31)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();

        try {
            assertThatThrownBy(() -> admin.createTable(tablePath, descriptor, false).get())
                    .cause()
                    .isInstanceOf(TooManyBucketsException.class)
                    .hasMessageContaining("Bucket count 31")
                    .hasMessageContaining("maximum limit 30");
            assertThat(admin.tableExists(tablePath).get()).isFalse();
        } finally {
            admin.dropTable(tablePath, true).get();
        }
    }

    @Test
    void testCreatePartitionedTableWithGlobalSecondaryIndex() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_partitioned_with_index");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .primaryKey("id", "region")
                        .index("name_idx", "name")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .comment("partitioned table with index")
                        .distributedBy(3, "id")
                        .partitionedBy("region")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        createTable(tablePath, descriptor, true);

        // Verify main table is partitioned
        TableInfo mainTableInfo = admin.getTableInfo(tablePath).get();
        assertThat(mainTableInfo.toTableDescriptor().isPartitioned()).isTrue();
        assertThat(mainTableInfo.toTableDescriptor().getPartitionKeys()).containsExactly("region");
        assertThat(mainTableInfo.toTableDescriptor().getSchema().getIndexes()).hasSize(1);

        // Verify index table exists and is NOT partitioned
        TablePath nameIndexTablePath = IndexTableUtils.indexTablePath(tablePath, "name_idx");
        assertThat(admin.tableExists(nameIndexTablePath).get()).isTrue();
        TableInfo indexInfo = admin.getTableInfo(nameIndexTablePath).get();
        assertThat(indexInfo.getTableConfig().getKvFormatVersion())
                .hasValue(ConfigOptions.KV_FORMAT_VERSION_2);
        assertThat(indexInfo.getTableConfig().getKvValueLayoutVersion())
                .hasValue(org.apache.fluss.row.encode.KvValueLayout.TAGGED.version());
        assertThat(indexInfo.toTableDescriptor().isPartitioned()).isFalse();

        // V2: partitioned index table has __partition_id system column
        assertThat(indexInfo.toTableDescriptor().getSchema().getColumnNames())
                .contains(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
    }

    @Test
    void testUserCannotCreateInternalSecondaryIndexTable() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_user_created_internal_index");
        admin.createDatabase(DB, org.apache.fluss.metadata.DatabaseDescriptor.EMPTY, true).get();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(1, "id")
                        .property(IndexTableUtils.TABLE_INDEX_META_MAIN_TABLE_ID, 1L)
                        .build();

        assertThatThrownBy(() -> admin.createTable(tablePath, descriptor, false).get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("internal secondary index table");

        assertThat(admin.tableExists(tablePath).get()).isFalse();
    }

    @Test
    void testCreateNonPkTableWithSecondaryIndexFailsBeforeMetadataSideEffects() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_non_pk_index_rejected");
        admin.createDatabase(DB, org.apache.fluss.metadata.DatabaseDescriptor.EMPTY, true).get();
        List<String> tablesBefore = admin.listTables(DB).get();

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .index("idx_name", "name")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();

        assertThatThrownBy(() -> admin.createTable(tablePath, descriptor, false).get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("secondary indexes")
                .hasMessageContaining("primary key");

        assertThat(admin.tableExists(tablePath).get()).isFalse();
        assertThat(admin.listTables(DB).get()).containsExactlyInAnyOrderElementsOf(tablesBefore);
    }

    @Test
    void directIndexDropRequiresItsOwnerToBeAbsent() throws Exception {
        TablePath mainPath = TablePath.of(DB, "tenant__orders");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .index("idx_name", "name")
                        .build();
        createTable(
                mainPath,
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build(),
                true);
        TablePath indexPath = IndexTableUtils.indexTablePath(mainPath, "idx_name");

        assertThatThrownBy(() -> admin.dropTable(indexPath, false).get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("internal secondary index table")
                .hasMessageContaining("owning main table");
        assertThat(admin.tableExists(mainPath).get()).isTrue();
        assertThat(admin.tableExists(indexPath).get()).isTrue();

        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().deleteTable(mainPath);
        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        waitUntil(
                () ->
                        !FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getCoordinatorLeaderAddress()
                                .isPresent(),
                Duration.ofMinutes(1),
                "Coordinator server node still exists in ZooKeeper");
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        admin.close();
        conn.close();
        setup();
        assertThat(admin.tableExists(mainPath).get()).isFalse();
        assertThat(admin.tableExists(indexPath).get()).isTrue();

        admin.dropTable(indexPath, false).get();
        assertThat(admin.tableExists(indexPath).get()).isFalse();
    }
}
