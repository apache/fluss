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
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
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
    void testCreateTableWithGlobalSecondaryIndex() throws Exception {
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
                        .distributedBy(3, "id")
                        .build();

        long mainTableId = createTable(tablePath, descriptor, true);

        // Verify main table
        TableInfo mainTableInfo = admin.getTableInfo(tablePath).get();
        assertThat(mainTableInfo.getSchemaId()).isEqualTo(1);
        assertThat(mainTableInfo.getTableConfig().getKvFormatVersion())
                .hasValue(ConfigOptions.KV_FORMAT_VERSION_2);
        assertThat(mainTableInfo.toTableDescriptor().getSchema().getIndexes()).hasSize(2);

        TablePath nameIndexTablePath = indexTablePath(tablePath, "name_idx");
        TablePath ageCityIndexTablePath = indexTablePath(tablePath, "age_city_idx");

        assertThat(admin.tableExists(nameIndexTablePath).get()).isTrue();
        TableInfo nameIndexInfo = admin.getTableInfo(nameIndexTablePath).get();
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
                        .index(
                                "name_idx",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
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

        long mainTableId = createTable(tablePath, descriptor, true);

        // Verify main table is partitioned
        TableInfo mainTableInfo = admin.getTableInfo(tablePath).get();
        assertThat(mainTableInfo.toTableDescriptor().isPartitioned()).isTrue();
        assertThat(mainTableInfo.toTableDescriptor().getPartitionKeys()).containsExactly("region");
        assertThat(mainTableInfo.toTableDescriptor().getSchema().getIndexes()).hasSize(1);

        // Verify index table exists and is NOT partitioned
        TablePath nameIndexTablePath = indexTablePath(tablePath, "name_idx");
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
                        .property(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID, 1L)
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
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                1)
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
    void testDropTableWithGlobalSecondaryIndexAutoDeletesIndexTables() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_table_to_drop_with_index");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .index(
                                "name_idx",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "age_idx",
                                IndexType.SECONDARY,
                                Arrays.asList("age"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        long mainTableId = createTable(tablePath, descriptor, true);

        assertThat(admin.tableExists(tablePath).get()).isTrue();

        TablePath nameIndexPath = indexTablePath(tablePath, "name_idx");
        TablePath ageIndexPath = indexTablePath(tablePath, "age_idx");

        assertThat(admin.tableExists(nameIndexPath).get()).isTrue();
        assertThat(admin.tableExists(ageIndexPath).get()).isTrue();

        // Drop main table
        admin.dropTable(tablePath, false).get();

        // Verify cascade delete
        assertThat(admin.tableExists(tablePath).get()).isFalse();
        assertThat(admin.tableExists(nameIndexPath).get()).isFalse();
        assertThat(admin.tableExists(ageIndexPath).get()).isFalse();
    }

    @Test
    void testUserCannotDropLiveInternalSecondaryIndexTableDirectly() throws Exception {
        for (String tableName :
                Arrays.asList("test_direct_drop_live_index_rejected", "tenant__orders_live")) {
            TablePath mainPath = TablePath.of(DB, tableName);
            createTable(mainPath, singleIndexTableDescriptor(), true);
            TablePath indexPath = indexTablePath(mainPath, "idx_name");

            assertThatThrownBy(() -> admin.dropTable(indexPath, false).get())
                    .cause()
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessageContaining("internal secondary index table")
                    .hasMessageContaining("owning main table");
            assertThat(admin.tableExists(mainPath).get()).isTrue();
            assertThat(admin.tableExists(indexPath).get()).isTrue();
        }
    }

    @Test
    void testUserCanDropOrphanInternalSecondaryIndexTable() throws Exception {
        for (String tableName :
                Arrays.asList("test_direct_drop_orphan_index_allowed", "tenant__orders_orphan")) {
            TablePath mainPath = TablePath.of(DB, tableName);
            createTable(mainPath, singleIndexTableDescriptor(), true);
            TablePath indexPath = indexTablePath(mainPath, "idx_name");

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

    @Test
    void testCascadeDropIgnoresNonOwnedTableAtIndexPath() throws Exception {
        TablePath mainPath = TablePath.of(DB, "test_drop_index_identity");
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        TableDescriptor mainDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(1, "id").build();
        TableDescriptor unrelatedDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(1, "id")
                        .build();

        long mainTableId = createTable(mainPath, mainDescriptor, true);
        TablePath indexPath = indexTablePath(mainPath, "idx_name");
        long unrelatedTableId =
                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getTableIdAndIncrement();
        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().deleteTable(indexPath);
        FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .registerFirstSchema(indexPath, unrelatedDescriptor.getSchema());
        FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .registerTable(
                        indexPath,
                        TableRegistration.newTable(
                                unrelatedTableId,
                                FLUSS_CLUSTER_EXTENSION
                                        .getZooKeeperClient()
                                        .getDefaultRemoteDataDir(),
                                unrelatedDescriptor),
                        false);
        assertThat(admin.getTableInfo(indexPath).get().isIndexTable()).isFalse();

        try {
            // Cascade drop discovers index tables by ownership (see
            // CoordinatorService.ownedIndexTableIds): only tables whose index metadata
            // points at the main table id are dropped together with the main table. The
            // unrelated table occupying the derived index path carries no index metadata,
            // so it is silently ignored instead of aborting the drop.
            admin.dropTable(mainPath, false).get();
            assertThat(admin.tableExists(mainPath).get()).isFalse();
            assertThat(admin.tableExists(indexPath).get()).isTrue();
        } finally {
            if (FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getTable(indexPath).isPresent()) {
                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().deleteTable(indexPath);
            }
            if (FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getTable(mainPath).isPresent()) {
                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().deleteTable(mainPath);
            }
        }
    }

    private static TableDescriptor singleIndexTableDescriptor() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        return TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
    }

    private static TablePath indexTablePath(TablePath mainTablePath, String indexName) {
        return TablePath.of(
                DB, IndexTableUtils.indexTableName(mainTablePath.getTableName(), indexName));
    }
}
