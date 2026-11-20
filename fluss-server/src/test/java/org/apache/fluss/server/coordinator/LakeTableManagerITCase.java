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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbAlterConfig;
import org.apache.fluss.rpc.messages.PbPrepareLakeTableRespForTable;
import org.apache.fluss.rpc.messages.PbTableOffsets;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotRequest;
import org.apache.fluss.server.entity.LakeTieringTableInfo;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.TestTableNames;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateDatabaseRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropDatabaseRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** ITCase for creating/dropping table for Fluss with lake storage configured . */
class LakeTableManagerITCase {

    private static final String TABLE_REMOTE_DIR_NAME = "table-remote-data";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(getClusterConfig())
                    .setRemoteDirNames(Arrays.asList("default-remote-data", TABLE_REMOTE_DIR_NAME))
                    .build();

    private final TestTableNames tableNames = new TestTableNames();

    private static Configuration getClusterConfig() {
        Configuration conf = Configuration.fromMap(getDataLakeFormat());
        conf.set(
                ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN);
        // The first directory is the global default. Allocate new tables to the second one.
        conf.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(0, 1));
        return conf;
    }

    private static Map<String, String> getDataLakeFormat() {
        Map<String, String> datalakeFormat = new HashMap<>();
        datalakeFormat.put(ConfigOptions.DATALAKE_FORMAT.key(), DataLakeFormat.PAIMON.toString());
        return datalakeFormat;
    }

    @Test
    void testCreateAndGetTable() throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();

        TablePath tablePath = tableNames.tablePath("fluss", "test_lake_table");

        // create the table
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        Map<String, String> properties =
                TableDescriptor.fromJsonBytes(
                                adminGateway
                                        .getTableInfo(newGetTableInfoRequest(tablePath))
                                        .get()
                                        .getTableJson())
                        .getProperties();
        Map<String, String> expectedTableDataLakeProperties = new HashMap<>();
        for (Map.Entry<String, String> dataLakePropertyEntry : getDataLakeFormat().entrySet()) {
            expectedTableDataLakeProperties.put(
                    "table." + dataLakePropertyEntry.getKey(), dataLakePropertyEntry.getValue());
        }
        assertThat(properties).containsAllEntriesOf(expectedTableDataLakeProperties);

        // test create table with datalake enabled
        TableDescriptor lakeTableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();
        TablePath lakeTablePath = tableNames.tablePath("fluss", "test_lake_enabled_table");
        // create the table
        adminGateway
                .createTable(newCreateTableRequest(lakeTablePath, lakeTableDescriptor, false))
                .get();
        // create again, should throw TableAlreadyExistException thrown by Fluss
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createTable(
                                                newCreateTableRequest(
                                                        lakeTablePath, lakeTableDescriptor, false))
                                        .get())
                .cause()
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessage("Table %s already exists.", lakeTablePath);
    }

    @Test
    void testPrepareLakeTableSnapshotUsesTableRemoteDataDir() throws Exception {
        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TablePath tablePath = tableNames.tablePath("fluss", "test_prepare_lake_snapshot");
        TableDescriptor tableDescriptor =
                newPkTable()
                        .withProperties(
                                Collections.singletonMap(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true"));
        gateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
        long tableId = gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        String tableRemoteDataDir = FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(TABLE_REMOTE_DIR_NAME);
        assertThat(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getTable(tablePath))
                .hasValueSatisfying(
                        table -> assertThat(table.remoteDataDir).isEqualTo(tableRemoteDataDir));

        PrepareLakeTableSnapshotRequest request = new PrepareLakeTableSnapshotRequest();
        PbTableOffsets tableOffsets = request.addBucketOffset().setTableId(tableId);
        tableOffsets
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        tableOffsets.addBucketOffset().setBucketId(0).setLogEndOffset(10L);
        tableOffsets.addBucketOffset().setBucketId(1).setLogEndOffset(20L);

        List<PbPrepareLakeTableRespForTable> results =
                gateway.prepareLakeTableSnapshot(request).get().getPrepareLakeTableRespsList();
        assertThat(results).hasSize(1);
        PbPrepareLakeTableRespForTable result = results.get(0);
        assertThat(result.getTableId()).isEqualTo(tableId);
        assertThat(result.hasErrorCode()).isFalse();

        FsPath offsetsPath = new FsPath(result.getLakeTableOffsetsPath());
        FsPath expectedMetadataDir =
                new FsPath(
                        FlussPaths.remoteLakeTableSnapshotDir(
                                tableRemoteDataDir, tablePath, tableId),
                        "metadata");
        assertThat(offsetsPath.getParent()).isEqualTo(expectedMetadataDir);

        TableBucketOffsets storedOffsets =
                TableBucketOffsets.fromJsonBytes(
                        Files.readAllBytes(Paths.get(offsetsPath.toUri())));
        assertThat(storedOffsets.getTableId()).isEqualTo(tableId);
        assertThat(storedOffsets.getOffsets())
                .containsOnly(
                        entry(new TableBucket(tableId, 0), 10L),
                        entry(new TableBucket(tableId, 1), 20L));
    }

    @Test
    void testAlterAndResetTableDatalakeProperties() throws Exception {
        AdminReadOnlyGateway gateway = getAdminOnlyGateway(true);
        AdminGateway adminGateway = getAdminGateway();

        String db1 = "test_alter_reset_datalake_db";
        String tb1 = "tb1";
        TablePath tablePath = TablePath.of(db1, tb1);
        // first create a database
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        // Step 1: create a table with datalake enabled and initial freshness (5min)
        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "5min");
        TableDescriptor tableDescriptor = newPkTable().withProperties(initialProperties);
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        // Step 2: verify initial properties
        GetTableInfoResponse response =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTable = TableDescriptor.fromJsonBytes(response.getTableJson());
        assertThat(gottenTable.getProperties().get(ConfigOptions.TABLE_DATALAKE_ENABLED.key()))
                .isEqualTo("true");
        assertThat(gottenTable.getProperties().get(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()))
                .isEqualTo("5min");

        // Step 3: alter table to change datalake freshness (SET operation)
        Map<String, String> setProperties = new HashMap<>();
        setProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "3min");

        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                setProperties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();

        // Step 4: verify freshness was updated to 3min
        GetTableInfoResponse responseAfterSet =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTableAfterSet =
                TableDescriptor.fromJsonBytes(responseAfterSet.getTableJson());
        assertThat(
                        gottenTableAfterSet
                                .getProperties()
                                .get(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()))
                .isEqualTo("3min");
        assertThat(
                        gottenTableAfterSet
                                .getProperties()
                                .get(ConfigOptions.TABLE_DATALAKE_ENABLED.key()))
                .isEqualTo("true");

        // Step 5: reset datalake freshness property (RESET operation)
        List<String> resetProperties = new ArrayList<>();
        resetProperties.add(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key());

        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                Collections.emptyMap(),
                                resetProperties,
                                Collections.emptyList(),
                                false))
                .get();

        // Step 6: verify freshness was removed but datalake.enabled remains
        GetTableInfoResponse responseAfterReset =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTableAfterReset =
                TableDescriptor.fromJsonBytes(responseAfterReset.getTableJson());
        assertThat(
                        gottenTableAfterReset
                                .getProperties()
                                .containsKey(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()))
                .isFalse();
        assertThat(
                        gottenTableAfterReset
                                .getProperties()
                                .get(ConfigOptions.TABLE_DATALAKE_ENABLED.key()))
                .isEqualTo("true");

        // Step 7: reset datalake enabled property
        List<String> resetProperties2 = new ArrayList<>();
        resetProperties2.add(ConfigOptions.TABLE_DATALAKE_ENABLED.key());

        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                Collections.emptyMap(),
                                resetProperties2,
                                Collections.emptyList(),
                                false))
                .get();

        // Step 8: verify datalake.enabled was also removed
        GetTableInfoResponse responseAfterReset2 =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTableAfterReset2 =
                TableDescriptor.fromJsonBytes(responseAfterReset2.getTableJson());
        assertThat(
                        gottenTableAfterReset2
                                .getProperties()
                                .containsKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key()))
                .isFalse();

        // cleanup
        adminGateway.dropTable(newDropTableRequest(db1, tb1, false)).get();
        adminGateway.dropDatabase(newDropDatabaseRequest(db1, false, true)).get();
    }

    @Test
    void testAlterDatalakeAutoCompaction() throws Exception {
        AdminGateway adminGateway = getAdminGateway();

        String db1 = "test_alter_datalake_auto_compaction_db";
        String tb1 = "tb1";
        TablePath tablePath = TablePath.of(db1, tb1);
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        adminGateway
                .createTable(
                        newCreateTableRequest(
                                tablePath, newPkTable().withProperties(initialProperties), false))
                .get();

        Map<String, String> setProperties = new HashMap<>();
        setProperties.put(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true");
        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                setProperties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();

        TableDescriptor disabledLakeTableDescriptor =
                TableDescriptor.fromJsonBytes(
                        adminGateway
                                .getTableInfo(newGetTableInfoRequest(tablePath))
                                .get()
                                .getTableJson());
        assertThat(
                        disabledLakeTableDescriptor
                                .getProperties()
                                .get(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key()))
                .isEqualTo("true");

        Map<String, String> resetProperties = new HashMap<>();
        resetProperties.put(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "false");
        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                resetProperties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();

        TableDescriptor tableDescriptorAfterReset =
                TableDescriptor.fromJsonBytes(
                        adminGateway
                                .getTableInfo(newGetTableInfoRequest(tablePath))
                                .get()
                                .getTableJson());
        assertThat(
                        tableDescriptorAfterReset
                                .getProperties()
                                .get(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key()))
                .isEqualTo("false");

        adminGateway.dropTable(newDropTableRequest(db1, tb1, false)).get();
        adminGateway.dropDatabase(newDropDatabaseRequest(db1, false, true)).get();
    }

    @Test
    void testAlterTableDatalakeFreshnessAffectsTiering() throws Exception {
        AdminGateway adminGateway = getAdminGateway();

        String db1 = "test_tiering_freshness_db";
        String tb1 = "tb1";
        TablePath tablePath = TablePath.of(db1, tb1);
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        // Step 1: Create a table with a large datalake freshness (10 minutes)
        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "10min");
        TableDescriptor tableDescriptor = newPkTable().withProperties(initialProperties);
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        // Get the table id for later verification
        GetTableInfoResponse response =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        long tableId = response.getTableId();

        LakeTableTieringManager tieringManager =
                FLUSS_CLUSTER_EXTENSION
                        .getCoordinatorServer()
                        .getCoordinatorService()
                        .getLakeTableTieringManager();

        // Wait a bit for the table to be registered in tiering manager
        Thread.sleep(1000);

        // Step 2: Try to request the table for tiering within 3 seconds, should NOT get it
        retry(
                Duration.ofSeconds(3),
                () -> {
                    assertThat(tieringManager.requestTable()).isNull();
                });

        // Step 3: Change freshness to a very small value (100ms)
        Map<String, String> setProperties = new HashMap<>();
        setProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "100ms");

        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                setProperties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();

        // Step 4: Now retry requesting the table, should get it within 3 seconds
        retry(
                Duration.ofSeconds(3),
                () -> {
                    LakeTieringTableInfo table = tieringManager.requestTable();
                    assertThat(table).isNotNull();
                    assertThat(table.tableId()).isEqualTo(tableId);
                    assertThat(table.tablePath()).isEqualTo(tablePath);
                });

        // cleanup
        adminGateway.dropTable(newDropTableRequest(db1, tb1, false)).get();
        adminGateway.dropDatabase(newDropDatabaseRequest(db1, false, true)).get();
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

    private static TableDescriptor newPkTable() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .withComment("a comment")
                                .column("b", DataTypes.STRING())
                                .primaryKey("a")
                                .build())
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
}
