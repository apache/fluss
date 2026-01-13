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
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbAlterConfig;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for creating/dropping table for Fluss with lake storage configured . */
class LakeTableManagerITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(Configuration.fromMap(getDataLakeFormat()))
                    .build();

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

        TablePath tablePath = TablePath.of("fluss", "test_lake_table");

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
        TablePath lakeTablePath = TablePath.of("fluss", "test_lake_enabled_table");
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
    void testAlterTableDatalakeFreshness() throws Exception {
        AdminReadOnlyGateway gateway = getAdminOnlyGateway(true);
        AdminGateway adminGateway = getAdminGateway();

        String db1 = "test_alter_freshness_db";
        String tb1 = "tb1";
        TablePath tablePath = TablePath.of(db1, tb1);
        // first create a database
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        // create a table with datalake enabled and initial freshness
        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "5min");
        TableDescriptor tableDescriptor = newPkTable().withProperties(initialProperties);
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        // get the table and check initial freshness
        GetTableInfoResponse response =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTable = TableDescriptor.fromJsonBytes(response.getTableJson());
        assertThat(gottenTable.getProperties().get(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()))
                .isEqualTo("5min");

        // alter table to change datalake freshness
        Map<String, String> setProperties = new HashMap<>();
        setProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "3min");

        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                alterTableProperties(setProperties, new ArrayList<>()),
                                Collections.emptyList(),
                                false))
                .get();

        // get the table and check updated freshness
        GetTableInfoResponse responseAfterAlter =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTableAfterAlter =
                TableDescriptor.fromJsonBytes(responseAfterAlter.getTableJson());

        String freshnessAfterAlter =
                gottenTableAfterAlter
                        .getProperties()
                        .get(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key());
        assertThat(freshnessAfterAlter).isEqualTo("3min");

        // cleanup
        adminGateway.dropTable(newDropTableRequest(db1, tb1, false)).get();
        adminGateway.dropDatabase(newDropDatabaseRequest(db1, false, true)).get();
    }

    @Test
    void testResetTableDatalakeProperties() throws Exception {
        AdminReadOnlyGateway gateway = getAdminOnlyGateway(true);
        AdminGateway adminGateway = getAdminGateway();

        String db1 = "test_reset_datalake_db";
        String tb1 = "tb1";
        TablePath tablePath = TablePath.of(db1, tb1);
        // first create a database
        adminGateway.createDatabase(newCreateDatabaseRequest(db1, false)).get();

        // create a table with datalake enabled and custom freshness
        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        initialProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "3min");
        TableDescriptor tableDescriptor = newPkTable().withProperties(initialProperties);
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        // get the table and check initial state
        GetTableInfoResponse response =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTable = TableDescriptor.fromJsonBytes(response.getTableJson());
        assertThat(gottenTable.getProperties().get(ConfigOptions.TABLE_DATALAKE_ENABLED.key()))
                .isEqualTo("true");
        assertThat(gottenTable.getProperties().get(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()))
                .isEqualTo("3min");

        // reset datalake freshness property
        List<String> resetProperties = new ArrayList<>();
        resetProperties.add(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key());

        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                alterTableProperties(new HashMap<>(), resetProperties),
                                Collections.emptyList(),
                                false))
                .get();

        // get the table and check freshness is removed
        GetTableInfoResponse responseAfterReset =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTableAfterReset =
                TableDescriptor.fromJsonBytes(responseAfterReset.getTableJson());

        // freshness should be removed from properties
        assertThat(
                        gottenTableAfterReset
                                .getProperties()
                                .containsKey(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()))
                .isFalse();
        // but datalake.enabled should still be there
        assertThat(
                        gottenTableAfterReset
                                .getProperties()
                                .get(ConfigOptions.TABLE_DATALAKE_ENABLED.key()))
                .isEqualTo("true");

        // reset datalake enabled property
        List<String> resetProperties2 = new ArrayList<>();
        resetProperties2.add(ConfigOptions.TABLE_DATALAKE_ENABLED.key());

        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                alterTableProperties(new HashMap<>(), resetProperties2),
                                Collections.emptyList(),
                                false))
                .get();

        // get the table and check datalake enabled is removed
        GetTableInfoResponse responseAfterReset2 =
                gateway.getTableInfo(newGetTableInfoRequest(tablePath)).get();
        TableDescriptor gottenTableAfterReset2 =
                TableDescriptor.fromJsonBytes(responseAfterReset2.getTableJson());

        // datalake.enabled should be removed from properties
        assertThat(
                        gottenTableAfterReset2
                                .getProperties()
                                .containsKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key()))
                .isFalse();

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
