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
import org.apache.fluss.exception.LakeTableAlreadyExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
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
        // create again, should throw TableAlreadyExistException thrown by lake
        assertThatThrownBy(
                        () ->
                                adminGateway
                                        .createTable(
                                                newCreateTableRequest(
                                                        lakeTablePath, lakeTableDescriptor, false))
                                        .get())
                .cause()
                .isInstanceOf(LakeTableAlreadyExistException.class)
                .hasMessage(
                        "The table %s already exists in paimon catalog, please first drop the table in paimon catalog or use a new table name.",
                        lakeTablePath);
    }
}
