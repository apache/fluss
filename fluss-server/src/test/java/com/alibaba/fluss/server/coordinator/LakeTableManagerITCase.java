/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.assertj.core.api.Assertions.assertThat;

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
        datalakeFormat.put("datalake.paimon.metastore", "filesystem");
        datalakeFormat.put("datalake.paimon.warehouse", "file:/tmp/paimon");
        return datalakeFormat;
    }

    @Test
    void testCreateAndGetTable() throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property("table.datalake.enabled", "true")
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
    }
}
