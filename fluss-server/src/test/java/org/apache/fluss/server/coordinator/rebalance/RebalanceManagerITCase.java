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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.AddServerTagRequest;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.assertj.core.api.Assertions.assertThat;

/** IT test for {@link RebalanceManager}. */
public class RebalanceManagerITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;
    private RebalanceManager rebalanceManager;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        rebalanceManager = FLUSS_CLUSTER_EXTENSION.getRebalanceManager();
    }

    @Test
    void testBuildClusterModel() throws Exception {
        // one none-partitioned table.
        long tableId1 =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        // one partitioned table.
        TablePath partitionTablePath = TablePath.of("test_db_1", "test_partition_table_1");
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();
        long tableId2 =
                createTable(FLUSS_CLUSTER_EXTENSION, partitionTablePath, partitionTableDescriptor);
        String partitionName1 = "b1";
        createPartition(
                FLUSS_CLUSTER_EXTENSION,
                partitionTablePath,
                new PartitionSpec(Collections.singletonMap("b", partitionName1)),
                false);

        ClusterModel clusterModel = rebalanceManager.buildClusterModel();
        assertThat(clusterModel.servers().size()).isEqualTo(3);
        assertThat(clusterModel.aliveServers().size()).isEqualTo(3);
        assertThat(clusterModel.offlineServers().size()).isEqualTo(0);
        assertThat(clusterModel.tables().size()).isEqualTo(2);
        assertThat(clusterModel.tables()).contains(tableId1, tableId2);

        // offline one table.
        AddServerTagRequest request =
                new AddServerTagRequest().setServerTag(ServerTag.PERMANENT_OFFLINE.value);
        request.addServerId(0);
        FLUSS_CLUSTER_EXTENSION.newCoordinatorClient().addServerTag(request).get();

        clusterModel = rebalanceManager.buildClusterModel();
        assertThat(clusterModel.servers().size()).isEqualTo(3);
        assertThat(clusterModel.aliveServers().size()).isEqualTo(2);
        assertThat(clusterModel.offlineServers().size()).isEqualTo(1);
        assertThat(clusterModel.tables().size()).isEqualTo(2);
        assertThat(clusterModel.tables()).contains(tableId1, tableId2);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }
}
