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

package org.apache.fluss.server.testutils;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the shared table readiness wait for automatic and manual partitions. */
class FlussClusterExtensionTest {
    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWaitUntilTableReadyUsesActualPartitions(boolean automatic) throws Exception {
        TablePath path = TablePath.of("ready", automatic ? "automatic" : "manual");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("pt", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .partitionedBy("pt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, automatic)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 1)
                        .build();
        long tableId = createTable(CLUSTER, path, descriptor);
        if (!automatic) {
            createPartition(
                    CLUSTER, path, new PartitionSpec(Collections.singletonMap("pt", "p1")), false);
        }
        CLUSTER.waitUntilTableReady(path);
        Map<String, PartitionRegistration> partitions =
                CLUSTER.getZooKeeperClient().getPartitionRegistrations(path);
        assertThat(partitions).hasSize(1);
        TableBucket bucket =
                new TableBucket(tableId, partitions.values().iterator().next().getPartitionId(), 0);
        assertThat(
                        CLUSTER.getTabletServers()
                                .iterator()
                                .next()
                                .getReplicaManager()
                                .getReplica(bucket))
                .isInstanceOf(ReplicaManager.OnlineReplica.class);
    }
}
