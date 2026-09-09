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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlussClusterExtension}. */
class FlussClusterExtensionTest {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @Test
    void testWaitUntilAllReplicaReadyRetriesWhenIsrContainsRemovedTabletServer() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "stale_isr_test");
        TableDescriptor tableDescriptor = DATA1_TABLE_DESCRIPTOR.withReplicationFactor(3);
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tableBucket);

        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        LeaderAndIsr originalLeaderAndIsr = zkClient.getLeaderAndIsr(tableBucket).get();
        int stoppedServer =
                originalLeaderAndIsr.isr().stream()
                        .filter(serverId -> serverId != originalLeaderAndIsr.leader())
                        .findFirst()
                        .get();
        List<Integer> shrunkIsr = new ArrayList<>(originalLeaderAndIsr.isr());
        shrunkIsr.remove(Integer.valueOf(stoppedServer));
        LeaderAndIsr shrunkLeaderAndIsr = originalLeaderAndIsr.newLeaderAndIsr(shrunkIsr);

        AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
        Thread convergeIsrThread =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep(50);
                                zkClient.updateLeaderAndIsr(
                                        tableBucket,
                                        shrunkLeaderAndIsr,
                                        zkClient.getCurrentEpoch().getCoordinatorEpochZkVersion());
                            } catch (Throwable t) {
                                asyncFailure.set(t);
                            }
                        });

        try {
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(stoppedServer);
            FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);

            zkClient.updateLeaderAndIsr(
                    tableBucket,
                    originalLeaderAndIsr,
                    zkClient.getCurrentEpoch().getCoordinatorEpochZkVersion());

            convergeIsrThread.start();
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tableBucket);
            convergeIsrThread.join();

            assertThat(asyncFailure.get()).isNull();
            assertThat(zkClient.getLeaderAndIsr(tableBucket)).hasValue(shrunkLeaderAndIsr);
        } finally {
            if (convergeIsrThread.isAlive()) {
                convergeIsrThread.join();
            }
            if (FLUSS_CLUSTER_EXTENSION.getTabletServerById(stoppedServer) == null) {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(stoppedServer);
                FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
            }
        }
    }
}
