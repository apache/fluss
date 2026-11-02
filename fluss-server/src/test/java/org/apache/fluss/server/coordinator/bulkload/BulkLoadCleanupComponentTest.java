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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/** Retention and coordinator fencing for BulkLoad terminal results. */
class BulkLoadCleanupComponentTest {

    private static final String SHA256 =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @Test
    void testResultGcRetainsUnexpiredResultsAndFencesStaleCoordinator() throws Exception {
        long tableId = 9405L;
        BulkLoadTransaction transaction = abortedTransaction(tableId);

        try (ZooKeeperClient zkClient = client()) {
            persistTransaction(zkClient, transaction);
            BulkLoadResultGc resultGc = resultGc(zkClient);

            runMaintenance(resultGc, zkClient, 0L, 20);
            assertThat(zkClient.pathExists(transactionPath(transaction))).isTrue();

            int staleEpochVersion = coordinatorEpochVersion(zkClient);
            zkClient.getCuratorClient()
                    .setData()
                    .forPath(ZkData.CoordinatorEpochZNode.path(), new byte[] {1});
            BulkLoadResultGc staleEpochGc = resultGc(zkClient);
            staleEpochGc.runMaintenance(100L, staleEpochVersion);
            staleEpochGc.runMaintenance(100L, staleEpochVersion);
            staleEpochGc.runMaintenance(100L, staleEpochVersion);
            assertThat(zkClient.pathExists(transactionPath(transaction))).isTrue();

            resultGc = resultGc(zkClient);
            for (int pass = 0;
                    pass < 50 && zkClient.pathExists(transactionPath(transaction));
                    pass++) {
                resultGc.runMaintenance(100L, coordinatorEpochVersion(zkClient));
            }

            assertThat(zkClient.pathExists(transactionPath(transaction))).isFalse();
            runMaintenance(resultGc, zkClient, 100L, 50);
            assertThat(zkClient.pathExists(ZkData.BulkLoadTableTransactionsZNode.path(tableId)))
                    .isFalse();
        }
    }

    private static ZooKeeperClient client() {
        return ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    private static BulkLoadResultGc resultGc(ZooKeeperClient zkClient) {
        return new BulkLoadResultGc(zkClient, Runnable::run, 0L);
    }

    private static void persistTransaction(
            ZooKeeperClient zkClient, BulkLoadTransaction transaction) throws Exception {
        if (!zkClient.pathExists(ZkData.CoordinatorEpochZNode.path())) {
            create(zkClient, ZkData.CoordinatorEpochZNode.path(), new byte[] {0});
        }
        create(
                zkClient,
                transactionPath(transaction),
                ZkData.BulkLoadTransactionsZNode.encode(transaction));
    }

    private static void runMaintenance(
            BulkLoadResultGc resultGc, ZooKeeperClient zkClient, long nowMs, int passes)
            throws Exception {
        for (int pass = 0; pass < passes; pass++) {
            resultGc.runMaintenance(nowMs, coordinatorEpochVersion(zkClient));
        }
    }

    private static int coordinatorEpochVersion(ZooKeeperClient zkClient) throws Exception {
        return zkClient.getDataWithStat(ZkData.CoordinatorEpochZNode.path()).getStat().getVersion();
    }

    private static void create(ZooKeeperClient zkClient, String path, byte[] data)
            throws Exception {
        zkClient.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }

    private static BulkLoadTransaction abortedTransaction(long tableId) {
        return new BulkLoadTransaction(
                handle(tableId, "550e8400-e29b-41d4-a716-446655440042"),
                BulkLoadState.ABORTED,
                "alice",
                "USER",
                "file:///warehouse",
                3,
                0,
                new long[] {7L},
                100L,
                SHA256,
                BulkLoadAbortReason.ABORTED_BY_CALLER,
                null);
    }

    private static BulkLoadHandle handle(long tableId, String bulkLoadId) {
        return new BulkLoadHandle(
                PhysicalTablePath.of(TablePath.of("db", "table_" + tableId)),
                tableId,
                null,
                bulkLoadId);
    }

    private static String transactionPath(BulkLoadTransaction transaction) {
        return ZkData.BulkLoadTableTransactionZNode.path(
                transaction.getTableId(), transaction.getBulkLoadId());
    }
}
