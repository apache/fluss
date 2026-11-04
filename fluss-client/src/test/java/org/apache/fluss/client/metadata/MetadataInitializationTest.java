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

package org.apache.fluss.client.metadata;

import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.testutils.common.CheckedThread;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests table mapping initialization from an existing Admin response. */
class MetadataInitializationTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "docs");
    private static final long TABLE_ID = 1622L;
    private static final TablePath OTHER_TABLE_PATH = TablePath.of("db", "other");
    private static final long OTHER_TABLE_ID = 42L;
    private static final long PARTITION_ID = 51L;
    private static final ServerNode TABLET_SERVER =
            new ServerNode(1, "localhost", 9080, ServerType.TABLET_SERVER);
    private static final ServerNode COORDINATOR =
            new ServerNode(0, "localhost", 9081, ServerType.COORDINATOR);

    @Test
    void testAddsBidirectionalMappingWithoutRpc() {
        TestingMetadataRpcClient rpcClient = noRpcClient();
        Cluster original = Cluster.empty();
        MetadataUpdater updater = new MetadataUpdater(rpcClient, new Configuration(), original);

        updater.ensureTableMapping(tableInfo());

        assertThat(updater.getCluster().getTableId(TABLE_PATH)).hasValue(TABLE_ID);
        assertThat(updater.getCluster().getTablePath(TABLE_ID)).hasValue(TABLE_PATH);
        assertThat(original.getTableId(TABLE_PATH)).isEmpty();
        assertThat(original.getTablePath(TABLE_ID)).isEmpty();
        assertThat(rpcClient.getRequests()).isEmpty();
    }

    @Test
    void testPreservesUnrelatedMetadataAndOriginalSnapshot() {
        TestingMetadataRpcClient rpcClient = noRpcClient();
        Cluster original = cluster(Collections.singletonMap(OTHER_TABLE_PATH, OTHER_TABLE_ID));
        MetadataUpdater updater = new MetadataUpdater(rpcClient, new Configuration(), original);

        updater.ensureTableMapping(tableInfo());

        Cluster updated = updater.getCluster();
        assertThat(updated.getTableId(TABLE_PATH)).hasValue(TABLE_ID);
        assertThat(updated.getTablePath(TABLE_ID)).hasValue(TABLE_PATH);
        assertThat(updated.getTableId(OTHER_TABLE_PATH)).hasValue(OTHER_TABLE_ID);
        assertThat(updated.getTablePath(OTHER_TABLE_ID)).hasValue(OTHER_TABLE_PATH);
        assertThat(updated.getAliveTabletServers()).isEqualTo(original.getAliveTabletServers());
        assertThat(updated.getCoordinatorServer()).isEqualTo(COORDINATOR);
        assertThat(updated.getBucketLocationsByPath())
                .isEqualTo(original.getBucketLocationsByPath());
        assertThat(updated.getPartitionIdByPath()).isEqualTo(original.getPartitionIdByPath());
        assertThat(updated.getBucketCountByTableOrPartition())
                .isEqualTo(original.getBucketCountByTableOrPartition());
        assertThat(original.getTableIdByPath())
                .containsExactlyEntriesOf(
                        Collections.singletonMap(OTHER_TABLE_PATH, OTHER_TABLE_ID));
        assertThat(original.getTablePath(TABLE_ID)).isEmpty();
        assertThat(rpcClient.getRequests()).isEmpty();
    }

    @Test
    void testExistingMappingDoesNotRebuildCluster() {
        Cluster original = cluster(Collections.singletonMap(TABLE_PATH, TABLE_ID));
        MetadataUpdater updater = new MetadataUpdater(noRpcClient(), new Configuration(), original);

        updater.ensureTableMapping(tableInfo());
        updater.ensureTableMapping(tableInfo());

        assertThat(updater.getCluster()).isSameAs(original);
    }

    @Test
    void testReplacesOldTableIdAndReverseMapping() {
        long oldTableId = TABLE_ID - 1;
        Cluster original = cluster(Collections.singletonMap(TABLE_PATH, oldTableId));
        MetadataUpdater updater = new MetadataUpdater(noRpcClient(), new Configuration(), original);

        updater.ensureTableMapping(tableInfo());

        assertThat(updater.getCluster().getTableId(TABLE_PATH)).hasValue(TABLE_ID);
        assertThat(updater.getCluster().getTablePath(TABLE_ID)).hasValue(TABLE_PATH);
        assertThat(updater.getCluster().getTablePath(oldTableId)).isEmpty();
        assertThat(original.getTablePath(oldTableId)).hasValue(TABLE_PATH);
    }

    @Test
    void testMappingMergeUsesSnapshotAfterConcurrentRefresh() throws Exception {
        TablePath refreshedTable = TablePath.of("db", "refreshed");
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer().setNodeId(1).setHost("localhost").setPort(9080);
        response.addTableMetadata()
                .setTableId(100L)
                .setTablePath()
                .setDatabaseName(refreshedTable.getDatabaseName())
                .setTableName(refreshedTable.getTableName());
        TestingMetadataRpcClient rpcClient =
                new TestingMetadataRpcClient(
                        request -> CompletableFuture.completedFuture(response));
        MetadataUpdater updater =
                new MetadataUpdater(
                        rpcClient,
                        new Configuration(),
                        cluster(Collections.singletonMap(OTHER_TABLE_PATH, OTHER_TABLE_ID)));
        CountDownLatch started = new CountDownLatch(1);
        CheckedThread mappingThread =
                new CheckedThread("ensure-table-mapping") {
                    @Override
                    public void go() {
                        started.countDown();
                        updater.ensureTableMapping(tableInfo());
                    }
                };

        try {
            synchronized (updater) {
                mappingThread.start();
                assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
                waitUntil(
                        () -> mappingThread.getState() == Thread.State.BLOCKED,
                        Duration.ofSeconds(10),
                        "Mapping merge did not wait for the metadata updater lock.");
                updater.updateMetadata(Collections.singleton(refreshedTable), null, null);
            }
        } finally {
            mappingThread.sync(10000);
        }

        assertThat(updater.getCluster().getTableId(TABLE_PATH)).hasValue(TABLE_ID);
        assertThat(updater.getCluster().getTableId(refreshedTable)).hasValue(100L);
        assertThat(updater.getCluster().getTableId(OTHER_TABLE_PATH)).hasValue(OTHER_TABLE_ID);
        assertThat(rpcClient.getRequests()).hasSize(1);
    }

    private static TestingMetadataRpcClient noRpcClient() {
        return new TestingMetadataRpcClient(
                request -> {
                    throw new AssertionError("Merging an existing TableInfo must not send an RPC.");
                });
    }

    private static TableInfo tableInfo() {
        return TableInfo.of(
                TABLE_PATH,
                TABLE_ID,
                1,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build(),
                null,
                0,
                0);
    }

    private static Cluster cluster(Map<TablePath, Long> tableMappings) {
        PhysicalTablePath partitionPath = PhysicalTablePath.of(OTHER_TABLE_PATH, "p1");
        BucketLocation bucket =
                new BucketLocation(
                        partitionPath,
                        new TableBucket(OTHER_TABLE_ID, PARTITION_ID, 0),
                        1,
                        new int[] {1});
        Map<TableOrPartition, Integer> bucketCounts = new HashMap<>();
        bucketCounts.put(TableOrPartition.ofTable(OTHER_TABLE_ID), 1);
        bucketCounts.put(TableOrPartition.ofPartition(PARTITION_ID), 1);
        return new Cluster(
                Collections.singletonMap(1, TABLET_SERVER),
                COORDINATOR,
                Collections.singletonMap(partitionPath, Collections.singletonList(bucket)),
                tableMappings,
                Collections.singletonMap(partitionPath, PARTITION_ID),
                bucketCounts);
    }
}
