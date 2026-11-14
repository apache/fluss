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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** IT test for update metadata of {@link MetadataUpdater}. */
class MetadataUpdaterITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(2).build();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPartialCacheThroughRpcAndClientRouting(boolean explicitCount) throws Exception {
        TablePath path = TablePath.of("fluss", "partial_routing_" + explicitCount);
        int recordCount = 30;
        long tableId;
        try (Connection connection =
                ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            connection
                    .getAdmin()
                    .createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA)
                                    .distributedBy(3, "a")
                                    .build(),
                            false)
                    .get();
            tableId = connection.getAdmin().getTableInfo(path).get().getTableId();
            try (Table table = connection.getTable(path)) {
                AppendWriter writer = table.newAppend().createWriter();
                for (int id = 0; id < recordCount; id++) {
                    writer.append(row(id, "row-" + id)).get(30, TimeUnit.SECONDS);
                }
            }
        }
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServers().iterator().next();
        TableMetadata full = server.getMetadataCache().getTableMetadata(path).get();
        server.getMetadataCache()
                .updateTableMetadata(
                        new TableMetadata(
                                full.getTableInfo(),
                                Collections.singletonList(full.getBucketMetadataList().get(0))));
        Cluster cluster =
                sendMetadataRequestAndRebuildCluster(
                        new TestCoordinatorGateway() {
                            @Override
                            public CompletableFuture<MetadataResponse> metadata(
                                    MetadataRequest request) {
                                return FLUSS_CLUSTER_EXTENSION
                                        .newTabletServerClientForNode(server.getServerId())
                                        .metadata(request)
                                        .thenApply(
                                                response -> {
                                                    assertThat(
                                                                    response.getTableMetadatasList()
                                                                            .get(0)
                                                                            .getBucketCount())
                                                            .isEqualTo(3);
                                                    if (!explicitCount) {
                                                        // Preserve the pre-extension response shape
                                                        // after the real RPC.
                                                        response.getTableMetadatasList()
                                                                .get(0)
                                                                .clearBucketCount();
                                                    }
                                                    return response;
                                                });
                            }
                        },
                        Collections.singleton(path));
        assertThat(cluster.getBucketCount(TableOrPartition.ofTable(tableId))).hasValue(3);
        assertThat(cluster.getBucketCountEpoch(tableId)).hasValue(0);
        assertThat(server.getMetadataCache().getTableMetadata(path).get().getBucketMetadataList())
                .hasSize(3);
        assertRecordsFromAllBuckets(full.getTableInfo(), cluster, recordCount);
    }

    private static void assertRecordsFromAllBuckets(
            TableInfo tableInfo, Cluster cluster, int expectedRecords) throws Exception {
        Configuration conf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        MetadataUpdater updater =
                new MetadataUpdater(FLUSS_CLUSTER_EXTENSION.getRpcClient(), conf, cluster);
        List<Integer> actualIds = new ArrayList<>();
        Map<Integer, Integer> recordsPerBucket = new HashMap<>();
        // Keep the RPC-rebuilt snapshot instead of refreshing it through Connection.getTable.
        try (FlussConnection connection =
                        (FlussConnection) ConnectionFactory.createConnection(conf);
                LogScanner scanner =
                        new LogScannerImpl(
                                conf,
                                tableInfo,
                                updater,
                                connection.getClientMetricGroup(),
                                connection.getOrCreateRemoteFileDownloader(),
                                null,
                                new ClientSchemaGetter(
                                        tableInfo.getTablePath(),
                                        tableInfo.getSchemaInfo(),
                                        connection.getAdmin()),
                                null)) {
            int bucketCount =
                    cluster.getBucketCountOrElseThrow(
                            TableOrPartition.ofTable(tableInfo.getTableId()));
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                scanner.subscribe(bucket, 0L);
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (actualIds.size() < expectedRecords && System.nanoTime() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofMillis(100));
                for (TableBucket bucket : records.buckets()) {
                    for (ScanRecord record : records.records(bucket)) {
                        int id = record.getRow().getInt(0);
                        actualIds.add(id);
                        assertThat(record.getRow().getString(1).toString()).isEqualTo("row-" + id);
                        int bucketRecords = recordsPerBucket.getOrDefault(bucket.getBucket(), 0);
                        assertThat(record.logOffset()).isEqualTo((long) bucketRecords);
                        recordsPerBucket.put(bucket.getBucket(), bucketRecords + 1);
                    }
                }
            }
        }
        assertThat(actualIds)
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, expectedRecords).boxed().collect(Collectors.toList()));
        assertThat(recordsPerBucket).containsOnlyKeys(0, 1, 2);
    }

    @Test
    void testRebuildClusterNTimes() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        Connection conn = ConnectionFactory.createConnection(clientConf);
        Admin admin = conn.getAdmin();
        TablePath tablePath = TablePath.of("fluss", "test");
        admin.createTable(tablePath, DATA1_TABLE_DESCRIPTOR, true).get();
        admin.close();
        conn.close();

        RpcClient rpcClient = FLUSS_CLUSTER_EXTENSION.getRpcClient();
        MetadataUpdater metadataUpdater = new MetadataUpdater(clientConf, rpcClient);
        // update metadata
        metadataUpdater.updateMetadata(Collections.singleton(tablePath), null, null);
        Cluster cluster = metadataUpdater.getCluster();

        // repeat 20K times to reproduce StackOverflowError if there is
        // any N levels UnmodifiableCollection
        for (int i = 0; i < 20000; i++) {
            cluster =
                    sendMetadataRequestAndRebuildCluster(
                            FLUSS_CLUSTER_EXTENSION.newCoordinatorClient(),
                            true,
                            cluster,
                            null,
                            null,
                            null);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGetTableLoadsTableMetadata(boolean partitioned) throws Exception {
        TablePath tablePath = TablePath.of("fluss", "mapping_only_" + partitioned);
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("region", DataTypes.STRING())
                                        .build())
                        .distributedBy(1);
        if (partitioned) {
            builder.partitionedBy("region");
        }
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createTable(tablePath, builder.build().withReplicationFactor(1), false).get();
            if (partitioned) {
                admin.createPartition(
                                tablePath,
                                new PartitionSpec(Collections.singletonMap("region", "p1")),
                                false)
                        .get();
            }
            try (Table table = connection.getTable(tablePath)) {
                MetadataUpdater updater = ((FlussConnection) connection).getMetadataUpdater();
                long tableId = table.getTableInfo().getTableId();
                assertThat(updater.getCluster().getTableId(tablePath)).hasValue(tableId);
                assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofTable(tableId)))
                        .isPresent();
                table.newAppend().createWriter().append(row(1, "p1")).get();
                try (Connection readerConnection =
                                ConnectionFactory.createConnection(
                                        FLUSS_CLUSTER_EXTENSION.getClientConfig());
                        Table readerTable = readerConnection.getTable(tablePath);
                        LogScanner scanner = readerTable.newScan().createLogScanner()) {
                    assertThat(
                                    ((FlussConnection) readerConnection)
                                            .getMetadataUpdater()
                                            .getCluster()
                                            .getBucketCount(TableOrPartition.ofTable(tableId)))
                            .isPresent();
                    if (partitioned) {
                        scanner.subscribeFromBeginning(
                                updater.getPartitionIdOrElseThrow(
                                        PhysicalTablePath.of(tablePath, "p1")),
                                0);
                    } else {
                        scanner.subscribeFromBeginning(0);
                    }
                    waitUntil(
                            () -> {
                                ScanRecords records = scanner.poll(Duration.ofMillis(100));
                                if (records.isEmpty()) {
                                    return false;
                                }
                                assertThat(records.count()).isEqualTo(1);
                                assertThat(records.iterator().next().getRow().getInt(0))
                                        .isEqualTo(1);
                                return true;
                            },
                            Duration.ofSeconds(10),
                            "Record was not read after subscribing the table");
                }
            }
        }
    }

    @Test
    void testUpdateWithEmptyMetadataResponse() throws Exception {
        RpcClient rpcClient = FLUSS_CLUSTER_EXTENSION.getRpcClient();
        MetadataUpdater metadataUpdater =
                new MetadataUpdater(FLUSS_CLUSTER_EXTENSION.getClientConfig(), rpcClient);

        // update metadata
        metadataUpdater.updateMetadata(null, null, null);
        Cluster cluster = metadataUpdater.getCluster();

        List<ServerNode> expectedServerNodes = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes();
        assertThat(expectedServerNodes).hasSize(2);
        assertThat(cluster.getAliveTabletServerList()).isEqualTo(expectedServerNodes);

        // then, stop coordinator server, can still update metadata
        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        metadataUpdater.updateMetadata(null, null, null);
        assertThat(cluster.getAliveTabletServerList()).isEqualTo(expectedServerNodes);

        // start a new tablet server, the tablet server will return empty metadata
        // response since no coordinator server to send newest metadata to the tablet server
        int newServerId = 2;
        FLUSS_CLUSTER_EXTENSION.startTabletServer(newServerId);

        // we mock a new cluster with only server 1 so that it'll only send request
        // to server 1, which will return empty resonate
        Cluster newCluster =
                new Cluster(
                        Collections.singletonMap(
                                newServerId,
                                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(newServerId)),
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        metadataUpdater = new MetadataUpdater(rpcClient, new Configuration(), newCluster);
        // shouldn't update metadata to empty since the empty metadata will be ignored
        metadataUpdater.updateMetadata(null, null, null);
        assertThat(metadataUpdater.getCluster().getAliveTabletServers())
                .isEqualTo(newCluster.getAliveTabletServers())
                .hasSize(1);

        // recover the coordinator
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
    }
}
