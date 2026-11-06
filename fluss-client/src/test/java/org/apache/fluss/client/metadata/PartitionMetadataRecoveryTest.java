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

import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Recovery of parent table mappings needed to decode partition metadata responses. */
class PartitionMetadataRecoveryTest {

    private static final TablePath TABLE = TablePath.of("db", "docs");
    private static final TablePath OTHER = TablePath.of("db", "other");
    private static final TablePath UNPARTITIONED = TablePath.of("db", "unpartitioned");
    private static final PhysicalTablePath FIRST = PhysicalTablePath.of(TABLE, "p1");
    private static final PhysicalTablePath SECOND = PhysicalTablePath.of(TABLE, "p2");
    private static final PhysicalTablePath THIRD = PhysicalTablePath.of(OTHER, "p3");
    private static final Map<TablePath, Long> TABLE_IDS = new HashMap<>();
    private static final Map<PhysicalTablePath, Long> PARTITION_IDS = new HashMap<>();
    private static final Map<Integer, ServerNode> SERVERS = new HashMap<>();

    static {
        TABLE_IDS.put(TABLE, 1622L);
        TABLE_IDS.put(OTHER, 1623L);
        TABLE_IDS.put(UNPARTITIONED, 1624L);
        PARTITION_IDS.put(FIRST, 10L);
        PARTITION_IDS.put(SECOND, 11L);
        PARTITION_IDS.put(THIRD, 12L);
        SERVERS.put(1, new ServerNode(1, "localhost", 9123, ServerType.TABLET_SERVER));
        SERVERS.put(2, new ServerNode(2, "localhost", 9124, ServerType.TABLET_SERVER));
    }

    @Test
    void testSinglePartitionLookupRepairsMissingTableMapping() {
        Scenario scenario = new Scenario(Collections.emptyMap());

        assertThat(scenario.updater.checkAndUpdatePartitionMetadata(FIRST)).isTrue();
        assertThat(scenario.requests).hasSize(1);
        assertThat(scenario.requests.get(0).getTablePathsList())
                .extracting(PbTablePath::getTableName)
                .containsExactly("docs");
        assertPartition(scenario.updater.getCluster(), FIRST);
        assertThat(scenario.updater.checkAndUpdatePartitionMetadata(FIRST)).isTrue();
        assertThat(scenario.requests).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBatchRefreshAddsOnlyMissingParentsAndKeepsExplicitTables(boolean otherTableCached) {
        Scenario scenario =
                new Scenario(
                        otherTableCached
                                ? Collections.singletonMap(OTHER, TABLE_IDS.get(OTHER))
                                : Collections.emptyMap());
        Set<PhysicalTablePath> paths =
                new HashSet<>(
                        Arrays.asList(FIRST, SECOND, THIRD, PhysicalTablePath.of(UNPARTITIONED)));

        scenario.updater.updatePhysicalTableMetadata(paths);

        assertThat(scenario.requests).hasSize(1);
        assertThat(scenario.requests.get(0).getTablePathsList())
                .extracting(PbTablePath::getTableName)
                .containsExactlyInAnyOrderElementsOf(
                        otherTableCached
                                ? Arrays.asList("docs", "unpartitioned")
                                : Arrays.asList("docs", "other", "unpartitioned"));
        for (PhysicalTablePath partition : Arrays.asList(FIRST, SECOND, THIRD)) {
            assertPartition(scenario.updater.getCluster(), partition);
        }
        assertThat(scenario.updater.getCluster().getTableId(UNPARTITIONED))
                .hasValue(TABLE_IDS.get(UNPARTITIONED));
    }

    @Test
    void testExistingMappingKeepsPartitionOnlyRequest() {
        Scenario scenario = new Scenario(Collections.singletonMap(TABLE, TABLE_IDS.get(TABLE)));

        assertThat(scenario.updater.checkAndUpdatePartitionMetadata(FIRST)).isTrue();

        assertThat(scenario.requests.get(0).getTablePathsList()).isEmpty();
        assertPartition(scenario.updater.getCluster(), FIRST);
    }

    @Test
    void testBackgroundTransientFailurePreservesCacheAndNextRefreshRecovers() {
        Scenario scenario = new Scenario(Collections.emptyMap());
        Cluster before = scenario.updater.getCluster();
        scenario.transientFailures = 1;

        scenario.updater.updatePhysicalTableMetadata(Collections.singleton(FIRST));
        assertThat(scenario.updater.getCluster()).isSameAs(before);
        assertThat(scenario.updater.getPartitionId(FIRST)).isEmpty();

        scenario.updater.updatePhysicalTableMetadata(Collections.singleton(FIRST));
        assertThat(scenario.requests).hasSize(2);
        assertPartition(scenario.updater.getCluster(), FIRST);
    }

    private static void assertPartition(Cluster cluster, PhysicalTablePath path) {
        long tableId = TABLE_IDS.get(path.getTablePath());
        long partitionId = PARTITION_IDS.get(path);
        assertThat(cluster.getTableId(path.getTablePath())).hasValue(tableId);
        assertThat(cluster.getTablePath(tableId)).hasValue(path.getTablePath());
        assertThat(cluster.getPartitionId(path)).hasValue(partitionId);
        assertThat(cluster.getBucketCount(TableOrPartition.ofPartition(partitionId))).hasValue(4);
        assertThat(cluster.getBucketLocation(new TableBucket(tableId, partitionId, 0)))
                .hasValueSatisfying(
                        location -> {
                            assertThat(location.getPhysicalTablePath()).isEqualTo(path);
                            assertThat(location.getLeader()).isEqualTo(1);
                        });
    }

    private static class Scenario {
        final List<MetadataRequest> requests;
        final MetadataUpdater updater;
        int transientFailures;

        Scenario(Map<TablePath, Long> cachedTables) {
            TestingMetadataRpcClient client =
                    new TestingMetadataRpcClient(
                            request -> {
                                if (transientFailures > 0) {
                                    transientFailures--;
                                    CompletableFuture<MetadataResponse> failure =
                                            new CompletableFuture<>();
                                    failure.completeExceptionally(
                                            new StaleMetadataException("refresh not ready"));
                                    return failure;
                                }
                                return CompletableFuture.completedFuture(response(request));
                            });
            requests = client.getRequests();
            Cluster cluster =
                    new Cluster(
                            SERVERS,
                            null,
                            Collections.emptyMap(),
                            new HashMap<>(cachedTables),
                            Collections.emptyMap(),
                            Collections.emptyMap());
            updater = new MetadataUpdater(client, new Configuration(), cluster);
        }

        private static MetadataResponse response(MetadataRequest request) {
            MetadataResponse response = new MetadataResponse();
            SERVERS.values()
                    .forEach(
                            server ->
                                    response.addTabletServer()
                                            .setNodeId(server.id())
                                            .setHost(server.host())
                                            .setPort(server.port()));
            request.getTablePathsList()
                    .forEach(
                            table ->
                                    response.addTableMetadata()
                                            .setTableId(
                                                    TABLE_IDS.get(
                                                            TablePath.of(
                                                                    table.getDatabaseName(),
                                                                    table.getTableName())))
                                            .setTablePath()
                                            .copyFrom(table));
            request.getPartitionsPathsList()
                    .forEach(
                            partition -> {
                                PhysicalTablePath path =
                                        PhysicalTablePath.of(
                                                TablePath.of(
                                                        partition.getDatabaseName(),
                                                        partition.getTableName()),
                                                partition.getPartitionName());
                                PbPartitionMetadata metadata =
                                        response.addPartitionMetadata()
                                                .setTableId(TABLE_IDS.get(path.getTablePath()))
                                                .setPartitionId(PARTITION_IDS.get(path))
                                                .setPartitionName(path.getPartitionName())
                                                .setBucketCount(4);
                                metadata.addBucketMetadata().setBucketId(0).setLeaderId(1);
                            });
            return response;
        }
    }
}
