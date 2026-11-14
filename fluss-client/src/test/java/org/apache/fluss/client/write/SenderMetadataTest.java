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

package org.apache.fluss.client.write;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidBucketRoutingException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests isolation between write targets while routing metadata is unavailable. */
class SenderMetadataTest {
    private final TestingBucketAssigner assigner = new TestingBucketAssigner();
    private RecordAccumulator accumulator;
    private Sender sender;

    @Test
    void testCoordinatorRefreshFailureDoesNotBlockHealthyTarget() throws Exception {
        TableInfo unhealthy = DATA1_TABLE_INFO;
        TableInfo healthy =
                TableInfo.of(
                        TablePath.of("db", "healthy"), 101L, 0, DATA1_TABLE_DESCRIPTOR, "", 0, 0);
        Map<TablePath, TableInfo> tables = new HashMap<>();
        tables.put(unhealthy.getTablePath(), unhealthy);
        tables.put(healthy.getTablePath(), healthy);
        AtomicInteger refreshes = new AtomicInteger();
        TestingMetadataUpdater updater =
                new TestingMetadataUpdater(tables) {
                    @Override
                    public Cluster updatePhysicalTableMetadataFromCoordinator(
                            Set<PhysicalTablePath> paths) {
                        refreshes.incrementAndGet();
                        throw new NotCoordinatorLeaderException("Election in progress");
                    }
                };
        initializeSender(updater);
        CompletableFuture<Exception> queued = new CompletableFuture<>();
        assigner.setBucketId(2);
        accumulator.append(
                WriteRecord.forArrowAppend(
                        unhealthy,
                        PhysicalTablePath.of(unhealthy.getTablePath()),
                        row(1, "queued"),
                        null),
                (tb, leo, e) -> queued.complete(e),
                updater.getCluster());
        Cluster original = updater.getCluster();
        Map<TableOrPartition, Integer> counts = new HashMap<>();
        counts.put(TableOrPartition.ofTable(unhealthy.getTableId()), 1);
        counts.put(TableOrPartition.ofTable(healthy.getTableId()), 3);
        Map<PhysicalTablePath, List<BucketLocation>> locations =
                new HashMap<>(original.getBucketLocationsByPath());
        PhysicalTablePath unhealthyPath = PhysicalTablePath.of(unhealthy.getTablePath());
        locations.put(unhealthyPath, locations.get(unhealthyPath).subList(0, 1));
        updater.updateCluster(
                new Cluster(
                        original.getAliveTabletServers(),
                        original.getCoordinatorServer(),
                        locations,
                        original.getTableIdByPath(),
                        Collections.emptyMap(),
                        counts,
                        original.getBucketCountEpochByTableId()));
        CompletableFuture<Exception> written = new CompletableFuture<>();
        assigner.setBucketId(0);
        accumulator.append(
                WriteRecord.forArrowAppend(
                        healthy,
                        PhysicalTablePath.of(healthy.getTablePath()),
                        row(2, "healthy"),
                        null),
                (tb, leo, e) -> written.complete(e),
                updater.getCluster());
        accumulator.beginFlush();
        sender.runOnce();
        assertThat(refreshes.get()).isOne();
        assertThat(queued).isNotDone();
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        updater.newTabletServerClientForNode(TestingMetadataUpdater.NODE1.id());
        assertThat(((ProduceLogRequest) gateway.getRequest(0)).getTableId())
                .isEqualTo(healthy.getTableId());
        ProduceLogResponse response =
                makeProduceLogResponse(
                        Collections.singletonList(
                                new ProduceLogResultForBucket(
                                        new TableBucket(healthy.getTableId(), 0), 0L, 1L)));
        gateway.response(0, response);
        assertThat(written.get(5, TimeUnit.SECONDS)).isNull();
        assertThat(sender.isRunning()).isTrue();
        assertThat(sender.getFatalError()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOutOfRangeBucketConfirmedByCoordinator(boolean coordinatorHasBucket) throws Exception {
        AtomicInteger metadataRequests = new AtomicInteger();
        TestingMetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO)) {
                    @Override
                    public CoordinatorGateway newCoordinatorServerClient() {
                        return new TestCoordinatorGateway() {
                            @Override
                            public CompletableFuture<MetadataResponse> metadata(
                                    MetadataRequest request) {
                                metadataRequests.incrementAndGet();
                                MetadataResponse response = new MetadataResponse();
                                ServerNode node = TestingMetadataUpdater.NODE1;
                                response.addTabletServer()
                                        .setNodeId(node.id())
                                        .setHost(node.host())
                                        .setPort(node.port());
                                PbTableMetadata table =
                                        response.addTableMetadata()
                                                .setTableId(DATA1_TABLE_ID)
                                                .setBucketCount(coordinatorHasBucket ? 3 : 1)
                                                .setBucketCountEpoch(2L);
                                table.setTablePath()
                                        .setDatabaseName(DATA1_TABLE_PATH.getDatabaseName())
                                        .setTableName(DATA1_TABLE_PATH.getTableName());
                                for (int i = 0; i < (coordinatorHasBucket ? 3 : 1); i++) {
                                    table.addBucketMetadata()
                                            .setBucketId(i)
                                            .setLeaderId(node.id())
                                            .addReplicaId(node.id());
                                }
                                return CompletableFuture.completedFuture(response);
                            }
                        };
                    }
                };
        Cluster original = metadataUpdater.getCluster();
        metadataUpdater.updateCluster(
                new Cluster(
                        original.getAliveTabletServers(),
                        original.getCoordinatorServer(),
                        original.getBucketLocationsByPath(),
                        original.getTableIdByPath(),
                        Collections.emptyMap(),
                        Collections.singletonMap(TableOrPartition.ofTable(DATA1_TABLE_ID), 3),
                        Collections.singletonMap(DATA1_TABLE_ID, 0L)));
        initializeSender(metadataUpdater);
        TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                TestingMetadataUpdater.NODE1.id());
        CompletableFuture<Exception> inflight = new CompletableFuture<>();
        CompletableFuture<Exception> queued = new CompletableFuture<>();
        assigner.setBucketId(tb1.getBucket());
        accumulator.append(
                WriteRecord.forArrowAppend(
                        DATA1_TABLE_INFO,
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
                        row(0, "inflight"),
                        null),
                (tb, leo, e) -> inflight.complete(e),
                metadataUpdater.getCluster());
        sender.runOnce();
        TableBucket missingBucket = new TableBucket(DATA1_TABLE_ID, 1);
        assigner.setBucketId(missingBucket.getBucket());
        accumulator.append(
                WriteRecord.forArrowAppend(
                        DATA1_TABLE_INFO,
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
                        row(1, "queued"),
                        null),
                (tb, leo, e) -> queued.complete(e),
                metadataUpdater.getCluster());
        PhysicalTablePath path = PhysicalTablePath.of(DATA1_TABLE_PATH);
        metadataUpdater.updateCluster(
                new Cluster(
                        original.getAliveTabletServers(),
                        original.getCoordinatorServer(),
                        Collections.singletonMap(
                                path,
                                Collections.singletonList(original.getBucketLocation(tb1).get())),
                        original.getTableIdByPath(),
                        Collections.emptyMap(),
                        Collections.singletonMap(TableOrPartition.ofTable(DATA1_TABLE_ID), 1),
                        Collections.singletonMap(DATA1_TABLE_ID, 1L)));
        accumulator.beginFlush();

        sender.runOnce();
        assertThat(metadataRequests.get()).isOne();
        if (coordinatorHasBucket) {
            // A stale tablet cache must not cause a valid buffered write to fail or be re-routed.
            assertThat(sender.isRunning()).isTrue();
            assertThat(queued).isNotDone();
            sender.runOnce();
            ProduceLogRequest request = (ProduceLogRequest) gateway.getRequest(1);
            assertThat(request.getBucketsReqAt(0).getBucketId()).isEqualTo(1);
            assertThat(request.getBucketsReqAt(0).getRoutingBucketCount()).isEqualTo(3);
            gateway.response(
                    0,
                    makeProduceLogResponse(
                            Collections.singletonList(new ProduceLogResultForBucket(tb1, 0L, 1L))));
            gateway.response(
                    0,
                    makeProduceLogResponse(
                            Collections.singletonList(
                                    new ProduceLogResultForBucket(missingBucket, 0L, 1L))));
            assertThat(inflight.get(10, TimeUnit.SECONDS)).isNull();
            assertThat(queued.get(10, TimeUnit.SECONDS)).isNull();
        } else {
            assertThat(sender.isRunning()).isTrue();
            assertThat(sender.getFatalError()).isNull();
            assertThat(inflight.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(InvalidBucketRoutingException.class);
            assertThat(queued.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(InvalidBucketRoutingException.class);
            assertThat(sender.numOfInFlightBatches(tb1)).isZero();
        }
        assertThat(accumulator.hasIncomplete()).isFalse();
        assertThat(accumulator.hasUnDrained()).isFalse();
        accumulator.awaitFlushCompletion();
    }

    private void initializeSender(TestingMetadataUpdater updater) {
        TestingWriterMetricGroup metrics = TestingWriterMetricGroup.newInstance();
        IdempotenceManager idempotence =
                new IdempotenceManager(false, 5, updater.newRandomTabletServerClient(), updater);
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ZERO);
        accumulator =
                new RecordAccumulator(
                        conf,
                        idempotence,
                        metrics,
                        SystemClock.getInstance(),
                        new ExponentialBackoff(0, 1, 0, 0),
                        (info, path) -> assigner);
        sender =
                new Sender(
                        accumulator,
                        1000,
                        1024 * 1024,
                        (short) -1,
                        3,
                        updater,
                        idempotence,
                        metrics);
    }

    @AfterEach
    void tearDown() {
        if (sender != null) {
            accumulator.abortAllBatches(new IllegalStateException("Test cleanup"));
            sender.destroyResources();
        }
    }
}
