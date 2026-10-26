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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests isolation between write targets while routing metadata is unavailable. */
class SenderMetadataTest {
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
        TestingWriterMetricGroup metrics = TestingWriterMetricGroup.newInstance();
        IdempotenceManager idempotence =
                new IdempotenceManager(false, 5, updater.newRandomTabletServerClient(), updater);
        TestingBucketAssigner assigner = new TestingBucketAssigner();
        RecordAccumulator accumulator =
                new RecordAccumulator(
                        new Configuration(),
                        idempotence,
                        metrics,
                        SystemClock.getInstance(),
                        new ExponentialBackoff(0, 1, 0, 0),
                        (info, path) -> assigner);
        Sender sender =
                new Sender(
                        accumulator,
                        1000,
                        1024 * 1024,
                        (short) -1,
                        3,
                        updater,
                        idempotence,
                        metrics);
        try {
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
                            counts));
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
        } finally {
            accumulator.abortAllBatches(new IllegalStateException("Test cleanup"));
            sender.destroyResources();
        }
    }
}
