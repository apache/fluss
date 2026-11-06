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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.testutils.common.CommonTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests publication of metadata responses racing with cache invalidation. */
class MetadataUpdaterConcurrencyTest {
    private static final TablePath TABLE_A = TablePath.of("db", "a");
    private static final TablePath TABLE_B = TablePath.of("db", "b");
    private static final PhysicalTablePath PATH_A = PhysicalTablePath.of(TABLE_A);
    private static final PhysicalTablePath PATH_B = PhysicalTablePath.of(TABLE_B);
    private static final PhysicalTablePath PARTITION_P = PhysicalTablePath.of(TABLE_A, "p");
    private static final PhysicalTablePath PARTITION_Q = PhysicalTablePath.of(TABLE_A, "q");
    private static final TableBucket BUCKET_A = new TableBucket(1, 0);
    private static final TableBucket BUCKET_B = new TableBucket(2, 0);
    private static final TableBucket BUCKET_P = new TableBucket(1, 11L, 0);
    private static final TableBucket BUCKET_Q = new TableBucket(1, 12L, 0);
    private static final ServerNode SERVER =
            new ServerNode(1, "localhost", 8080, ServerType.TABLET_SERVER);

    private final ExecutorService executor = Executors.newFixedThreadPool(2);
    private final ControllableRpcClient rpcClient = new ControllableRpcClient();

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "true, true"})
    void testRefreshKeepsInvalidationAndUnrelatedUpdate(
            boolean includesInvalidatedTable, boolean repeatedInvalidation) throws Exception {
        MetadataUpdater updater = updater(false);
        if (repeatedInvalidation) {
            updater.invalidPhysicalTableBucketMeta(Collections.singleton(PATH_A));
        }
        Cluster beforeInvalidation = updater.getCluster();
        Future<?> refresh =
                executor.submit(
                        () ->
                                updater.updateMetadata(
                                        includesInvalidatedTable
                                                ? new HashSet<>(Arrays.asList(TABLE_A, TABLE_B))
                                                : Collections.singleton(TABLE_B),
                                        null,
                                        null));
        CompletableFuture<ApiMessage> pending = rpcClient.takeRequest();
        // This must finish while the metadata RPC is still pending, as on a network callback.
        executor.submit(() -> updater.invalidPhysicalTableBucketMeta(Collections.singleton(PATH_A)))
                .get(10, TimeUnit.SECONDS);
        if (repeatedInvalidation) {
            assertThat(updater.getCluster()).isSameAs(beforeInvalidation);
        }
        MetadataResponse response = response();
        addTable(response, TABLE_B, 2, 2);
        if (includesInvalidatedTable) {
            addTable(response, TABLE_A, 1, 2);
        }
        pending.complete(response);
        refresh.get(10, TimeUnit.SECONDS);

        assertThat(updater.getCluster().leaderFor(BUCKET_A)).isNull();
        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofTable(1))).isEmpty();
        assertThat(updater.getCluster().leaderFor(BUCKET_B)).isEqualTo(2);
        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofTable(2))).hasValue(1);
        assertThat(rpcClient.requests).isEmpty();
        refreshA(updater);
    }

    @Test
    void testNodeRefreshKeepsConcurrentInvalidation() throws Exception {
        MetadataUpdater updater = updater(false);
        Future<?> refresh = executor.submit(updater::refreshClusterUntilAvailable);
        CompletableFuture<ApiMessage> pending = rpcClient.takeRequest();
        updater.invalidPhysicalTableBucketMeta(Collections.singleton(PATH_A));
        pending.complete(response());
        refresh.get(10, TimeUnit.SECONDS);
        assertThat(updater.getCluster().leaderFor(BUCKET_A)).isNull();
        assertThat(updater.getCluster().leaderFor(BUCKET_B)).isEqualTo(1);
    }

    @Test
    void testInvalidationByIdWithoutCachedPartitionPath() throws Exception {
        MetadataUpdater updater = updater(true);
        updater.invalidPhysicalTableBucketAndPartitionMeta(Collections.singleton(PARTITION_P));
        Future<?> refresh =
                executor.submit(
                        () ->
                                updater.updateMetadata(
                                        null, Arrays.asList(PARTITION_P, PARTITION_Q), null));
        CompletableFuture<ApiMessage> pending = rpcClient.takeRequest();
        updater.invalidPhysicalTableBucketMetaByIds(
                null, Collections.singleton(new TablePartition(1, 11)));
        MetadataResponse response = response();
        addPartition(response, "p", 11, 8);
        addPartition(response, "q", 12, 6);
        pending.complete(response);
        refresh.get(10, TimeUnit.SECONDS);

        assertThat(updater.getCluster().getPartitionId(PARTITION_P)).isEmpty();
        assertThat(updater.getCluster().leaderFor(BUCKET_P)).isNull();
        assertThat(updater.getCluster().leaderFor(BUCKET_Q)).isEqualTo(2);
        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofTable(1))).hasValue(6);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionInvalidationRejectsItsIdentityAndCountOnly(boolean repeatedInvalidation)
            throws Exception {
        MetadataUpdater updater = updater(true);
        if (repeatedInvalidation) {
            updater.invalidPhysicalTableBucketAndPartitionMeta(Collections.singleton(PARTITION_P));
        }
        Cluster beforeInvalidation = updater.getCluster();
        Future<?> refresh =
                executor.submit(
                        () ->
                                updater.updateMetadata(
                                        null, Arrays.asList(PARTITION_P, PARTITION_Q), null));
        CompletableFuture<ApiMessage> pending = rpcClient.takeRequest();
        updater.invalidPhysicalTableBucketAndPartitionMeta(Collections.singleton(PARTITION_P));
        if (repeatedInvalidation) {
            assertThat(updater.getCluster()).isSameAs(beforeInvalidation);
        }
        MetadataResponse response = response();
        addPartition(response, "p", 11, 8);
        addPartition(response, "q", 12, 6);
        pending.complete(response);
        refresh.get(10, TimeUnit.SECONDS);

        Cluster cluster = updater.getCluster();
        assertThat(cluster.getPartitionId(PARTITION_P)).isEmpty();
        assertThat(cluster.leaderFor(BUCKET_P)).isNull();
        assertThat(cluster.getBucketCount(TableOrPartition.ofPartition(11))).isEmpty();
        assertThat(cluster.getPartitionId(PARTITION_Q)).hasValue(12L);
        assertThat(cluster.leaderFor(BUCKET_Q)).isEqualTo(2);
        assertThat(cluster.getBucketCount(TableOrPartition.ofPartition(12))).hasValue(6);
        assertThat(cluster.getBucketCount(TableOrPartition.ofTable(1))).hasValue(6);
    }

    @Test
    void testTableInvalidationRejectsDependentPartitionCounts() throws Exception {
        MetadataUpdater updater = updater(true);
        Future<?> refresh =
                executor.submit(
                        () ->
                                updater.updateMetadata(
                                        null, Collections.singleton(PARTITION_P), null));
        CompletableFuture<ApiMessage> pending = rpcClient.takeRequest();
        updater.invalidPhysicalTableBucketMeta(Collections.singleton(PATH_A));
        MetadataResponse response = response();
        addPartition(response, "p", 11, 8);
        pending.complete(response);
        refresh.get(10, TimeUnit.SECONDS);

        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofTable(1))).isEmpty();
        assertThat(updater.getCluster().getBucketCount(TableOrPartition.ofPartition(11)))
                .hasValue(4);
        assertThat(updater.getCluster().leaderFor(BUCKET_P)).isEqualTo(1);
    }

    @Test
    void testConcurrentInvalidationsAreBothPreserved() throws Exception {
        MetadataUpdater updater = updater(false);
        CountDownLatch firstCopyStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        AbstractSet<PhysicalTablePath> gatedPaths =
                new AbstractSet<PhysicalTablePath>() {
                    @Override
                    public Iterator<PhysicalTablePath> iterator() {
                        return Collections.singleton(PATH_A).iterator();
                    }

                    @Override
                    public int size() {
                        return 1;
                    }

                    @Override
                    public boolean contains(Object path) {
                        if (first.compareAndSet(true, false)) {
                            firstCopyStarted.countDown();
                            await(releaseFirst);
                        }
                        return PATH_A.equals(path);
                    }
                };
        Future<?> invalidateA =
                executor.submit(() -> updater.invalidPhysicalTableBucketMeta(gatedPaths));
        try {
            await(firstCopyStarted);
            AtomicReference<Thread> secondThread = new AtomicReference<>();
            Future<?> invalidateB =
                    executor.submit(
                            () -> {
                                secondThread.set(Thread.currentThread());
                                updater.invalidPhysicalTableBucketAndPartitionMeta(
                                        Collections.singleton(PATH_B));
                            });
            // An unprotected writer completes against the old snapshot; a protected one waits.
            CommonTestUtils.waitUntil(
                    () ->
                            invalidateB.isDone()
                                    || (secondThread.get() != null
                                            && secondThread.get().getState()
                                                    == Thread.State.BLOCKED),
                    Duration.ofSeconds(10),
                    "Second invalidation did not reach the cache update");
            releaseFirst.countDown();
            invalidateA.get(10, TimeUnit.SECONDS);
            invalidateB.get(10, TimeUnit.SECONDS);
            assertThat(updater.getCluster().leaderFor(BUCKET_A)).isNull();
            assertThat(updater.getCluster().leaderFor(BUCKET_B)).isNull();
            assertThat(updater.getCluster().getBucketCountByTableOrPartition()).isEmpty();
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void testBootstrapResetKeepsCacheEmptyAndAllowsNextRefresh() throws Exception {
        MetadataUpdater updater = updater(false);
        Future<?> failedRefresh =
                executor.submit(
                        () -> updater.updateMetadata(Collections.singleton(TABLE_A), null, null));
        rpcClient.takeRequest().completeExceptionally(new NetworkException("offline"));
        failedRefresh.get(10, TimeUnit.SECONDS);

        Future<?> bootstrap = executor.submit(updater::refreshClusterUntilAvailable);
        CompletableFuture<ApiMessage> pending = rpcClient.takeRequest();
        executor.submit(() -> updater.invalidPhysicalTableBucketMeta(Collections.singleton(PATH_B)))
                .get(10, TimeUnit.SECONDS);
        pending.complete(response());
        bootstrap.get(10, TimeUnit.SECONDS);
        assertThat(updater.getCluster().getTableIdByPath()).isEmpty();
        assertThat(updater.getCluster().getPartitionIdByPath()).isEmpty();
        assertThat(updater.getCluster().getBucketLocationsByPath()).isEmpty();
        assertThat(updater.getCluster().getBucketCountByTableOrPartition()).isEmpty();
        refreshA(updater);
    }

    private void refreshA(MetadataUpdater updater) throws Exception {
        Future<?> refresh =
                executor.submit(
                        () -> updater.updateMetadata(Collections.singleton(TABLE_A), null, null));
        MetadataResponse response = response();
        addTable(response, TABLE_A, 1, 2);
        rpcClient.takeRequest().complete(response);
        refresh.get(10, TimeUnit.SECONDS);
        assertThat(updater.leaderFor(TABLE_A, BUCKET_A)).isEqualTo(2);
    }

    private MetadataUpdater updater(boolean partitioned) {
        Map<PhysicalTablePath, List<BucketLocation>> locations = new HashMap<>();
        Map<PhysicalTablePath, Long> partitions = new HashMap<>();
        Map<TableOrPartition, Integer> counts = new HashMap<>();
        if (partitioned) {
            locations.put(PARTITION_P, locations(PARTITION_P, BUCKET_P, 4));
            locations.put(PARTITION_Q, locations(PARTITION_Q, BUCKET_Q, 2));
            partitions.put(PARTITION_P, 11L);
            partitions.put(PARTITION_Q, 12L);
            counts.put(TableOrPartition.ofTable(1), 4);
            counts.put(TableOrPartition.ofPartition(11), 4);
            counts.put(TableOrPartition.ofPartition(12), 2);
        } else {
            locations.put(PATH_A, locations(PATH_A, BUCKET_A, 1));
            counts.put(TableOrPartition.ofTable(1), 1);
        }
        locations.put(PATH_B, locations(PATH_B, BUCKET_B, 1));
        counts.put(TableOrPartition.ofTable(2), 1);
        Map<TablePath, Long> ids = new HashMap<>();
        ids.put(TABLE_A, 1L);
        ids.put(TABLE_B, 2L);
        Cluster cluster =
                new Cluster(
                        Collections.singletonMap(1, SERVER),
                        null,
                        locations,
                        ids,
                        partitions,
                        counts);
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList("localhost:8080"));
        return new MetadataUpdater(rpcClient, configuration, cluster);
    }

    private static List<BucketLocation> locations(
            PhysicalTablePath path, TableBucket bucket, int count) {
        List<BucketLocation> locations = new ArrayList<>();
        for (int id = 0; id < count; id++) {
            locations.add(
                    new BucketLocation(
                            path,
                            new TableBucket(bucket.getTableId(), bucket.getPartitionId(), id),
                            1,
                            new int[] {1, 2}));
        }
        return locations;
    }

    private static MetadataResponse response() {
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer().setNodeId(1).setHost("localhost").setPort(8080);
        response.addTabletServer().setNodeId(2).setHost("localhost").setPort(8081);
        return response;
    }

    private static void addTable(MetadataResponse response, TablePath path, long id, int leader) {
        PbTableMetadata table = response.addTableMetadata().setTableId(id);
        table.setTablePath()
                .setDatabaseName(path.getDatabaseName())
                .setTableName(path.getTableName());
        table.addBucketMetadata().setBucketId(0).setLeaderId(leader).addReplicaId(leader);
    }

    private static void addPartition(MetadataResponse response, String name, long id, int count) {
        PbPartitionMetadata partition =
                response.addPartitionMetadata()
                        .setTableId(1)
                        .setPartitionId(id)
                        .setPartitionName(name)
                        .setBucketCount(count);
        for (int bucket = 0; bucket < count; bucket++) {
            partition.addBucketMetadata().setBucketId(bucket).setLeaderId(2).addReplicaId(2);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class ControllableRpcClient implements RpcClient {
        private final BlockingQueue<CompletableFuture<ApiMessage>> requests =
                new LinkedBlockingQueue<>();

        CompletableFuture<ApiMessage> takeRequest() throws Exception {
            CompletableFuture<ApiMessage> request = requests.poll(10, TimeUnit.SECONDS);
            assertThat(request).isNotNull();
            return request;
        }

        @Override
        public CompletableFuture<ApiMessage> sendRequest(
                ServerNode node, ApiKeys apiKey, ApiMessage request) {
            CompletableFuture<ApiMessage> response = new CompletableFuture<>();
            requests.add(response);
            return response;
        }

        @Override
        public boolean connect(ServerNode node) {
            return true;
        }

        @Override
        public CompletableFuture<Void> disconnect(String serverUid) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean isReady(String serverUid) {
            return true;
        }

        @Override
        public void close() {}
    }
}
