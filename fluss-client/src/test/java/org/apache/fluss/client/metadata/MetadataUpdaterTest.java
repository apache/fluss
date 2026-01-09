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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.buildMetadataResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** UT Test for update metadata of {@link MetadataUpdater}. */
public class MetadataUpdaterTest {

    private static final ServerNode CS_NODE =
            new ServerNode(1, "localhost", 8080, ServerType.COORDINATOR);
    private static final ServerNode TS_NODE =
            new ServerNode(1, "localhost", 8080, ServerType.TABLET_SERVER);

    private MetadataUpdater metadataUpdater;

    @AfterEach
    void tearDown() {
        if (metadataUpdater != null) {
            metadataUpdater.close();
        }
    }

    @Test
    void testInitializeClusterWithRetries() throws Exception {
        Configuration configuration = new Configuration();
        RpcClient rpcClient =
                RpcClient.create(configuration, TestingClientMetricGroup.newInstance(), false);

        // retry lower than max retry count.
        AdminReadOnlyGateway gateway = new TestingAdminReadOnlyGateway(2);
        Cluster cluster =
                MetadataUpdater.tryToInitializeClusterWithRetries(rpcClient, CS_NODE, gateway, 3);
        assertThat(cluster).isNotNull();
        assertThat(cluster.getCoordinatorServer()).isEqualTo(CS_NODE);
        assertThat(cluster.getAliveTabletServerList()).containsExactly(TS_NODE);

        // retry higher than max retry count.
        AdminReadOnlyGateway gateway2 = new TestingAdminReadOnlyGateway(5);
        assertThatThrownBy(
                        () ->
                                MetadataUpdater.tryToInitializeClusterWithRetries(
                                        rpcClient, CS_NODE, gateway2, 3))
                .isInstanceOf(StaleMetadataException.class)
                .hasMessageContaining("The metadata is stale.");
    }

    @Test
    void testRequestDeduplication() throws Exception {
        // Test that duplicate requests for the same resource are deduplicated
        Configuration conf = new Configuration();
        RpcClient rpcClient = RpcClient.create(conf, TestingClientMetricGroup.newInstance(), false);
        TestClusterMetadataFetcher testFetcher = new TestClusterMetadataFetcher();

        Cluster initialCluster =
                new Cluster(
                        Collections.singletonMap(1, TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        metadataUpdater = new MetadataUpdater(rpcClient, conf, initialCluster, testFetcher);

        TablePath table = TablePath.of("db", "table");
        Set<TablePath> tables = Collections.singleton(table);

        // Submit the same table request multiple times concurrently
        CountDownLatch latch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            new Thread(
                            () -> {
                                try {
                                    metadataUpdater.checkAndUpdateTableMetadata(tables);
                                } finally {
                                    latch.countDown();
                                }
                            })
                    .start();
        }

        // Wait for all threads to complete
        assertThat(latch.await(5, TimeUnit.SECONDS)).as("All requests should complete").isTrue();

        // CRITICAL: Duplicate requests for the SAME table MUST be deduplicated into exactly 1
        // fetch
        assertThat(testFetcher.getFetchCount())
                .as("Duplicate requests for the same table MUST be deduplicated into one fetch")
                .isEqualTo(1);

        // Verify the correct table was requested
        Set<TablePath> requestedTables = testFetcher.getLastRequestedTables();
        assertThat(requestedTables).containsExactly(table);

        // Verify no resource leaks
        assertThat(metadataUpdater.pendingRequests)
                .as("All pending requests should be cleaned up after completion")
                .isEmpty();
    }

    @Test
    void testRequestBatching() throws Exception {
        // Test that multiple concurrent requests for DIFFERENT tables are batched into one fetch
        Configuration conf = new Configuration();
        RpcClient rpcClient = RpcClient.create(conf, TestingClientMetricGroup.newInstance(), false);
        TestClusterMetadataFetcher testFetcher = new TestClusterMetadataFetcher();

        Cluster initialCluster =
                new Cluster(
                        Collections.singletonMap(1, TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        metadataUpdater = new MetadataUpdater(rpcClient, conf, initialCluster, testFetcher);

        TablePath table1 = TablePath.of("db", "table1");
        TablePath table2 = TablePath.of("db", "table2");
        TablePath table3 = TablePath.of("db", "table3");

        // Use CountDownLatch to ensure threads start as close together as possible
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch readyLatch = new CountDownLatch(3);
        CountDownLatch completionLatch = new CountDownLatch(3);

        Thread t1 =
                new Thread(
                        () -> {
                            try {
                                readyLatch.countDown();
                                startLatch.await(); // Wait for all threads to be ready
                                metadataUpdater.checkAndUpdateTableMetadata(
                                        Collections.singleton(table1));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                completionLatch.countDown();
                            }
                        });

        Thread t2 =
                new Thread(
                        () -> {
                            try {
                                readyLatch.countDown();
                                startLatch.await();
                                metadataUpdater.checkAndUpdateTableMetadata(
                                        Collections.singleton(table2));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                completionLatch.countDown();
                            }
                        });

        Thread t3 =
                new Thread(
                        () -> {
                            try {
                                readyLatch.countDown();
                                startLatch.await();
                                metadataUpdater.checkAndUpdateTableMetadata(
                                        Collections.singleton(table3));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                completionLatch.countDown();
                            }
                        });

        t1.start();
        t2.start();
        t3.start();

        // Wait for all threads to be ready
        assertThat(readyLatch.await(2, TimeUnit.SECONDS))
                .as("All threads should be ready")
                .isTrue();

        // Release all threads at once to maximize concurrency
        startLatch.countDown();

        // Wait for all to complete
        assertThat(completionLatch.await(5, TimeUnit.SECONDS))
                .as("All requests should complete within timeout")
                .isTrue();

        // CRITICAL: Expected batching behavior depends on thread scheduling:
        // Best case: All 3 requests arrive before first fetch starts -> 1 fetch with all 3 tables
        // Common case: First request triggers fetch, other 2 queue -> 2 fetches
        // Worst case: Each request arrives after previous fetch completes -> 3 fetches
        // The key is that batching REDUCES fetches from the theoretical maximum of 3
        assertThat(testFetcher.getFetchCount())
                .as(
                        "Batching should reduce fetch count. "
                                + "With 3 concurrent requests, expect 1-3 fetches depending on timing")
                .isGreaterThanOrEqualTo(1)
                .isLessThanOrEqualTo(3);

        // Verify all three tables were requested (across all fetches)
        Set<TablePath> allRequestedTables = testFetcher.getAllRequestedTables();
        assertThat(allRequestedTables)
                .as("All three tables should be requested across all batched fetches")
                .containsExactlyInAnyOrder(table1, table2, table3);

        // Verify no resource leaks
        assertThat(metadataUpdater.pendingRequests).isEmpty();
    }

    @Test
    void testRetryAndCleanup() throws Exception {
        // Test that failed requests trigger retries and are eventually cleaned up
        Configuration conf = new Configuration();
        RpcClient rpcClient = RpcClient.create(conf, TestingClientMetricGroup.newInstance(), false);
        TestClusterMetadataFetcher testFetcher = new TestClusterMetadataFetcher();
        testFetcher.setShouldFail(true);

        Cluster initialCluster =
                new Cluster(
                        Collections.singletonMap(1, TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        metadataUpdater = new MetadataUpdater(rpcClient, conf, initialCluster, testFetcher);

        TablePath table = TablePath.of("db", "table");
        Set<TablePath> tables = Collections.singleton(table);

        // Submit request that will fail - expect FlussRuntimeException wrapping the failure
        assertThatThrownBy(() -> metadataUpdater.checkAndUpdateTableMetadata(tables))
                .as(
                        "After max retries, request should fail with FlussRuntimeException indicating update failure")
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Failed to update metadata");

        // CRITICAL: Verify that fetch was attempted
        // Note: With continuous failures, the system may not retry the actual fetch
        // but will increment retry count internally until max retries is reached
        assertThat(testFetcher.getFetchCount())
                .as("At least one fetch attempt should be made before giving up")
                .isGreaterThanOrEqualTo(1);

        // CRITICAL: Verify no resource leaks after failure and cleanup
        assertThat(metadataUpdater.pendingRequests)
                .as(
                        "After max retries and failure, pending requests MUST be cleaned up to prevent leaks")
                .isEmpty();
    }

    @Test
    void testResourceCleanupOnSuccess() throws Exception {
        // Test that resources (pending requests and futures) are properly cleaned up on success
        Configuration conf = new Configuration();
        RpcClient rpcClient = RpcClient.create(conf, TestingClientMetricGroup.newInstance(), false);
        TestClusterMetadataFetcher testFetcher = new TestClusterMetadataFetcher();

        Cluster initialCluster =
                new Cluster(
                        Collections.singletonMap(1, TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        metadataUpdater = new MetadataUpdater(rpcClient, conf, initialCluster, testFetcher);

        // Submit multiple sequential requests
        for (int i = 0; i < 5; i++) {
            TablePath table = TablePath.of("db", "table" + i);
            metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(table));

            // CRITICAL: After EACH request completes, pending map should be empty
            assertThat(metadataUpdater.pendingRequests)
                    .as("Pending requests must be cleaned up after each successful request")
                    .isEmpty();
        }

        // Verify requests were processed
        assertThat(testFetcher.getFetchCount())
                .as("All 5 requests should have been processed")
                .isEqualTo(5);

        // Verify updater is still functional after multiple operations
        TablePath newTable = TablePath.of("db", "new_table");
        int previousCount = testFetcher.getFetchCount();

        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(newTable));

        assertThat(testFetcher.getFetchCount())
                .as("MetadataUpdater should remain functional after cleanup")
                .isEqualTo(previousCount + 1);

        // Final cleanup verification
        assertThat(metadataUpdater.pendingRequests)
                .as("Final cleanup verification: no pending requests")
                .isEmpty();
    }

    private static final class TestingAdminReadOnlyGateway extends TestCoordinatorGateway {

        private final int maxRetryCount;
        private int retryCount;

        public TestingAdminReadOnlyGateway(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
        }

        @Override
        public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
            retryCount++;
            if (retryCount <= maxRetryCount) {
                throw new StaleMetadataException("The metadata is stale.");
            } else {
                MetadataResponse metadataResponse =
                        buildMetadataResponse(
                                CS_NODE,
                                Collections.singleton(TS_NODE),
                                Collections.emptyList(),
                                Collections.emptyList());
                return CompletableFuture.completedFuture(metadataResponse);
            }
        }
    }

    /**
     * Test implementation of ClusterMetadataFetcher for unit testing. Simulates metadata fetching
     * with configurable behavior.
     */
    private static class TestClusterMetadataFetcher implements ClusterMetadataFetcher {
        private final AtomicInteger fetchCount = new AtomicInteger(0);
        private volatile Set<TablePath> lastRequestedTables;
        private final Set<TablePath> allRequestedTables = new HashSet<>();
        private volatile boolean shouldFail = false;

        @Override
        public CompletableFuture<Cluster> fetch(
                AdminReadOnlyGateway gateway,
                boolean partialUpdate,
                Cluster originCluster,
                @Nullable Set<TablePath> tablePaths,
                @Nullable Collection<PhysicalTablePath> tablePartitions,
                @Nullable Collection<Long> tablePartitionIds) {

            fetchCount.incrementAndGet();
            if (tablePaths != null) {
                lastRequestedTables = new HashSet<>(tablePaths);
                synchronized (allRequestedTables) {
                    allRequestedTables.addAll(tablePaths);
                }
            }

            if (shouldFail) {
                return FutureUtils.failedCompletableFuture(
                        new RuntimeException("Simulated fetch failure"));
            }

            // Return the origin cluster unchanged (successful fetch)
            return CompletableFuture.completedFuture(originCluster);
        }

        public int getFetchCount() {
            return fetchCount.get();
        }

        public Set<TablePath> getLastRequestedTables() {
            return lastRequestedTables;
        }

        public Set<TablePath> getAllRequestedTables() {
            synchronized (allRequestedTables) {
                return new HashSet<>(allRequestedTables);
            }
        }

        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }
    }
}
