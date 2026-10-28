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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import org.apache.fluss.server.entity.AdjustIsrResultForBucket;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrResponseData;
import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static org.apache.fluss.server.zk.ZooKeeperUtils.startZookeeperClient;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.builder;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for committed-state handling in {@link CoordinatorEventProcessor} AdjustIsr requests. */
class CoordinatorEventProcessorAdjustIsrTest extends CoordinatorEventProcessorTestBase {

    private static final int N_BUCKETS = 3;
    private static final int REPLICATION_FACTOR = 3;

    private static final TableDescriptor TEST_TABLE =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .primaryKey("a")
                                    .build())
                    .distributedBy(N_BUCKETS, "a")
                    .property(ConfigOptions.TABLE_KV_STANDBY_REPLICA_ENABLED.key(), "true")
                    .build()
                    .withReplicationFactor(REPLICATION_FACTOR);

    private TestingAdjustIsrZooKeeperClient testingAdjustIsrZooKeeperClient;

    @Override
    @AfterEach
    void afterEach() throws Exception {
        try {
            super.afterEach();
        } finally {
            if (testingAdjustIsrZooKeeperClient != null) {
                testingAdjustIsrZooKeeperClient.close();
                testingAdjustIsrZooKeeperClient = null;
            }
        }
    }

    @Test
    void testProcessAdjustIsr() throws Exception {
        Map<TableBucket, LeaderAndIsr> bucketLeaderAndIsrMap =
                createAdjustIsrTestTable("process_adjust_isr");

        // The normal batch path should commit every proposed change.
        AdjustIsrResponse adjustIsrResponse = adjustIsr(bucketLeaderAndIsrMap);
        Map<TableBucket, AdjustIsrResultForBucket> resultForBucketMap =
                getAdjustIsrResponseData(adjustIsrResponse);

        assertAdjustIsrResponseCount(adjustIsrResponse, bucketLeaderAndIsrMap.size());
        assertThat(resultForBucketMap.keySet())
                .containsExactlyInAnyOrderElementsOf(bucketLeaderAndIsrMap.keySet());
        assertThat(resultForBucketMap.values()).allMatch(AdjustIsrResultForBucket::succeeded);
        assertAdjustedIsrInstalled(bucketLeaderAndIsrMap, bucketLeaderAndIsrMap.keySet());
    }

    @Test
    void testProcessAdjustIsrFallsBackToSuccessfulIndividualUpdates() throws Exception {
        Map<TableBucket, LeaderAndIsr> originalLeaderAndIsrs =
                createAdjustIsrTestTable("adjust_isr_individual_success");
        // Force the batch write to fail, but allow every per-bucket retry to reach ZooKeeper.
        useTestingAdjustIsrZooKeeperClient(true, Collections.emptySet());

        AdjustIsrResponse response = adjustIsr(originalLeaderAndIsrs);
        Map<TableBucket, AdjustIsrResultForBucket> results = getAdjustIsrResponseData(response);

        // Every fallback write is committed, so each bucket succeeds and is installed.
        assertAdjustIsrResponseCount(response, originalLeaderAndIsrs.size());
        assertThat(results.values()).allMatch(AdjustIsrResultForBucket::succeeded);
        assertThat(testingAdjustIsrZooKeeperClient.getIndividualUpdates().keySet())
                .containsExactlyInAnyOrderElementsOf(originalLeaderAndIsrs.keySet());
        assertAdjustedIsrInstalled(originalLeaderAndIsrs, originalLeaderAndIsrs.keySet());
    }

    @Test
    void testProcessAdjustIsrInstallsOnlyCommittedIndividualUpdates() throws Exception {
        Map<TableBucket, LeaderAndIsr> originalLeaderAndIsrs =
                createAdjustIsrTestTable("adjust_isr_mixed_individual_results");
        List<TableBucket> buckets =
                Arrays.asList(originalLeaderAndIsrs.keySet().toArray(new TableBucket[0]));
        TableBucket failedBucket = buckets.get(1);
        Set<TableBucket> committedBuckets = new HashSet<>(originalLeaderAndIsrs.keySet());
        committedBuckets.remove(failedBucket);
        // Simulate a mixed fallback result: this bucket fails while all other retries succeed.
        useTestingAdjustIsrZooKeeperClient(true, Collections.singleton(failedBucket));

        AdjustIsrResponse response = adjustIsr(originalLeaderAndIsrs);
        Map<TableBucket, AdjustIsrResultForBucket> results = getAdjustIsrResponseData(response);

        // The failed bucket must have one error, not an error followed by a success.
        assertAdjustIsrResponseCount(response, originalLeaderAndIsrs.size());
        assertThat(results.get(failedBucket).failed()).isTrue();
        assertThat(results.entrySet())
                .filteredOn(entry -> !entry.getKey().equals(failedBucket))
                .allMatch(entry -> entry.getValue().succeeded());
        // Only successful ZooKeeper writes may cross the committed-state boundary.
        assertAdjustedIsrInstalled(originalLeaderAndIsrs, committedBuckets);
    }

    @Test
    void testProcessAdjustIsrInstallsNothingWhenAllIndividualUpdatesFail() throws Exception {
        Map<TableBucket, LeaderAndIsr> originalLeaderAndIsrs =
                createAdjustIsrTestTable("adjust_isr_individual_failure");
        // Fail the batch write and every per-bucket fallback write.
        useTestingAdjustIsrZooKeeperClient(true, originalLeaderAndIsrs.keySet());

        AdjustIsrResponse response = adjustIsr(originalLeaderAndIsrs);
        Map<TableBucket, AdjustIsrResultForBucket> results = getAdjustIsrResponseData(response);

        // No proposed change was committed, so CoordinatorContext must retain all old states.
        assertAdjustIsrResponseCount(response, originalLeaderAndIsrs.size());
        assertThat(results.values()).allMatch(AdjustIsrResultForBucket::failed);
        assertAdjustedIsrInstalled(originalLeaderAndIsrs, Collections.emptySet());
    }

    @Test
    void testProcessAdjustIsrKeepsValidationAndPersistenceResultsUnique() throws Exception {
        Map<TableBucket, LeaderAndIsr> originalLeaderAndIsrs =
                createAdjustIsrTestTable("adjust_isr_validation_and_persistence");
        List<TableBucket> buckets =
                Arrays.asList(originalLeaderAndIsrs.keySet().toArray(new TableBucket[0]));
        TableBucket invalidBucket = buckets.get(0);
        TableBucket persistenceFailureBucket = buckets.get(1);
        LeaderAndIsr original = originalLeaderAndIsrs.get(invalidBucket);
        Map<TableBucket, LeaderAndIsr> requestedLeaderAndIsrs =
                new HashMap<>(originalLeaderAndIsrs);
        // A lower leader epoch makes this request fail validation before any ZooKeeper write.
        requestedLeaderAndIsrs.put(
                invalidBucket,
                new LeaderAndIsr(
                        original.leader(),
                        original.leaderEpoch() - 1,
                        original.isr(),
                        original.standbyReplicas(),
                        original.coordinatorEpoch(),
                        original.bucketEpoch()));
        // A different valid bucket reaches the fallback path but fails its individual write.
        useTestingAdjustIsrZooKeeperClient(true, Collections.singleton(persistenceFailureBucket));

        AdjustIsrResponse response = adjustIsr(requestedLeaderAndIsrs);
        Map<TableBucket, AdjustIsrResultForBucket> results = getAdjustIsrResponseData(response);

        // Validation and persistence failures are each reported exactly once.
        assertAdjustIsrResponseCount(response, requestedLeaderAndIsrs.size());
        assertThat(results.get(invalidBucket).failed()).isTrue();
        assertThat(results.get(persistenceFailureBucket).failed()).isTrue();
        // Validation failures must never enter either persistence attempt.
        assertThat(testingAdjustIsrZooKeeperClient.getBatchUpdates())
                .doesNotContainKey(invalidBucket);
        assertThat(testingAdjustIsrZooKeeperClient.getIndividualUpdates())
                .doesNotContainKey(invalidBucket);
        assertAdjustedIsrInstalled(
                originalLeaderAndIsrs,
                originalLeaderAndIsrs.keySet().stream()
                        .filter(
                                bucket ->
                                        !bucket.equals(invalidBucket)
                                                && !bucket.equals(persistenceFailureBucket))
                        .collect(Collectors.toSet()));
    }

    @Test
    void testProcessAdjustIsrRejectsRequestFromNonLeader() throws Exception {
        Map.Entry<TableBucket, LeaderAndIsr> entry =
                createTableAndGetLeaderAndIsr("adjust_isr_from_non_leader");
        TableBucket tableBucket = entry.getKey();
        LeaderAndIsr currentLeaderAndIsr = entry.getValue();
        int nonLeader =
                currentLeaderAndIsr.isr().stream()
                        .filter(replica -> replica != currentLeaderAndIsr.leader())
                        .findFirst()
                        .get();
        LeaderAndIsr invalidLeaderAndIsr =
                new LeaderAndIsr(
                        nonLeader,
                        currentLeaderAndIsr.leaderEpoch(),
                        currentLeaderAndIsr.isr(),
                        currentLeaderAndIsr.standbyReplicas(),
                        currentLeaderAndIsr.coordinatorEpoch(),
                        currentLeaderAndIsr.bucketEpoch());

        AdjustIsrResultForBucket invalidResult = submitAdjustIsr(tableBucket, invalidLeaderAndIsr);

        assertThat(invalidResult.getError().error())
                .isEqualTo(Errors.FENCED_LEADER_EPOCH_EXCEPTION);
        assertThat(invalidResult.getErrorMessage())
                .contains(
                        String.format(
                                "request leader %s does not match current leader %s",
                                nonLeader, currentLeaderAndIsr.leader()));
        Optional<LeaderAndIsr> storedLeaderAndIsr =
                fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket));
        assertThat(storedLeaderAndIsr).contains(currentLeaderAndIsr);
    }

    @Test
    void testProcessAdjustIsrRejectsIsrWithoutCurrentLeader() throws Exception {
        Map.Entry<TableBucket, LeaderAndIsr> entry =
                createTableAndGetLeaderAndIsr("adjust_isr_without_current_leader");
        TableBucket tableBucket = entry.getKey();
        LeaderAndIsr currentLeaderAndIsr = entry.getValue();
        List<Integer> isrWithoutLeader =
                currentLeaderAndIsr.isr().stream()
                        .filter(replica -> replica != currentLeaderAndIsr.leader())
                        .collect(Collectors.toList());
        LeaderAndIsr invalidLeaderAndIsr =
                new LeaderAndIsr(
                        currentLeaderAndIsr.leader(),
                        currentLeaderAndIsr.leaderEpoch(),
                        isrWithoutLeader,
                        currentLeaderAndIsr.standbyReplicas(),
                        currentLeaderAndIsr.coordinatorEpoch(),
                        currentLeaderAndIsr.bucketEpoch());

        AdjustIsrResultForBucket invalidResult = submitAdjustIsr(tableBucket, invalidLeaderAndIsr);
        assertThat(invalidResult.getError().error()).isEqualTo(Errors.INELIGIBLE_REPLICA_EXCEPTION);
        assertThat(invalidResult.getErrorMessage())
                .contains(
                        String.format(
                                "leader %s is not in the new ISR", currentLeaderAndIsr.leader()));
        Optional<LeaderAndIsr> storedLeaderAndIsr =
                fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket));
        assertThat(storedLeaderAndIsr).contains(currentLeaderAndIsr);
    }

    private Map<TableBucket, LeaderAndIsr> createAdjustIsrTestTable(String tableName)
            throws Exception {
        initCoordinatorChannel();
        TableAssignment tableAssignment =
                generateAssignment(
                        N_BUCKETS,
                        REPLICATION_FACTOR,
                        new TabletServerInfo[] {
                            new TabletServerInfo(0, "rack0"),
                            new TabletServerInfo(1, "rack1"),
                            new TabletServerInfo(2, "rack2")
                        });
        long tableId =
                metadataManager.createTable(
                        TablePath.of(defaultDatabase, tableName),
                        remoteDataDir,
                        TEST_TABLE,
                        tableAssignment,
                        false);

        return waitValue(
                () ->
                        fromCtx(
                                ctx -> {
                                    Map<TableBucket, LeaderAndIsr> leaderAndIsrs = new HashMap<>();
                                    ctx.bucketLeaderAndIsr()
                                            .forEach(
                                                    (tableBucket, leaderAndIsr) -> {
                                                        if (tableBucket.getTableId() == tableId) {
                                                            leaderAndIsrs.put(
                                                                    tableBucket, leaderAndIsr);
                                                        }
                                                    });
                                    return leaderAndIsrs.size() == N_BUCKETS
                                            ? Optional.of(leaderAndIsrs)
                                            : Optional.empty();
                                }),
                Duration.ofMinutes(1),
                "leader not elected");
    }

    private Map.Entry<TableBucket, LeaderAndIsr> createTableAndGetLeaderAndIsr(String tableName)
            throws Exception {
        return createAdjustIsrTestTable(tableName).entrySet().iterator().next();
    }

    private void useTestingAdjustIsrZooKeeperClient(
            boolean failBatchUpdate, Set<TableBucket> failedIndividualUpdates) {
        testingAdjustIsrZooKeeperClient = new TestingAdjustIsrZooKeeperClient(remoteDataDir);
        CoordinatorEventProcessor previousEventProcessor = eventProcessor;
        CoordinatorEventProcessor testingEventProcessor =
                buildCoordinatorEventProcessor(testingAdjustIsrZooKeeperClient);
        // Start the replacement first so it can load the existing table state before the original
        // processor is shut down. Failure injection is enabled only after startup initialization.
        testingEventProcessor.startup();
        previousEventProcessor.shutdown();
        eventProcessor = testingEventProcessor;
        testingAdjustIsrZooKeeperClient.enableFailureInjection(
                failBatchUpdate, failedIndividualUpdates);
    }

    private AdjustIsrResponse adjustIsr(Map<TableBucket, LeaderAndIsr> leaderAndIsrs)
            throws Exception {
        CompletableFuture<AdjustIsrResponse> response = new CompletableFuture<>();
        eventProcessor
                .getCoordinatorEventManager()
                .put(new AdjustIsrReceivedEvent(leaderAndIsrs, response));
        return response.get();
    }

    private AdjustIsrResultForBucket submitAdjustIsr(
            TableBucket tableBucket, LeaderAndIsr newLeaderAndIsr) throws Exception {
        CompletableFuture<AdjustIsrResponse> response = new CompletableFuture<>();
        eventProcessor
                .getCoordinatorEventManager()
                .put(
                        new AdjustIsrReceivedEvent(
                                Collections.singletonMap(tableBucket, newLeaderAndIsr), response));
        return getAdjustIsrResponseData(response.get()).get(tableBucket);
    }

    private void assertAdjustIsrResponseCount(AdjustIsrResponse response, int expectedCount) {
        // Inspect the raw protobuf response: converting it to a map would hide duplicate results
        // for the same bucket, such as an error followed by an unconditional success.
        assertThat(
                        response.getTablesRespsList().stream()
                                .mapToInt(tableResponse -> tableResponse.getBucketsRespsCount())
                                .sum())
                .isEqualTo(expectedCount);
    }

    private void assertAdjustedIsrInstalled(
            Map<TableBucket, LeaderAndIsr> originalLeaderAndIsrs, Set<TableBucket> committedBuckets)
            throws Exception {
        for (Map.Entry<TableBucket, LeaderAndIsr> entry : originalLeaderAndIsrs.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            LeaderAndIsr originalLeaderAndIsr = entry.getValue();
            Optional<LeaderAndIsr> installedLeaderAndIsr =
                    fromCtx(ctx -> ctx.getBucketLeaderAndIsr(tableBucket));
            // A committed bucket advances its bucket epoch; an uncommitted bucket must remain
            // equal to the state observed before the request.
            assertThat(installedLeaderAndIsr)
                    .contains(
                            committedBuckets.contains(tableBucket)
                                    ? originalLeaderAndIsr.newLeaderAndIsr(
                                            originalLeaderAndIsr.isr())
                                    : originalLeaderAndIsr);
        }
    }

    private void initCoordinatorChannel() throws Exception {
        makeSendLeaderAndStopRequestAlwaysSuccess(
                testCoordinatorChannelManager,
                Arrays.stream(zookeeperClient.getSortedTabletServerList())
                        .boxed()
                        .collect(Collectors.toSet()),
                Collections.emptySet());
    }

    private <T> T fromCtx(java.util.function.Function<CoordinatorContext, T> retrieveFunction)
            throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(retrieveFunction);
        eventProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    private CoordinatorEventProcessor buildCoordinatorEventProcessor(
            ZooKeeperClient coordinatorZooKeeperClient) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        conf.set(ConfigOptions.COORDINATOR_OFFLINE_LEADER_RETRY_DELAY, Duration.ofDays(1));
        return new CoordinatorEventProcessor(
                coordinatorZooKeeperClient,
                serverMetadataCache,
                testCoordinatorChannelManager,
                new CoordinatorContext(zkEpoch),
                replicaCapacityController,
                autoPartitionManager,
                lakeTableTieringManager,
                TestingMetricGroups.COORDINATOR_METRICS,
                conf,
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory("test-coordinator-io")),
                metadataManager,
                kvSnapshotLeaseManager,
                scheduler,
                SystemClock.getInstance());
    }

    private static final class TestingAdjustIsrZooKeeperClient extends ZooKeeperClient {

        private final Map<TableBucket, LeaderAndIsr> batchUpdates = new HashMap<>();
        private final Map<TableBucket, LeaderAndIsr> individualUpdates = new HashMap<>();

        private boolean failureInjectionEnabled;
        private boolean failBatchUpdate;
        private Set<TableBucket> failedIndividualUpdates = Collections.emptySet();

        private TestingAdjustIsrZooKeeperClient(String remoteDataDir) {
            super(createCuratorFrameworkWrapper(), createConfiguration(remoteDataDir));
        }

        private void enableFailureInjection(
                boolean failBatchUpdate, Set<TableBucket> failedIndividualUpdates) {
            this.failBatchUpdate = failBatchUpdate;
            this.failedIndividualUpdates = new HashSet<>(failedIndividualUpdates);
            // Startup uses the real ZooKeeper behavior; only the AdjustIsr call under test should
            // observe the configured failures.
            failureInjectionEnabled = true;
        }

        @Override
        public void batchUpdateLeaderAndIsr(
                Map<TableBucket, LeaderAndIsr> leaderAndIsrs, int expectedZkVersion)
                throws Exception {
            if (!failureInjectionEnabled) {
                super.batchUpdateLeaderAndIsr(leaderAndIsrs, expectedZkVersion);
                return;
            }
            // Record every proposed change before throwing so tests can verify validation
            // filtering.
            batchUpdates.putAll(leaderAndIsrs);
            if (failBatchUpdate) {
                throw new RuntimeException("Expected batch update failure");
            }
            super.batchUpdateLeaderAndIsr(leaderAndIsrs, expectedZkVersion);
        }

        @Override
        public void updateLeaderAndIsr(
                TableBucket tableBucket, LeaderAndIsr leaderAndIsr, int expectedZkVersion)
                throws Exception {
            // Record all fallback attempts, but persist only the buckets not selected to fail.
            individualUpdates.put(tableBucket, leaderAndIsr);
            if (failedIndividualUpdates.contains(tableBucket)) {
                throw new RuntimeException("Expected individual update failure");
            }
            super.updateLeaderAndIsr(tableBucket, leaderAndIsr, expectedZkVersion);
        }

        private Map<TableBucket, LeaderAndIsr> getBatchUpdates() {
            return batchUpdates;
        }

        private Map<TableBucket, LeaderAndIsr> getIndividualUpdates() {
            return individualUpdates;
        }

        private static CuratorFrameworkWithUnhandledErrorListener createCuratorFrameworkWrapper() {
            Builder builder =
                    builder()
                            .connectString(
                                    ZOO_KEEPER_EXTENSION_WRAPPER
                                            .getCustomExtension()
                                            .getConnectString())
                            .namespace("fluss")
                            .retryPolicy(new ExponentialBackoffRetry(1000, 3));
            return startZookeeperClient(builder, NOPErrorHandler.INSTANCE);
        }

        private static Configuration createConfiguration(String remoteDataDir) {
            Configuration configuration = new Configuration();
            configuration.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
            return configuration;
        }
    }
}
