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

package org.apache.fluss.server.index;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Failover ITCase for the index push replication pipeline. Tests that:
 *
 * <ul>
 *   <li>After main-table leader failover, the new leader resumes index push from the checkpointed
 *       indexPushedOffset.
 *   <li>After index-table leader failover, the push pipeline retries and the entry eventually
 *       becomes visible.
 * </ul>
 *
 * <p>Requires a 4-node cluster with replication factor 3 for leader failover.
 */
class IndexPushFailoverITCase {

    private static final String DB = "test_failover_db";
    private static final String INDEX_NAME = "idx_b";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int TABLET_SERVER_COUNT = 4;
    private static final int INDEX_BUCKET_COUNT = 3;
    private static final int TARGET_FAILOVER_INDEX_BUCKET_COUNT = 8;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(TABLET_SERVER_COUNT)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        return conf;
    }

    /**
     * After writing data to the main table, kill the index table's leader. The push pipeline should
     * retry via the LeaderResolver and the entry should eventually appear on the new index leader.
     */
    @Test
    void testIndexTableLeaderFailoverRetries() throws Throwable {
        TablePath mainPath = TablePath.of(DB, "main_idx_failover");

        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                TARGET_FAILOVER_INDEX_BUCKET_COUNT)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();

        TestingIndexTable table =
                new TestingIndexTable(FLUSS_CLUSTER_EXTENSION, mainPath, descriptor, INDEX_NAME);
        long mainTableId = table.mainTableId;
        long indexTableId = table.indexTableId;
        TableBucket mainBucket = new TableBucket(mainTableId, 0);
        int mainLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);
        Replica mainReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        IndexReplicationEncoder indexEncoder = table.indexEncoder(mainBucket);

        // Offset zero is the exclusive next-read position before the first source record.
        assertThat(mainReplica.getSyncIndexPushedOffset())
                .as("the first SYNC write must start from the offset-zero baseline")
                .isZero();
        assertThat(mainReplica.getAllIndexPushedOffset()).isZero();

        TableAssignment sourceAssignment =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTableAssignment(mainTableId)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Source main-table assignment is missing from ZK"));
        List<Integer> sourceBucketReplicas =
                sourceAssignment.getBucketAssignment(mainBucket.getBucket()).getReplicas();
        assertThat(sourceBucketReplicas)
                .as("source main-table leader must belong to source bucket's RF=3 assignment")
                .contains(mainLeader);

        List<Integer> targetOnlyServers = new ArrayList<>();
        for (int serverId = 0; serverId < TABLET_SERVER_COUNT; serverId++) {
            assertThat(FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId))
                    .as("tablet server %s must be live before target selection", serverId)
                    .isNotNull();
            if (!sourceBucketReplicas.contains(serverId)) {
                targetOnlyServers.add(serverId);
            }
        }
        if (targetOnlyServers.size() != 1) {
            throw new AssertionError(
                    "Source bucket assignment must leave exactly one target-only live server; "
                            + "source bucket replicas="
                            + sourceBucketReplicas
                            + ", live servers=0.."
                            + (TABLET_SERVER_COUNT - 1));
        }
        int targetOnlyServer = targetOnlyServers.get(0);

        // Select an actual index bucket led by the one server that cannot host a source-bucket
        // replica. The written value below hashes to this exact bucket, so stopping its leader
        // leaves source WAL quorum intact while making the target unavailable.
        int[] indexBucketLeaders = new int[TARGET_FAILOVER_INDEX_BUCKET_COUNT];
        int targetIdxBucket = -1;
        for (int i = 0; i < TARGET_FAILOVER_INDEX_BUCKET_COUNT; i++) {
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(indexTableId, i));
            indexBucketLeaders[i] = leader;
            if (targetIdxBucket == -1 && leader == targetOnlyServer) {
                targetIdxBucket = i;
            }
        }
        if (targetIdxBucket == -1) {
            throw new AssertionError(
                    "No index bucket is led by target-only server "
                            + targetOnlyServer
                            + "; source bucket replicas="
                            + sourceBucketReplicas
                            + "; index bucket leaders="
                            + Arrays.toString(indexBucketLeaders));
        }

        String selectedValue =
                valueForIndexBucket(indexEncoder, "target-failover", targetIdxBucket);
        byte[] indexKey = encodeIndexKey(indexEncoder, selectedValue, 1);
        TableBucket idxTb = new TableBucket(indexTableId, targetIdxBucket);
        int stoppedTargetLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxTb);
        assertThat(stoppedTargetLeader)
                .as("selected index bucket leader must be the target-only server")
                .isEqualTo(targetOnlyServer);
        assertThat(stoppedTargetLeader)
                .as("selected index bucket leader must remain the observed target leader")
                .isEqualTo(indexBucketLeaders[targetIdxBucket]);
        assertThat(sourceBucketReplicas)
                .as("target-only index leader must not host a source-bucket replica")
                .doesNotContain(stoppedTargetLeader);

        IndexSendBuffer sourceSendBuffer =
                FLUSS_CLUSTER_EXTENSION
                        .getTabletServerById(mainLeader)
                        .getReplicaManager()
                        .getIndexReplicationManager()
                        .sendBuffer();
        boolean coordinatorStopped = false;
        boolean targetLeaderStopped = false;
        Throwable primaryFailure = null;
        try {
            // Keep the old target assignment frozen so the source sender must observe the stopped
            // target before a replacement can become available.
            FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
            coordinatorStopped = true;
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(stoppedTargetLeader);
            targetLeaderStopped = true;

            // Submit the source write while the exact physical target is unavailable. Do not wait
            // for replacement leadership before the push enters the retry path.
            CompletableFuture<PutKvResponse> sourceWrite =
                    FLUSS_CLUSTER_EXTENSION
                            .newTabletServerClientForNode(mainLeader)
                            .putKv(
                                    newPutKvRequest(
                                            mainTableId,
                                            0,
                                            1,
                                            genKvRecordBatch(new Object[] {1, selectedValue})));

            // IndexBatch.attempts() increments only when a failed batch is put back into this
            // exact target bucket's deque. With the coordinator stopped, a replacement cannot
            // race this observation; the source SYNC future and pushed offset must remain pending.
            waitUntil(
                    () -> hasRetriedTargetBatch(sourceSendBuffer, idxTb),
                    TIMEOUT,
                    "wait for the selected target index batch to fail and requeue");
            assertThat(sourceWrite.isDone())
                    .as("source SYNC write must remain unresolved while the target is unavailable")
                    .isFalse();
            assertThat(mainReplica.getSyncIndexPushedOffset())
                    .as("sync progress must remain at the pre-write baseline during retry")
                    .isZero();
            assertThat(mainReplica.getAllIndexPushedOffset()).isZero();

            FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
            coordinatorStopped = false;
            waitUntil(
                    () -> FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxTb) != stoppedTargetLeader,
                    TIMEOUT,
                    "wait for the selected target index bucket to fail over");
            sourceWrite.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Verify the exact physical key on the replacement leader for the exact bucket that
            // was unavailable during the source write.
            waitUntil(
                    () -> {
                        try {
                            Replica idxReplica =
                                    FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(idxTb);
                            List<org.apache.fluss.utils.ByteArraySlice> result =
                                    idxReplica.lookups(Collections.singletonList(indexKey));
                            return result.get(0) != null;
                        } catch (Exception e) {
                            return false;
                        }
                    },
                    TIMEOUT,
                    "wait for index entry after selected target leader failover");
            Replica replacementTargetLeader =
                    FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(idxTb);
            assertThat(replacementTargetLeader.lookups(Collections.singletonList(indexKey)).get(0))
                    .as("exact physical index key must be present on the replacement target leader")
                    .isNotNull();

            // One source row at offset 0 advances the exact completed prefix to 1. An exact check
            // catches both a retry that never completed and an accidental skip past source WAL.
            waitUntil(
                    () -> mainReplica.getSyncIndexPushedOffset() == 1L,
                    TIMEOUT,
                    "pushed offset must equal the one-row source WAL end after retry");
            assertThat(mainReplica.getSyncIndexPushedOffset()).isEqualTo(1L);
        } catch (Throwable failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            if (coordinatorStopped) {
                cleanup(cleanupFailures, FLUSS_CLUSTER_EXTENSION::startCoordinatorServer);
            }
            if (targetLeaderStopped) {
                cleanup(
                        cleanupFailures,
                        () -> FLUSS_CLUSTER_EXTENSION.startTabletServer(stoppedTargetLeader));
            }
            cleanup(
                    cleanupFailures,
                    () -> FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(TABLET_SERVER_COUNT));
            reportCleanupFailures(primaryFailure, cleanupFailures);
        }
    }

    private static byte[] encodeIndexKey(IndexReplicationEncoder encoder, String b, int a) {
        return encoder.encodeDataEntry(GenericRow.of(a, fromString(b))).key();
    }

    private static int targetBucket(IndexReplicationEncoder encoder, String b, int a) {
        return encoder.encodeDataEntry(GenericRow.of(a, fromString(b))).targetBucket();
    }

    private static String valueForIndexBucket(
            IndexReplicationEncoder encoder, String prefix, int expectedBucket) {
        for (int suffix = 0; suffix < 100_000; suffix++) {
            String candidate = prefix + '-' + suffix;
            if (targetBucket(encoder, candidate, 1) == expectedBucket) {
                return candidate;
            }
        }
        throw new AssertionError("No value found for index bucket " + expectedBucket);
    }

    /** Read-only test observation of the retry state for one physical target. */
    private static boolean hasRetriedTargetBatch(IndexSendBuffer sendBuffer, TableBucket target) {
        return sendBuffer.hasRetriedBatchForTesting(target);
    }

    private static void cleanup(List<Throwable> failures, CleanupAction action) {
        try {
            action.run();
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            failures.add(failure);
        }
    }

    private static void reportCleanupFailures(
            Throwable primaryFailure, List<Throwable> cleanupFailures) throws Throwable {
        if (cleanupFailures.isEmpty()) {
            return;
        }
        AssertionError cleanupFailure =
                new AssertionError("index target failover IT cleanup failed");
        cleanupFailures.forEach(cleanupFailure::addSuppressed);
        if (primaryFailure != null) {
            primaryFailure.addSuppressed(cleanupFailure);
        } else {
            throw cleanupFailure;
        }
    }

    @FunctionalInterface
    private interface CleanupAction {
        void run() throws Throwable;
    }
}
