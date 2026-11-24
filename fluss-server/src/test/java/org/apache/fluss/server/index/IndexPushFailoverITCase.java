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
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableAssignment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.index.TestingIndexTable.INDEX_NAME;
import static org.apache.fluss.server.index.TestingIndexTable.indexEntry;
import static org.apache.fluss.server.index.TestingIndexTable.upsertRequest;
import static org.apache.fluss.server.index.TestingIndexTable.valueForIndexBucket;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests SYNC index replication when only the target bucket leader fails. */
class IndexPushFailoverITCase {

    private static final String DB = "test_failover_db";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int TABLET_SERVER_COUNT = 4;
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

    @Test
    void testIndexTableLeaderFailoverRetries() throws Throwable {
        TablePath mainPath = TablePath.of(DB, "main_idx_failover");

        Schema schema =
                TestingIndexTable.indexedSchema(
                        IndexVisibility.SYNC, TARGET_FAILOVER_INDEX_BUCKET_COUNT);
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
        assertThat(sourceBucketReplicas).hasSize(3).doesNotHaveDuplicates().contains(mainLeader);
        TableBucket idxTb = indexBucketOutsideSourceReplicas(indexTableId, sourceBucketReplicas);
        String selectedValue =
                valueForIndexBucket(indexEncoder, "target-failover", idxTb.getBucket());
        byte[] indexKey = indexEntry(indexEncoder, 1, selectedValue).key();
        int stoppedTargetLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxTb);
        assertThat(sourceBucketReplicas).doesNotContain(stoppedTargetLeader);

        IndexSendBuffer sourceSendBuffer =
                FLUSS_CLUSTER_EXTENSION
                        .getTabletServerById(mainLeader)
                        .getReplicaManager()
                        .getIndexReplicationManager()
                        .sendBuffer();
        // Keep the old target assignment frozen so the source sender must observe the stopped
        // target before a replacement can become available.
        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        CompletableFuture<PutKvResponse> sourceWrite;
        try {
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(stoppedTargetLeader);

            // Submit the source write while the exact physical target is unavailable. Do not wait
            // for replacement leadership before the push enters the retry path.
            sourceWrite =
                    FLUSS_CLUSTER_EXTENSION
                            .newTabletServerClientForNode(mainLeader)
                            .putKv(upsertRequest(mainBucket, 1, selectedValue));

            // IndexBatch.requeueCount() increments only when a failed batch is put back into this
            // exact target bucket's deque. With the coordinator stopped, a replacement cannot
            // race this observation; the source SYNC future and pushed offset must remain pending.
            waitUntil(
                    () -> sourceSendBuffer.hasRetriedBatchForTesting(idxTb),
                    TIMEOUT,
                    "wait for the selected target index batch to fail and requeue");
            assertThat(sourceWrite.isDone())
                    .as("source SYNC write must remain unresolved while the target is unavailable")
                    .isFalse();
            assertThat(mainReplica.getSyncIndexPushedOffset())
                    .as("sync progress must remain at the pre-write baseline during retry")
                    .isZero();
            assertThat(mainReplica.getAllIndexPushedOffset()).isZero();
        } finally {
            FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
        }
        waitUntil(
                () -> FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxTb) != stoppedTargetLeader,
                TIMEOUT,
                "wait for the selected target index bucket to fail over");
        PutKvResponse ack = sourceWrite.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(ack.getBucketsRespsList())
                .singleElement()
                .satisfies(bucket -> assertThat(bucket.hasErrorCode()).isFalse());

        Replica replacementTargetLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(idxTb);
        assertThat(replacementTargetLeader.lookups(Collections.singletonList(indexKey)).get(0))
                .as("exact physical index key must be present on the replacement target leader")
                .isNotNull();

        assertThat(mainReplica.getSyncIndexPushedOffset()).isEqualTo(1L);
    }

    private static TableBucket indexBucketOutsideSourceReplicas(
            long indexTableId, List<Integer> sourceReplicas) {
        for (int i = 0; i < TARGET_FAILOVER_INDEX_BUCKET_COUNT; i++) {
            TableBucket bucket = new TableBucket(indexTableId, i);
            if (!sourceReplicas.contains(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(bucket))) {
                return bucket;
            }
        }
        throw new AssertionError("No index leader outside source replicas " + sourceReplicas);
    }
}
