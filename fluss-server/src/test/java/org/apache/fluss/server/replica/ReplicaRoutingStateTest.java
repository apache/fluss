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

package org.apache.fluss.server.replica;

import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for the routing state a {@link Replica} is armed with on leader activation and the request
 * validation driven by it.
 */
final class ReplicaRoutingStateTest extends ReplicaTestBase {

    /**
     * A bucket no other test class in this package uses. {@code TestingMetricGroups} caches bucket
     * metric groups per (table, bucket) in a static registry, so sharing a bucket coordinate hands
     * stale gauges across test classes.
     */
    private static final int TEST_BUCKET = 5;

    @Test
    void testRoutingBucketCountValidationAppliesOnlyToHashDistributedTables() throws Exception {
        TableBucket keylessTb = new TableBucket(DATA1_TABLE_ID, TEST_BUCKET);

        // A legacy coordinator's notification (no routing fields) fails leader activation loudly:
        // the upgrade contract requires the CoordinatorServer to be upgraded first.
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> legacyFuture =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        notifyDataWithRoutingState(
                                PhysicalTablePath.of(DATA1_TABLE_PATH), keylessTb, null, null)),
                legacyFuture::complete);
        assertThat(legacyFuture.get().get(0).getError().error())
                .isEqualTo(Errors.UNSUPPORTED_VERSION);
        assertThat(legacyFuture.get().get(0).getError().messageWithFallback())
                .contains("upgrade the CoordinatorServer first");
        assertThat(replicaManager.getReplicaOrException(keylessTb).isLeader()).isFalse();

        // DATA1 has no bucket key (round-robin/sticky distribution): which bucket a record lands in
        // carries no semantic meaning, so routing bucket count validation is skipped for it. Even a
        // mismatched count, and even after its metadata epoch advances, must not fail the write.
        makeLeaderWithRoutingState(PhysicalTablePath.of(DATA1_TABLE_PATH), keylessTb, 3, 0L);
        assertThat(replicaManager.getReplicaOrException(keylessTb).isLeader()).isTrue();
        replicaManager.validateRoutingBucketCount(keylessTb, 3);
        replicaManager.validateRoutingBucketCount(keylessTb, 4);
        replicaManager.validateRoutingBucketCount(keylessTb, 0);
        replicaManager.maybeUpdateMetadataCache(
                INITIAL_COORDINATOR_EPOCH,
                new ClusterMetadata(
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(
                                new TableMetadata(
                                        TableInfo.of(
                                                DATA1_TABLE_PATH,
                                                DATA1_TABLE_ID,
                                                1,
                                                DATA1_TABLE_DESCRIPTOR,
                                                DEFAULT_REMOTE_DATA_DIR,
                                                1L,
                                                1L,
                                                1L),
                                        Collections.emptyList())),
                        Collections.emptyList()));
        replicaManager.validateRoutingBucketCount(keylessTb, 4);
        replicaManager.validateRoutingBucketCount(keylessTb, 0);

        // DATA2 is DISTRIBUTED BY (a): a hash-distributed table where a stale count would misroute
        // the key, so its routing bucket count IS validated.
        TableBucket keyedTb = new TableBucket(DATA2_TABLE_ID, TEST_BUCKET);
        makeLeaderWithRoutingState(PhysicalTablePath.of(DATA2_TABLE_PATH), keyedTb, 3, 0L);
        assertThat(replicaManager.getReplicaOrException(keyedTb).isLeader()).isTrue();
        replicaManager.validateRoutingBucketCount(keyedTb, 3);
        assertThatThrownBy(() -> replicaManager.validateRoutingBucketCount(keyedTb, 4))
                .isInstanceOf(StaleMetadataException.class);

        // A legacy client (no count) passes on a non-rescaled hash table...
        replicaManager.validateRoutingBucketCount(keyedTb, 0);
        // ...but is rejected once an ALTER advances the metadata cache to epoch 1 through
        // UpdateMetadata, which does not re-notify the already active replica.
        replicaManager.maybeUpdateMetadataCache(
                INITIAL_COORDINATOR_EPOCH,
                new ClusterMetadata(
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(
                                new TableMetadata(
                                        TableInfo.of(
                                                DATA2_TABLE_PATH,
                                                DATA2_TABLE_ID,
                                                1,
                                                DATA2_TABLE_DESCRIPTOR,
                                                DEFAULT_REMOTE_DATA_DIR,
                                                1L,
                                                1L,
                                                1L),
                                        Collections.emptyList())),
                        Collections.emptyList()));
        assertThat(replicaManager.getReplicaOrException(keyedTb).getBucketCountEpoch())
                .isEqualTo(0L);
        assertThatThrownBy(() -> replicaManager.validateRoutingBucketCount(keyedTb, 0))
                .isInstanceOf(StaleMetadataException.class);

        // An unknown bucket keeps the downstream per-bucket error semantics: validation passes.
        replicaManager.validateRoutingBucketCount(new TableBucket(DATA2_TABLE_ID, 99), 3);
    }

    private void makeLeaderWithRoutingState(
            PhysicalTablePath physicalTablePath,
            TableBucket tb,
            Integer bucketCount,
            Long bucketCountEpoch)
            throws Exception {
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        notifyDataWithRoutingState(
                                physicalTablePath, tb, bucketCount, bucketCountEpoch)),
                future::complete);
        assertThat(future.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tb));
    }

    private static NotifyLeaderAndIsrData notifyDataWithRoutingState(
            PhysicalTablePath physicalTablePath,
            TableBucket tb,
            Integer bucketCount,
            Long bucketCountEpoch) {
        return new NotifyLeaderAndIsrData(
                physicalTablePath,
                tb,
                Collections.singletonList(TABLET_SERVER_ID),
                new LeaderAndIsr(
                        TABLET_SERVER_ID,
                        INITIAL_LEADER_EPOCH,
                        Collections.singletonList(TABLET_SERVER_ID),
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        INITIAL_BUCKET_EPOCH),
                bucketCount,
                bucketCountEpoch);
    }
}
