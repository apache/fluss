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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests recovery of the permanent partition write fence during bucket leader failover. */
class FrozenPartitionLeaderRecoveryITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(2)
                    .setClusterConf(clusterConfiguration())
                    .build();

    @Test
    void testPromotedFollowerRestoresFrozenFenceFromPartitionRegistration() throws Exception {
        TablePath tablePath = TablePath.of("frozen_partition_recovery", "log_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 2)
                        .build();
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        long partitionId =
                RpcMessageTestUtils.createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("b", "2024")),
                        false);
        TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);

        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        LeaderAndIsr initialLeaderAndIsr =
                waitValue(
                        () -> {
                            Optional<LeaderAndIsr> leaderAndIsr =
                                    zooKeeperClient.getLeaderAndIsr(tableBucket);
                            if (leaderAndIsr.isPresent() && leaderAndIsr.get().isr().size() == 2) {
                                return leaderAndIsr;
                            }
                            return Optional.empty();
                        },
                        Duration.ofMinutes(1),
                        "Partition replicas did not join the ISR");
        int oldLeader = initialLeaderAndIsr.leader();

        assertThat(zooKeeperClient.markPartitionFrozen(tablePath, "2024", tableId, partitionId))
                .hasValueSatisfying(registration -> assertThat(registration.isFrozen()).isTrue());

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(oldLeader);
        try {
            LeaderAndIsr promotedLeaderAndIsr =
                    waitValue(
                            () -> {
                                Optional<LeaderAndIsr> leaderAndIsr =
                                        zooKeeperClient.getLeaderAndIsr(tableBucket);
                                if (leaderAndIsr.isPresent()
                                        && leaderAndIsr.get().leader() != LeaderAndIsr.NO_LEADER
                                        && leaderAndIsr.get().leader() != oldLeader) {
                                    return leaderAndIsr;
                                }
                                return Optional.empty();
                            },
                            Duration.ofMinutes(1),
                            "Frozen partition follower was not promoted");
            Replica promotedLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);

            assertThat(promotedLeaderAndIsr.leader()).isNotEqualTo(oldLeader);
            assertThatThrownBy(
                            () -> promotedLeader.appendRecordsToLeader(MemoryLogRecords.EMPTY, 1))
                    .isInstanceOf(InvalidPartitionException.class)
                    .hasMessageContaining("is frozen and no longer accepts writes");
        } finally {
            FLUSS_CLUSTER_EXTENSION.startTabletServer(oldLeader);
        }
    }

    @Test
    void testPromotedLeaderReportsAlreadyCommittedLakeDataWithoutAnotherCommit() throws Exception {
        TablePath path = TablePath.of("frozen_partition_recovery", "lake_table");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 2)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();
        long tableId = RpcMessageTestUtils.createTable(FLUSS_CLUSTER_EXTENSION, path, descriptor);
        long partitionId =
                RpcMessageTestUtils.createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        path,
                        new PartitionSpec(Collections.singletonMap("b", "2024")),
                        false);
        TableBucket bucket = new TableBucket(tableId, partitionId, 0);
        ZooKeeperClient zk = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        LeaderAndIsr initial =
                waitValue(
                        () -> zk.getLeaderAndIsr(bucket).filter(leader -> leader.isr().size() == 2),
                        Duration.ofMinutes(1),
                        "Replicas did not join ISR");
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(bucket);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        long endOffset = replica.getLogTablet().localLogEndOffset();
        waitUntil(
                () -> replica.getLogTablet().getHighWatermark() == endOffset,
                Duration.ofSeconds(30),
                "Records did not replicate");
        CommitLakeTableSnapshotRequest commit = new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo snapshot = commit.addTablesReq();
        snapshot.setTableId(tableId).setSnapshotId(1L);
        snapshot.addBucketsReq()
                .setPartitionId(partitionId)
                .setBucketId(0)
                .setLogEndOffset(endOffset)
                .setMaxTimestamp(0L);
        FLUSS_CLUSTER_EXTENSION.newCoordinatorClient().commitLakeTableSnapshot(commit).get();
        // Persist the fence without sending a freeze RPC to the current leader.
        // Only the promoted follower can restore it and report readiness.
        assertThat(zk.markPartitionFrozen(path, "2024", tableId, partitionId)).isPresent();
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(initial.leader());
        try {
            waitUntil(
                    () -> !zk.getPartition(path, "2024").isPresent(),
                    Duration.ofMinutes(1),
                    "Promoted leader did not report the frozen bucket as fully tiered");
        } finally {
            FLUSS_CLUSTER_EXTENSION.startTabletServer(initial.leader());
        }
    }

    private static Configuration clusterConfiguration() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        return conf;
    }
}
