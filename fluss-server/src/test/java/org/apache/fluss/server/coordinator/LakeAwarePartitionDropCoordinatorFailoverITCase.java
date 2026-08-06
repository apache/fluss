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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests recovery of an in-progress lake-aware partition drop after Coordinator failover. */
class LakeAwarePartitionDropCoordinatorFailoverITCase {

    private static final DateTimeFormatter PARTITION_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMddHH");

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfiguration())
                    .build();

    @Test
    void testCoordinatorFailoverReacquiresFrozenOffsetsAndFinishesDrop() throws Exception {
        TablePath tablePath = TablePath.of("lake_aware_drop_recovery", "log_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "b")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.HOUR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 0)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 24)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_DROP_ENSURE_TIERED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        String expiredPartition =
                PARTITION_FORMAT.format(ZonedDateTime.now(ZoneOffset.UTC).minusHours(2));
        long partitionId =
                RpcMessageTestUtils.createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("b", expiredPartition)),
                        false);
        TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
        leaderReplica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        long frozenOffset = leaderReplica.getLogTablet().localLogEndOffset();
        assertThat(frozenOffset).isPositive();
        waitUntil(
                () -> leaderReplica.getLogTablet().getHighWatermark() == frozenOffset,
                Duration.ofSeconds(30),
                "Partition high watermark did not reach its log end offset");

        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        coordinatorGateway
                .alterTable(
                        RpcMessageTestUtils.newAlterTableRequest(
                                tablePath,
                                Collections.singletonMap(
                                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(),
                                        "0"),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();

        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        PartitionRegistration frozenRegistration =
                waitValue(
                        () -> {
                            Optional<PartitionRegistration> registration =
                                    zooKeeperClient.getPartition(tablePath, expiredPartition);
                            return registration.isPresent() && registration.get().isFrozen()
                                    ? registration
                                    : Optional.empty();
                        },
                        Duration.ofMinutes(1),
                        "Expired partition was not frozen");
        assertThat(frozenRegistration.getPartitionId()).isEqualTo(partitionId);
        assertThat(zooKeeperClient.getLakeTableSnapshot(tableId, null)).isEmpty();

        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();

        commitLakeSnapshot(tableId, tableBucket, 1L, frozenOffset - 1);
        waitUntilLakeOffset(tableId, tableBucket, frozenOffset - 1);

        // Let the recovered AutoPartitionManager run four 500 ms checks. The new drop manager must
        // reacquire the frozen offset from the replica rather than treating the durable flag as an
        // offset boundary, so an offset below it must not delete the partition.
        Thread.sleep(Duration.ofSeconds(2).toMillis());
        assertThat(zooKeeperClient.getPartition(tablePath, expiredPartition)).isPresent();

        commitLakeSnapshot(tableId, tableBucket, 2L, frozenOffset);
        waitUntilLakeOffset(tableId, tableBucket, frozenOffset);
        waitUntil(
                () -> !zooKeeperClient.getPartition(tablePath, expiredPartition).isPresent(),
                Duration.ofMinutes(1),
                "Recovered lake-aware drop did not delete the fully tiered partition");
    }

    private static Configuration clusterConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        configuration.set(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL, Duration.ofMillis(500));
        return configuration;
    }

    private static void commitLakeSnapshot(
            long tableId, TableBucket tableBucket, long snapshotId, long logEndOffset)
            throws Exception {
        CommitLakeTableSnapshotRequest request = new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo tableSnapshot = request.addTablesReq();
        tableSnapshot.setTableId(tableId).setSnapshotId(snapshotId);
        PbLakeTableOffsetForBucket bucketOffset = tableSnapshot.addBucketsReq();
        bucketOffset
                .setPartitionId(tableBucket.getPartitionId())
                .setBucketId(tableBucket.getBucket())
                .setLogEndOffset(logEndOffset)
                .setMaxTimestamp(0L);

        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        coordinatorGateway.commitLakeTableSnapshot(request).get();
    }

    private static void waitUntilLakeOffset(
            long tableId, TableBucket tableBucket, long expectedOffset) {
        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        waitUntil(
                () -> {
                    Optional<LakeTableSnapshot> snapshot =
                            zooKeeperClient.getLakeTableSnapshot(tableId, null);
                    return snapshot.isPresent()
                            && snapshot.get().getLogEndOffset(tableBucket).orElse(-1L)
                                    == expectedOffset;
                },
                Duration.ofMinutes(1),
                "Lake snapshot offset was not committed");
    }
}
