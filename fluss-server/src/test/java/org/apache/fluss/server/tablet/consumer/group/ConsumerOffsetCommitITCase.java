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

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConsumerGroupOwnerLoadingException;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for consumer-offset commits across owner changes. */
class ConsumerOffsetCommitITCase {

    private static final String GROUP_ID = "group-with-leader-change";
    private static final Duration WAIT_TIMEOUT = Duration.ofMinutes(1);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS, 1)
                                    .set(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR, 3))
                    .build();

    @Test
    void testCommitConsumerOffsetsAfterLeaderChange() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TablePath.of("test_db", "consumer_offset_source"),
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(1, "id")
                                .build());
        TableBucket sourceBucket = new TableBucket(tableId, 0);
        TabletServerGateway discoveryGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                        FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id());
        int oldOwner = findOwner(discoveryGateway);

        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        TableRegistration offsetTable = zkClient.getTable(SystemTables.CONSUMER_OFFSETS).get();
        assertThat(offsetTable.bucketCount).isEqualTo(1);
        TableBucket offsetTableBucket = new TableBucket(offsetTable.tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(offsetTableBucket);
        List<Integer> replicas =
                zkClient.getTableAssignment(offsetTable.tableId)
                        .get()
                        .getBucketAssignment(0)
                        .getReplicas();
        assertThat(replicas).hasSize(3).contains(oldOwner);
        retry(
                WAIT_TIMEOUT,
                () ->
                        assertThat(zkClient.getLeaderAndIsr(offsetTableBucket).get().isr())
                                .containsExactlyInAnyOrderElementsOf(replicas));

        TabletServerGateway oldGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(oldOwner);
        assertCommitResult(oldGateway, sourceBucket, 10L, "before-leader-change", Errors.NONE);
        assertStoredOffset(
                oldGateway, offsetTableBucket, sourceBucket, 10L, "before-leader-change");

        int newOwner = replicas.stream().filter(id -> id != oldOwner).findFirst().get();
        transferLeader(offsetTableBucket, oldOwner, newOwner, replicas);
        retry(
                WAIT_TIMEOUT,
                () ->
                        assertThat(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(offsetTableBucket))
                                .isEqualTo(newOwner));
        FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(offsetTableBucket, oldOwner);

        // The old owner stays online and must reject the request instead of accepting a write.
        assertCommitResult(
                oldGateway, sourceBucket, 999L, "rejected", Errors.NOT_CONSUMER_GROUP_OWNER);
        retry(WAIT_TIMEOUT, () -> assertThat(findOwner(oldGateway)).isEqualTo(newOwner));
        TabletServerGateway newGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(newOwner);

        // Read the old value before overwriting it, proving the new leader recovered the commit.
        assertStoredOffset(
                newGateway, offsetTableBucket, sourceBucket, 10L, "before-leader-change");
        assertCommitResult(newGateway, sourceBucket, 20L, "after-leader-change", Errors.NONE);
        assertStoredOffset(newGateway, offsetTableBucket, sourceBucket, 20L, "after-leader-change");
    }

    private static int findOwner(TabletServerGateway gateway) {
        return waitValue(
                () -> {
                    try {
                        return Optional.of(
                                gateway.findConsumerGroupOwner(
                                                new FindConsumerGroupOwnerRequest()
                                                        .setGroupId(GROUP_ID))
                                        .get()
                                        .getOwner()
                                        .getNodeId());
                    } catch (ExecutionException e) {
                        Throwable cause = ExceptionUtils.stripExecutionException(e);
                        if (cause instanceof ConsumerGroupOwnerNotAvailableException
                                || cause instanceof ConsumerGroupOwnerLoadingException) {
                            return Optional.empty();
                        }
                        throw e;
                    }
                },
                WAIT_TIMEOUT,
                "Consumer group owner did not become available.");
    }

    private static void transferLeader(
            TableBucket tableBucket, int oldOwner, int newOwner, List<Integer> replicas)
            throws Exception {
        List<Integer> newReplicas = new ArrayList<>(replicas);
        newReplicas.remove(Integer.valueOf(newOwner));
        newReplicas.add(0, newOwner);
        Map<TableBucket, RebalancePlanForBucket> plan =
                Collections.singletonMap(
                        tableBucket,
                        new RebalancePlanForBucket(
                                tableBucket, oldOwner, newOwner, replicas, newReplicas));
        String rebalanceId = "consumer-offset-leader-change";
        FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .registerRebalanceTask(
                        new RebalanceTask(rebalanceId, RebalanceStatus.NOT_STARTED, plan));
        AccessContextEvent<Void> event =
                new AccessContextEvent<>(
                        context -> {
                            FLUSS_CLUSTER_EXTENSION
                                    .getRebalanceManager()
                                    .registerRebalance(
                                            rebalanceId, plan, RebalanceStatus.NOT_STARTED);
                            return null;
                        });
        FLUSS_CLUSTER_EXTENSION
                .getCoordinatorServer()
                .getCoordinatorEventProcessor()
                .getCoordinatorEventManager()
                .put(event);
        event.getResultFuture().get();
    }

    private static void assertCommitResult(
            TabletServerGateway gateway,
            TableBucket sourceBucket,
            long offset,
            String metadata,
            Errors expectedError)
            throws Exception {
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(GROUP_ID);
        request.addTable()
                .setTableId(sourceBucket.getTableId())
                .addBucket()
                .setBucketId(sourceBucket.getBucket())
                .setOffset(offset)
                .setMetadata(metadata);
        CommitConsumerOffsetsResponse response = gateway.commitConsumerOffsets(request).get();
        assertThat(response.getTablesList()).hasSize(1);
        assertThat(response.getTableAt(0).getTableId()).isEqualTo(sourceBucket.getTableId());
        assertThat(response.getTableAt(0).getBucketsList()).hasSize(1);
        PbCommitConsumerOffsetsResponseBucket bucket = response.getTableAt(0).getBucketAt(0);
        assertThat(bucket.getBucketId()).isEqualTo(sourceBucket.getBucket());
        assertThat(bucket.hasPartitionId()).isFalse();
        if (expectedError == Errors.NONE) {
            assertThat(bucket.hasErrorCode()).isFalse();
        } else {
            assertThat(bucket.getErrorCode()).isEqualTo(expectedError.code());
        }
    }

    private static void assertStoredOffset(
            TabletServerGateway gateway,
            TableBucket offsetTableBucket,
            TableBucket sourceBucket,
            long expectedOffset,
            String expectedMetadata)
            throws Exception {
        byte[] key =
                ConsumerOffsetSchema.encodeOffsetKey(
                        ConsumerOffsetSchema.createOffsetKeyEncoder(),
                        GROUP_ID,
                        sourceBucket.getTableId(),
                        sourceBucket.getPartitionId(),
                        sourceBucket.getBucket());
        int schemaId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getCurrentSchemaId(SystemTables.CONSUMER_OFFSETS);
        ValueDecoder decoder =
                new ValueDecoder(
                        new TestingSchemaGetter(schemaId, ConsumerOffsetSchema.getTableSchema()),
                        KvFormat.COMPACTED);
        // KV flushing is asynchronous, so wait for the committed value to become visible.
        retry(
                WAIT_TIMEOUT,
                () -> {
                    LookupResponse response =
                            gateway.lookup(
                                            newLookupRequest(
                                                    offsetTableBucket.getTableId(),
                                                    offsetTableBucket.getBucket(),
                                                    key))
                                    .get();
                    assertThat(response.getBucketsRespsCount()).isEqualTo(1);
                    PbLookupRespForBucket bucket = response.getBucketsRespAt(0);
                    assertThat(bucket.hasErrorCode()).isFalse();
                    assertThat(bucket.getValuesCount()).isEqualTo(1);
                    assertThat(bucket.getValueAt(0).hasValues()).isTrue();
                    InternalRow row = decoder.decodeValue(bucket.getValueAt(0).getValues()).row;
                    assertThat(row.getString(0).toString()).isEqualTo(GROUP_ID);
                    assertThat(row.getInt(1))
                            .isEqualTo(ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE);
                    assertThat(row.getLong(2)).isEqualTo(sourceBucket.getTableId());
                    assertThat(row.getLong(3))
                            .isEqualTo(ConsumerOffsetSchema.NON_PARTITIONED_PARTITION_ID);
                    assertThat(row.getInt(4)).isEqualTo(sourceBucket.getBucket());
                    assertThat(row.getLong(5)).isEqualTo(expectedOffset);
                    assertThat(row.getString(6).toString()).isEqualTo(expectedMetadata);
                });
    }
}
