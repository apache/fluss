/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConsumerGroupOwnerLoadingException;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsRequestBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsRequestTable;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsRequestBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsRequestTable;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseTable;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for reading committed consumer offsets. */
class ConsumerOffsetFetchITCase {

    private static final Duration WAIT_TIMEOUT = Duration.ofMinutes(1);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS, 1)
                                    .set(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR, 1))
                    .build();

    @Test
    void testFetchAllAndSelectedOffsetsForNonPartitionedTable() throws Exception {
        String groupId = "fetch-non-partitioned-group";
        TablePath tablePath = TablePath.of("fetch_non_partitioned_db", "source_table");
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(3, "id")
                                .build());
        TableBucket committedBucket0 = new TableBucket(tableId, 0);
        TableBucket committedBucket1 = new TableBucket(tableId, 1);
        TableBucket missingBucket = new TableBucket(tableId, 2);

        TabletServerGateway ownerGateway = ownerGateway(groupId);
        commitOffset(ownerGateway, groupId, committedBucket0, 10L, null);
        commitOffset(ownerGateway, groupId, committedBucket1, 20L, "metadata");

        retry(
                WAIT_TIMEOUT,
                () ->
                        assertThat(fetchOffsets(ownerGateway, allOffsetsRequest(groupId)))
                                .containsOnlyKeys(committedBucket0, committedBucket1)
                                .containsEntry(
                                        committedBucket0, new ConsumerOffsetAndMetadata(10L, null))
                                .containsEntry(
                                        committedBucket1,
                                        new ConsumerOffsetAndMetadata(20L, "metadata")));

        retry(
                WAIT_TIMEOUT,
                () ->
                        assertThat(
                                        fetchOffsets(
                                                ownerGateway,
                                                selectedOffsetsRequest(
                                                        groupId, committedBucket0, missingBucket)))
                                .containsOnlyKeys(committedBucket0, missingBucket)
                                .containsEntry(
                                        committedBucket0, new ConsumerOffsetAndMetadata(10L, null))
                                .containsEntry(missingBucket, null));

        String emptyGroupId = "fetch-empty-group";
        retry(
                WAIT_TIMEOUT,
                () ->
                        assertThat(fetchOffsets(ownerGateway, allOffsetsRequest(emptyGroupId)))
                                .isEmpty());
    }

    @Test
    void testFetchOffsetsForPartitionedTable() throws Exception {
        String groupId = "fetch-partitioned-group";
        TablePath tablePath = TablePath.of("fetch_partitioned_db", "source_table");
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("region", DataTypes.STRING())
                                                .primaryKey("id", "region")
                                                .build())
                                .distributedBy(1, "id")
                                .partitionedBy("region")
                                .build());
        long partitionId =
                createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("region", "cn")),
                        false);
        TableBucket sourceBucket = new TableBucket(tableId, partitionId, 0);

        TabletServerGateway ownerGateway = ownerGateway(groupId);
        commitOffset(ownerGateway, groupId, sourceBucket, 30L, "partitioned");

        retry(
                WAIT_TIMEOUT,
                () ->
                        assertThat(fetchOffsets(ownerGateway, allOffsetsRequest(groupId)))
                                .containsOnlyKeys(sourceBucket)
                                .containsEntry(
                                        sourceBucket,
                                        new ConsumerOffsetAndMetadata(30L, "partitioned")));
    }

    private static TabletServerGateway ownerGateway(String groupId) throws Exception {
        int nodeId = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id();
        TabletServerGateway discoveryGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(nodeId);
        int ownerId = findOwner(discoveryGateway, groupId);

        TableRegistration offsetsTable =
                waitValue(
                        () ->
                                FLUSS_CLUSTER_EXTENSION
                                        .getZooKeeperClient()
                                        .getTable(SystemTables.CONSUMER_OFFSETS),
                        WAIT_TIMEOUT,
                        "Consumer offsets table was not created for group " + groupId);
        TableBucket offsetsBucket = new TableBucket(offsetsTable.tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(offsetsBucket);
        return FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(ownerId);
    }

    private static int findOwner(TabletServerGateway gateway, String groupId) {
        return waitValue(
                () -> {
                    try {
                        return Optional.of(
                                gateway.findConsumerGroupOwner(
                                                new FindConsumerGroupOwnerRequest()
                                                        .setGroupId(groupId))
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
                "Consumer group owner was not found for group " + groupId);
    }

    private static GetConsumerOffsetsRequest allOffsetsRequest(String groupId) {
        return new GetConsumerOffsetsRequest().setGroupId(groupId).setAllOffsets(true);
    }

    private static GetConsumerOffsetsRequest selectedOffsetsRequest(
            String groupId, TableBucket... buckets) {
        GetConsumerOffsetsRequest request =
                new GetConsumerOffsetsRequest().setGroupId(groupId).setAllOffsets(false);
        for (TableBucket bucket : buckets) {
            PbGetConsumerOffsetsRequestTable table = request.addTable();
            table.setTableId(bucket.getTableId());
            PbGetConsumerOffsetsRequestBucket requestBucket = table.addBucket();
            if (bucket.getPartitionId() != null) {
                requestBucket.setPartitionId(bucket.getPartitionId());
            }
            requestBucket.setBucketId(bucket.getBucket());
        }
        return request;
    }

    private static void commitOffset(
            TabletServerGateway gateway,
            String groupId,
            TableBucket sourceBucket,
            long offset,
            String metadata)
            throws Exception {
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(groupId);
        PbCommitConsumerOffsetsRequestTable table = request.addTable();
        table.setTableId(sourceBucket.getTableId());
        PbCommitConsumerOffsetsRequestBucket bucket = table.addBucket();
        if (sourceBucket.getPartitionId() != null) {
            bucket.setPartitionId(sourceBucket.getPartitionId());
        }
        bucket.setBucketId(sourceBucket.getBucket()).setOffset(offset);
        if (metadata != null) {
            bucket.setMetadata(metadata);
        }

        CommitConsumerOffsetsResponse response = gateway.commitConsumerOffsets(request).get();
        assertThat(response.getTablesList()).hasSize(1);
        assertThat(response.getTableAt(0).getBucketsList()).hasSize(1);
        PbCommitConsumerOffsetsResponseBucket responseBucket =
                response.getTableAt(0).getBucketAt(0);
        assertThat(responseBucket.hasErrorCode()).isFalse();
    }

    private static Map<TableBucket, ConsumerOffsetAndMetadata> fetchOffsets(
            TabletServerGateway gateway, GetConsumerOffsetsRequest request) throws Exception {
        GetConsumerOffsetsResponse response = gateway.getConsumerOffsets(request).get();
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new LinkedHashMap<>();
        for (PbGetConsumerOffsetsResponseTable table : response.getTablesList()) {
            for (PbGetConsumerOffsetsResponseBucket bucket : table.getBucketsList()) {
                TableBucket tableBucket =
                        new TableBucket(
                                table.getTableId(),
                                bucket.hasPartitionId() ? bucket.getPartitionId() : null,
                                bucket.getBucketId());
                ConsumerOffsetAndMetadata offsetAndMetadata =
                        bucket.hasOffset()
                                ? new ConsumerOffsetAndMetadata(
                                        bucket.getOffset(),
                                        bucket.hasMetadata() ? bucket.getMetadata() : null)
                                : null;
                offsets.put(tableBucket, offsetAndMetadata);
            }
        }
        return offsets;
    }
}
