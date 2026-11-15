/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF Software Foundation licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.metadata.GroupId;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.DeleteConsumerGroupsRequest;
import org.apache.fluss.rpc.messages.DeleteConsumerGroupsResponse;
import org.apache.fluss.rpc.messages.DeleteConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.DeleteConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.ListConsumerGroupsRequest;
import org.apache.fluss.rpc.messages.ListConsumerGroupsResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.messages.PbDeletedGroup;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.messages.PbListedGroup;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ByteArraySlice;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfos;
import static org.apache.fluss.security.acl.OperationType.DESCRIBE;
import static org.apache.fluss.security.acl.OperationType.DROP;
import static org.apache.fluss.security.acl.OperationType.READ;
import static org.apache.fluss.security.acl.OperationType.WRITE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.dropTable;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for consumer-group management authorization. */
class ConsumerGroupManagementAuthorizationITCase {

    private static final String FIRST_GROUP = "first-group";
    private static final String SECOND_GROUP = "second-group";
    private static final Duration WAIT_TIMEOUT = Duration.ofMinutes(1);
    private static final TablePath SOURCE_TABLE = TablePath.of("group_auth_db", "source");
    private static final TablePath SECOND_SOURCE_TABLE =
            TablePath.of("group_auth_db", "second_source");

    private final FlussClusterExtension cluster =
            FlussClusterExtension.builder()
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.AUTHORIZER_ENABLED, true)
                                    .set(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS, 1)
                                    .set(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR, 1))
                    .build();

    private NettyClient client;
    private TabletServerGateway clientGateway;
    private TabletServerGateway internalGateway;
    private ServerNode clientNode;
    private TableBucket sourceBucket;
    private TableBucket secondSourceBucket;
    private TableBucket offsetsTableBucket;

    @BeforeEach
    void setUp() throws Exception {
        cluster.start();

        TableDescriptor sourceDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build();
        long tableId = createTable(cluster, SOURCE_TABLE, sourceDescriptor);
        sourceBucket = new TableBucket(tableId, 0);
        cluster.waitUntilAllReplicaReady(sourceBucket);

        TableDescriptor secondSourceDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(2)
                        .build();
        long secondTableId = createTable(cluster, SECOND_SOURCE_TABLE, secondSourceDescriptor);
        secondSourceBucket = new TableBucket(secondTableId, 0);
        cluster.waitUntilAllReplicaReady(secondSourceBucket);
        cluster.waitUntilAllReplicaReady(new TableBucket(secondTableId, 1));

        clientNode = cluster.getTabletServerNodes("CLIENT").get(0);
        client = new NettyClient(new Configuration(), TestingClientMetricGroup.newInstance());
        clientGateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> clientNode, client, TabletServerGateway.class);
        internalGateway = cluster.newTabletServerClientForNode(clientNode.id());

        retry(
                WAIT_TIMEOUT,
                () ->
                        assertThat(
                                        internalGateway.findConsumerGroupOwner(
                                                new FindConsumerGroupOwnerRequest()
                                                        .setGroupId(FIRST_GROUP)))
                                .succeedsWithin(WAIT_TIMEOUT)
                                .extracting(response -> response.getOwner().getNodeId())
                                .isEqualTo(clientNode.id()));

        TableRegistration offsetsTable =
                waitValue(
                        () -> cluster.getZooKeeperClient().getTable(SystemTables.CONSUMER_OFFSETS),
                        WAIT_TIMEOUT,
                        "Consumer offsets table was not created");
        offsetsTableBucket = new TableBucket(offsetsTable.tableId, 0);
        cluster.waitUntilAllReplicaReady(offsetsTableBucket);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            cluster.close();
        }
    }

    @Test
    void testListConsumerGroupsUsesGroupLevelVisibility() throws Exception {
        commitOffset(FIRST_GROUP, 10L);
        commitOffset(SECOND_GROUP, 20L);

        assertThat(listGroups()).isEmpty();

        grant(DESCRIBE, Resource.group(FIRST_GROUP));
        assertThat(listGroups()).containsExactly(FIRST_GROUP);

        grant(DESCRIBE, Resource.cluster());
        assertThat(listGroups()).containsExactlyInAnyOrder(FIRST_GROUP, SECOND_GROUP);
    }

    @Test
    void testDeleteConsumerGroupsRemovesMetadataAndOffsets() throws Exception {
        commitOffset(FIRST_GROUP, 10L);
        grant(DROP, Resource.group(FIRST_GROUP));
        grant(DESCRIBE, Resource.group(FIRST_GROUP));
        grant(DESCRIBE, Resource.table(SOURCE_TABLE));

        clientGateway
                .deleteConsumerGroups(
                        new DeleteConsumerGroupsRequest()
                                .addAllGroups(Collections.singletonList(FIRST_GROUP)))
                .get();

        assertThat(listGroups()).isEmpty();
        assertThat(getAllOffsets(FIRST_GROUP)).isEmpty();

        // Deletion is not fenced; a later commit recreates the group.
        commitOffset(FIRST_GROUP, 20L);
        assertThat(listGroups()).containsExactly(FIRST_GROUP);
        assertThat(getAllOffsets(FIRST_GROUP)).containsExactly(20L);
    }

    @Test
    void testDeleteConsumerGroupsSucceedsForEmptyAndMissingGroups() throws Exception {
        commitOffset(FIRST_GROUP, 10L);
        grant(WRITE, Resource.group(FIRST_GROUP));
        grant(DROP, Resource.group(FIRST_GROUP));
        grant(DROP, Resource.group(SECOND_GROUP));

        deleteOffsets(sourceBucket);
        assertThat(lookupOffset(sourceBucket)).containsOnlyNulls();
        assertThat(listGroups()).containsExactly(FIRST_GROUP);

        DeleteConsumerGroupsResponse response =
                clientGateway
                        .deleteConsumerGroups(
                                new DeleteConsumerGroupsRequest()
                                        .addAllGroups(Arrays.asList(FIRST_GROUP, SECOND_GROUP)))
                        .get();

        assertThat(response.getGroupsList())
                .allSatisfy(group -> assertThat(group.hasErrorCode()).isFalse())
                .extracting(PbDeletedGroup::getGroupId)
                .containsExactlyInAnyOrder(FIRST_GROUP, SECOND_GROUP);
        assertThat(listGroups()).isEmpty();
    }

    @Test
    void testDeleteConsumerOffsetsRequiresOnlyGroupWrite() throws Exception {
        commitOffset(FIRST_GROUP, 10L);

        grant(READ, Resource.group(FIRST_GROUP));
        assertThatThrownBy(() -> clientGateway.deleteConsumerOffsets(deleteRequest()).get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining("WRITE");
        assertThat(lookupOffset(sourceBucket)).doesNotContainNull();

        grant(WRITE, Resource.group(FIRST_GROUP));
        deleteOffsets(sourceBucket);
        assertThat(lookupOffset(sourceBucket)).containsOnlyNulls();
    }

    @Test
    void testGroupDropDoesNotReplaceGroupWriteForOffsetDeletion() throws Exception {
        commitOffset(FIRST_GROUP, 10L);
        grant(DROP, Resource.group(FIRST_GROUP));

        assertThatThrownBy(() -> clientGateway.deleteConsumerOffsets(deleteRequest()).get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining("WRITE");
    }

    @Test
    void testGroupWriteWithoutDropDeletesOffsetsButNotGroup() throws Exception {
        commitOffset(FIRST_GROUP, 10L);
        grant(WRITE, Resource.group(FIRST_GROUP));

        deleteOffsets(sourceBucket);
        assertThat(lookupOffset(sourceBucket)).containsOnlyNulls();

        assertThatThrownBy(
                        () ->
                                clientGateway
                                        .deleteConsumerGroups(
                                                new DeleteConsumerGroupsRequest()
                                                        .addAllGroups(
                                                                Collections.singletonList(
                                                                        FIRST_GROUP)))
                                        .get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining("DROP");
    }

    @Test
    void testDeleteConsumerOffsetsTreatsMissingOffsetsAsNoOp() throws Exception {
        grant(WRITE, Resource.group(FIRST_GROUP));
        deleteOffsets(sourceBucket);
    }

    @Test
    void testDeletedSourceTableRequiresOnlyGroupWrite() throws Exception {
        commitOffset(FIRST_GROUP, 10L);
        grant(WRITE, Resource.group(FIRST_GROUP));

        dropTable(cluster, SOURCE_TABLE);
        retry(
                WAIT_TIMEOUT,
                () -> assertThat(cluster.getZooKeeperClient().tableExist(SOURCE_TABLE)).isFalse());

        assertThat(lookupOffset(sourceBucket)).doesNotContainNull();
        deleteOffsets(sourceBucket);
        assertThat(lookupOffset(sourceBucket)).containsOnlyNulls();
    }

    @Test
    void testDeleteConsumerOffsetsDoesNotRequireSourceTableMetadata() throws Exception {
        commitOffset(FIRST_GROUP, secondSourceBucket, 20L);
        grant(WRITE, Resource.group(FIRST_GROUP));

        TabletServerMetadataCache metadataCache =
                cluster.getTabletServerById(clientNode.id()).getMetadataCache();
        metadataCache.clearTableMetadata();

        assertThat(cluster.getZooKeeperClient().tableExist(SECOND_SOURCE_TABLE)).isTrue();
        assertThat(metadataCache.getTablePath(secondSourceBucket.getTableId())).isEmpty();
        assertThat(lookupOffset(secondSourceBucket)).doesNotContainNull();
        deleteOffsets(secondSourceBucket);
        assertThat(lookupOffset(secondSourceBucket)).containsOnlyNulls();
    }

    @Test
    void testDeleteConsumerGroupsRemovesOffsetsAcrossTablesAndBuckets() throws Exception {
        commitOffset(FIRST_GROUP, sourceBucket, 10L);
        commitOffset(FIRST_GROUP, secondSourceBucket, 20L);
        commitOffset(FIRST_GROUP, new TableBucket(secondSourceBucket.getTableId(), 1), 30L);

        grant(DROP, Resource.group(FIRST_GROUP));
        grant(DESCRIBE, Resource.group(FIRST_GROUP));
        grant(DESCRIBE, Resource.table(SOURCE_TABLE));
        grant(DESCRIBE, Resource.table(SECOND_SOURCE_TABLE));

        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = getAllOffsetsByBucket(FIRST_GROUP);
        assertThat(offsets).hasSize(3);
        assertThat(offsets.keySet())
                .containsExactlyInAnyOrder(
                        sourceBucket,
                        secondSourceBucket,
                        new TableBucket(secondSourceBucket.getTableId(), 1));

        clientGateway
                .deleteConsumerGroups(
                        new DeleteConsumerGroupsRequest()
                                .addAllGroups(Collections.singletonList(FIRST_GROUP)))
                .get();

        assertThat(listGroups()).isEmpty();
        assertThat(getAllOffsetsByBucket(FIRST_GROUP)).isEmpty();
    }

    @Test
    void testDeleteConsumerOffsetsForUnknownSourceTableIsNoOp() throws Exception {
        commitOffset(FIRST_GROUP, 10L);
        grant(WRITE, Resource.group(FIRST_GROUP));

        deleteOffsets(new TableBucket(Long.MAX_VALUE, 0));
        assertThat(lookupOffset(sourceBucket)).doesNotContainNull();
    }

    private void deleteOffsets(TableBucket tableBucket) throws Exception {
        DeleteConsumerOffsetsResponse response =
                clientGateway.deleteConsumerOffsets(deleteRequest(tableBucket)).get();
        assertThat(response.getTablesList()).hasSize(1);
        assertThat(response.getTableAt(0).getTableId()).isEqualTo(tableBucket.getTableId());
        assertThat(response.getTableAt(0).getBucketsList()).hasSize(1);
        assertThat(response.getTableAt(0).getBucketAt(0).getBucketId())
                .isEqualTo(tableBucket.getBucket());
        assertThat(response.getTableAt(0).getBucketAt(0).hasErrorCode()).isFalse();
    }

    private List<ByteArraySlice> lookupOffset(TableBucket tableBucket) throws Exception {
        // Read the stored key directly so source-table visibility cannot hide a failed deletion.
        byte[] key =
                new ConsumerOffsetSchema()
                        .encodeOffsetKey(
                                GroupId.of(FIRST_GROUP),
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                tableBucket.getBucket());
        return cluster.getTabletServerById(clientNode.id())
                .getReplicaManager()
                .getReplicaOrException(offsetsTableBucket)
                .lookups(Collections.singletonList(key));
    }

    private void commitOffset(String groupId, long offset) throws Exception {
        commitOffset(groupId, sourceBucket, offset);
    }

    private void commitOffset(String groupId, TableBucket sourceBucket, long offset)
            throws Exception {
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(groupId);
        request.addTable()
                .setTableId(sourceBucket.getTableId())
                .addBucket()
                .setBucketId(sourceBucket.getBucket())
                .setOffset(offset);

        CommitConsumerOffsetsResponse response =
                internalGateway.commitConsumerOffsets(request).get();
        assertThat(response.getTablesList()).hasSize(1);
        PbCommitConsumerOffsetsResponseTable table = response.getTableAt(0);
        assertThat(table.getBucketsList()).hasSize(1);
        PbCommitConsumerOffsetsResponseBucket bucket = table.getBucketAt(0);
        assertThat(bucket.hasErrorCode()).isFalse();
    }

    private List<String> listGroups() throws Exception {
        ListConsumerGroupsResponse response =
                clientGateway.listConsumerGroups(new ListConsumerGroupsRequest()).get();
        return response.getGroupsList().stream()
                .map(PbListedGroup::getGroupId)
                .collect(Collectors.toList());
    }

    private List<Long> getAllOffsets(String groupId) throws Exception {
        return new ArrayList<>(
                getAllOffsetsByBucket(groupId).values().stream()
                        .map(offset -> offset == null ? null : offset.getOffset())
                        .collect(Collectors.toList()));
    }

    private Map<TableBucket, ConsumerOffsetAndMetadata> getAllOffsetsByBucket(String groupId)
            throws Exception {
        GetConsumerOffsetsResponse response =
                clientGateway
                        .getConsumerOffsets(
                                new GetConsumerOffsetsRequest()
                                        .setGroupId(groupId)
                                        .setAllOffsets(true))
                        .get();
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new LinkedHashMap<>();
        for (PbGetConsumerOffsetsResponseTable table : response.getTablesList()) {
            for (PbGetConsumerOffsetsResponseBucket bucket : table.getBucketsList()) {
                TableBucket tableBucket =
                        new TableBucket(
                                table.getTableId(),
                                bucket.hasPartitionId() ? bucket.getPartitionId() : null,
                                bucket.getBucketId());
                offsets.put(
                        tableBucket,
                        bucket.hasOffset()
                                ? new ConsumerOffsetAndMetadata(
                                        bucket.getOffset(),
                                        bucket.hasMetadata() ? bucket.getMetadata() : null)
                                : null);
            }
        }
        return offsets;
    }

    private DeleteConsumerOffsetsRequest deleteRequest() {
        return deleteRequest(sourceBucket.getTableId());
    }

    private DeleteConsumerOffsetsRequest deleteRequest(long tableId) {
        return deleteRequest(new TableBucket(tableId, sourceBucket.getBucket()));
    }

    private DeleteConsumerOffsetsRequest deleteRequest(TableBucket tableBucket) {
        DeleteConsumerOffsetsRequest request =
                new DeleteConsumerOffsetsRequest().setGroupId(FIRST_GROUP);
        request.addTable()
                .setTableId(tableBucket.getTableId())
                .addBucket()
                .setBucketId(tableBucket.getBucket());
        if (tableBucket.getPartitionId() != null) {
            request.getTableAt(0).getBucketAt(0).setPartitionId(tableBucket.getPartitionId());
        }
        return request;
    }

    private void grant(OperationType operation, Resource resource) throws Exception {
        List<AclBinding> bindings =
                Collections.singletonList(
                        new AclBinding(
                                resource,
                                new AccessControlEntry(
                                        FlussPrincipal.ANONYMOUS,
                                        "*",
                                        operation,
                                        PermissionType.ALLOW)));
        cluster.newCoordinatorClient()
                .createAcls(new CreateAclsRequest().addAllAcls(toPbAclInfos(bindings)))
                .get();
        cluster.waitUntilAuthenticationSync(bindings, true);
    }
}
