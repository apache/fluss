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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfos;
import static org.apache.fluss.security.acl.OperationType.ALTER;
import static org.apache.fluss.security.acl.OperationType.READ;
import static org.apache.fluss.security.acl.OperationType.WRITE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for source-table authorization of consumer-offset commits. */
class ConsumerOffsetCommitAuthorizationITCase {

    private static final String GROUP_ID = "commit-authorization";
    private static final Duration WAIT_TIMEOUT = Duration.ofMinutes(1);
    private static final TablePath FIRST_TABLE = TablePath.of("auth_db", "first_source");
    private static final TablePath SECOND_TABLE = TablePath.of("auth_db", "second_source");

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
    private TableBucket firstBucket;
    private TableBucket anotherFirstTableBucket;
    private TableBucket secondBucket;
    private TableBucket offsetTableBucket;

    @BeforeEach
    void setUp() throws Exception {
        cluster.start();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(2)
                        .build();
        long firstTableId = createTable(cluster, FIRST_TABLE, descriptor);
        long secondTableId = createTable(cluster, SECOND_TABLE, descriptor);
        firstBucket = new TableBucket(firstTableId, 0);
        anotherFirstTableBucket = new TableBucket(firstTableId, 1);
        secondBucket = new TableBucket(secondTableId, 0);
        cluster.waitUntilAllReplicaReady(firstBucket);
        cluster.waitUntilAllReplicaReady(secondBucket);

        ServerNode clientNode = cluster.getTabletServerNodes("CLIENT").get(0);
        // Use a separate client so the external endpoint cannot reuse an internal connection.
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
                                                        .setGroupId(GROUP_ID)))
                                .succeedsWithin(WAIT_TIMEOUT)
                                .extracting(response -> response.getOwner().getNodeId())
                                .isEqualTo(clientNode.id()));
        offsetTableBucket =
                new TableBucket(
                        cluster.getZooKeeperClient()
                                .getTable(SystemTables.CONSUMER_OFFSETS)
                                .get()
                                .tableId,
                        0);
        cluster.waitUntilAllReplicaReady(offsetTableBucket);
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
    void testSourceReadPermissionIsSufficient() throws Exception {
        grant(READ, Resource.table(FIRST_TABLE));
        grant(READ, Resource.table(SECOND_TABLE));

        assertSuccessfulCommit(
                clientGateway, 10L, firstBucket, anotherFirstTableBucket, secondBucket);
        assertStoredOffset(firstBucket, 10L);
        assertStoredOffset(anotherFirstTableBucket, 10L);
        assertStoredOffset(secondBucket, 10L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnauthorizedSourceRejectsEntireRequest(boolean hasClusterAlter) throws Exception {
        grant(READ, Resource.table(FIRST_TABLE));
        grant(WRITE, Resource.table(SECOND_TABLE));
        if (hasClusterAlter) {
            grant(ALTER, Resource.cluster());
        }
        assertSuccessfulCommit(internalGateway, 10L, firstBucket, secondBucket);
        assertStoredOffset(firstBucket, 10L);
        assertStoredOffset(secondBucket, 10L);

        assertThatThrownBy(
                        () ->
                                clientGateway
                                        .commitConsumerOffsets(
                                                newCommitRequest(20L, firstBucket, secondBucket))
                                        .get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining("READ")
                .hasMessageContaining(SECOND_TABLE.toString());
        assertStoredOffset(firstBucket, 10L);
        assertStoredOffset(secondBucket, 10L);
    }

    @Test
    void testUnknownSourceTableIsRejected() throws Exception {
        grant(READ, Resource.table(FIRST_TABLE));
        assertThatThrownBy(
                        () ->
                                clientGateway
                                        .commitConsumerOffsets(
                                                newCommitRequest(
                                                        10L,
                                                        firstBucket,
                                                        new TableBucket(Long.MAX_VALUE, 0)))
                                        .get())
                .rootCause()
                .isInstanceOf(UnknownTableOrBucketException.class);
    }

    @Test
    void testEmptyRequestRequiresNoTablePermissions() throws Exception {
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(GROUP_ID);
        assertThat(clientGateway.commitConsumerOffsets(request).get().getTablesList()).isEmpty();
        request.addTable().setTableId(Long.MAX_VALUE);
        assertThat(clientGateway.commitConsumerOffsets(request).get().getTablesList()).isEmpty();
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

    private static CommitConsumerOffsetsRequest newCommitRequest(
            long offset, TableBucket... buckets) {
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(GROUP_ID);
        for (TableBucket bucket : buckets) {
            request.addTable()
                    .setTableId(bucket.getTableId())
                    .addBucket()
                    .setBucketId(bucket.getBucket())
                    .setOffset(offset);
        }
        return request;
    }

    private static void assertSuccessfulCommit(
            TabletServerGateway gateway, long offset, TableBucket... buckets) throws Exception {
        CommitConsumerOffsetsResponse response =
                gateway.commitConsumerOffsets(newCommitRequest(offset, buckets)).get();
        List<TableBucket> committedBuckets = new ArrayList<>();
        for (PbCommitConsumerOffsetsResponseTable table : response.getTablesList()) {
            for (PbCommitConsumerOffsetsResponseBucket bucket : table.getBucketsList()) {
                assertThat(bucket.hasErrorCode()).isFalse();
                committedBuckets.add(new TableBucket(table.getTableId(), bucket.getBucketId()));
            }
        }
        assertThat(committedBuckets).containsExactlyInAnyOrder(buckets);
    }

    private void assertStoredOffset(TableBucket sourceBucket, long expectedOffset)
            throws Exception {
        byte[] key =
                new ConsumerOffsetSchema()
                        .encodeOffsetKey(
                                GROUP_ID,
                                sourceBucket.getTableId(),
                                sourceBucket.getPartitionId(),
                                sourceBucket.getBucket());
        ZooKeeperClient zkClient = cluster.getZooKeeperClient();
        ValueDecoder decoder =
                new ValueDecoder(
                        new TestingSchemaGetter(
                                zkClient.getCurrentSchemaId(SystemTables.CONSUMER_OFFSETS),
                                ConsumerOffsetSchema.getTableSchema()),
                        KvFormat.COMPACTED);
        retry(
                WAIT_TIMEOUT,
                () -> {
                    PbLookupRespForBucket result =
                            internalGateway
                                    .lookup(
                                            newLookupRequest(
                                                    offsetTableBucket.getTableId(),
                                                    offsetTableBucket.getBucket(),
                                                    key))
                                    .get()
                                    .getBucketsRespAt(0);
                    assertThat(result.hasErrorCode()).isFalse();
                    assertThat(result.getValuesCount()).isEqualTo(1);
                    assertThat(result.getValueAt(0).hasValues()).isTrue();
                    assertThat(decoder.decodeValue(result.getValueAt(0).getValues()).row.getLong(5))
                            .isEqualTo(expectedOffset);
                });
    }
}
