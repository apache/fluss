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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsRequestBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsRequestTable;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseTable;
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
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfos;
import static org.apache.fluss.security.acl.OperationType.DESCRIBE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for source-table authorization of consumer-offset fetches. */
class ConsumerOffsetFetchAuthorizationITCase {

    private static final String GROUP_ID = "fetch-authorization";
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
    private TableBucket secondBucket;

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

        TableRegistration offsetsTable =
                waitValue(
                        () -> cluster.getZooKeeperClient().getTable(SystemTables.CONSUMER_OFFSETS),
                        WAIT_TIMEOUT,
                        "Consumer offsets table was not created for group " + GROUP_ID);
        cluster.waitUntilAllReplicaReady(new TableBucket(offsetsTable.tableId, 0));
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
    void testSelectedFetchRequiresSourceDescribePermission() throws Exception {
        grant(DESCRIBE, Resource.table(FIRST_TABLE));
        commitOffset(internalGateway, firstBucket, 10L);
        commitOffset(internalGateway, secondBucket, 20L);

        assertThatThrownBy(
                        () ->
                                clientGateway
                                        .getConsumerOffsets(
                                                selectedOffsetsRequest(firstBucket, secondBucket))
                                        .get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining("DESCRIBE")
                .hasMessageContaining(SECOND_TABLE.toString());

        assertThat(fetchOffsets(clientGateway, selectedOffsetsRequest(firstBucket)))
                .containsOnlyKeys(firstBucket)
                .containsEntry(firstBucket, new ConsumerOffsetAndMetadata(10L, null));
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

    private static GetConsumerOffsetsRequest selectedOffsetsRequest(TableBucket... buckets) {
        GetConsumerOffsetsRequest request =
                new GetConsumerOffsetsRequest().setGroupId(GROUP_ID).setAllOffsets(false);
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
            TabletServerGateway gateway, TableBucket sourceBucket, long offset) throws Exception {
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(GROUP_ID);
        request.addTable()
                .setTableId(sourceBucket.getTableId())
                .addBucket()
                .setBucketId(sourceBucket.getBucket())
                .setOffset(offset);

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
}
