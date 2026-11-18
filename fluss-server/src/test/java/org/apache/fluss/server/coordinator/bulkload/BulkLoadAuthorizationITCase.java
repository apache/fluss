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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.DropAclsRequest;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.GetBulkLoadInfoRequest;
import org.apache.fluss.rpc.messages.ListInProgressBulkLoadsRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.types.DataTypes;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfos;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests table permissions and asynchronous metadata cleanup over real Coordinator RPCs. */
class BulkLoadAuthorizationITCase {
    private static final TablePath PATH = TablePath.of("bulkload_auth", "orders");
    private static final Duration WAIT = Duration.ofSeconds(30);
    private final FlussClusterExtension cluster =
            FlussClusterExtension.builder()
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(new Configuration().set(ConfigOptions.AUTHORIZER_ENABLED, true))
                    .build();
    private NettyClient client;
    private CoordinatorGateway external;
    private CoordinatorGateway internal;

    @BeforeEach
    void setUp() throws Exception {
        cluster.start();
        createTable(
                cluster,
                PATH,
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("k", DataTypes.INT())
                                        .primaryKey("k")
                                        .build())
                        .distributedBy(1, "k")
                        .build());
        client = new NettyClient(new Configuration(), TestingClientMetricGroup.newInstance());
        external =
                GatewayClientProxy.createGatewayProxy(
                        () -> cluster.getCoordinatorServerNode("CLIENT"),
                        client,
                        CoordinatorGateway.class);
        internal = cluster.newCoordinatorClient();
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
    void testWriteAndDescribePermissionsAreCheckedPerRequest() throws Exception {
        PbBulkLoadHandle handle =
                internal.beginBulkLoad(begin("internal-load")).get().getStatus().getHandle();
        GetBulkLoadInfoRequest info = new GetBulkLoadInfoRequest().setHandle(handle);
        ListInProgressBulkLoadsRequest list = new ListInProgressBulkLoadsRequest();
        list.setTablePath()
                .setDatabaseName(PATH.getDatabaseName())
                .setTableName(PATH.getTableName());
        AbortBulkLoadRequest abort = new AbortBulkLoadRequest().setHandle(handle);
        assertDenied(() -> external.beginBulkLoad(begin("external-load")).get());
        assertDenied(() -> external.getBulkLoadInfo(info).get());
        assertDenied(() -> external.listInProgressBulkLoads(list).get());

        acl(OperationType.DESCRIBE, true);
        assertThat(external.getBulkLoadInfo(info).get().getStatus().getState())
                .isEqualTo(BulkLoadTransaction.RUNNING);
        assertThat(external.listInProgressBulkLoads(list).get().getTransactionsCount())
                .isEqualTo(1);
        assertDenied(
                () ->
                        external.allocateBulkLoadBucketSnapshot(
                                        new AllocateBulkLoadBucketSnapshotRequest()
                                                .setHandle(handle)
                                                .setBucketId(0))
                                .get());
        assertDenied(
                () ->
                        external.commitBulkLoadBucketSnapshot(
                                        new CommitBulkLoadBucketSnapshotRequest()
                                                .setHandle(handle)
                                                .setBucketId(0))
                                .get());
        assertDenied(
                () -> external.commitBulkLoad(new CommitBulkLoadRequest().setHandle(handle)).get());
        assertDenied(() -> external.abortBulkLoad(abort).get());

        acl(OperationType.WRITE, true);
        assertThat(external.abortBulkLoad(abort).get().getStatus().getState())
                .isEqualTo(BulkLoadTransaction.ABORTED);
        acl(OperationType.WRITE, false);
        assertDenied(() -> external.abortBulkLoad(abort).get());
    }

    @Test
    void testDropCleansRunningMetadataWithoutWaitingForRetention() throws Exception {
        PbBulkLoadHandle handle =
                internal.beginBulkLoad(begin("load-1")).get().getStatus().getHandle();
        DropTableRequest request = new DropTableRequest().setIgnoreIfNotExists(false);
        request.setTablePath()
                .setDatabaseName(PATH.getDatabaseName())
                .setTableName(PATH.getTableName());
        internal.dropTable(request).get();
        retry(
                WAIT,
                () ->
                        assertThat(
                                        cluster.getZooKeeperClient()
                                                .getCuratorClient()
                                                .checkExists()
                                                .forPath(
                                                        BulkLoadMetadataStore.tablePath(
                                                                handle.getTableId())))
                                .isNull());
    }

    private static void assertDenied(ThrowingCallable call) {
        assertThatThrownBy(call).rootCause().isInstanceOf(AuthorizationException.class);
    }

    private void acl(OperationType operation, boolean allow) throws Exception {
        List<AclBinding> bindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.table(PATH),
                                new AccessControlEntry(
                                        FlussPrincipal.ANONYMOUS,
                                        "*",
                                        operation,
                                        PermissionType.ALLOW)));
        if (allow) {
            internal.createAcls(new CreateAclsRequest().addAllAcls(toPbAclInfos(bindings))).get();
        } else {
            DropAclsRequest request = new DropAclsRequest();
            request.addAclFilter()
                    .setResourceType(ResourceType.TABLE.getCode())
                    .setResourceName(PATH.toString())
                    .setPrincipalName(FlussPrincipal.ANONYMOUS.getName())
                    .setPrincipalType(FlussPrincipal.ANONYMOUS.getType())
                    .setHost("*")
                    .setOperationType(operation.getCode())
                    .setPermissionType(PermissionType.ALLOW.getCode());
            internal.dropAcls(request).get();
        }
        cluster.waitUntilAuthenticationSync(bindings, allow);
    }

    private static BeginBulkLoadRequest begin(String id) {
        BeginBulkLoadRequest request = new BeginBulkLoadRequest().setBulkLoadId(id);
        request.setTablePath()
                .setDatabaseName(PATH.getDatabaseName())
                .setTableName(PATH.getTableName());
        return request;
    }
}
