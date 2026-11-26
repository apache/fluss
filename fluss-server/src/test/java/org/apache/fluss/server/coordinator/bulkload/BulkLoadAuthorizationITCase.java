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
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.GetBulkLoadInfoRequest;
import org.apache.fluss.rpc.messages.ListInProgressBulkLoadsRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfos;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies table permissions for BulkLoad RPCs. */
class BulkLoadAuthorizationITCase {
    private static final TablePath PATH = TablePath.of("bulkload_auth", "orders");

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(new Configuration().set(ConfigOptions.AUTHORIZER_ENABLED, true))
                    .build();

    @Test
    void testDescribeAndWritePermissions() throws Exception {
        long tableId = createTable(CLUSTER, PATH, DATA1_TABLE_DESCRIPTOR_PK.withBucketCount(1));
        CLUSTER.waitAndGetLeaderReplica(new TableBucket(tableId, 0));
        CoordinatorGateway internal = CLUSTER.newCoordinatorClient();
        CoordinatorGateway external = CLUSTER.newCoordinatorClient("CLIENT");
        PbBulkLoadHandle handle =
                internal.beginBulkLoad(begin("internal-load")).get().getStatus().getHandle();
        GetBulkLoadInfoRequest info = new GetBulkLoadInfoRequest().setHandle(handle);
        ListInProgressBulkLoadsRequest list = new ListInProgressBulkLoadsRequest();
        list.setTablePath()
                .setDatabaseName(PATH.getDatabaseName())
                .setTableName(PATH.getTableName());
        AbortBulkLoadRequest abort = new AbortBulkLoadRequest().setHandle(handle);
        assertDenied(() -> external.getBulkLoadInfo(info).get());
        assertDenied(() -> external.listInProgressBulkLoads(list).get());

        grant(internal, OperationType.DESCRIBE);
        assertThat(external.getBulkLoadInfo(info).get().getStatus().getState())
                .isEqualTo(BulkLoadTransaction.RUNNING);
        assertThat(external.listInProgressBulkLoads(list).get().getTransactionsCount())
                .isEqualTo(1);
        assertDenied(() -> external.beginBulkLoad(begin("external-load")).get());
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

        grant(internal, OperationType.WRITE);
        assertThat(external.abortBulkLoad(abort).get().getStatus().getState())
                .isEqualTo(BulkLoadTransaction.ABORTED);

        PbBulkLoadHandle unknownTableHandle =
                new PbBulkLoadHandle().setTableId(Long.MAX_VALUE).setBulkLoadId("internal-load");
        assertThatThrownBy(
                        () ->
                                external.getBulkLoadInfo(
                                                new GetBulkLoadInfoRequest()
                                                        .setHandle(unknownTableHandle))
                                        .get())
                .hasRootCauseInstanceOf(UnknownTableOrBucketException.class);
    }

    private static void assertDenied(ThrowingCallable call) {
        assertThatThrownBy(call).rootCause().isInstanceOf(AuthorizationException.class);
    }

    private static void grant(CoordinatorGateway coordinator, OperationType operation)
            throws Exception {
        List<AclBinding> bindings =
                Collections.singletonList(
                        new AclBinding(
                                Resource.table(PATH),
                                new AccessControlEntry(
                                        FlussPrincipal.ANONYMOUS,
                                        "*",
                                        operation,
                                        PermissionType.ALLOW)));
        coordinator.createAcls(new CreateAclsRequest().addAllAcls(toPbAclInfos(bindings))).get();
        CLUSTER.waitUntilAuthenticationSync(bindings, true);
    }

    private static BeginBulkLoadRequest begin(String id) {
        BeginBulkLoadRequest request = new BeginBulkLoadRequest().setBulkLoadId(id);
        request.setTablePath()
                .setDatabaseName(PATH.getDatabaseName())
                .setTableName(PATH.getTableName());
        return request;
    }
}
