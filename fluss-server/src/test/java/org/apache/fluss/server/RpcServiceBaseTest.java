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

package org.apache.fluss.server;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestData;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.messages.DescribeBucketsResponse;
import org.apache.fluss.rpc.messages.GetClusterHealthRequest;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;
import org.apache.fluss.rpc.messages.ListAclsRequest;
import org.apache.fluss.rpc.messages.ListAclsResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbAclInfo;
import org.apache.fluss.rpc.messages.PbBucketInfo;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.util.CommonRpcMessageUtils;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.authorizer.AclCreateResult;
import org.apache.fluss.server.authorizer.AclDeleteResult;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.CoordinatorMetadataProvider;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.TableMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.server.RpcServiceBase.validateKvSnapshotMetadataVersion;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RpcServiceBase}. */
class RpcServiceBaseTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testJointMetadataRejectsPartitionFromReplacedTable(boolean cached) {
        TableInfo table = tableInfo(KvValueLayout.TAGGED);
        TableInfo other = tableInfo(KvValueLayout.PLAIN);
        TablePath path = table.getTablePath();
        PhysicalTablePath partitionPath = PhysicalTablePath.of(path, "p");
        PartitionMetadata old =
                new PartitionMetadata(other.getTableId(), "p", 11, Collections.emptyList(), 1);
        CoordinatorMetadataProvider provider =
                new CoordinatorMetadataProvider(null, null, null) {
                    @Override
                    public Optional<TableMetadata> getTableMetadataFromCache(TablePath requested) {
                        return Optional.of(
                                new TableMetadata(
                                        requested.equals(path) ? table : other,
                                        Collections.emptyList()));
                    }

                    @Override
                    public List<TableMetadata> getTablesMetadataFromZK(
                            Collection<TablePath> unknown) {
                        assertThat(unknown).isEmpty();
                        return Collections.emptyList();
                    }

                    @Override
                    public Optional<PartitionMetadata> getPartitionMetadataFromCache(
                            PhysicalTablePath requested) {
                        return cached ? Optional.of(old) : Optional.empty();
                    }

                    @Override
                    public Map<PhysicalTablePath, PartitionMetadata> getPartitionsMetadataFromZK(
                            Collection<PhysicalTablePath> unknown) {
                        return unknown.contains(partitionPath)
                                ? Collections.singletonMap(partitionPath, old)
                                : Collections.emptyMap();
                    }
                };
        MetadataRequest request = new MetadataRequest();
        request.addTablePath()
                .setDatabaseName(path.getDatabaseName())
                .setTableName(path.getTableName());
        // Both IDs occur in the response. The partition must match its own parent by path.
        request.addTablePath()
                .setDatabaseName(other.getTablePath().getDatabaseName())
                .setTableName(other.getTablePath().getTableName());
        request.addPartitionsPath()
                .setDatabaseName(path.getDatabaseName())
                .setTableName(path.getTableName())
                .setPartitionName("p");
        assertThatThrownBy(
                        () ->
                                new TestingRpcService(null)
                                        .processMetadataRequest(
                                                request,
                                                "CLIENT",
                                                session((short) 1),
                                                null,
                                                new CoordinatorMetadataCache(),
                                                provider))
                .hasCauseInstanceOf(StaleMetadataException.class)
                .isInstanceOfSatisfying(
                        UnknownTableOrBucketException.class,
                        failure ->
                                assertThat(ApiError.fromThrowable(failure).exception())
                                        .isInstanceOf(UnknownTableOrBucketException.class));
    }

    @Test
    void testValidateKvSnapshotMetadataVersion() {
        TableInfo taggedTableInfo = tableInfo(KvValueLayout.TAGGED);
        TableInfo plainTableInfo = tableInfo(KvValueLayout.PLAIN);

        assertThatThrownBy(() -> validateKvSnapshotMetadataVersion((short) 0, taggedTableInfo))
                .isInstanceOf(UnsupportedVersionException.class);
        validateKvSnapshotMetadataVersion((short) 1, taggedTableInfo);
        validateKvSnapshotMetadataVersion((short) 0, plainTableInfo);
    }

    @Test
    void testAddBucketInfoNormalizesNoLeader() {
        DescribeBucketsResponse response = new DescribeBucketsResponse();
        BucketMetadata bucketMetadata =
                new BucketMetadata(0, NO_LEADER, 3, Arrays.asList(1, 2, 3), Arrays.asList(1, 2), 4);

        RpcServiceBase.addBucketInfo(response, null, null, bucketMetadata);

        PbBucketInfo bucketInfo = response.getBucketInfosList().get(0);
        assertThat(bucketInfo.hasLeaderId()).isFalse();
        assertThat(bucketInfo.hasLeaderEpoch()).isFalse();
        assertThat(bucketInfo.hasBucketEpoch()).isTrue();
        assertThat(bucketInfo.getBucketEpoch()).isEqualTo(4);
        assertThat(bucketInfo.getReplicaIds()).containsExactly(1, 2, 3);
        assertThat(bucketInfo.getIsrs()).containsExactly(1, 2);
    }

    @Test
    void testListAclsFiltersGroupAclsForVersionZero() throws Exception {
        assertThat(ApiKeys.LIST_ACLS.highestSupportedVersion).isEqualTo((short) 1);
        AclBinding databaseAcl = aclBinding(Resource.database("db1"));
        AclBinding groupAcl = aclBinding(Resource.group("group1"));
        TestingRpcService service =
                new TestingRpcService(new TestingAuthorizer(Arrays.asList(databaseAcl, groupAcl)));

        service.setCurrentSession(session((short) 0));
        ListAclsResponse versionZero =
                service.listAcls(
                                new ListAclsRequest()
                                        .setAclFilter(
                                                CommonRpcMessageUtils.toPbAclFilter(
                                                        AclBindingFilter.ANY)))
                        .join();
        assertThat(versionZero.getAclsList())
                .extracting(PbAclInfo::getResourceType)
                .containsExactly((int) ResourceType.DATABASE.getCode());

        service.setCurrentSession(session((short) 1));
        ListAclsResponse versionOne =
                service.listAcls(
                                new ListAclsRequest()
                                        .setAclFilter(
                                                CommonRpcMessageUtils.toPbAclFilter(
                                                        AclBindingFilter.ANY)))
                        .join();
        assertThat(versionOne.getAclsList())
                .extracting(PbAclInfo::getResourceType)
                .containsExactlyInAnyOrder(
                        (int) ResourceType.DATABASE.getCode(), (int) ResourceType.GROUP.getCode());
    }

    private static Session session(short apiVersion) {
        return new Session(
                apiVersion,
                "CLIENT",
                false,
                InetAddress.getLoopbackAddress(),
                FlussPrincipal.ANONYMOUS);
    }

    private static AclBinding aclBinding(Resource resource) {
        return new AclBinding(
                resource,
                new AccessControlEntry(
                        new FlussPrincipal("consumer", "User"),
                        "*",
                        OperationType.DESCRIBE,
                        PermissionType.ALLOW));
    }

    private static final class TestingRpcService extends RpcServiceBase {
        private TestingRpcService(Authorizer authorizer) {
            super(null, ServerType.COORDINATOR, null, null, authorizer, null, null);
        }

        @Override
        public void authorizeTable(OperationType operationType, long tableId) {}

        @Override
        protected TableInfo getTableInfo(long tableId) {
            return null;
        }

        @Override
        public String name() {
            return "test-rpc-service";
        }

        @Override
        public void shutdown() {}

        @Override
        public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
            return CompletableFuture.completedFuture(new MetadataResponse());
        }

        @Override
        public CompletableFuture<GetClusterHealthResponse> getClusterHealth(
                GetClusterHealthRequest request) {
            return CompletableFuture.completedFuture(new GetClusterHealthResponse());
        }
    }

    private static final class TestingAuthorizer implements Authorizer {
        private final Collection<AclBinding> acls;

        private TestingAuthorizer(Collection<AclBinding> acls) {
            this.acls = acls;
        }

        @Override
        public void startup() {}

        @Override
        public void close() {}

        @Override
        public boolean isAuthorized(
                Session session, OperationType operationType, Resource resource) {
            return true;
        }

        @Override
        public void authorize(Session session, OperationType operationType, Resource resource) {}

        @Override
        public Collection<Resource> filterByAuthorized(
                Session session, OperationType operation, java.util.List<Resource> resources) {
            return resources;
        }

        @Override
        public java.util.List<AclCreateResult> addAcls(
                Session session, java.util.List<AclBinding> aclBindings) {
            return Collections.emptyList();
        }

        @Override
        public java.util.List<AclDeleteResult> dropAcls(
                Session session, java.util.List<AclBindingFilter> filters) {
            return Collections.emptyList();
        }

        @Override
        public Collection<AclBinding> listAcls(Session session, AclBindingFilter filter) {
            return acls;
        }
    }

    private static TableInfo tableInfo(KvValueLayout layout) {
        Map<String, String> properties =
                new HashMap<>(TestData.DATA1_TABLE_DESCRIPTOR_PK.getProperties());
        properties.put(
                ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                String.valueOf(layout.version()));
        TableDescriptor descriptor = TestData.DATA1_TABLE_DESCRIPTOR_PK.withProperties(properties);
        return TableInfo.of(
                TablePath.of("db", "table_" + layout.version()),
                layout.version(),
                1,
                descriptor,
                TestData.DEFAULT_REMOTE_DATA_DIR,
                1L,
                1L);
    }
}
