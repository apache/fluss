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
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.DropAclsRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.PbAclInfo;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AccessControlEntryFilter;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceFilter;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclBindingFilters;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toPbAclInfos;
import static org.apache.fluss.security.acl.OperationType.ALL;
import static org.apache.fluss.security.acl.OperationType.DESCRIBE;
import static org.apache.fluss.security.acl.OperationType.READ;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for consumer-group authorization. */
class ConsumerGroupAuthorizationITCase {

    private static final String GROUP_ID = "default-consumer";
    private static final Duration WAIT_TIMEOUT = Duration.ofMinutes(1);

    private FlussClusterExtension cluster;
    private NettyClient client;
    private TabletServerGateway clientGateway;
    private ServerNode clientNode;

    private void startCluster(boolean bypassGroupAcls) throws Exception {
        cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(1)
                        .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                        .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                        .setClusterConf(
                                new Configuration()
                                        .set(ConfigOptions.AUTHORIZER_ENABLED, true)
                                        .set(
                                                ConfigOptions.AUTHORIZER_GROUP_ACL_BYPASS_ENABLED,
                                                bypassGroupAcls)
                                        .set(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS, 1)
                                        .set(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR, 1))
                        .build();
        cluster.start();
        clientNode = cluster.getTabletServerNodes("CLIENT").get(0);
        // Keep external requests from reusing the cluster's internal-listener connections.
        client = new NettyClient(new Configuration(), TestingClientMetricGroup.newInstance());
        clientGateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> clientNode, client, TabletServerGateway.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }
    }

    @Test
    void testFindOwnerRequiresGroupDescribeBeforeInitializingSystemTable() throws Exception {
        startCluster(false);
        assertOwnerDiscoveryDenied(GROUP_ID);
        grant(ALL, Resource.cluster());
        grant(ALL, Resource.table("default", "consumer"));
        grant(DESCRIBE, Resource.group("other-group"));

        assertOwnerDiscoveryDenied(GROUP_ID);
        assertThat(cluster.getZooKeeperClient().tableExist(SystemTables.CONSUMER_OFFSETS))
                .isFalse();
    }

    @ParameterizedTest
    @EnumSource(
            value = OperationType.class,
            names = {"DESCRIBE", "READ", "WRITE"})
    void testFindOwnerWithGroupPermission(OperationType operation) throws Exception {
        startCluster(false);
        List<AclBinding> bindings = grant(operation, Resource.group(GROUP_ID));
        assertThat(cluster.getZooKeeperClient().tableExist(SystemTables.CONSUMER_OFFSETS))
                .isFalse();

        assertOwnerDiscoveryAllowed();
        assertOwnerDiscoveryDenied("other-group");

        AclBindingFilter filter =
                new AclBindingFilter(
                        new ResourceFilter(ResourceType.GROUP, GROUP_ID),
                        AccessControlEntryFilter.ANY);
        cluster.newCoordinatorClient()
                .dropAcls(
                        new DropAclsRequest()
                                .addAllAclFilters(
                                        toPbAclBindingFilters(Collections.singletonList(filter))))
                .get();
        cluster.waitUntilAuthenticationSync(bindings, false);
        assertOwnerDiscoveryDenied(GROUP_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInvalidGroupNamesAreRejectedByRpc(boolean bypassGroupAcls) throws Exception {
        startCluster(bypassGroupAcls);
        for (String groupId :
                Arrays.asList(
                        "",
                        "default.consumer",
                        "default/consumer",
                        "default:consumer",
                        "消费组",
                        "__internal",
                        "default-*",
                        StringUtils.repeat("a", 201))) {
            assertThatThrownBy(
                            () ->
                                    clientGateway
                                            .findConsumerGroupOwner(
                                                    new FindConsumerGroupOwnerRequest()
                                                            .setGroupId(groupId))
                                            .get())
                    .rootCause()
                    .isInstanceOf(InvalidConsumerGroupIdException.class);

            PbAclInfo acl =
                    new PbAclInfo()
                            .setResourceType(ResourceType.GROUP.getCode())
                            .setResourceName(groupId)
                            .setPrincipalName(FlussPrincipal.ANONYMOUS.getName())
                            .setPrincipalType(FlussPrincipal.ANONYMOUS.getType())
                            .setOperationType(DESCRIBE.getCode())
                            .setPermissionType(PermissionType.ALLOW.getCode())
                            .setHost("*");
            assertThatThrownBy(
                            () ->
                                    cluster.newCoordinatorClient()
                                            .createAcls(
                                                    new CreateAclsRequest()
                                                            .addAllAcls(
                                                                    Collections.singletonList(acl)))
                                            .get())
                    .rootCause()
                    .isInstanceOf(InvalidConsumerGroupIdException.class);
        }
        assertThat(cluster.getZooKeeperClient().listResourcesByType(ResourceType.GROUP)).isEmpty();

        grant(DESCRIBE, Resource.group("*"));
        assertThatThrownBy(
                        () ->
                                clientGateway
                                        .findConsumerGroupOwner(
                                                new FindConsumerGroupOwnerRequest().setGroupId("*"))
                                        .get())
                .rootCause()
                .isInstanceOf(InvalidConsumerGroupIdException.class);
        assertThat(cluster.getZooKeeperClient().tableExist(SystemTables.CONSUMER_OFFSETS))
                .isFalse();
    }

    @Test
    void testGroupAclBypassPreservesTableAndAclAdministrationChecks() throws Exception {
        startCluster(true);
        assertOwnerDiscoveryAllowed();
        List<AclBinding> bindings =
                grant(new FlussPrincipal("other-user", "USER"), READ, Resource.group(GROUP_ID));
        assertOwnerDiscoveryAllowed();

        assertThatThrownBy(
                        () ->
                                clientGateway
                                        .getTableInfo(
                                                newGetTableInfoRequest(
                                                        SystemTables.CONSUMER_OFFSETS))
                                        .get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class);

        CoordinatorGateway externalCoordinator =
                GatewayClientProxy.createGatewayProxy(
                        () -> cluster.getCoordinatorServerNode("CLIENT"),
                        client,
                        CoordinatorGateway.class);
        assertThatThrownBy(
                        () ->
                                externalCoordinator
                                        .createAcls(
                                                new CreateAclsRequest()
                                                        .addAllAcls(toPbAclInfos(bindings)))
                                        .get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class);
        AclBindingFilter groupFilter =
                new AclBindingFilter(
                        new ResourceFilter(ResourceType.GROUP, GROUP_ID),
                        AccessControlEntryFilter.ANY);
        assertThatThrownBy(
                        () ->
                                externalCoordinator
                                        .dropAcls(
                                                new DropAclsRequest()
                                                        .addAllAclFilters(
                                                                toPbAclBindingFilters(
                                                                        Collections.singletonList(
                                                                                groupFilter))))
                                        .get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class);
    }

    private void assertOwnerDiscoveryAllowed() throws Exception {
        retry(
                WAIT_TIMEOUT,
                () ->
                        assertThat(
                                        clientGateway.findConsumerGroupOwner(
                                                new FindConsumerGroupOwnerRequest()
                                                        .setGroupId(GROUP_ID)))
                                .succeedsWithin(WAIT_TIMEOUT)
                                .satisfies(
                                        response -> {
                                            assertThat(response.getOwner().getNodeId())
                                                    .isEqualTo(clientNode.id());
                                            assertThat(response.getOwner().getHost())
                                                    .isEqualTo(clientNode.host());
                                            assertThat(response.getOwner().getPort())
                                                    .isEqualTo(clientNode.port());
                                        }));
    }

    private List<AclBinding> grant(OperationType operation, Resource resource) throws Exception {
        return grant(FlussPrincipal.ANONYMOUS, operation, resource);
    }

    private List<AclBinding> grant(
            FlussPrincipal principal, OperationType operation, Resource resource) throws Exception {
        List<AclBinding> bindings =
                Collections.singletonList(
                        new AclBinding(
                                resource,
                                new AccessControlEntry(
                                        principal, "*", operation, PermissionType.ALLOW)));
        cluster.newCoordinatorClient()
                .createAcls(new CreateAclsRequest().addAllAcls(toPbAclInfos(bindings)))
                .get();
        cluster.waitUntilAuthenticationSync(bindings, true);
        return bindings;
    }

    private void assertOwnerDiscoveryDenied(String groupId) {
        assertThatThrownBy(
                        () ->
                                clientGateway
                                        .findConsumerGroupOwner(
                                                new FindConsumerGroupOwnerRequest()
                                                        .setGroupId(groupId))
                                        .get())
                .rootCause()
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining("DESCRIBE")
                .hasMessageContaining(groupId);
    }
}
