/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli.sql;

import org.apache.fluss.cli.config.ConnectionConfig;
import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.CreateAclsResult;
import org.apache.fluss.client.admin.DropAclsResult;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlExecutorAclClusterTest {

    @Test
    void testShowAcls() throws Exception {
        Admin admin = mock(Admin.class);
        AclBinding binding =
                new AclBinding(
                        new Resource(ResourceType.TABLE, "db.tbl"),
                        new AccessControlEntry(
                                new FlussPrincipal("user1", "User"),
                                AccessControlEntry.WILD_CARD_HOST,
                                OperationType.READ,
                                PermissionType.ALLOW));
        when(admin.listAcls(any(AclBindingFilter.class)))
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(binding)));

        String output = executeSql(admin, "SHOW ACLS");

        assertThat(output).contains("ACLs:");
        assertThat(output).contains("db.tbl");
    }

    @Test
    void testCreateAcl() throws Exception {
        Admin admin = mock(Admin.class);
        CreateAclsResult result = mock(CreateAclsResult.class);
        when(result.all()).thenReturn(CompletableFuture.completedFuture(null));
        when(admin.createAcls(any())).thenReturn(result);

        String output =
                executeSql(
                        admin,
                        "CREATE ACL (resource_type=TABLE, resource_name='db.tbl', principal='user1', principal_type=User, operation=READ, permission=ALLOW)");

        assertThat(output).contains("ACL created");
    }

    @Test
    void testDropAcl() throws Exception {
        Admin admin = mock(Admin.class);
        DropAclsResult result = mock(DropAclsResult.class);
        when(result.all()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
        when(admin.dropAcls(any())).thenReturn(result);

        String output =
                executeSql(
                        admin,
                        "DROP ACL FILTER (resource_type=TABLE, resource_name='db.tbl', principal='user1')");

        assertThat(output).contains("ACLs dropped: 0");
    }

    @Test
    void testShowClusterConfigs() throws Exception {
        Admin admin = mock(Admin.class);
        ConfigEntry entry =
                new ConfigEntry("k1", "v1", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG);
        when(admin.describeClusterConfigs())
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(entry)));

        String output = executeSql(admin, "SHOW CLUSTER CONFIGS");

        assertThat(output).contains("Cluster Configs:");
        assertThat(output).contains("k1");
    }

    @Test
    void testAlterClusterSet() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterClusterConfigs(any())).thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER CLUSTER SET (k1='v1')");

        assertThat(output).contains("Cluster configs updated successfully");
    }

    @Test
    void testRebalanceCluster() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.rebalance(any())).thenReturn(CompletableFuture.completedFuture("rb1"));

        String output =
                executeSql(admin, "REBALANCE CLUSTER WITH GOALS (\"REPLICA_DISTRIBUTION\")");

        assertThat(output).contains("Rebalance started");
        assertThat(output).contains("rb1");
    }

    @Test
    void testShowRebalance() throws Exception {
        Admin admin = mock(Admin.class);
        RebalanceProgress progress =
                new RebalanceProgress(
                        "rb1", RebalanceStatus.REBALANCING, 0.5d, Collections.emptyMap());
        when(admin.listRebalanceProgress(eq("rb1")))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(progress)));

        String output = executeSql(admin, "SHOW REBALANCE ID rb1");

        assertThat(output).contains("Rebalance progress:");
    }

    @Test
    void testShowServers() throws Exception {
        Admin admin = mock(Admin.class);
        ServerNode node = new ServerNode(1, "localhost", 1234, ServerType.COORDINATOR);
        when(admin.getServerNodes())
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(node)));

        String output = executeSql(admin, "SHOW SERVERS");

        assertThat(output).contains("Server Nodes:");
        assertThat(output).contains("localhost");
    }

    @Test
    void testCancelRebalance() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.cancelRebalance(eq("rb1"))).thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "CANCEL REBALANCE ID rb1");

        assertThat(output).contains("Rebalance cancelled: rb1");
    }

    @Test
    void testShowRebalanceWithoutId() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.listRebalanceProgress(any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        String output = executeSql(admin, "SHOW REBALANCE");

        assertThat(output).contains("No rebalance in progress");
    }

    @Test
    void testCancelRebalanceWithoutId() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.cancelRebalance(any())).thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "CANCEL REBALANCE");

        assertThat(output).contains("Rebalance cancelled");
    }

    private static String executeSql(Admin admin, String sql) throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(admin);
        ConnectionManager connectionManager = new StubConnectionManager(connection);
        StringWriter writer = new StringWriter();
        SqlExecutor executor = new SqlExecutor(connectionManager, new PrintWriter(writer));
        executor.executeSql(sql);
        return writer.toString();
    }

    private static class StubConnectionManager extends ConnectionManager {
        private final Connection connection;

        StubConnectionManager(Connection connection) {
            super(new ConnectionConfig(new Configuration()));
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }
    }
}
