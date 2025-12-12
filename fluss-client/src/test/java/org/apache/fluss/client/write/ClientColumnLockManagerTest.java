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

package org.apache.fluss.client.write;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.ColumnLockConflictException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ClientColumnLockManager}. */
class ClientColumnLockManagerTest {

    private static final long TABLE_ID = 100L;
    private static final String OWNER_ID = "test-owner";
    private static final int SCHEMA_ID = 1;
    private static final int[] COLUMNS = new int[] {1, 2, 3};
    private static final long TTL_MS = 30000L;

    private TestingMetadataUpdater metadataUpdater;
    private Map<Integer, TestTabletServerGateway> gatewayMap;
    private ClientColumnLockManager lockManager;

    @BeforeEach
    void setup() {
        // Create testing metadata updater with 3 tablet servers
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        metadataUpdater = new TestingMetadataUpdater(tableInfos);

        // Setup cluster with 3 tablet servers
        ServerNode node1 = new ServerNode(1, "localhost", 90, ServerType.TABLET_SERVER, "rack1");
        ServerNode node2 = new ServerNode(2, "localhost", 91, ServerType.TABLET_SERVER, "rack2");
        ServerNode node3 = new ServerNode(3, "localhost", 92, ServerType.TABLET_SERVER, "rack3");

        Map<Integer, ServerNode> tabletServers = new HashMap<>();
        tabletServers.put(1, node1);
        tabletServers.put(2, node2);
        tabletServers.put(3, node3);

        Cluster cluster =
                new Cluster(
                        tabletServers,
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        metadataUpdater.updateCluster(cluster);

        // Create gateways for each tablet server
        gatewayMap = new HashMap<>();
        gatewayMap.put(1, createSuccessGateway());
        gatewayMap.put(2, createSuccessGateway());
        gatewayMap.put(3, createSuccessGateway());

        // Override the gateway creation
        metadataUpdater = new TestingMetadataUpdaterWithCustomGateways(tableInfos, gatewayMap);
        metadataUpdater.updateCluster(cluster);

        lockManager = new ClientColumnLockManager(metadataUpdater);
    }

    @AfterEach
    void teardown() {
        if (lockManager != null) {
            lockManager.close();
        }
    }

    private TestTabletServerGateway createSuccessGateway() {
        return new TestTabletServerGateway(false, Collections.emptySet()) {
            @Override
            public CompletableFuture<org.apache.fluss.rpc.messages.AcquireColumnLockResponse>
                    acquireColumnLock(
                            org.apache.fluss.rpc.messages.AcquireColumnLockRequest request) {
                org.apache.fluss.rpc.messages.AcquireColumnLockResponse response =
                        new org.apache.fluss.rpc.messages.AcquireColumnLockResponse();
                response.setAcquiredTimeMs(System.currentTimeMillis());
                return CompletableFuture.completedFuture(response);
            }

            @Override
            public CompletableFuture<org.apache.fluss.rpc.messages.ReleaseColumnLockResponse>
                    releaseColumnLock(
                            org.apache.fluss.rpc.messages.ReleaseColumnLockRequest request) {
                org.apache.fluss.rpc.messages.ReleaseColumnLockResponse response =
                        new org.apache.fluss.rpc.messages.ReleaseColumnLockResponse();
                return CompletableFuture.completedFuture(response);
            }

            @Override
            public CompletableFuture<org.apache.fluss.rpc.messages.RenewColumnLockResponse>
                    renewColumnLock(org.apache.fluss.rpc.messages.RenewColumnLockRequest request) {
                org.apache.fluss.rpc.messages.RenewColumnLockResponse response =
                        new org.apache.fluss.rpc.messages.RenewColumnLockResponse();
                response.setRenewedTimeMs(System.currentTimeMillis());
                return CompletableFuture.completedFuture(response);
            }
        };
    }

    private TestTabletServerGateway createConflictGateway() {
        return new TestTabletServerGateway(false, Collections.emptySet()) {
            @Override
            public CompletableFuture<org.apache.fluss.rpc.messages.AcquireColumnLockResponse>
                    acquireColumnLock(
                            org.apache.fluss.rpc.messages.AcquireColumnLockRequest request) {
                CompletableFuture<org.apache.fluss.rpc.messages.AcquireColumnLockResponse> future =
                        new CompletableFuture<>();
                future.completeExceptionally(new ColumnLockConflictException("Lock conflict"));
                return future;
            }
        };
    }

    @Test
    void testAcquireLock_Success() throws Exception {
        // Acquire lock on all 3 tablet servers
        CompletableFuture<ClientColumnLockManager.LockKey> lockFuture =
                lockManager.acquireLock(TABLE_ID, OWNER_ID, SCHEMA_ID, COLUMNS, TTL_MS);

        ClientColumnLockManager.LockKey lockKey = lockFuture.get(5, TimeUnit.SECONDS);

        assertThat(lockKey).isNotNull();
        assertThat(lockKey.getTableId()).isEqualTo(TABLE_ID);
    }

    @Test
    void testAcquireLock_ConflictOnOneServer_RollbackAll() {
        // Server 2 returns conflict
        gatewayMap.put(2, createConflictGateway());

        CompletableFuture<ClientColumnLockManager.LockKey> lockFuture =
                lockManager.acquireLock(TABLE_ID, OWNER_ID, SCHEMA_ID, COLUMNS, TTL_MS);

        assertThatThrownBy(() -> lockFuture.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ColumnLockConflictException.class);
    }

    @Test
    void testReleaseLock_Success() throws Exception {
        // Acquire lock first
        CompletableFuture<ClientColumnLockManager.LockKey> lockFuture =
                lockManager.acquireLock(TABLE_ID, OWNER_ID, SCHEMA_ID, COLUMNS, TTL_MS);
        ClientColumnLockManager.LockKey lockKey = lockFuture.get(5, TimeUnit.SECONDS);

        // Release lock
        CompletableFuture<Void> releaseFuture = lockManager.releaseLock(lockKey, OWNER_ID);

        // Should complete without exception
        releaseFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    void testReleaseLock_NonExistentLock_NoException() throws Exception {
        // Try to release a lock that doesn't exist
        ClientColumnLockManager.LockKey fakeLockKey =
                new ClientColumnLockManager.LockKey(TABLE_ID, COLUMNS);

        CompletableFuture<Void> releaseFuture = lockManager.releaseLock(fakeLockKey, OWNER_ID);

        // Should complete without exception (warning logged)
        releaseFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    void testAcquireLock_DefaultTTL() throws Exception {
        // Acquire lock with null TTL (should use default)
        CompletableFuture<ClientColumnLockManager.LockKey> lockFuture =
                lockManager.acquireLock(TABLE_ID, OWNER_ID, SCHEMA_ID, COLUMNS, null);

        ClientColumnLockManager.LockKey lockKey = lockFuture.get(5, TimeUnit.SECONDS);

        assertThat(lockKey).isNotNull();
        assertThat(lockKey.getTableId()).isEqualTo(TABLE_ID);
    }

    @Test
    void testAcquireLock_AllColumns() throws Exception {
        // Acquire lock for all columns (null)
        CompletableFuture<ClientColumnLockManager.LockKey> lockFuture =
                lockManager.acquireLock(TABLE_ID, OWNER_ID, SCHEMA_ID, null, TTL_MS);

        ClientColumnLockManager.LockKey lockKey = lockFuture.get(5, TimeUnit.SECONDS);

        assertThat(lockKey).isNotNull();
        assertThat(lockKey.getTableId()).isEqualTo(TABLE_ID);
    }

    @Test
    void testAcquireLock_NoAliveServers_Failure() {
        // Update cluster with no tablet servers
        Cluster emptyCluster =
                new Cluster(
                        Collections.emptyMap(),
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        metadataUpdater.updateCluster(emptyCluster);

        CompletableFuture<ClientColumnLockManager.LockKey> lockFuture =
                lockManager.acquireLock(TABLE_ID, OWNER_ID, SCHEMA_ID, COLUMNS, TTL_MS);

        assertThatThrownBy(() -> lockFuture.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("No alive TabletServers");
    }

    @Test
    void testClose_ClearsActiveLocks() throws Exception {
        // Acquire lock
        CompletableFuture<ClientColumnLockManager.LockKey> lockFuture =
                lockManager.acquireLock(TABLE_ID, OWNER_ID, SCHEMA_ID, COLUMNS, TTL_MS);
        lockFuture.get(5, TimeUnit.SECONDS);

        // Close manager
        lockManager.close();

        // After close, new acquire should fail
        CompletableFuture<ClientColumnLockManager.LockKey> lockFuture2 =
                lockManager.acquireLock(TABLE_ID, OWNER_ID, SCHEMA_ID, COLUMNS, TTL_MS);

        assertThatThrownBy(() -> lockFuture2.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("ClientColumnLockManager has been closed");
    }

    @Test
    void testLockKey_Equality() {
        ClientColumnLockManager.LockKey key1 =
                new ClientColumnLockManager.LockKey(TABLE_ID, COLUMNS);
        ClientColumnLockManager.LockKey key2 =
                new ClientColumnLockManager.LockKey(TABLE_ID, COLUMNS);
        ClientColumnLockManager.LockKey key3 =
                new ClientColumnLockManager.LockKey(TABLE_ID + 1, COLUMNS);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotEqualTo(key3);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    }

    @Test
    void testLockKey_NullColumns() {
        ClientColumnLockManager.LockKey key1 = new ClientColumnLockManager.LockKey(TABLE_ID, null);
        ClientColumnLockManager.LockKey key2 = new ClientColumnLockManager.LockKey(TABLE_ID, null);
        ClientColumnLockManager.LockKey key3 =
                new ClientColumnLockManager.LockKey(TABLE_ID, COLUMNS);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotEqualTo(key3);
    }

    /** Custom testing metadata updater that allows injection of custom gateways. */
    private static class TestingMetadataUpdaterWithCustomGateways extends TestingMetadataUpdater {
        private final Map<Integer, TestTabletServerGateway> customGateways;

        public TestingMetadataUpdaterWithCustomGateways(
                Map<TablePath, TableInfo> tableInfos,
                Map<Integer, TestTabletServerGateway> customGateways) {
            super(tableInfos);
            this.customGateways = customGateways;
        }

        @Override
        public TabletServerGateway newTabletServerClientForNode(int serverId) {
            if (cluster.getTabletServer(serverId) == null) {
                return null;
            }
            return customGateways.get(serverId);
        }
    }
}
