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

package com.alibaba.fluss.server;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.MetadataCacheMissException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbPhysicalTablePath;
import com.alibaba.fluss.rpc.messages.PbTablePath;
import com.alibaba.fluss.rpc.netty.server.Session;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.server.authorizer.Authorizer;
import com.alibaba.fluss.server.metadata.BucketMetadata;
import com.alibaba.fluss.server.metadata.MetadataFunctionProvider;
import com.alibaba.fluss.server.metadata.PartitionMetadata;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metadata.TableMetadata;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit test for {@link RpcServiceBase} async metadata methods. */
class RpcServiceBaseTest {

    private ZooKeeperClient mockZkClient;
    private TablePath testTablePath;
    private PhysicalTablePath testPartitionPath;
    private long testTableId;
    private long testPartitionId;
    private List<BucketMetadata> testBucketMetadata;

    @BeforeEach
    void beforeEach() {
        mockZkClient = mock(ZooKeeperClient.class);

        testTablePath = TablePath.of("testDb", "testTable");
        testPartitionPath = PhysicalTablePath.of(testTablePath, "partition1");
        testTableId = 1L;
        testPartitionId = 100L;
        testBucketMetadata =
                Lists.newArrayList(
                        new BucketMetadata(0, 1, 1, Arrays.asList(1, 2, 3)),
                        new BucketMetadata(1, 2, 2, Arrays.asList(4, 5, 6)));
    }

    private Session createTestSession() throws java.net.UnknownHostException {
        return new Session(
                (short) 1,
                "default",
                false,
                java.net.InetAddress.getByName("127.0.0.1"),
                FlussPrincipal.ANY);
    }

    // A test implementation of RpcServiceBase to access the protected processMetadataRequest method
    private static class TestRpcServiceBase extends RpcServiceBase {
        public TestRpcServiceBase() {
            super(null, null, null, null, null);
        }

        @Override
        public void processMetadataRequest(
                MetadataRequest request,
                String listenerName,
                Session session,
                Authorizer authorizer,
                ServerMetadataCache metadataCache,
                MetadataFunctionProvider functionProvider,
                CompletableFuture<MetadataResponse> responseFuture) {
            super.processMetadataRequest(
                    request,
                    listenerName,
                    session,
                    authorizer,
                    metadataCache,
                    functionProvider,
                    responseFuture);
        }

        @Override
        public String name() {
            return "TestRpcService";
        }

        @Override
        public void shutdown() {}

        @Override
        public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
            return null;
        }
    }

    @Test
    void testGetTableMetadataFromZkAsync_Success() throws Exception {
        // Setup test data
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(1, 2, 3));
        bucketAssignments.put(1, BucketAssignment.of(2, 3, 4));
        TableAssignment tableAssignment = new TableAssignment(bucketAssignments);

        // Mock ZooKeeper responses
        when(mockZkClient.getTableAssignment(testTableId)).thenReturn(Optional.of(tableAssignment));
        when(mockZkClient.getLeaderAndIsr(new TableBucket(testTableId, null, 0)))
                .thenReturn(Optional.of(new LeaderAndIsr(1, 1, Arrays.asList(1, 2, 3), 1, 1)));
        when(mockZkClient.getLeaderAndIsr(new TableBucket(testTableId, null, 1)))
                .thenReturn(Optional.of(new LeaderAndIsr(2, 1, Arrays.asList(2, 3, 4), 1, 1)));

        // Execute test
        CompletableFuture<List<BucketMetadata>> future =
                RpcServiceBase.getTableMetadataFromZkAsync(
                        mockZkClient, testTablePath, testTableId, false);

        // Verify results
        List<BucketMetadata> result = future.get();
        assertThat(result).hasSize(2);

        BucketMetadata bucket0 = result.get(0);
        assertThat(bucket0.getBucketId()).isEqualTo(0);
        assertThat(bucket0.getLeaderId()).hasValue(1);
        assertThat(bucket0.getReplicas()).containsExactly(1, 2, 3);

        BucketMetadata bucket1 = result.get(1);
        assertThat(bucket1.getBucketId()).isEqualTo(1);
        assertThat(bucket1.getLeaderId()).hasValue(2);
        assertThat(bucket1.getReplicas()).containsExactly(2, 3, 4);
    }

    @Test
    void testGetTableMetadataFromZkAsync_NoTableAssignment() throws Exception {
        // Mock ZooKeeper to return empty table assignment
        when(mockZkClient.getTableAssignment(testTableId)).thenReturn(Optional.empty());

        // Execute test
        CompletableFuture<List<BucketMetadata>> future =
                RpcServiceBase.getTableMetadataFromZkAsync(
                        mockZkClient, testTablePath, testTableId, false);

        // Verify results - should return empty list for non-partitioned table
        List<BucketMetadata> result = future.get();
        assertThat(result).isEmpty();
    }

    @Test
    void testGetTableMetadataFromZkAsync_NoTableAssignmentForPartitionedTable() throws Exception {
        // Mock ZooKeeper to return empty table assignment for partitioned table
        when(mockZkClient.getTableAssignment(testTableId)).thenReturn(Optional.empty());

        // Execute test - should log warning but not throw exception
        CompletableFuture<List<BucketMetadata>> future =
                RpcServiceBase.getTableMetadataFromZkAsync(
                        mockZkClient, testTablePath, testTableId, true);

        // Verify results - should return empty list
        List<BucketMetadata> result = future.get();
        assertThat(result).isEmpty();
    }

    @Test
    void testGetTableMetadataFromZkAsync_ZooKeeperException() throws Exception {
        // Mock ZooKeeper to throw exception
        when(mockZkClient.getTableAssignment(testTableId))
                .thenThrow(new RuntimeException("ZK connection failed"));

        // Execute test
        CompletableFuture<List<BucketMetadata>> future =
                RpcServiceBase.getTableMetadataFromZkAsync(
                        mockZkClient, testTablePath, testTableId, false);

        // Verify exception is thrown
        assertThatThrownBy(() -> future.get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Failed to get metadata for table");
    }

    @Test
    void testGetPartitionMetadataFromZkAsync_Success() throws Exception {
        // Setup test data
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(1, 2, 3));
        bucketAssignments.put(1, BucketAssignment.of(2, 3, 4));
        TableAssignment tableAssignment = new TableAssignment(bucketAssignments);

        // Mock ZooKeeper responses
        when(mockZkClient.getPartition(testTablePath, "partition1"))
                .thenReturn(Optional.of(new TablePartition(testTableId, testPartitionId)));
        when(mockZkClient.getPartitionAssignment(testPartitionId))
                .thenReturn(Optional.of(new PartitionAssignment(testTableId, bucketAssignments)));
        when(mockZkClient.getLeaderAndIsr(new TableBucket(testTableId, testPartitionId, 0)))
                .thenReturn(Optional.of(new LeaderAndIsr(1, 1, Arrays.asList(1, 2, 3), 1, 1)));
        when(mockZkClient.getLeaderAndIsr(new TableBucket(testTableId, testPartitionId, 1)))
                .thenReturn(Optional.of(new LeaderAndIsr(2, 1, Arrays.asList(2, 3, 4), 1, 1)));

        // Execute test
        CompletableFuture<PartitionMetadata> future =
                RpcServiceBase.getPartitionMetadataFromZkAsync(testPartitionPath, mockZkClient);

        // Verify results
        PartitionMetadata result = future.get();
        assertThat(result.getPartitionId()).isEqualTo(testPartitionId);
        assertThat(result.getTableId()).isEqualTo(testTableId);
        assertThat(result.getBucketMetadataList()).hasSize(2);

        BucketMetadata bucket0 = result.getBucketMetadataList().get(0);
        assertThat(bucket0.getBucketId()).isEqualTo(0);
        assertThat(bucket0.getLeaderId()).hasValue(1);
        assertThat(bucket0.getReplicas()).containsExactly(1, 2, 3);

        BucketMetadata bucket1 = result.getBucketMetadataList().get(1);
        assertThat(bucket1.getBucketId()).isEqualTo(1);
        assertThat(bucket1.getLeaderId()).hasValue(2);
        assertThat(bucket1.getReplicas()).containsExactly(2, 3, 4);
    }

    @Test
    void testGetPartitionMetadataFromZkAsync_PartitionNotExist() throws Exception {
        // Mock ZooKeeper to return empty partition
        when(mockZkClient.getPartition(testTablePath, "nonexistent")).thenReturn(Optional.empty());

        // Execute test
        PhysicalTablePath nonexistentPath = PhysicalTablePath.of(testTablePath, "nonexistent");
        CompletableFuture<PartitionMetadata> future =
                RpcServiceBase.getPartitionMetadataFromZkAsync(nonexistentPath, mockZkClient);

        // Verify exception is thrown
        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PartitionNotExistException.class);
    }

    @Test
    void testConcurrentGetTableMetadataFromZkAsync_SameKey() throws Exception {
        // Setup test data
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(1, 2, 3));
        TableAssignment tableAssignment = new TableAssignment(bucketAssignments);

        // Mock ZooKeeper responses
        when(mockZkClient.getTableAssignment(testTableId)).thenReturn(Optional.of(tableAssignment));
        when(mockZkClient.getLeaderAndIsr(any(TableBucket.class)))
                .thenReturn(Optional.of(new LeaderAndIsr(1, 1, Arrays.asList(1, 2, 3), 1, 1)));

        // Execute concurrent requests with same key
        CompletableFuture<List<BucketMetadata>> future1 =
                RpcServiceBase.getTableMetadataFromZkAsync(
                        mockZkClient, testTablePath, testTableId, false);
        CompletableFuture<List<BucketMetadata>> future2 =
                RpcServiceBase.getTableMetadataFromZkAsync(
                        mockZkClient, testTablePath, testTableId, false);

        // Verify both requests return the same result
        List<BucketMetadata> result1 = future1.get();
        List<BucketMetadata> result2 = future2.get();
        assertThat(result1).isEqualTo(result2);
        assertThat(result1).hasSize(1);
    }

    @Test
    void testConcurrentGetPartitionMetadataFromZkAsync_SameKey() throws Exception {
        // Setup test data
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(1, 2, 3));

        // Mock ZooKeeper responses
        when(mockZkClient.getPartition(testTablePath, "partition1"))
                .thenReturn(Optional.of(new TablePartition(testPartitionId, testTableId)));
        when(mockZkClient.getPartitionAssignment(testPartitionId))
                .thenReturn(Optional.of(new PartitionAssignment(testTableId, bucketAssignments)));
        when(mockZkClient.getLeaderAndIsr(any(TableBucket.class)))
                .thenReturn(Optional.of(new LeaderAndIsr(1, 1, Arrays.asList(1, 2, 3), 1, 1)));

        // Execute concurrent requests with same key
        CompletableFuture<PartitionMetadata> future1 =
                RpcServiceBase.getPartitionMetadataFromZkAsync(testPartitionPath, mockZkClient);
        CompletableFuture<PartitionMetadata> future2 =
                RpcServiceBase.getPartitionMetadataFromZkAsync(testPartitionPath, mockZkClient);

        // Verify both requests return the same result
        PartitionMetadata result1 = future1.get();
        PartitionMetadata result2 = future2.get();
        assertThat(result1).isEqualTo(result2);
    }

    @Test
    void testGetTableMetadataFromZkAsync_MapEntryRemovedOnSuccess() throws Exception {
        // Setup test data
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(1, 2, 3));
        TableAssignment tableAssignment = new TableAssignment(bucketAssignments);

        // Mock ZooKeeper responses
        when(mockZkClient.getTableAssignment(testTableId)).thenReturn(Optional.of(tableAssignment));
        when(mockZkClient.getLeaderAndIsr(any(TableBucket.class)))
                .thenReturn(Optional.of(new LeaderAndIsr(1, 1, Arrays.asList(1, 2, 3), 1, 1)));

        // Check that the map is initially empty
        assertThat(getPendingTableMetadataMap()).isEmpty();

        // Execute test
        CompletableFuture<List<BucketMetadata>> future =
                RpcServiceBase.getTableMetadataFromZkAsync(
                        mockZkClient, testTablePath, testTableId, false);

        // The entry should be added to the map
        assertThat(getPendingTableMetadataMap()).hasSize(1);

        // Wait for completion
        future.get();

        // After completion, the entry should be removed from the map
        assertThat(getPendingTableMetadataMap()).isEmpty();
    }

    @Test
    void testGetTableMetadataFromZkAsync_MapEntryRemovedOnFailure() throws Exception {
        // Mock ZooKeeper to throw exception
        when(mockZkClient.getTableAssignment(testTableId))
                .thenThrow(new RuntimeException("ZK connection failed"));

        // Check that the map is initially empty
        assertThat(getPendingTableMetadataMap()).isEmpty();

        // Execute test
        CompletableFuture<List<BucketMetadata>> future =
                RpcServiceBase.getTableMetadataFromZkAsync(
                        mockZkClient, testTablePath, testTableId, false);

        // The entry should be added to the map
        assertThat(getPendingTableMetadataMap()).hasSize(1);

        // Wait for completion and expect exception
        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlussRuntimeException.class);

        // After failure, the entry should be removed from the map
        assertThat(getPendingTableMetadataMap()).isEmpty();
    }

    @Test
    void testGetPartitionMetadataFromZkAsync_MapEntryRemovedOnSuccess() throws Exception {
        // Setup test data
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(1, 2, 3));
        TableAssignment tableAssignment = new TableAssignment(bucketAssignments);

        // Mock ZooKeeper responses
        when(mockZkClient.getPartition(testTablePath, "partition1"))
                .thenReturn(Optional.of(new TablePartition(testPartitionId, testTableId)));
        when(mockZkClient.getPartitionAssignment(testPartitionId))
                .thenReturn(Optional.of(new PartitionAssignment(testTableId, bucketAssignments)));
        when(mockZkClient.getLeaderAndIsr(any(TableBucket.class)))
                .thenReturn(Optional.of(new LeaderAndIsr(1, 1, Arrays.asList(1, 2, 3), 1, 1)));

        // Check that the map is initially empty
        assertThat(getPendingPartitionMetadataMap()).isEmpty();

        // Execute test
        CompletableFuture<PartitionMetadata> future =
                RpcServiceBase.getPartitionMetadataFromZkAsync(testPartitionPath, mockZkClient);

        // The entry should be added to the map
        assertThat(getPendingPartitionMetadataMap()).hasSize(1);

        // Wait for completion
        future.get();

        // After completion, the entry should be removed from the map
        assertThat(getPendingPartitionMetadataMap()).isEmpty();
    }

    @Test
    void testGetPartitionMetadataFromZkAsync_MapEntryRemovedOnFailure() throws Exception {
        // Mock ZooKeeper to throw exception
        when(mockZkClient.getPartition(testTablePath, "partition1"))
                .thenThrow(new RuntimeException("ZK connection failed"));

        // Check that the map is initially empty
        assertThat(getPendingPartitionMetadataMap()).isEmpty();

        // Execute test
        CompletableFuture<PartitionMetadata> future =
                RpcServiceBase.getPartitionMetadataFromZkAsync(testPartitionPath, mockZkClient);

        // The entry should be added to the map
        assertThat(getPendingPartitionMetadataMap()).hasSize(1);

        // Wait for completion and expect exception
        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlussRuntimeException.class);

        // After failure, the entry should be removed from the map
        assertThat(getPendingPartitionMetadataMap()).isEmpty();
    }

    @Test
    void testProcessMetadataRequest_TableFromCache() throws Exception {
        // Setup mocks
        MetadataRequest request = new MetadataRequest();
        PbTablePath pbTablePath = new PbTablePath().setDatabaseName("db").setTableName("table");
        request.addTablePath().copyFrom(pbTablePath);

        String listenerName = "default";
        Session session = createTestSession();
        Authorizer authorizer = mock(Authorizer.class);
        ServerMetadataCache metadataCache = mock(ServerMetadataCache.class);
        MetadataFunctionProvider functionProvider = mock(MetadataFunctionProvider.class);

        when(authorizer.isAuthorized(any(), any(), any())).thenReturn(true);
        TablePath tablePath = TablePath.of("db", "table");
        TableInfo partitionTableInfo =
                TableInfo.of(
                        tablePath,
                        testTableId,
                        1,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        TableMetadata tableMetadata =
                new TableMetadata(partitionTableInfo, Collections.emptyList());
        when(functionProvider.getTableMetadataFromCache(eq(tablePath)))
                .thenReturn(Optional.of(tableMetadata));

        ServerNode coordinatorServer =
                new ServerNode(1, "localhost", 9090, ServerType.TABLET_SERVER);
        when(metadataCache.getCoordinatorServer(eq(listenerName))).thenReturn(coordinatorServer);
        Map<Integer, ServerNode> tabletServers = new HashMap<>();
        tabletServers.put(1, new ServerNode(1, "localhost", 9091, ServerType.TABLET_SERVER));
        when(metadataCache.getAllAliveTabletServers(eq(listenerName))).thenReturn(tabletServers);

        CompletableFuture<MetadataResponse> responseFuture = new CompletableFuture<>();
        TestRpcServiceBase rpcServiceBase = new TestRpcServiceBase();

        // Execute test
        rpcServiceBase.processMetadataRequest(
                request,
                listenerName,
                session,
                authorizer,
                metadataCache,
                functionProvider,
                responseFuture);

        // Verify results
        MetadataResponse response = responseFuture.get();
        assertThat(response.hasCoordinatorServer()).isTrue();
        assertThat(response.getCoordinatorServer().getNodeId()).isEqualTo(1);
        assertThat(response.getTabletServersCount()).isEqualTo(1);
        assertThat(response.getTableMetadatasCount()).isEqualTo(1);
        assertThat(response.getPartitionMetadatasCount()).isEqualTo(0);

        // Verify that getTableMetadataFromZk was not called
        verify(functionProvider, never()).getTableMetadataFromZk(any());
    }

    @Test
    void testProcessMetadataRequest_TableFromZk() throws Exception {
        // Setup mocks
        MetadataRequest request = new MetadataRequest();
        PbTablePath pbTablePath = new PbTablePath().setDatabaseName("db").setTableName("table");
        request.addTablePath().copyFrom(pbTablePath);

        String listenerName = "default";
        Session session = createTestSession();
        Authorizer authorizer = mock(Authorizer.class);
        ServerMetadataCache metadataCache = mock(ServerMetadataCache.class);
        MetadataFunctionProvider functionProvider = mock(MetadataFunctionProvider.class);

        when(authorizer.isAuthorized(any(), any(), any())).thenReturn(true);
        TablePath tablePath = TablePath.of("db", "table");
        when(functionProvider.getTableMetadataFromCache(eq(tablePath)))
                .thenReturn(Optional.empty());

        TableInfo partitionTableInfo =
                TableInfo.of(
                        tablePath,
                        testTableId,
                        1,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        TableMetadata tableMetadata =
                new TableMetadata(partitionTableInfo, Collections.emptyList());
        when(functionProvider.getTableMetadataFromZk(eq(tablePath)))
                .thenReturn(CompletableFuture.completedFuture(tableMetadata));

        ServerNode coordinatorServer =
                new ServerNode(1, "localhost", 9090, ServerType.TABLET_SERVER);
        when(metadataCache.getCoordinatorServer(eq(listenerName))).thenReturn(coordinatorServer);
        Map<Integer, ServerNode> tabletServers = new HashMap<>();
        tabletServers.put(1, new ServerNode(1, "localhost", 9091, ServerType.TABLET_SERVER));
        when(metadataCache.getAllAliveTabletServers(eq(listenerName))).thenReturn(tabletServers);

        CompletableFuture<MetadataResponse> responseFuture = new CompletableFuture<>();
        TestRpcServiceBase rpcServiceBase = new TestRpcServiceBase();

        // Execute test
        rpcServiceBase.processMetadataRequest(
                request,
                listenerName,
                session,
                authorizer,
                metadataCache,
                functionProvider,
                responseFuture);

        // Verify results
        MetadataResponse response = responseFuture.get();
        assertThat(response.hasCoordinatorServer()).isTrue();
        assertThat(response.getCoordinatorServer().getNodeId()).isEqualTo(1);
        assertThat(response.getTabletServersCount()).isEqualTo(1);
        assertThat(response.getTableMetadatasCount()).isEqualTo(1);
        assertThat(response.getPartitionMetadatasCount()).isEqualTo(0);

        // Verify that getTableMetadataFromZk was called
        verify(functionProvider).getTableMetadataFromZk(eq(tablePath));
    }

    @Test
    void testProcessMetadataRequest_PartitionFromCache() throws Exception {
        // Setup mocks
        MetadataRequest request = new MetadataRequest();
        PbPhysicalTablePath pbPartitionPath =
                new PbPhysicalTablePath()
                        .setDatabaseName("db")
                        .setTableName("table")
                        .setPartitionName("partition");
        request.addPartitionsPath().copyFrom(pbPartitionPath);

        String listenerName = "default";
        Session session = createTestSession();
        Authorizer authorizer = mock(Authorizer.class);
        ServerMetadataCache metadataCache = mock(ServerMetadataCache.class);
        MetadataFunctionProvider functionProvider = mock(MetadataFunctionProvider.class);

        when(authorizer.isAuthorized(any(), any(), any())).thenReturn(true);
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(TablePath.of("db", "table"), "partition");
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(
                        testTableId, "partition", testPartitionId, testBucketMetadata);
        when(functionProvider.getPartitionMetadataFromCache(eq(partitionPath)))
                .thenReturn(Optional.of(partitionMetadata));

        ServerNode coordinatorServer =
                new ServerNode(1, "localhost", 9090, ServerType.TABLET_SERVER);
        when(metadataCache.getCoordinatorServer(eq(listenerName))).thenReturn(coordinatorServer);
        Map<Integer, ServerNode> tabletServers = new HashMap<>();
        tabletServers.put(1, new ServerNode(1, "localhost", 9091, ServerType.TABLET_SERVER));
        when(metadataCache.getAllAliveTabletServers(eq(listenerName))).thenReturn(tabletServers);

        CompletableFuture<MetadataResponse> responseFuture = new CompletableFuture<>();
        TestRpcServiceBase rpcServiceBase = new TestRpcServiceBase();

        // Execute test
        rpcServiceBase.processMetadataRequest(
                request,
                listenerName,
                session,
                authorizer,
                metadataCache,
                functionProvider,
                responseFuture);

        // Verify results
        MetadataResponse response = responseFuture.get();
        assertThat(response.hasCoordinatorServer()).isTrue();
        assertThat(response.getCoordinatorServer().getNodeId()).isEqualTo(1);
        assertThat(response.getTabletServersCount()).isEqualTo(1);
        assertThat(response.getTableMetadatasCount()).isEqualTo(0);
        assertThat(response.getPartitionMetadatasCount()).isEqualTo(1);

        // Verify that getPartitionMetadataFromZk was not called
        verify(functionProvider, never()).getPartitionMetadataFromZk(any());
    }

    @Test
    void testProcessMetadataRequest_PartitionFromZk() throws Exception {
        // Setup mocks
        MetadataRequest request = new MetadataRequest();
        PbPhysicalTablePath pbPartitionPath =
                new PbPhysicalTablePath()
                        .setDatabaseName("db")
                        .setTableName("table")
                        .setPartitionName("partition");
        request.addPartitionsPath().copyFrom(pbPartitionPath);

        String listenerName = "default";
        Session session = createTestSession();
        Authorizer authorizer = mock(Authorizer.class);
        ServerMetadataCache metadataCache = mock(ServerMetadataCache.class);
        MetadataFunctionProvider functionProvider = mock(MetadataFunctionProvider.class);

        when(authorizer.isAuthorized(any(), any(), any())).thenReturn(true);
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(TablePath.of("db", "table"), "partition");
        when(functionProvider.getPartitionMetadataFromCache(eq(partitionPath)))
                .thenReturn(Optional.empty());

        PartitionMetadata partitionMetadata =
                new PartitionMetadata(
                        testTableId, "partition", testPartitionId, testBucketMetadata);
        when(functionProvider.getPartitionMetadataFromZk(eq(partitionPath)))
                .thenReturn(CompletableFuture.completedFuture(partitionMetadata));

        ServerNode coordinatorServer =
                new ServerNode(1, "localhost", 9090, ServerType.TABLET_SERVER);
        when(metadataCache.getCoordinatorServer(eq(listenerName))).thenReturn(coordinatorServer);
        Map<Integer, ServerNode> tabletServers = new HashMap<>();
        tabletServers.put(1, new ServerNode(1, "localhost", 9091, ServerType.TABLET_SERVER));
        when(metadataCache.getAllAliveTabletServers(eq(listenerName))).thenReturn(tabletServers);

        CompletableFuture<MetadataResponse> responseFuture = new CompletableFuture<>();
        TestRpcServiceBase rpcServiceBase = new TestRpcServiceBase();

        // Execute test
        rpcServiceBase.processMetadataRequest(
                request,
                listenerName,
                session,
                authorizer,
                metadataCache,
                functionProvider,
                responseFuture);

        // Verify results
        MetadataResponse response = responseFuture.get();
        assertThat(response.hasCoordinatorServer()).isTrue();
        assertThat(response.getCoordinatorServer().getNodeId()).isEqualTo(1);
        assertThat(response.getTabletServersCount()).isEqualTo(1);
        assertThat(response.getTableMetadatasCount()).isEqualTo(0);
        assertThat(response.getPartitionMetadatasCount()).isEqualTo(1);

        // Verify that getPartitionMetadataFromZk was called
        verify(functionProvider).getPartitionMetadataFromZk(eq(partitionPath));
    }

    @Test
    void testProcessMetadataRequest_CacheOnlyModeWithMissingTable() throws Exception {
        // Setup mocks
        MetadataRequest request = new MetadataRequest();
        request.setCacheOnly(true); // Enable cache only mode
        PbTablePath pbTablePath = new PbTablePath().setDatabaseName("db").setTableName("table");
        request.addTablePath().copyFrom(pbTablePath);

        String listenerName = "default";
        Session session = createTestSession();
        Authorizer authorizer = mock(Authorizer.class);
        ServerMetadataCache metadataCache = mock(ServerMetadataCache.class);
        MetadataFunctionProvider functionProvider = mock(MetadataFunctionProvider.class);

        when(authorizer.isAuthorized(any(), any(), any())).thenReturn(true);
        TablePath tablePath = TablePath.of("db", "table");
        when(functionProvider.getTableMetadataFromCache(eq(tablePath)))
                .thenReturn(Optional.empty()); // Not in cache
        when(functionProvider.getTableMetadataFromZk(eq(tablePath)))
                .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<MetadataResponse> responseFuture = new CompletableFuture<>();
        TestRpcServiceBase rpcServiceBase = new TestRpcServiceBase();

        // Execute test
        rpcServiceBase.processMetadataRequest(
                request,
                listenerName,
                session,
                authorizer,
                metadataCache,
                functionProvider,
                responseFuture);

        // Verify exception
        assertThat(responseFuture).isCompletedExceptionally();
        assertThatThrownBy(responseFuture::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(MetadataCacheMissException.class)
                .hasMessageContaining("Table meta not found in metadata cache");
    }

    @Test
    void testProcessMetadataRequest_CacheOnlyModeWithMissingPartition() throws Exception {
        // Setup mocks
        MetadataRequest request = new MetadataRequest();
        request.setCacheOnly(true); // Enable cache only mode
        PbPhysicalTablePath pbPartitionPath =
                new PbPhysicalTablePath()
                        .setDatabaseName("db")
                        .setTableName("table")
                        .setPartitionName("partition");
        request.addPartitionsPath().copyFrom(pbPartitionPath);

        String listenerName = "default";
        Session session = createTestSession();
        Authorizer authorizer = mock(Authorizer.class);
        ServerMetadataCache metadataCache = mock(ServerMetadataCache.class);
        MetadataFunctionProvider functionProvider = mock(MetadataFunctionProvider.class);

        when(authorizer.isAuthorized(any(), any(), any())).thenReturn(true);
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(TablePath.of("db", "table"), "partition");
        when(functionProvider.getPartitionMetadataFromCache(eq(partitionPath)))
                .thenReturn(Optional.empty()); // Not in cache
        when(functionProvider.getPartitionMetadataFromZk(eq(partitionPath)))
                .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<MetadataResponse> responseFuture = new CompletableFuture<>();
        TestRpcServiceBase rpcServiceBase = new TestRpcServiceBase();

        // Execute test
        rpcServiceBase.processMetadataRequest(
                request,
                listenerName,
                session,
                authorizer,
                metadataCache,
                functionProvider,
                responseFuture);

        // Verify exception
        assertThat(responseFuture).isCompletedExceptionally();
        assertThatThrownBy(responseFuture::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(MetadataCacheMissException.class)
                .hasMessageContaining("Partition meta not found in metadata cache");
    }

    @Test
    void testProcessMetadataRequest_PartitionByIdFromCache() throws Exception {
        // Setup mocks
        MetadataRequest request = new MetadataRequest();
        request.setPartitionsIds(new long[] {100L});

        String listenerName = "default";
        Session session = createTestSession();
        Authorizer authorizer = mock(Authorizer.class);
        ServerMetadataCache metadataCache = mock(ServerMetadataCache.class);
        MetadataFunctionProvider functionProvider = mock(MetadataFunctionProvider.class);

        when(authorizer.isAuthorized(any(), any(), any())).thenReturn(true);
        PhysicalTablePath partitionPath =
                PhysicalTablePath.of(TablePath.of("db", "table"), "partition");
        when(functionProvider.getPhysicalTablePathFromCache(100L))
                .thenReturn(Optional.of(partitionPath));

        PartitionMetadata partitionMetadata =
                new PartitionMetadata(
                        testTableId, "partition", testPartitionId, testBucketMetadata);
        when(functionProvider.getPartitionMetadataFromCache(eq(partitionPath)))
                .thenReturn(Optional.of(partitionMetadata));

        ServerNode coordinatorServer =
                new ServerNode(1, "localhost", 9090, ServerType.TABLET_SERVER);
        when(metadataCache.getCoordinatorServer(eq(listenerName))).thenReturn(coordinatorServer);
        Map<Integer, ServerNode> tabletServers = new HashMap<>();
        tabletServers.put(1, new ServerNode(1, "localhost", 9091, ServerType.TABLET_SERVER));
        when(metadataCache.getAllAliveTabletServers(eq(listenerName))).thenReturn(tabletServers);

        CompletableFuture<MetadataResponse> responseFuture = new CompletableFuture<>();
        TestRpcServiceBase rpcServiceBase = new TestRpcServiceBase();

        // Execute test
        rpcServiceBase.processMetadataRequest(
                request,
                listenerName,
                session,
                authorizer,
                metadataCache,
                functionProvider,
                responseFuture);

        // Verify results
        MetadataResponse response = responseFuture.get();
        assertThat(response.hasCoordinatorServer()).isTrue();
        assertThat(response.getCoordinatorServer().getNodeId()).isEqualTo(1);
        assertThat(response.getTabletServersCount()).isEqualTo(1);
        assertThat(response.getTableMetadatasCount()).isEqualTo(0);
        assertThat(response.getPartitionMetadatasCount()).isEqualTo(1);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<?, ?> getPendingTableMetadataMap() throws Exception {
        Field field =
                RpcServiceBase.class.getDeclaredField("PENDING_TABLE_METADATA_FROM_ZK_FUTURES");
        field.setAccessible(true);
        return (ConcurrentHashMap<?, ?>) field.get(null);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<?, ?> getPendingPartitionMetadataMap() throws Exception {
        Field field =
                RpcServiceBase.class.getDeclaredField("PENDING_PARTITION_METADATA_FROM_ZK_FUTURES");
        field.setAccessible(true);
        return (ConcurrentHashMap<?, ?>) field.get(null);
    }
}
