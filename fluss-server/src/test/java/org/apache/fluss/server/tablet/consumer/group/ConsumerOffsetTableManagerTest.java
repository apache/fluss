/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.CreateDatabaseResponse;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.CreateTableResponse;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.zk.ZooKeeperClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link ConsumerOffsetTableManager}. */
class ConsumerOffsetTableManagerTest {

    private static final int BUCKET_COUNT = 1;
    private static final TablePath TABLE_PATH = SystemTables.CONSUMER_OFFSETS;

    private ZooKeeperClient zkClient;
    private MetadataManager metadataManager;
    private TabletServerMetadataCache metadataCache;
    private TabletServerMetadataProvider metadataProvider;
    private CoordinatorGateway coordinatorGateway;
    private ConsumerOffsetTableManager tableManager;

    @BeforeEach
    void setup() throws Exception {
        zkClient = mock(ZooKeeperClient.class);
        metadataManager = mock(MetadataManager.class);
        metadataCache = mock(TabletServerMetadataCache.class);
        metadataProvider = mock(TabletServerMetadataProvider.class);
        coordinatorGateway = mock(CoordinatorGateway.class);
        ExecutorService ioExecutor = mock(ExecutorService.class);

        when(zkClient.tableExist(TABLE_PATH)).thenReturn(false);
        when(zkClient.databaseExists(TABLE_PATH.getDatabaseName())).thenReturn(true);
        when(metadataManager.getTable(TABLE_PATH)).thenReturn(newTableInfo());
        when(coordinatorGateway.createDatabase(any(CreateDatabaseRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CreateDatabaseResponse()));
        when(coordinatorGateway.createTable(any(CreateTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CreateTableResponse()));
        doAnswer(
                        invocation -> {
                            invocation.getArgument(0, Runnable.class).run();
                            return null;
                        })
                .when(ioExecutor)
                .execute(any(Runnable.class));

        ConsumerOffsetTableConfig tableConfig =
                new ConsumerOffsetTableConfig(
                        MemorySize.ofMebiBytes(1),
                        Duration.ofDays(1),
                        BUCKET_COUNT,
                        1,
                        Duration.ofSeconds(10));
        tableManager =
                new ConsumerOffsetTableManager(
                        tableConfig,
                        zkClient,
                        metadataManager,
                        metadataCache,
                        metadataProvider,
                        coordinatorGateway,
                        ioExecutor);
    }

    @Test
    void testResolveOwnerFromCache() throws Exception {
        String listenerName = "CLIENT";
        int leaderId = 0;
        BucketMetadata bucketMetadata =
                new BucketMetadata(0, leaderId, 1, Collections.singletonList(leaderId));
        ServerNode owner = new ServerNode(leaderId, "localhost", 9123, ServerType.TABLET_SERVER);
        when(metadataCache.getCachedBucketMetadata(TABLE_PATH, 0))
                .thenReturn(Optional.of(bucketMetadata));
        when(metadataCache.getTabletServer(leaderId, listenerName)).thenReturn(Optional.of(owner));

        assertThat(tableManager.resolveOwnerAsync(0, listenerName).get()).isEqualTo(owner);

        verify(metadataProvider, never()).getTablesMetadataFromZK(any());
        verify(zkClient, never()).getTabletServer(leaderId);
    }

    @Test
    void testResolveOwnerFromZooKeeperOnCacheMiss() throws Exception {
        String listenerName = "CLIENT";
        int leaderId = 0;
        BucketMetadata bucketMetadata =
                new BucketMetadata(0, leaderId, 1, Collections.singletonList(leaderId));
        ServerNode owner = new ServerNode(leaderId, "localhost", 9123, ServerType.TABLET_SERVER);
        when(metadataCache.getCachedBucketMetadata(TABLE_PATH, 0)).thenReturn(Optional.empty());
        when(metadataProvider.getTablesMetadataFromZK(Collections.singleton(TABLE_PATH)))
                .thenReturn(
                        Collections.singletonList(
                                new TableMetadata(
                                        newTableInfo(),
                                        Collections.singletonList(bucketMetadata))));
        when(metadataCache.getTabletServer(leaderId, listenerName)).thenReturn(Optional.of(owner));

        assertThat(tableManager.resolveOwnerAsync(0, listenerName).get()).isEqualTo(owner);

        verify(metadataProvider).getTablesMetadataFromZK(Collections.singleton(TABLE_PATH));
    }

    @Test
    void testCreateTableThroughCoordinator() throws Exception {
        when(zkClient.databaseExists(TABLE_PATH.getDatabaseName())).thenReturn(false);

        assertThat(tableManager.ensureTableAsync().get()).isEqualTo(BUCKET_COUNT);

        verify(zkClient, times(1)).tableExist(TABLE_PATH);
        verify(metadataManager).getTable(TABLE_PATH);
        verify(metadataManager, never()).createTable(any(), any(), any(), any(), anyBoolean());

        ArgumentCaptor<CreateDatabaseRequest> databaseRequestCaptor =
                ArgumentCaptor.forClass(CreateDatabaseRequest.class);
        verify(coordinatorGateway).createDatabase(databaseRequestCaptor.capture());
        CreateDatabaseRequest databaseRequest = databaseRequestCaptor.getValue();
        assertThat(databaseRequest.getDatabaseName()).isEqualTo(TABLE_PATH.getDatabaseName());
        assertThat(databaseRequest.isIgnoreIfExists()).isTrue();

        ArgumentCaptor<CreateTableRequest> tableRequestCaptor =
                ArgumentCaptor.forClass(CreateTableRequest.class);
        verify(coordinatorGateway).createTable(tableRequestCaptor.capture());
        CreateTableRequest tableRequest = tableRequestCaptor.getValue();
        assertThat(tableRequest.getTablePath().getDatabaseName())
                .isEqualTo(TABLE_PATH.getDatabaseName());
        assertThat(tableRequest.getTablePath().getTableName()).isEqualTo(TABLE_PATH.getTableName());
        assertThat(tableRequest.isIgnoreIfExists()).isTrue();
        TableDescriptor descriptor = TableDescriptor.fromJsonBytes(tableRequest.getTableJson());
        assertThat(descriptor.getTableDistribution().get().getBucketCount().get())
                .isEqualTo(BUCKET_COUNT);
        assertThat(descriptor.getReplicationFactor()).isEqualTo(1);
    }

    @Test
    void testCreationFailureCanBeRetried() throws Exception {
        CompletableFuture<CreateTableResponse> failedCreation = new CompletableFuture<>();
        failedCreation.completeExceptionally(new IllegalStateException("creation failed"));
        when(coordinatorGateway.createTable(any(CreateTableRequest.class)))
                .thenReturn(
                        failedCreation,
                        CompletableFuture.completedFuture(new CreateTableResponse()));

        assertThatThrownBy(() -> tableManager.ensureTableAsync().get())
                .cause()
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class)
                .hasRootCauseMessage("creation failed");

        assertThat(tableManager.ensureTableAsync().get()).isEqualTo(BUCKET_COUNT);

        verify(coordinatorGateway, times(2)).createTable(any(CreateTableRequest.class));
        verify(metadataManager, times(1)).getTable(TABLE_PATH);
    }

    @Test
    void testExistingTableDoesNotCallCoordinator() throws Exception {
        when(zkClient.tableExist(TABLE_PATH)).thenReturn(true);

        assertThat(tableManager.ensureTableAsync().get()).isEqualTo(BUCKET_COUNT);

        verify(coordinatorGateway, never()).createDatabase(any(CreateDatabaseRequest.class));
        verify(coordinatorGateway, never()).createTable(any(CreateTableRequest.class));
    }

    private static TableInfo newTableInfo() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(ConsumerOffsetSchema.getTableSchema())
                        .logFormat(LogFormat.COMPACTED)
                        .distributedBy(BUCKET_COUNT, ConsumerOffsetSchema.getBucketKeyColumn())
                        .build()
                        .withBucketCount(BUCKET_COUNT)
                        .withReplicationFactor(1);
        return TableInfo.of(TABLE_PATH, 1L, 1, descriptor, "/tmp/fluss", 1L, 1L);
    }
}
