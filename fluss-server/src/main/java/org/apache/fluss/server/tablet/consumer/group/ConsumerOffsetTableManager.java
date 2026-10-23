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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.ConsumerGroupOwnerLoadingException;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TabletServerRegistration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Creates and resolves metadata for the {@code sys.consumer_offsets} system table. */
@Internal
public final class ConsumerOffsetTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerOffsetTableManager.class);

    private final ConsumerOffsetTableConfig tableConfig;
    private final ZooKeeperClient zkClient;
    private final MetadataManager metadataManager;
    private final TabletServerMetadataCache metadataCache;
    private final TabletServerMetadataProvider metadataProvider;
    private final CoordinatorGateway coordinatorGateway;
    private final ExecutorService ioExecutor;
    private final AtomicReference<CompletableFuture<Integer>> tableInitFuture =
            new AtomicReference<>();

    public ConsumerOffsetTableManager(
            ConsumerOffsetTableConfig tableConfig,
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            TabletServerMetadataCache metadataCache,
            TabletServerMetadataProvider metadataProvider,
            CoordinatorGateway coordinatorGateway,
            ExecutorService ioExecutor) {
        this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null");
        this.zkClient = checkNotNull(zkClient, "zkClient must not be null");
        this.metadataManager = checkNotNull(metadataManager, "metadataManager must not be null");
        this.metadataCache = checkNotNull(metadataCache, "metadataCache must not be null");
        this.metadataProvider = checkNotNull(metadataProvider, "metadataProvider must not be null");
        this.coordinatorGateway =
                checkNotNull(coordinatorGateway, "coordinatorGateway must not be null");
        this.ioExecutor = checkNotNull(ioExecutor, "ioExecutor must not be null");
    }

    public TablePath tablePath() {
        return SystemTables.CONSUMER_OFFSETS;
    }

    /**
     * Resolves the owner of the requested bucket from the local metadata snapshot, falling back to
     * ZooKeeper when the bucket, leader, or advertised server endpoint is not yet cached. A
     * successful bucket fallback also refreshes the local metadata cache.
     */
    public CompletableFuture<ServerNode> resolveOwnerAsync(int bucketId, String listenerName) {
        Optional<BucketMetadata> cachedBucket =
                metadataCache.getCachedBucketMetadata(SystemTables.CONSUMER_OFFSETS, bucketId);
        if (cachedBucket.isPresent() && cachedBucket.get().getLeaderId().isPresent()) {
            Optional<ServerNode> cachedOwner =
                    metadataCache.getTabletServer(
                            cachedBucket.get().getLeaderId().getAsInt(), listenerName);
            if (cachedOwner.isPresent()) {
                return CompletableFuture.completedFuture(cachedOwner.get());
            }
        }

        return CompletableFuture.supplyAsync(
                () -> resolveOwnerFromZooKeeper(bucketId, listenerName), ioExecutor);
    }

    private ServerNode resolveOwnerFromZooKeeper(int bucketId, String listenerName) {
        try {
            List<TableMetadata> tableMetadataList =
                    metadataProvider.getTablesMetadataFromZK(
                            Collections.singleton(SystemTables.CONSUMER_OFFSETS));
            BucketMetadata bucketMetadata =
                    tableMetadataList.get(0).getBucketMetadataList().stream()
                            .filter(metadata -> metadata.getBucketId() == bucketId)
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new ConsumerGroupOwnerNotAvailableException(
                                                    "Bucket "
                                                            + bucketId
                                                            + " is not available yet in "
                                                            + SystemTables.CONSUMER_OFFSETS));
            OptionalInt leaderIdOpt = bucketMetadata.getLeaderId();
            if (!leaderIdOpt.isPresent()) {
                throw new ConsumerGroupOwnerLoadingException(
                        "No leader yet for bucket "
                                + bucketId
                                + " in "
                                + SystemTables.CONSUMER_OFFSETS);
            }

            int leaderId = leaderIdOpt.getAsInt();
            Optional<ServerNode> cachedOwner =
                    metadataCache.getTabletServer(leaderId, listenerName);
            if (cachedOwner.isPresent()) {
                return cachedOwner.get();
            }

            TabletServerRegistration registration =
                    zkClient.getTabletServer(leaderId)
                            .orElseThrow(
                                    () ->
                                            new ConsumerGroupOwnerNotAvailableException(
                                                    "Group owner node "
                                                            + leaderId
                                                            + " is not registered in ZooKeeper"));
            ServerNode owner =
                    new ServerInfo(
                                    leaderId,
                                    registration.getRack(),
                                    registration.getEndpoints(),
                                    ServerType.TABLET_SERVER,
                                    registration.getResource())
                            .node(listenerName);
            if (owner == null) {
                throw new ConsumerGroupOwnerNotAvailableException(
                        "Group owner node "
                                + leaderId
                                + " is not advertised on listener "
                                + listenerName);
            }
            return owner;
        } catch (ConsumerGroupOwnerLoadingException | ConsumerGroupOwnerNotAvailableException e) {
            throw e;
        } catch (Exception e) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "Failed to resolve the owner of "
                            + SystemTables.CONSUMER_OFFSETS
                            + " bucket "
                            + bucketId,
                    e);
        }
    }

    /**
     * Ensures that the consumer-offset table exists without blocking an RPC worker thread.
     * Concurrent requests share the same initialization future. The completed future contains the
     * actual bucket count of the existing or newly created table.
     */
    public CompletableFuture<Integer> ensureTableAsync() {
        // Retry only when another thread wins the CAS and its initialization is concurrently
        // cleared after a fast failure. This loop never waits for the initialization itself.
        while (true) {
            CompletableFuture<Integer> current = tableInitFuture.get();
            if (current != null) {
                return current;
            }

            CompletableFuture<Integer> created = new CompletableFuture<>();
            if (tableInitFuture.compareAndSet(null, created)) {
                submitTableInitialization(created);
                return created;
            }
        }
    }

    private void submitTableInitialization(CompletableFuture<Integer> initializationFuture) {
        try {
            ioExecutor.execute(
                    () -> {
                        try {
                            initializationFuture.complete(createOrResolveTableBucketCount());
                        } catch (Exception e) {
                            completeInitializationExceptionally(initializationFuture, e);
                        } catch (Error error) {
                            abortInitializationAfterFatalError(initializationFuture, error);
                            throw error;
                        }
                    });
        } catch (Exception e) {
            completeInitializationExceptionally(initializationFuture, e);
        } catch (Error error) {
            abortInitializationAfterFatalError(initializationFuture, error);
            throw error;
        }
    }

    private void completeInitializationExceptionally(
            CompletableFuture<Integer> initializationFuture, Exception cause) {
        tableInitFuture.compareAndSet(initializationFuture, null);
        ConsumerGroupOwnerNotAvailableException exception =
                cause instanceof ConsumerGroupOwnerNotAvailableException
                        ? (ConsumerGroupOwnerNotAvailableException) cause
                        : new ConsumerGroupOwnerNotAvailableException(
                                "Failed to initialize " + SystemTables.CONSUMER_OFFSETS, cause);
        LOG.warn("Failed to initialize {}.", SystemTables.CONSUMER_OFFSETS, cause);
        initializationFuture.completeExceptionally(exception);
    }

    private void abortInitializationAfterFatalError(
            CompletableFuture<Integer> initializationFuture, Error error) {
        tableInitFuture.compareAndSet(initializationFuture, null);
        initializationFuture.completeExceptionally(error);
    }

    private int createOrResolveTableBucketCount() {
        try {
            if (zkClient.tableExist(SystemTables.CONSUMER_OFFSETS)) {
                LOG.debug("System table {} already exists.", SystemTables.CONSUMER_OFFSETS);
                return metadataManager.getTable(SystemTables.CONSUMER_OFFSETS).getNumBuckets();
            }

            String databaseName = SystemTables.CONSUMER_OFFSETS.getDatabaseName();
            if (!zkClient.databaseExists(databaseName)) {
                CreateDatabaseRequest createDatabaseRequest =
                        new CreateDatabaseRequest()
                                .setDatabaseName(databaseName)
                                .setDatabaseJson(DatabaseDescriptor.builder().build().toJsonBytes())
                                .setIgnoreIfExists(true);
                coordinatorGateway.createDatabase(createDatabaseRequest).get();
            }

            int bucketCount = tableConfig.getOffsetsTableNumBuckets();
            int replicationFactor = tableConfig.getOffsetsTableReplicationFactor();
            TableDescriptor tableDescriptor =
                    TableDescriptor.builder()
                            .schema(ConsumerOffsetSchema.getTableSchema())
                            .logFormat(LogFormat.COMPACTED)
                            .distributedBy(bucketCount, ConsumerOffsetSchema.getBucketKeyColumn())
                            .property(
                                    ConfigOptions.TABLE_LOG_TTL, tableConfig.getOffsetsRetention())
                            .property(
                                    ConfigOptions.TABLE_LOG_LOCAL_TTL,
                                    ConsumerOffsetTableConfig.OFFSETS_TABLE_LOCAL_LOG_TTL)
                            .property(ConfigOptions.TABLE_KV_TTL, tableConfig.getOffsetsRetention())
                            .property(ConfigOptions.TABLE_REPLICATION_FACTOR, replicationFactor)
                            .build();

            CreateTableRequest createTableRequest =
                    new CreateTableRequest()
                            .setTableJson(tableDescriptor.toJsonBytes())
                            .setIgnoreIfExists(true);
            createTableRequest
                    .setTablePath()
                    .setDatabaseName(SystemTables.CONSUMER_OFFSETS.getDatabaseName())
                    .setTableName(SystemTables.CONSUMER_OFFSETS.getTableName());
            coordinatorGateway.createTable(createTableRequest).get();
            return metadataManager.getTable(SystemTables.CONSUMER_OFFSETS).getNumBuckets();
        } catch (Exception e) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "Failed to get or create system table: " + SystemTables.CONSUMER_OFFSETS, e);
        }
    }
}
