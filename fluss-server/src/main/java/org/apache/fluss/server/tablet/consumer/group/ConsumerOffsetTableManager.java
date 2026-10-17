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
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.utils.TableAssignmentUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TableAssignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Creates and resolves metadata for the {@code sys.consumer_offsets} system table. */
@Internal
public final class ConsumerOffsetTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerOffsetTableManager.class);

    private static final TablePath CONSUMER_OFFSET_TABLE_PATH =
            new TablePath(
                    ConsumerOffsetTableConfig.SYSTEM_DATABASE,
                    ConsumerOffsetTableConfig.CONSUMER_OFFSET_TABLE_NAME);

    private final ConsumerOffsetTableConfig tableConfig;
    private final ZooKeeperClient zkClient;
    private final MetadataManager metadataManager;
    private final TabletServerMetadataCache metadataCache;
    private final ExecutorService ioExecutor;
    private final AtomicReference<CompletableFuture<Integer>> tableInitFuture =
            new AtomicReference<>();

    public ConsumerOffsetTableManager(
            ConsumerOffsetTableConfig tableConfig,
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            TabletServerMetadataCache metadataCache,
            ExecutorService ioExecutor) {
        this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null");
        this.zkClient = checkNotNull(zkClient, "zkClient must not be null");
        this.metadataManager = checkNotNull(metadataManager, "metadataManager must not be null");
        this.metadataCache = checkNotNull(metadataCache, "metadataCache must not be null");
        this.ioExecutor = checkNotNull(ioExecutor, "ioExecutor must not be null");
    }

    public TablePath tablePath() {
        return CONSUMER_OFFSET_TABLE_PATH;
    }

    /** Returns the requested bucket from the local metadata snapshot without accessing ZK. */
    public Optional<BucketMetadata> getCachedBucketMetadata(int bucketId) {
        return metadataCache.getCachedBucketMetadata(CONSUMER_OFFSET_TABLE_PATH, bucketId);
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
                                "Failed to initialize " + CONSUMER_OFFSET_TABLE_PATH, cause);
        LOG.warn("Failed to initialize {}.", CONSUMER_OFFSET_TABLE_PATH, cause);
        initializationFuture.completeExceptionally(exception);
    }

    private void abortInitializationAfterFatalError(
            CompletableFuture<Integer> initializationFuture, Error error) {
        tableInitFuture.compareAndSet(initializationFuture, null);
        initializationFuture.completeExceptionally(error);
    }

    private int createOrResolveTableBucketCount() {
        try {
            if (zkClient.tableExist(CONSUMER_OFFSET_TABLE_PATH)) {
                LOG.debug("System table {} already exists.", CONSUMER_OFFSET_TABLE_PATH);
                return metadataManager.getTable(CONSUMER_OFFSET_TABLE_PATH).getNumBuckets();
            }

            String databaseName = CONSUMER_OFFSET_TABLE_PATH.getDatabaseName();
            if (!zkClient.databaseExists(databaseName)) {
                metadataManager.createDatabase(
                        databaseName, DatabaseDescriptor.builder().build(), true);
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
                            .property(ConfigOptions.TABLE_KV_TTL, tableConfig.getOffsetsRetention())
                            .property(
                                    ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION,
                                    KvValueLayout.TAGGED.version())
                            .build()
                            .withBucketCount(bucketCount)
                            .withReplicationFactor(replicationFactor);

            Set<TabletServerInfo> serverInfos = metadataCache.getAliveTabletServerInfos();
            TabletServerInfo[] servers = serverInfos.toArray(new TabletServerInfo[0]);
            if (servers.length == 0) {
                throw new ConsumerGroupOwnerNotAvailableException(
                        "No alive tablet server is available to create "
                                + CONSUMER_OFFSET_TABLE_PATH);
            }
            if (servers.length < replicationFactor) {
                throw new ConsumerGroupOwnerNotAvailableException(
                        "Not enough alive tablet servers to create "
                                + CONSUMER_OFFSET_TABLE_PATH
                                + ": required replication factor is "
                                + replicationFactor
                                + ", but only "
                                + servers.length
                                + " servers are available");
            }
            TableAssignment tableAssignment =
                    TableAssignmentUtils.generateAssignment(
                            bucketCount, replicationFactor, servers);

            try {
                metadataManager.createTable(
                        CONSUMER_OFFSET_TABLE_PATH,
                        zkClient.getDefaultRemoteDataDir(),
                        tableDescriptor,
                        tableAssignment,
                        true);
            } catch (Exception e) {
                if (!zkClient.tableExist(CONSUMER_OFFSET_TABLE_PATH)) {
                    throw e;
                }
                LOG.debug("System table {} was created concurrently.", CONSUMER_OFFSET_TABLE_PATH);
            }
            return metadataManager.getTable(CONSUMER_OFFSET_TABLE_PATH).getNumBuckets();
        } catch (Exception e) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "Failed to get or create system table: " + CONSUMER_OFFSET_TABLE_PATH, e);
        }
    }
}
