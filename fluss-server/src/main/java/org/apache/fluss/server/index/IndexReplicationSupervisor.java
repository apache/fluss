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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecordReadContext.SchemaResolution;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metadata.MetadataProvider;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.IndexTableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Owns the replication registration for one source replica. A leader that lacks index metadata
 * retries on the next metadata update. Stopping the replica unregisters its workers' source and
 * releases the replication resources.
 */
@Internal
public final class IndexReplicationSupervisor {

    /** Receives sync and all-index progress after an index window advances. */
    @FunctionalInterface
    public interface IndexProgressListener {
        /** Called with the current sync ack watermark and conservative all-index replay floor. */
        void onProgress(long syncIndexPushedOffset, long allIndexPushedOffset);
    }

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicationSupervisor.class);

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final TabletServerMetadataCache metadataCache;
    @Nullable private final MetadataProvider metadataProvider;
    private final IndexReplicationManager manager;
    private final Counter retries;
    // ReplicaManager serializes leadership changes, metadata refresh and shutdown.
    private boolean awaitingMetadata;

    // HW notifications also run outside the replica state-change lock.
    @Nullable private volatile IndexReplicator indexReplicator;

    IndexReplicationSupervisor(
            TableInfo tableInfo,
            TableBucket tableBucket,
            TabletServerMetadataCache metadataCache,
            @Nullable MetadataProvider metadataProvider,
            IndexReplicationManager manager,
            Counter retries) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.metadataCache = metadataCache;
        this.metadataProvider = metadataProvider;
        this.manager = checkNotNull(manager, "manager");
        this.retries = retries;
    }

    public boolean isAwaitingMetadata() {
        return awaitingMetadata;
    }

    /**
     * Called after the leader HW advances. Signals the read-pool worker owning this replicator so
     * it polls the newly committed WAL window. No-op when no replicator is active.
     */
    public void onHighWatermarkAdvanced() {
        IndexReplicator replicator = indexReplicator;
        if (replicator != null) {
            replicator.wakeup();
        }
    }

    /** Shares resolved index tables across deferred sources for one metadata update. */
    public static final class MetadataRetry {
        private final Map<Long, Optional<List<TableInfo>>> tablesBySource = new HashMap<>();

        private Optional<List<TableInfo>> resolve(IndexReplicationSupervisor source) {
            return tablesBySource.computeIfAbsent(
                    source.tableInfo.getTableId(), ignored -> source.resolveIndexTables());
        }
    }

    /** Starts the source at its replay floor. Leader transitions resolve metadata independently. */
    public void start(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            IndexProgressListener onProgress,
            long replayStartOffset,
            @Nullable MetadataRetry retry) {
        Optional<List<TableInfo>> tables =
                retry == null ? resolveIndexTables() : retry.resolve(this);
        if (!tables.isPresent()) {
            awaitingMetadata = true;
            retries.inc();
            return;
        }
        List<IndexReplicationEncoder> indexEncoders =
                IndexReplicationEncoder.createAll(tableInfo, tableBucket, tables.get());
        LogRecordReadContext readContext =
                LogRecordReadContext.createReadContext(
                        tableInfo, false, SchemaResolution.DYNAMIC, null, schemaGetter, null);
        IndexReplicator replicator =
                manager.createReplicator(
                        logTablet, indexEncoders, readContext, replayStartOffset, onProgress);
        // Register with the read pool; the pool worker will catch up on any WAL entries already
        // committed before this replicator was created.
        indexReplicator = replicator;
        try {
            manager.register(replicator);
        } catch (RuntimeException | Error failure) {
            indexReplicator = null;
            closeReplicator(replicator);
            throw failure;
        }
        awaitingMetadata = false;
        LOG.info("IndexReplicator (WAL-driven) registered for {}", tableBucket);
    }

    private Optional<List<TableInfo>> resolveIndexTables() {
        RuntimeException failure;
        try {
            return Optional.of(
                    IndexReplicationEncoder.resolveIndexTables(tableInfo, metadataCache));
        } catch (IllegalStateException missingOrInvalid) {
            failure = missingOrInvalid;
        }
        if (metadataProvider != null) {
            try {
                List<TablePath> paths = new ArrayList<>();
                for (Schema.Index index : tableInfo.getSchema().getIndexes()) {
                    paths.add(
                            IndexTableUtils.indexTablePath(
                                    tableInfo.getTablePath(), index.getIndexName()));
                }
                metadataProvider.getTablesMetadataFromZK(paths);
                return Optional.of(
                        IndexReplicationEncoder.resolveIndexTables(tableInfo, metadataCache));
            } catch (RuntimeException refreshFailure) {
                failure = refreshFailure;
            }
        }
        LOG.info(
                "Deferring index replication for table {} until the next metadata update.",
                tableInfo.getTablePath(),
                failure);
        return Optional.empty();
    }

    /** Detaches the source before changing its role, rebuilding its KV state or shutting down. */
    public void stop() {
        IndexReplicator r = indexReplicator;
        indexReplicator = null;
        awaitingMetadata = false;
        if (r == null) {
            return;
        }
        manager.unregister(r);
        closeReplicator(r);
    }

    private void closeReplicator(IndexReplicator replicator) {
        try {
            replicator.close();
        } catch (Exception e) {
            LOG.warn("Error closing IndexReplicator for {}", tableBucket, e);
        }
    }
}
