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
import java.util.List;

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
    private volatile boolean awaitingMetadata;
    private final Object lifecycleLock = new Object();

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

    /**
     * Called when the replica becomes leader. If the table has secondary indexes and the metadata
     * is available, starts the {@link IndexReplicator} immediately; otherwise waits for a metadata
     * update.
     *
     * @param logTablet the log tablet of the leader replica
     * @param schemaGetter the schema getter for the table
     * @param onProgress callback fired after each window completes with sync/all index progress;
     *     typically wired to {@code Replica::advanceIndexProgress}
     * @param replayStartOffset the non-negative exclusive next source WAL offset restored by the
     *     owning replica when the table has secondary indexes; ignored when it has none
     */
    public void onBecomeLeader(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            IndexProgressListener onProgress,
            long replayStartOffset) {
        stop();
        List<Schema.Index> indexes = tableInfo.getSchema().getIndexes();
        if (indexes.isEmpty()) {
            return;
        }

        maybeStartIndexReplicator(logTablet, schemaGetter, indexes, onProgress, replayStartOffset);
    }

    /**
     * Retries starting the index replicator if it was previously deferred due to missing metadata.
     * Called by {@code ReplicaManager} after a metadata update.
     */
    public void retryDeferredStart(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            IndexProgressListener onProgress,
            long replayStartOffset) {
        if (!awaitingMetadata) {
            return;
        }
        List<Schema.Index> indexes = tableInfo.getSchema().getIndexes();
        if (indexes.isEmpty()) {
            return;
        }
        maybeStartIndexReplicator(logTablet, schemaGetter, indexes, onProgress, replayStartOffset);
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

    private void maybeStartIndexReplicator(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            List<Schema.Index> indexes,
            IndexProgressListener onProgress,
            long replayStartOffset) {
        List<IndexReplicationEncoder> indexEncoders;
        try {
            indexEncoders =
                    IndexReplicationEncoder.createAll(tableInfo, tableBucket, metadataCache);
        } catch (IllegalStateException initialFailure) {
            if (metadataProvider == null) {
                deferIndexReplication(initialFailure);
                return;
            }
            try {
                List<TablePath> indexTablePaths = new ArrayList<>(indexes.size());
                for (Schema.Index index : indexes) {
                    indexTablePaths.add(
                            IndexTableUtils.indexTablePath(
                                    tableInfo.getTablePath(), index.getIndexName()));
                }
                metadataProvider.getTablesMetadataFromZK(indexTablePaths);
                indexEncoders =
                        IndexReplicationEncoder.createAll(tableInfo, tableBucket, metadataCache);
            } catch (RuntimeException refreshFailure) {
                deferIndexReplication(refreshFailure);
                return;
            }
        }
        LogRecordReadContext readContext =
                LogRecordReadContext.createReadContext(
                        tableInfo, false, SchemaResolution.DYNAMIC, null, schemaGetter, null);
        IndexReplicator replicator =
                manager.createReplicator(
                        logTablet, indexEncoders, readContext, replayStartOffset, onProgress);
        // Register with the read pool; the pool worker will catch up on any WAL entries already
        // committed before this replicator was created.
        installIndexReplicator(replicator);
        LOG.info("IndexReplicator (WAL-driven) registered for {}", tableBucket);
    }

    private void deferIndexReplication(RuntimeException metadataFailure) {
        awaitingMetadata = true;
        retries.inc();
        LOG.info(
                "Index table metadata is unavailable or inconsistent for {}; deferring "
                        + "IndexReplicator until the next metadata update.",
                tableBucket,
                metadataFailure);
    }

    private void installIndexReplicator(IndexReplicator replicator) {
        try {
            synchronized (lifecycleLock) {
                IndexReplicator previous = indexReplicator;
                boolean wasAwaitingMetadata = awaitingMetadata;
                indexReplicator = replicator;
                awaitingMetadata = false;
                try {
                    manager.register(replicator);
                } catch (RuntimeException | Error failure) {
                    indexReplicator = previous;
                    awaitingMetadata = wasAwaitingMetadata;
                    throw failure;
                }
            }
        } catch (RuntimeException | Error failure) {
            closeReplicator(replicator);
            throw failure;
        }
    }

    /** Detaches the source before changing its role, rebuilding its KV state or shutting down. */
    public void stop() {
        IndexReplicator r;
        synchronized (lifecycleLock) {
            r = indexReplicator;
            indexReplicator = null;
            awaitingMetadata = false;
        }
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
