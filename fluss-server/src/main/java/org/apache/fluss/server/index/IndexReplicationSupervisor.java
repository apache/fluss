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
 * Per-{@code Replica} supervisor for source-table index replication.
 *
 * <p>This supervisor does not drive replication itself. It registers the leader-side {@link
 * IndexReplicator} with the server-global {@link IndexReplicationManager}. Its workers use {@link
 * IndexReplicationScheduler} to drive replication, staging encoded batches into the shared {@link
 * IndexSendBuffer} for {@link IndexSender} to dispatch. HW advances signal the owning worker to
 * poll.
 *
 * <h3>State machine</h3>
 *
 * <pre>
 *   NOT_STARTED --&gt; DEFERRED   (metadata not yet available)
 *   NOT_STARTED --&gt; RUNNING    (metadata available, replicator started)
 *   DEFERRED   --&gt; RUNNING    (metadata arrived, retry succeeded)
 *   RUNNING    --&gt; NOT_STARTED (become follower / close)
 * </pre>
 */
@Internal
public final class IndexReplicationSupervisor {

    /** Receives sync and all-index progress after an index window advances. */
    @FunctionalInterface
    public interface IndexProgressListener {
        /** Called with the current sync ack watermark and conservative all-index replay floor. */
        void onProgress(long syncIndexPushedOffset, long allIndexPushedOffset);
    }

    /** Lifecycle state of the index replicator for this replica. */
    enum State {
        NOT_STARTED,
        DEFERRED,
        RUNNING
    }

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicationSupervisor.class);

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final TabletServerMetadataCache metadataCache;
    @Nullable private final MetadataProvider metadataProvider;
    private final IndexReplicationManager manager;
    private volatile State state = State.NOT_STARTED;
    private final Object lifecycleLock = new Object();

    @Nullable private volatile IndexReplicator indexReplicator;

    IndexReplicationSupervisor(
            TableInfo tableInfo,
            TableBucket tableBucket,
            TabletServerMetadataCache metadataCache,
            @Nullable MetadataProvider metadataProvider,
            IndexReplicationManager manager) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.metadataCache = metadataCache;
        this.metadataProvider = metadataProvider;
        this.manager = checkNotNull(manager, "manager");
    }

    // ------------------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------------------

    /** Stops any previous source replicator before leader storage is rebuilt. */
    public void prepareForLeader() {
        stopIndexReplicator();
    }

    /**
     * Called when the replica becomes leader. If the table has secondary indexes and the metadata
     * is available, starts the {@link IndexReplicator} immediately; otherwise transitions to {@link
     * State#DEFERRED}.
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
        prepareForLeader();

        // Start IndexReplicator for main tables with secondary indexes
        List<Schema.Index> indexes = tableInfo.getSchema().getIndexes();
        if (indexes.isEmpty()) {
            return;
        }

        maybeStartIndexReplicator(logTablet, schemaGetter, indexes, onProgress, replayStartOffset);
    }

    /** Called when the replica becomes follower. Stops the index replicator if running. */
    public void onBecomeFollower() {
        stopIndexReplicator();
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
        if (state != State.DEFERRED) {
            return;
        }
        List<Schema.Index> indexes = tableInfo.getSchema().getIndexes();
        if (indexes.isEmpty()) {
            return;
        }
        maybeStartIndexReplicator(logTablet, schemaGetter, indexes, onProgress, replayStartOffset);
    }

    /** Called when the replica is being deleted. */
    public void close() {
        stopIndexReplicator();
    }

    // ------------------------------------------------------------------------------------
    // High-watermark callback
    // ------------------------------------------------------------------------------------

    /**
     * Called after the leader HW advances. Signals the read-pool worker owning this replicator so
     * it polls the newly committed WAL window. No-op when no replicator is active.
     */
    public void onHighWatermarkAdvanced() {
        IndexReplicator replicator = indexReplicator;
        if (replicator != null) {
            replicator.onHighWatermarkAdvanced();
            replicator.wakeup();
        }
    }

    /** Returns how long the active replicator has been behind without advancing. */
    public long noProgressTimeMs() {
        IndexReplicator r = this.indexReplicator;
        return state == State.RUNNING && r != null ? r.noProgressTimeMs() : 0L;
    }

    // ------------------------------------------------------------------------------------
    // State queries
    // ------------------------------------------------------------------------------------

    /**
     * Returns whether the source or one of its indexes has a reported failure awaiting progress.
     */
    public boolean hasFailure() {
        IndexReplicator replicator = indexReplicator;
        return replicator != null && replicator.hasFailure();
    }

    // ------------------------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------------------------

    private void maybeStartIndexReplicator(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            List<Schema.Index> indexes,
            IndexProgressListener onProgress,
            long replayStartOffset) {
        List<TablePath> indexTablePaths = new ArrayList<>(indexes.size());
        for (Schema.Index index : indexes) {
            indexTablePaths.add(
                    TablePath.of(
                            tableInfo.getTablePath().getDatabaseName(),
                            IndexTableUtils.indexTableName(
                                    tableInfo.getTablePath().getTableName(),
                                    index.getIndexName())));
        }

        List<IndexSpec> indexSpecs;
        try {
            indexSpecs = IndexSpecFactory.buildIndexSpecs(tableInfo, tableBucket, metadataCache);
        } catch (IllegalStateException initialFailure) {
            if (metadataProvider == null) {
                deferIndexReplication(initialFailure);
                return;
            }
            try {
                metadataProvider.getTablesMetadataFromZK(indexTablePaths);
                indexSpecs =
                        IndexSpecFactory.buildIndexSpecs(tableInfo, tableBucket, metadataCache);
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
                        logTablet, indexSpecs, readContext, replayStartOffset, onProgress);
        // Register with the read pool; the pool worker will catch up on any WAL entries already
        // committed before this replicator was created.
        installIndexReplicator(replicator);
        replicator.onHighWatermarkAdvanced();
        LOG.info("IndexReplicator (WAL-driven) registered for {}", tableBucket);
    }

    private void deferIndexReplication(RuntimeException metadataFailure) {
        state = State.DEFERRED;
        LOG.info(
                "Index table metadata is unavailable or inconsistent for {}; deferring "
                        + "IndexReplicator until the next metadata update.",
                tableBucket,
                metadataFailure);
    }

    void installIndexReplicator(IndexReplicator replicator) {
        RuntimeException registrationFailure = null;
        Error registrationError = null;
        synchronized (lifecycleLock) {
            State previousState = state;
            IndexReplicator previousReplicator = indexReplicator;
            indexReplicator = replicator;
            state = State.RUNNING;
            try {
                manager.register(replicator);
            } catch (RuntimeException e) {
                indexReplicator = previousReplicator;
                state = previousState;
                registrationFailure = e;
            } catch (Error e) {
                indexReplicator = previousReplicator;
                state = previousState;
                registrationError = e;
            }
        }
        if (registrationFailure != null || registrationError != null) {
            try {
                replicator.close();
            } catch (Exception e) {
                LOG.warn("Error closing unregistered IndexReplicator for {}", tableBucket, e);
            }
            if (registrationFailure != null) {
                throw registrationFailure;
            }
            throw registrationError;
        }
    }

    private void stopIndexReplicator() {
        IndexReplicator r;
        synchronized (lifecycleLock) {
            r = indexReplicator;
            indexReplicator = null;
            state = State.NOT_STARTED;
        }
        if (r == null) {
            return;
        }
        manager.unregister(r);
        try {
            r.close();
        } catch (Exception e) {
            LOG.warn("Error closing IndexReplicator for {}", tableBucket, e);
        }
    }
}
