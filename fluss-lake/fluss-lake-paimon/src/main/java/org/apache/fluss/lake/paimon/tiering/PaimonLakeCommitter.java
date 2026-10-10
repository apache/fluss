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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.paimon.tiering.markdone.PaimonPartitionMarkDone;
import org.apache.fluss.lake.paimon.tiering.markdone.PartitionMarkDoneState;
import org.apache.fluss.lake.paimon.tiering.markdone.PartitionMarkDoneStateJsonSerde;
import org.apache.fluss.lake.paimon.utils.DvTableReadableSnapshotRetriever;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/** Implementation of {@link LakeCommitter} for Paimon. */
public class PaimonLakeCommitter
        implements PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeCommitter.class);

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;
    private final String commitUser;
    private final TablePath tablePath;
    private final TablePath lakeTablePath;
    private final long tableId;
    private final Configuration flussClientConfig;
    private final TableInfo tableInfo;
    private final boolean markDoneEnabled;
    private TableCommitImpl tableCommit;

    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();

    public PaimonLakeCommitter(
            PaimonCatalogProvider paimonCatalogProvider, CommitterInitContext committerInitContext)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        this.tableInfo = committerInitContext.tableInfo();
        this.tablePath = committerInitContext.tablePath();
        this.lakeTablePath = tableInfo.getLakeTablePath();
        this.tableId = tableInfo.getTableId();
        this.flussClientConfig = committerInitContext.flussClientConfig();
        this.fileStoreTable =
                getTable(
                        lakeTablePath,
                        tableInfo.getTableConfig().isDataLakeAutoExpireSnapshot()
                                || committerInitContext
                                        .lakeTieringConfig()
                                        .get(ConfigOptions.LAKE_TIERING_AUTO_EXPIRE_SNAPSHOT));
        this.commitUser = fileStoreTable.coreOptions().createCommitUser();
        this.markDoneEnabled =
                committerInitContext
                                .lakeTieringConfig()
                                .get(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                        && PaimonPartitionMarkDone.isEnabled(fileStoreTable, tableInfo);
    }

    @Override
    public PaimonCommittable toCommittable(List<PaimonWriteResult> paimonWriteResults)
            throws IOException {
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        for (PaimonWriteResult paimonWriteResult : paimonWriteResults) {
            for (CommitMessage commitMessage : paimonWriteResult.commitMessages()) {
                committable.addFileCommittable(commitMessage);
            }
        }
        return new PaimonCommittable(committable);
    }

    @Override
    public LakeCommitResult commit(
            PaimonCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        ManifestCommittable manifestCommittable = committable.manifestCommittable();
        snapshotProperties.forEach(manifestCommittable::addProperty);

        try {
            long committedSnapshotId = commit(manifestCommittable);

            // Collect cumulative table stats from the exact snapshot that was just committed.
            TieringStats stats = computeTableStats();

            // deletion vector is disabled, committed snapshot is readable
            if (!fileStoreTable.coreOptions().deletionVectorsEnabled()) {
                return LakeCommitResult.committedIsReadable(committedSnapshotId, stats);
            } else {
                // retrieve the readable snapshot during commit
                try (DvTableReadableSnapshotRetriever retriever =
                        new DvTableReadableSnapshotRetriever(
                                tablePath, tableId, fileStoreTable, flussClientConfig)) {
                    DvTableReadableSnapshotRetriever.ReadableSnapshotResult readableSnapshotResult =
                            retriever.getReadableSnapshotAndOffsets(committedSnapshotId);
                    if (readableSnapshotResult == null) {
                        return LakeCommitResult.unknownReadableSnapshot(committedSnapshotId, stats);
                    } else {
                        long earliestSnapshotIdToKeep =
                                readableSnapshotResult.getEarliestSnapshotIdToKeep();
                        if (earliestSnapshotIdToKeep >= 0) {
                            LOG.info(
                                    "earliest snapshot ID to keep for table {} is {}. "
                                            + "Snapshots before this ID can be safely deleted from Fluss.",
                                    tablePath,
                                    earliestSnapshotIdToKeep);
                        }
                        return LakeCommitResult.withReadableSnapshot(
                                committedSnapshotId,
                                readableSnapshotResult.getReadableSnapshotId(),
                                readableSnapshotResult.getTieredOffsets(),
                                readableSnapshotResult.getReadableOffsets(),
                                earliestSnapshotIdToKeep,
                                stats);
                    }
                }
            }
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    @Override
    public boolean preparePartitionMarkDone(PaimonCommittable committable) {
        if (!markDoneEnabled) {
            return false;
        }
        ManifestCommittable manifestCommittable = committable.manifestCommittable();
        String stateJson = null;
        boolean stateChanged = false;
        try (PaimonPartitionMarkDone partitionMarkDone =
                new PaimonPartitionMarkDone(fileStoreTable, tableInfo)) {
            CommittedLakeSnapshot latestCommit = loadLatestFlussCommit(null);
            stateJson =
                    latestCommit == null
                            ? null
                            : latestCommit
                                    .getSnapshotProperties()
                                    .get(PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY);
            PartitionMarkDoneState previousState = parseMarkDoneState(stateJson);
            PartitionMarkDoneState newState =
                    partitionMarkDone.markIdlePartitionsDone(
                            previousState,
                            partitionMarkDone.extractTieredPartitions(manifestCommittable));
            stateChanged = !newState.equals(previousState);
            stateJson = PartitionMarkDoneStateJsonSerde.toJson(newState);
        } catch (Exception e) {
            stateChanged = false;
            LOG.warn(
                    "Failed to prepare partition mark-done for table {}, will retry in a later round.",
                    tablePath,
                    e);
        }
        if (stateJson != null) {
            manifestCommittable.addProperty(
                    PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY, stateJson);
        }
        return stateChanged;
    }

    private PartitionMarkDoneState parseMarkDoneState(@Nullable String markDoneStateJson) {
        if (markDoneStateJson == null) {
            return PartitionMarkDoneState.empty();
        }
        try {
            return PartitionMarkDoneStateJsonSerde.fromJson(markDoneStateJson);
        } catch (UnsupportedVersionException e) {
            // The caller skips mark-done and retains the original JSON for data commits.
            throw e;
        } catch (Exception e) {
            LOG.warn(
                    "Corrupt mark-done state of table {}, re-initializing via cold start.",
                    tablePath,
                    e);
            return PartitionMarkDoneState.empty();
        }
    }

    /** Commits a Paimon snapshot and returns its ID recorded by {@link PaimonCommitCallback}. */
    private long commit(ManifestCommittable manifestCommittable) throws Exception {
        // clear any residue left by a previous failed commit on the same thread
        currentCommitSnapshotId.remove();
        try {
            tableCommit = fileStoreTable.newCommit(commitUser);
            // don't skip empty commits: tiering relies on empty snapshots to persist bucket
            // offsets when only empty WAL batches were consumed, and mark-done maintenance
            // commits properties-only snapshots
            tableCommit.ignoreEmptyCommit(false);
            tableCommit.commit(manifestCommittable);
            return checkNotNull(
                    currentCommitSnapshotId.get(),
                    "Paimon committed snapshot id must be non-null.");
        } finally {
            currentCommitSnapshotId.remove();
        }
    }

    /** Computes cumulative table stats from the latest snapshot by REST API. */
    @Nullable
    private TieringStats computeTableStats() {
        Identifier identifier =
                new Identifier(lakeTablePath.getDatabaseName(), lakeTablePath.getTableName());
        try {
            Optional<TableSnapshot> snapshot = paimonCatalog.loadSnapshot(identifier);
            if (!snapshot.isPresent()) {
                LOG.warn(
                        "No snapshot found for table {}, "
                                + "fileSize and recordCount will be reported as -1.",
                        tablePath);
                return null;
            }
            TableSnapshot tableSnapshot = snapshot.get();
            return new TieringStats(tableSnapshot.fileSizeInBytes(), tableSnapshot.recordCount());
        } catch (Exception e) {
            LOG.debug(
                    "Failed to load snapshot for table {}, "
                            + "fileSize and recordCount will be reported as -1.",
                    tablePath,
                    e);
            return null;
        }
    }

    @Override
    public void abort(PaimonCommittable committable) throws IOException {
        tableCommit = fileStoreTable.newCommit(commitUser);
        tableCommit.abort(committable.manifestCommittable().fileCommittables());
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        CommittedLakeSnapshot latestCommit = loadLatestFlussCommit(latestLakeSnapshotIdOfFluss);
        if (latestCommit != null
                && !latestCommit
                        .getSnapshotProperties()
                        .containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)) {
            throw new IOException("Failed to load committed lake snapshot properties from Paimon.");
        }
        return latestCommit;
    }

    /**
     * Loads the latest Fluss snapshot ID and its commit properties. An empty properties map means
     * the commit exists but its metadata is unavailable; null means no newer Fluss commit exists.
     */
    @Nullable
    private CommittedLakeSnapshot loadLatestFlussCommit(@Nullable Long knownSnapshotId)
            throws IOException {
        Snapshot latestSnapshot = getCommittedLatestSnapshotOfLake();
        // we get the latest snapshot committed by fluss,
        // but the latest snapshot is not greater than knownSnapshotId, no any missing
        // snapshot, return directly
        if (latestSnapshot == null
                || (knownSnapshotId != null && latestSnapshot.id() <= knownSnapshotId)) {
            return null;
        }
        Snapshot propertiesSnapshot = findLatestSnapshotWithOffsets(latestSnapshot);
        return new CommittedLakeSnapshot(
                latestSnapshot.id(),
                propertiesSnapshot == null
                        ? Collections.emptyMap()
                        : propertiesSnapshot.properties());
    }

    @Nullable
    private Snapshot getCommittedLatestSnapshotOfLake() throws IOException {
        // get the latest snapshot committed by fluss or latest committed id
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long userCommittedSnapshotIdOrLatestCommitId =
                fileStoreTable
                        .snapshotManager()
                        .pickOrLatest(
                                snapshot -> isFlussLakeTieringCommitUser(snapshot.commitUser()));
        // no any snapshot, return null directly
        if (userCommittedSnapshotIdOrLatestCommitId == null) {
            return null;
        }

        // pick the snapshot
        Snapshot snapshot = snapshotManager.tryGetSnapshot(userCommittedSnapshotIdOrLatestCommitId);

        if (!isFlussLakeTieringCommitUser(snapshot.commitUser())) {
            // the snapshot is still not committed by Fluss, return directly
            return null;
        }
        return snapshot;
    }

    /**
     * Every Fluss commit advancing tiering offsets persists the offsets property. Later Paimon
     * maintenance snapshots do not advance those offsets and may omit the property.
     */
    @Nullable
    private Snapshot findLatestSnapshotWithOffsets(Snapshot latestSnapshot) throws IOException {
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long earliestId = snapshotManager.earliestSnapshotId();
        if (earliestId == null) {
            return null;
        }
        for (long id = latestSnapshot.id(); id >= earliestId; id--) {
            try {
                Snapshot snapshot =
                        id == latestSnapshot.id()
                                ? latestSnapshot
                                : snapshotManager.tryGetSnapshot(id);
                if (!isFlussLakeTieringCommitUser(snapshot.commitUser())) {
                    continue;
                }
                Map<String, String> properties = snapshot.properties();
                if (properties != null
                        && properties.containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)) {
                    return snapshot;
                }
                // A legacy data commit without offsets must not borrow an older round's offsets.
                if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
                    return null;
                }
            } catch (FileNotFoundException ignored) {
                // The snapshot may have expired during the lookup.
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        try {
            if (tableCommit != null) {
                tableCommit.close();
            }
            if (paimonCatalog != null) {
                paimonCatalog.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close PaimonLakeCommitter.", e);
        }
    }

    private FileStoreTable getTable(TablePath tablePath, boolean isAutoSnapshotExpiration)
            throws IOException {
        try {
            FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));

            Map<String, String> dynamicOptions = new HashMap<>();
            dynamicOptions.put(
                    CoreOptions.COMMIT_CALLBACKS.key(),
                    PaimonLakeCommitter.PaimonCommitCallback.class.getName());
            dynamicOptions.put(
                    CoreOptions.COMMIT_USER_PREFIX.key(), FLUSS_LAKE_TIERING_COMMIT_USER);

            boolean writeOnly = !isAutoSnapshotExpiration;
            dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), Boolean.toString(writeOnly));

            // For non-write-only modes, we enable 'end-input.check-partition-expire' to ensure
            // Paimon triggers partition expiration on every commit.
            // Note: This is necessary even if 'paimon.partition.expiration-check-interval' is
            // already configured. Because the Fluss tiering service creates a fresh TableCommit
            // instance for each commit, the interval-based expiration check will not be triggered
            // correctly otherwise.
            if (!writeOnly) {
                dynamicOptions.put(
                        CoreOptions.END_INPUT_CHECK_PARTITION_EXPIRE.key(),
                        Boolean.TRUE.toString());
            }

            return table.copy(dynamicOptions);
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Paimon.", e);
        }
    }

    private static boolean isFlussLakeTieringCommitUser(String commitUser) {
        return commitUser.startsWith(FLUSS_LAKE_TIERING_COMMIT_USER);
    }

    /** A {@link CommitCallback} to save paimon commit snapshot info. */
    public static class PaimonCommitCallback implements CommitCallback {

        @Override
        public void call(Context context) {
            currentCommitSnapshotId.set(context.snapshot.id());
        }

        @Override
        public void retry(ManifestCommittable manifestCommittable) {
            // do-nothing
        }

        @Override
        public void close() throws Exception {
            // do-nothing
        }
    }
}
