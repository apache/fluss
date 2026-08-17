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
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionMarkDoneMaintainer;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.paimon.utils.DvTableReadableSnapshotRetriever;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.function.SupplierWithException;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/** Implementation of {@link LakeCommitter} for Paimon. */
public class PaimonLakeCommitter
        implements LakeCommitter<PaimonWriteResult, PaimonCommittable>,
                PartitionMarkDoneMaintainer {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeCommitter.class);

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;
    private final TablePath tablePath;
    private final long tableId;
    private final Configuration flussClientConfig;
    @Nullable private final PaimonPartitionMarkDone partitionMarkDone;
    private TableCommitImpl tableCommit;

    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();

    public PaimonLakeCommitter(
            PaimonCatalogProvider paimonCatalogProvider, CommitterInitContext committerInitContext)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        this.tablePath = committerInitContext.tablePath();
        this.tableId = committerInitContext.tableInfo().getTableId();
        this.flussClientConfig = committerInitContext.flussClientConfig();
        this.fileStoreTable =
                getTable(
                        committerInitContext.tablePath(),
                        committerInitContext
                                        .tableInfo()
                                        .getTableConfig()
                                        .isDataLakeAutoExpireSnapshot()
                                || committerInitContext
                                        .lakeTieringConfig()
                                        .get(ConfigOptions.LAKE_TIERING_AUTO_EXPIRE_SNAPSHOT));
        PaimonPartitionMarkDone partitionMarkDone = null;
        if (committerInitContext
                        .lakeTieringConfig()
                        .get(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                && PaimonPartitionMarkDone.isEnabled(committerInitContext.tableInfo())) {
            try {
                partitionMarkDone =
                        new PaimonPartitionMarkDone(
                                fileStoreTable, committerInitContext.tableInfo());
            } catch (Exception e) {
                // an invalid mark-done configuration only disables mark-done, never fails
                // the committer
                LOG.warn(
                        "Invalid partition mark-done configuration for table {}, "
                                + "partition mark-done is disabled.",
                        tablePath,
                        e);
            }
        }
        this.partitionMarkDone = partitionMarkDone;
    }

    @Override
    public PaimonCommittable toCommittable(List<PaimonWriteResult> paimonWriteResults)
            throws IOException {
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        for (PaimonWriteResult paimonWriteResult : paimonWriteResults) {
            committable.addFileCommittable(paimonWriteResult.commitMessage());
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
            if (partitionMarkDone != null) {
                runPartitionMarkDone(manifestCommittable);
            }

            long committedSnapshotId = commitManifest(manifestCommittable);

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

    /**
     * Runs partition mark-done for a data commit and attaches the state (re-attached even if
     * unchanged) so the latest Fluss-committed snapshot always holds the full state. Known runtime
     * mark-done failures are only logged and don't fail the data commit: the previous state is
     * re-attached if readable, otherwise the next round re-initializes via cold start — a lossy
     * last resort that can't recover zero-file pending partitions.
     */
    private void runPartitionMarkDone(ManifestCommittable manifestCommittable) {
        checkNotNull(partitionMarkDone);
        String stateJson = null;
        try {
            String previousStateJson = getLatestMarkDoneStateJson();
            stateJson = previousStateJson;
            Set<String> tieredPartitions =
                    partitionMarkDone.extractTieredPartitions(
                            manifestCommittable.fileCommittables());
            String newStateJson = partitionMarkDone.run(previousStateJson, tieredPartitions);
            if (newStateJson != null) {
                stateJson = newStateJson;
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to run partition mark-done for table {}, "
                            + "the data commit continues without it.",
                    tablePath,
                    e);
        }
        if (stateJson != null) {
            manifestCommittable.addProperty(
                    PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY, stateJson);
        }
    }

    @Nullable
    private String getLatestMarkDoneStateJson() throws IOException {
        Snapshot latestFlussSnapshot =
                getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);
        if (latestFlussSnapshot == null) {
            return null;
        }
        // null when the properties snapshot can't be found (e.g. expired): re-initialize via
        // cold start, the mark-done actions are idempotent
        Snapshot propertiesSnapshot = findRoundPropertiesSnapshot(latestFlussSnapshot);
        return propertiesSnapshot == null
                ? null
                : propertiesSnapshot
                        .properties()
                        .get(PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY);
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot commitMarkDoneMaintenance(
            SupplierWithException<String, IOException> offsetsFileProvider) throws IOException {
        if (partitionMarkDone == null) {
            return null;
        }
        // mark-done itself is best-effort: a failure only skips this round and is retried in
        // a later round; failures of the snapshot commit below still propagate
        String newStateJson;
        try {
            Snapshot latestFlussSnapshot =
                    getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);
            if (latestFlussSnapshot == null) {
                // never tiered by Fluss, cold start happens with the first data commit
                return null;
            }
            // null when the properties snapshot can't be found (e.g. expired): re-initialize
            // via cold start, the mark-done actions are idempotent
            Snapshot propertiesSnapshot = findRoundPropertiesSnapshot(latestFlussSnapshot);
            String previousStateJson =
                    propertiesSnapshot == null
                            ? null
                            : propertiesSnapshot
                                    .properties()
                                    .get(PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY);
            newStateJson = partitionMarkDone.run(previousStateJson, Collections.emptySet());
        } catch (Exception e) {
            LOG.warn(
                    "Failed to run partition mark-done maintenance for table {}, "
                            + "will retry in a later round.",
                    tablePath,
                    e);
            return null;
        }
        if (newStateJson == null) {
            return null;
        }

        try {
            // persist the new state with a properties-only snapshot carrying a freshly
            // prepared offsets file: offsets files are deleted along with their snapshot
            // metadata, so they must never be shared across snapshots
            String offsetsFilePath = offsetsFileProvider.get();
            Map<String, String> snapshotProperties = new HashMap<>();
            snapshotProperties.put(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsetsFilePath);
            snapshotProperties.put(PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY, newStateJson);

            ManifestCommittable manifestCommittable = new ManifestCommittable(COMMIT_IDENTIFIER);
            snapshotProperties.forEach(manifestCommittable::addProperty);
            long committedSnapshotId = commitManifest(manifestCommittable);
            return new CommittedLakeSnapshot(committedSnapshotId, snapshotProperties);
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    /**
     * Commits the manifest committable and returns the id of the snapshot created for it, as
     * recorded by {@link PaimonCommitCallback}. Shared by the data commit and the mark-done
     * maintenance commit which both carry the bucket offsets property of the round.
     */
    private long commitManifest(ManifestCommittable manifestCommittable) throws Exception {
        // clear any residue left by a previous failed commit on the same thread
        currentCommitSnapshotId.remove();
        try {
            tableCommit = fileStoreTable.newCommit(FLUSS_LAKE_TIERING_COMMIT_USER);
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
                new Identifier(tablePath.getDatabaseName(), tablePath.getTableName());
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
        tableCommit = fileStoreTable.newCommit(FLUSS_LAKE_TIERING_COMMIT_USER);
        tableCommit.abort(committable.manifestCommittable().fileCommittables());
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        Snapshot latestLakeSnapshotOfLake =
                getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);
        if (latestLakeSnapshotOfLake == null) {
            return null;
        }

        // we get the latest snapshot committed by fluss,
        // but the latest snapshot is not greater than latestLakeSnapshotIdOfFluss, no any missing
        // snapshot, return directly
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshotOfLake.id() <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        // the round may end with a maintenance tail (e.g. partition expiration), its offsets
        // and state live on the nearest properties-carrying snapshot of the same round
        Snapshot propertiesSnapshot = findRoundPropertiesSnapshot(latestLakeSnapshotOfLake);
        if (propertiesSnapshot == null) {
            throw new IOException("Failed to load committed lake snapshot properties from Paimon.");
        }

        return new CommittedLakeSnapshot(
                latestLakeSnapshotOfLake.id(), propertiesSnapshot.properties());
    }

    @Nullable
    private Snapshot getCommittedLatestSnapshotOfLake(String commitUser) throws IOException {
        // get the latest snapshot committed by fluss or latest committed id
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long userCommittedSnapshotIdOrLatestCommitId =
                fileStoreTable
                        .snapshotManager()
                        .pickOrLatest((snapshot -> snapshot.commitUser().equals(commitUser)));
        // no any snapshot, return null directly
        if (userCommittedSnapshotIdOrLatestCommitId == null) {
            return null;
        }

        // pick the snapshot
        Snapshot snapshot = snapshotManager.tryGetSnapshot(userCommittedSnapshotIdOrLatestCommitId);

        if (!snapshot.commitUser().equals(commitUser)) {
            // the snapshot is still not committed by Fluss, return directly
            return null;
        }
        return snapshot;
    }

    /**
     * Finds the snapshot carrying the offsets (and mark-done state) properties of the tiering round
     * the given latest Fluss snapshot belongs to: Paimon may append a tail of maintenance snapshots
     * after ours within the same commit call (e.g. batched partition expiration OVERWRITE snapshots
     * without properties), so walk back over such a tail. Returns null when it can't be found (e.g.
     * a legacy v0.7 commit, or the properties snapshot was expired).
     */
    @Nullable
    private Snapshot findRoundPropertiesSnapshot(Snapshot latestFlussSnapshot) {
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Snapshot snapshot = latestFlussSnapshot;
        while (true) {
            if (FLUSS_LAKE_TIERING_COMMIT_USER.equals(snapshot.commitUser())
                    && snapshot.properties() != null
                    && snapshot.properties().containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)) {
                return snapshot;
            }
            // only walk over the maintenance tail of the round
            if (snapshot.properties() != null
                    || snapshot.commitKind() != Snapshot.CommitKind.OVERWRITE
                    || !FLUSS_LAKE_TIERING_COMMIT_USER.equals(snapshot.commitUser())
                    || snapshot.id() == Snapshot.FIRST_SNAPSHOT_ID) {
                return null;
            }
            try {
                snapshot = snapshotManager.tryGetSnapshot(snapshot.id() - 1);
            } catch (Exception e) {
                // the previous snapshot has been expired
                return null;
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (partitionMarkDone != null) {
                partitionMarkDone.close();
            }
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

    /** A {@link CommitCallback} to save paimon commit snapshot info. */
    public static class PaimonCommitCallback implements CommitCallback {

        @Override
        public void call(
                List<SimpleFileEntry> baseFiles,
                List<ManifestEntry> deltaFiles,
                List<IndexManifestEntry> indexFiles,
                Snapshot snapshot) {
            currentCommitSnapshotId.set(snapshot.id());
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
