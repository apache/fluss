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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.io.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Implementation of {@link LakeCommitter} for Iceberg. */
public class IcebergLakeCommitter implements LakeCommitter<IcebergWriteResult, IcebergCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergLakeCommitter.class);

    private static final String COMMITTER_USER = "commit-user";

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();

    public IcebergLakeCommitter(IcebergCatalogProvider icebergCatalogProvider, TablePath tablePath)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(tablePath);
        // register iceberg listener
        Listeners.register(new IcebergSnapshotCreateListener(), CreateSnapshotEvent.class);
    }

    @Override
    public IcebergCommittable toCommittable(List<IcebergWriteResult> icebergWriteResults) {
        // Aggregate all write results into a single committable
        IcebergCommittable.Builder builder = IcebergCommittable.builder();

        for (IcebergWriteResult result : icebergWriteResults) {
            WriteResult writeResult = result.getWriteResult();

            // Add data files
            for (DataFile dataFile : writeResult.dataFiles()) {
                builder.addDataFile(dataFile);
            }
            // Add delete files
            for (DeleteFile deleteFile : writeResult.deleteFiles()) {
                builder.addDeleteFile(deleteFile);
            }

            RewriteDataFileResult rewriteDataFileResult = result.rewriteDataFileResult();
            if (rewriteDataFileResult != null) {
                builder.addRewriteDataFileResult(rewriteDataFileResult);
            }
        }

        return builder.build();
    }

    @Override
    public long commit(IcebergCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        try {
            // Refresh table to get latest metadata
            icebergTable.refresh();

            SnapshotUpdate<?> snapshotUpdate;
            if (committable.getDeleteFiles().isEmpty()) {
                // Simple append-only case: only data files, no delete files or compaction
                AppendFiles appendFiles = icebergTable.newAppend();
                committable.getDataFiles().forEach(appendFiles::appendFile);
                snapshotUpdate = appendFiles;
            } else {
                /*
                 Row delta validations are not needed for streaming changes that write equality
                 deletes. Equality deletes are applied to data in all previous sequence numbers,
                 so retries may push deletes further in the future, but do not affect correctness.
                 Position deletes committed to the table in this path are used only to delete rows
                 from data files that are being added in this commit. There is no way for data
                 files added along with the delete files to be concurrently removed, so there is
                 no need to validate the files referenced by the position delete files that are
                 being committed.
                */
                RowDelta rowDelta = icebergTable.newRowDelta();
                committable.getDataFiles().forEach(rowDelta::addRows);
                committable.getDeleteFiles().forEach(rowDelta::addDeletes);
                snapshotUpdate = rowDelta;
            }

            // commit written files
            long snapshotId = commit(snapshotUpdate, snapshotProperties);

            // There exists rewrite files, commit rewrite files
            List<RewriteDataFileResult> rewriteDataFileResults =
                    committable.rewriteDataFileResults();
            if (!rewriteDataFileResults.isEmpty()) {
                Long rewriteCommitSnapshotId =
                        commitRewrite(rewriteDataFileResults, snapshotProperties);
                if (rewriteCommitSnapshotId != null) {
                    snapshotId = rewriteCommitSnapshotId;
                }
            }
            return checkNotNull(snapshotId, "Iceberg committed snapshot id must be non-null.");
        } catch (Exception e) {
            throw new IOException("Failed to commit to Iceberg table.", e);
        }
    }

    private Long commitRewrite(
            List<RewriteDataFileResult> rewriteDataFileResults,
            Map<String, String> snapshotProperties) {
        icebergTable.refresh();
        RewriteFiles rewriteFiles = icebergTable.newRewrite();
        try {
            if (rewriteDataFileResults.stream()
                            .map(RewriteDataFileResult::snapshotId)
                            .distinct()
                            .count()
                    > 1) {
                throw new IllegalArgumentException(
                        "Rewrite data file results must have same snapshot id.");
            }
            rewriteFiles.validateFromSnapshot(rewriteDataFileResults.get(0).snapshotId());
            for (RewriteDataFileResult rewriteDataFileResult : rewriteDataFileResults) {
                rewriteDataFileResult.addedDataFiles().forEach(rewriteFiles::addFile);
                rewriteDataFileResult.deletedDataFiles().forEach(rewriteFiles::deleteFile);
            }
            return commit(rewriteFiles, snapshotProperties);
        } catch (Exception e) {
            List<String> rewriteAddedDataFiles =
                    rewriteDataFileResults.stream()
                            .flatMap(
                                    rewriteDataFileResult ->
                                            rewriteDataFileResult.addedDataFiles().stream())
                            .map(ContentFile::location)
                            .collect(Collectors.toList());
            LOG.error(
                    "Failed to commit rewrite files to iceberg, delete rewrite added files {}.",
                    rewriteAddedDataFiles,
                    e);
            // we need to abort new rewrite files
            CatalogUtil.deleteFiles(icebergTable.io(), rewriteAddedDataFiles, "data file", true);
            return null;
        }
    }

    private long commit(SnapshotUpdate<?> snapshotUpdate, Map<String, String> snapshotProperties) {
        // add snapshot properties
        snapshotUpdate.set(COMMITTER_USER, FLUSS_LAKE_TIERING_COMMIT_USER);
        for (Map.Entry<String, String> entry : snapshotProperties.entrySet()) {
            snapshotUpdate.set(entry.getKey(), entry.getValue());
        }
        // do commit
        snapshotUpdate.commit();
        Long commitSnapshotId = currentCommitSnapshotId.get();
        currentCommitSnapshotId.remove();
        return commitSnapshotId;
    }

    @Override
    public void abort(IcebergCommittable committable) {
        List<String> dataFilesToDelete =
                committable.getDataFiles().stream()
                        .map(ContentFile::location)
                        .collect(Collectors.toList());
        CatalogUtil.deleteFiles(icebergTable.io(), dataFilesToDelete, "data file", true);

        List<String> deleteFilesToDelete =
                committable.getDeleteFiles().stream()
                        .map(ContentFile::location)
                        .collect(Collectors.toList());
        CatalogUtil.deleteFiles(icebergTable.io(), deleteFilesToDelete, "delete file", true);
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        Snapshot latestLakeSnapshot =
                getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);

        if (latestLakeSnapshot == null) {
            return null;
        }

        // Check if there's a gap between Fluss and Iceberg snapshots
        if (latestLakeSnapshotIdOfFluss != null) {
            Snapshot latestLakeSnapshotOfFluss = icebergTable.snapshot(latestLakeSnapshotIdOfFluss);
            if (latestLakeSnapshotOfFluss == null) {
                throw new IllegalStateException(
                        "Referenced Fluss snapshot "
                                + latestLakeSnapshotIdOfFluss
                                + " not found in Iceberg table");
            }
            // note: we need to use sequence number to compare,
            // we can't use snapshot id as the snapshot id is not ordered
            if (latestLakeSnapshot.sequenceNumber() <= latestLakeSnapshotOfFluss.sequenceNumber()) {
                return null;
            }
        }

        // Reconstruct bucket offsets from snapshot properties
        Map<String, String> properties = latestLakeSnapshot.summary();
        if (properties == null) {
            throw new IOException(
                    "Failed to load committed lake snapshot properties from Iceberg.");
        }

        return new CommittedLakeSnapshot(latestLakeSnapshot.snapshotId(), properties);
    }

    @Override
    public void close() throws Exception {
        try {
            if (icebergCatalog != null && icebergCatalog instanceof AutoCloseable) {
                ((AutoCloseable) icebergCatalog).close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close IcebergLakeCommitter.", e);
        }
    }

    private Table getTable(TablePath tablePath) throws IOException {
        try {
            TableIdentifier tableId = toIceberg(tablePath);
            return icebergCatalog.loadTable(tableId);
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Iceberg.", e);
        }
    }

    @Nullable
    private Snapshot getCommittedLatestSnapshotOfLake(String commitUser) {
        icebergTable.refresh();

        // Find the latest snapshot committed by Fluss
        List<Snapshot> snapshots = (List<Snapshot>) icebergTable.snapshots();
        // snapshots() returns snapshots in chronological order (oldest to newest), Reverse to find
        // most recent snapshot committed by Fluss
        for (int i = snapshots.size() - 1; i >= 0; i--) {
            Snapshot snapshot = snapshots.get(i);
            Map<String, String> summary = snapshot.summary();
            if (summary != null && commitUser.equals(summary.get(COMMITTER_USER))) {
                return snapshot;
            }
        }
        return null;
    }

    /** A {@link Listener} to listen the iceberg create snapshot event. */
    public static class IcebergSnapshotCreateListener implements Listener<CreateSnapshotEvent> {
        @Override
        public void notify(CreateSnapshotEvent createSnapshotEvent) {
            currentCommitSnapshotId.set(createSnapshotEvent.snapshotId());
        }
    }
}
