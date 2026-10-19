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

package org.apache.fluss.client.tiering;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Engine-agnostic tiering committer that commits write results to a lake and the Fluss cluster.
 *
 * <p>This class extracts the core commit logic from engine-specific operators so that any compute
 * engine (Flink, Spark, etc.) can reuse the same commit protocol.
 *
 * @param <WriteResult> the type of individual write results produced by lake writers
 * @param <Committable> the type of the aggregated committable for the lake
 */
public class TieringCommitter<WriteResult, Committable> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringCommitter.class);

    private final Admin admin;
    private final Configuration flussConf;
    private final Configuration lakeTieringConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;

    /**
     * Creates a committer with dependencies shared by all table commit rounds.
     *
     * <p>The caller owns and closes the admin and snapshot committer. A lake committer is created
     * and closed for each call to {@link #commitWriteResults}.
     */
    public TieringCommitter(
            Admin admin,
            Configuration flussConf,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory,
            FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter) {
        this.admin = admin;
        this.flussConf = flussConf;
        this.lakeTieringConfig = lakeTieringConfig;
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = flussTableLakeSnapshotCommitter;
    }

    /**
     * Commits the collected write results for one table to the lake and Fluss. When buckets
     * advanced their tiered offsets without writing any data (e.g. splits only covering empty WAL
     * batches), an empty lake snapshot is committed to persist the tiering progress.
     *
     * <p>Always returns a non-null {@link TieringCommitResult}. When nothing needs to be committed,
     * {@link TieringCommitResult#getCommittable()} is {@code null} and stats are {@code null}.
     */
    public TieringCommitResult<Committable> commitWriteResults(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        // filter down to buckets that actually produced data
        List<TableBucketWriteResult<WriteResult>> nonEmptyResults =
                committableWriteResults.stream()
                        .filter(r -> r.writeResult() != null)
                        .collect(Collectors.toList());

        // collect tiered offsets from all buckets, including those finished without writing
        // data, otherwise their splits would be regenerated forever; buckets with unknown
        // progress (e.g. splits skipped in this round) are excluded
        Map<TableBucket, Long> logEndOffsets = new HashMap<>();
        Map<TableBucket, Long> logMaxTieredTimestamps = new HashMap<>();
        for (TableBucketWriteResult<WriteResult> writeResult : committableWriteResults) {
            if (writeResult.logEndOffset() < 0) {
                continue;
            }
            TableBucket tableBucket = writeResult.tableBucket();
            logEndOffsets.put(tableBucket, writeResult.logEndOffset());
            if (writeResult.maxTimestamp() >= 0) {
                logMaxTieredTimestamps.put(tableBucket, writeResult.maxTimestamp());
            }
        }

        boolean hasTieringProgress = !nonEmptyResults.isEmpty() || !logEndOffsets.isEmpty();
        if (!hasTieringProgress) {
            LOG.info(
                    "Commit tiering write results is empty for table {}, table path {}",
                    tableId,
                    tablePath);
        } else if (nonEmptyResults.isEmpty()) {
            LOG.info(
                    "No data was written for table {} (table path {}) but buckets {} advanced "
                            + "their tiered offsets, committing an empty lake snapshot to "
                            + "persist the tiering progress.",
                    tableId,
                    tablePath,
                    logEndOffsets.keySet());
        }

        TableInfo currentTableInfo = getAndValidateTableInfo(tablePath, tableId);
        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter(
                        new TieringCommitterInitContext(
                                tablePath, currentTableInfo, lakeTieringConfig, flussConf))) {
            List<WriteResult> writeResults =
                    nonEmptyResults.stream()
                            .map(TableBucketWriteResult::writeResult)
                            .collect(Collectors.toList());
            // to committable
            Committable committable = lakeCommitter.toCommittable(writeResults);
            // before commit to lake, check fluss not missing any lake snapshot committed by fluss
            LakeSnapshot flussCurrentLakeSnapshot = getLatestLakeSnapshot(tablePath);
            Long knownSnapshotId =
                    flussCurrentLakeSnapshot == null
                            ? null
                            : flussCurrentLakeSnapshot.getSnapshotId();
            CommittedLakeSnapshot recoveredSnapshot =
                    checkMissingLakeSnapshotAndRecover(
                            tablePath,
                            tableId,
                            lakeCommitter,
                            committable,
                            knownSnapshotId,
                            hasTieringProgress);
            boolean hasLakeSnapshot = knownSnapshotId != null || recoveredSnapshot != null;
            boolean markDoneSupported =
                    lakeTieringConfig.get(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                            && lakeTieringFactory.supportsPartitionMarkDone();
            // Empty rounds only prepare mark-done after a lake snapshot exists, so initialization
            // alone does not create the first snapshot.
            boolean markDoneStateChanged =
                    markDoneSupported
                            && (hasTieringProgress || hasLakeSnapshot)
                            && preparePartitionMarkDone(lakeCommitter, committable);
            if (!hasTieringProgress && !markDoneStateChanged) {
                // Nothing was written, no tiered offset advanced, and no partition mark-done
                // state changed, so there is nothing to commit.
                return new TieringCommitResult<>(null, null);
            }

            // The coordinator prepares the same merge when writing the offsets file. Pass the
            // complete map to the lake committer so it does not need access to that file.
            Map<TableBucket, Long> tieredLogEndOffsets = new HashMap<>();
            if (flussCurrentLakeSnapshot != null) {
                tieredLogEndOffsets.putAll(flussCurrentLakeSnapshot.getTableBucketsOffset());
            }
            tieredLogEndOffsets.putAll(logEndOffsets);

            // get the lake bucket offsets file storing the log end offsets
            String lakeBucketTieredOffsetsFile =
                    flussTableLakeSnapshotCommitter.prepareLakeSnapshot(
                            tableId, tablePath, logEndOffsets);
            // record the lake snapshot bucket offsets file to snapshot property
            Map<String, String> snapshotProperties =
                    Collections.singletonMap(
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, lakeBucketTieredOffsetsFile);
            LakeCommitResult lakeCommitResult =
                    lakeCommitter.commit(
                            committable,
                            new LakeCommitter.CommitContext(
                                    snapshotProperties, tieredLogEndOffsets));
            // commit to fluss
            flussTableLakeSnapshotCommitter.commit(
                    tableId,
                    tablePath,
                    lakeCommitResult,
                    lakeBucketTieredOffsetsFile,
                    logEndOffsets,
                    logMaxTieredTimestamps);
            return new TieringCommitResult<>(
                    hasTieringProgress ? committable : null,
                    hasTieringProgress ? lakeCommitResult.getTieringStats() : null);
        }
    }

    private TableInfo getAndValidateTableInfo(TablePath tablePath, long tableId) throws Exception {
        // Check if the table was dropped and recreated during tiering.
        // If the current table id differs from the committable's table id, fail this commit
        // to avoid dirty commit to a newly created table.
        TableInfo currentTableInfo = admin.getTableInfo(tablePath).get();
        if (currentTableInfo.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "The current table id %s for table path %s is different from the table id %s in the committable. "
                                    + "This usually happens when a table was dropped and recreated during tiering. "
                                    + "Aborting commit to prevent dirty commit.",
                            currentTableInfo.getTableId(), tablePath, tableId));
        }
        return currentTableInfo;
    }

    @SuppressWarnings("unchecked")
    private boolean preparePartitionMarkDone(
            LakeCommitter<WriteResult, Committable> lakeCommitter, Committable committable)
            throws IOException {
        checkState(
                lakeCommitter instanceof PartitionMarkDoneCommitter,
                "Lake tiering factory %s declares partition mark-done support but its committer does not.",
                lakeTieringFactory.getClass().getName());
        return ((PartitionMarkDoneCommitter<WriteResult, Committable>) lakeCommitter)
                .preparePartitionMarkDone(committable);
    }

    @Nullable
    private LakeSnapshot getLatestLakeSnapshot(TablePath tablePath) throws Exception {
        LakeSnapshot flussCurrentLakeSnapshot;
        try {
            flussCurrentLakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        } catch (Exception e) {
            Throwable throwable = e.getCause();
            if (throwable instanceof LakeTableSnapshotNotExistException) {
                // do-nothing
                flussCurrentLakeSnapshot = null;
            } else {
                throw e;
            }
        }
        return flussCurrentLakeSnapshot;
    }

    @Nullable
    private CommittedLakeSnapshot checkMissingLakeSnapshotAndRecover(
            TablePath tablePath,
            long tableId,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            Committable committable,
            @Nullable Long knownSnapshotId,
            boolean hasTieringProgress)
            throws Exception {
        // get Fluss missing lake snapshot in Lake
        CommittedLakeSnapshot missingCommittedSnapshot =
                lakeCommitter.getMissingLakeSnapshot(knownSnapshotId);
        if (missingCommittedSnapshot == null) {
            return null;
        }
        String lakeSnapshotOffsetPath =
                missingCommittedSnapshot
                        .getSnapshotProperties()
                        .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);

        // should only will happen in v0.7 which won't put offsets info
        // to properties
        if (lakeSnapshotOffsetPath == null) {
            throw new IllegalStateException(
                    String.format(
                            "Can't find %s field from snapshot property.",
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY));
        }

        // the fluss-offsets will be a json string if it's tiered by v0.8,
        // since this code path should be rare, we do not consider backward compatibility
        // and throw IllegalStateException directly
        String trimmedPath = lakeSnapshotOffsetPath.trim();
        if (trimmedPath.contains("{")) {
            throw new IllegalStateException(
                    String.format(
                            "The %s field in snapshot property is a JSON string (tiered by v0.8), "
                                    + "which is not supported to restore. Snapshot ID: %d, Table: {tablePath=%s, tableId=%d}.",
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                            missingCommittedSnapshot.getLakeSnapshotId(),
                            tablePath,
                            tableId));
        }

        // commit this missing snapshot to fluss
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                missingCommittedSnapshot.getLakeSnapshotId(),
                lakeSnapshotOffsetPath,
                // don't care readable snapshot and offsets,
                null,
                // use empty log offsets, log max timestamp, since we can't know that
                // in last tiering, it doesn't matter for they are just used to
                // report metrics
                Collections.emptyMap(),
                Collections.emptyMap(),
                LakeCommitResult.KEEP_ALL_PREVIOUS);
        if (hasTieringProgress) {
            // Fluss's known snapshot is older than the lake snapshot committed by Fluss.
            // This round read from stale offsets, so its data has already been committed to
            // the lake. Fail the commit to avoid writing duplicate data.
            // Abort this committable to delete the written files before retrying from the
            // recovered snapshot.
            lakeCommitter.abort(committable);
            throw new IllegalStateException(
                    String.format(
                            "The current Fluss's lake snapshot %d is less than"
                                    + " lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                    + " missing snapshot: %s.",
                            knownSnapshotId,
                            missingCommittedSnapshot.getLakeSnapshotId(),
                            tablePath,
                            tableId,
                            missingCommittedSnapshot));
        }
        return missingCommittedSnapshot;
    }
}
