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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;

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

    /**
     * Commits the collected write results for one table to the lake and Fluss.
     *
     * <p>Always returns a non-null {@link TieringCommitResult}. When all buckets produced no data
     * (empty commit), {@link TieringCommitResult#getCommittable()} is {@code null} and stats are
     * {@code null}.
     */
    public TieringCommitResult<Committable> commitWriteResults(
            Admin admin,
            long tableId,
            TablePath tablePath,
            Configuration flussConf,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory,
            FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        // filter down to buckets that actually produced data
        List<TableBucketWriteResult<WriteResult>> nonEmptyResults =
                committableWriteResults.stream()
                        .filter(r -> r.writeResult() != null)
                        .collect(Collectors.toList());

        // all buckets were empty — nothing to commit to the lake
        if (nonEmptyResults.isEmpty()) {
            LOG.info(
                    "Commit tiering write results is empty for table {}, table path {}",
                    tableId,
                    tablePath);
            return new TieringCommitResult<>(null, null);
        }

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

        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter(
                        new TieringCommitterInitContext(
                                tablePath, currentTableInfo, lakeTieringConfig, flussConf))) {
            List<WriteResult> writeResults =
                    nonEmptyResults.stream()
                            .map(TableBucketWriteResult::writeResult)
                            .collect(Collectors.toList());

            Map<TableBucket, Long> logEndOffsets = new HashMap<>();
            Map<TableBucket, Long> logMaxTieredTimestamps = new HashMap<>();
            for (TableBucketWriteResult<WriteResult> writeResult : nonEmptyResults) {
                TableBucket tableBucket = writeResult.tableBucket();
                logEndOffsets.put(tableBucket, writeResult.logEndOffset());
                logMaxTieredTimestamps.put(tableBucket, writeResult.maxTimestamp());
            }

            // to committable
            Committable committable = lakeCommitter.toCommittable(writeResults);
            // before commit to lake, check fluss not missing any lake snapshot committed by fluss
            LakeSnapshot flussCurrentLakeSnapshot = getLatestLakeSnapshot(admin, tablePath);
            checkFlussNotMissingLakeSnapshot(
                    flussTableLakeSnapshotCommitter,
                    tablePath,
                    tableId,
                    lakeCommitter,
                    committable,
                    flussCurrentLakeSnapshot == null
                            ? null
                            : flussCurrentLakeSnapshot.getSnapshotId());

            // get the lake bucket offsets file storing the log end offsets
            String lakeBucketTieredOffsetsFile =
                    flussTableLakeSnapshotCommitter.prepareLakeSnapshot(
                            tableId, tablePath, logEndOffsets);

            // record the lake snapshot bucket offsets file to snapshot property
            Map<String, String> snapshotProperties =
                    Collections.singletonMap(
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, lakeBucketTieredOffsetsFile);
            LakeCommitResult lakeCommitResult =
                    lakeCommitter.commit(committable, snapshotProperties);
            // commit to fluss
            flussTableLakeSnapshotCommitter.commit(
                    tableId,
                    tablePath,
                    lakeCommitResult,
                    lakeBucketTieredOffsetsFile,
                    logEndOffsets,
                    logMaxTieredTimestamps);
            return new TieringCommitResult<>(committable, lakeCommitResult.getTieringStats());
        }
    }

    @Nullable
    private LakeSnapshot getLatestLakeSnapshot(Admin admin, TablePath tablePath) throws Exception {
        LakeSnapshot flussCurrentLakeSnapshot;
        try {
            flussCurrentLakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        } catch (Exception e) {
            Throwable throwable = e.getCause();
            if (throwable instanceof LakeTableSnapshotNotExistException) {
                flussCurrentLakeSnapshot = null;
            } else {
                throw e;
            }
        }
        return flussCurrentLakeSnapshot;
    }

    private void checkFlussNotMissingLakeSnapshot(
            FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter,
            TablePath tablePath,
            long tableId,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            Committable committable,
            Long flussCurrentLakeSnapshot)
            throws Exception {
        // get Fluss missing lake snapshot in Lake
        CommittedLakeSnapshot missingCommittedSnapshot =
                lakeCommitter.getMissingLakeSnapshot(flussCurrentLakeSnapshot);

        // fluss's known snapshot is less than lake snapshot committed by fluss
        // fail this commit since the data is read from the log end-offset of a invalid fluss
        // known lake snapshot, which means the data already has been committed to lake,
        // not to commit to lake to avoid data duplicated
        if (missingCommittedSnapshot != null) {
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
            // abort this committable to delete the written files
            lakeCommitter.abort(committable);
            throw new IllegalStateException(
                    String.format(
                            "The current Fluss's lake snapshot %d is less than"
                                    + " lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                    + " missing snapshot: %s.",
                            flussCurrentLakeSnapshot,
                            missingCommittedSnapshot.getLakeSnapshotId(),
                            tablePath,
                            tableId,
                            missingCommittedSnapshot));
        }
    }
}
