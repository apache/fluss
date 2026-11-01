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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.event.RowPosRequestEvent;
import org.apache.fluss.flink.tiering.source.TableBucketWriteResult;
import org.apache.fluss.flink.tiering.source.TieringSource;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.RowPosSstUploader;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbLakeDvEntry;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_NEXT_FILE_ID_PROPERTY;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A Flink operator to aggregate {@link WriteResult}s by table to {@link Committable} which will
 * then be committed to lake & Fluss cluster.
 *
 * <p>It will collect all {@link TableBucketWriteResult}s which wraps {@link WriteResult} written by
 * {@link LakeWriter} in {@link TieringSource} operator.
 *
 * <p>When it collects all {@link TableBucketWriteResult}s of a round of tiering for a table, it
 * will combine all the {@link WriteResult}s to {@link Committable} via method {@link
 * LakeCommitter#toCommittable(List)}, and then call method {@link LakeCommitter#commit(Object,
 * Map)} to commit to lake.
 *
 * <p>Finally, it will also commit the committed lake snapshot to Fluss cluster to make Fluss aware
 * of the tiering progress.
 */
public class TieringCommitOperator<WriteResult, Committable>
        extends AbstractStreamOperator<CommittableMessage<Committable>>
        implements OneInputStreamOperator<
                TableBucketWriteResult<WriteResult>, CommittableMessage<Committable>> {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final Configuration lakeTieringConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final Configuration dataLakeConfig;
    private final String dataLakeFormat;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    private Connection connection;
    private Admin admin;

    // gateway to send event to flink source coordinator
    private final OperatorEventGateway operatorEventGateway;

    // tableid -> write results
    private final Map<Long, List<TableBucketWriteResult<WriteResult>>>
            collectedTableBucketWriteResults;

    // RowPos scan state
    // tableId -> collected RowPos results
    private final Map<Long, List<RowPosResult>> pendingRowPosResults;
    // tableId -> expected count of RowPos results
    private final Map<Long, Integer> expectedRowPosResultCounts;
    // tableId -> partition -> bucket -> deleted file names
    private final Map<Long, Map<String, Map<Integer, List<String>>>> pendingDeletedFiles;
    // tableId -> next file id counter
    private final Map<Long, Integer> nextFileIdByTable;
    // tableId -> table path (for deferred commit after DV scan completes)
    private final Map<Long, TablePath> pendingTablePaths;
    // tableId -> compact snapshot id (for index.json writing after DV scan completes)
    private final Map<Long, Long> pendingCompactSnapshotIds;

    /**
     * The result of one table's commit round, holding the lake committable (nullable for empty
     * commits where no data was written) and the associated tiering statistics.
     */
    private final class CommitResult {
        /** The lake committable, or {@code null} if nothing was written in this round. */
        @Nullable final Committable committable;
        /** Per-table tiering statistics collected during this round. */
        @Nullable final TieringStats stats;

        CommitResult(@Nullable Committable committable, @Nullable TieringStats stats) {
            this.committable = committable;
            this.stats = stats;
        }
    }

    public TieringCommitOperator(
            StreamOperatorParameters<CommittableMessage<Committable>> parameters,
            Configuration flussConf,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory,
            Configuration dataLakeConfig,
            String dataLakeFormat) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.dataLakeConfig = dataLakeConfig;
        this.dataLakeFormat = dataLakeFormat;
        this.flussTableLakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        this.collectedTableBucketWriteResults = new HashMap<>();
        this.pendingRowPosResults = new HashMap<>();
        this.expectedRowPosResultCounts = new HashMap<>();
        this.pendingDeletedFiles = new HashMap<>();
        this.nextFileIdByTable = new HashMap<>();
        this.pendingTablePaths = new HashMap<>();
        this.pendingCompactSnapshotIds = new HashMap<>();
        this.flussConfig = flussConf;
        this.lakeTieringConfig = lakeTieringConfig;
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        this.operatorEventGateway =
                parameters
                        .getOperatorEventDispatcher()
                        .getOperatorEventGateway(TieringSource.TIERING_SOURCE_OPERATOR_UID);
    }

    @Override
    public void open() {
        flussTableLakeSnapshotCommitter.open();
        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();
    }

    @Override
    public void processElement(StreamRecord<TableBucketWriteResult<WriteResult>> streamRecord)
            throws Exception {
        TableBucketWriteResult<WriteResult> tableBucketWriteResult = streamRecord.getValue();

        // check if this is a RowPos scan result from Reader
        if (tableBucketWriteResult.isRowPosResult()) {
            handleRowPosResult(tableBucketWriteResult);
            return;
        }

        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        long tableId = tableBucket.getTableId();
        registerTableBucketWriteResult(tableId, tableBucketWriteResult);

        // may collect all write results for the table
        List<TableBucketWriteResult<WriteResult>> committableWriteResults =
                collectTableAllBucketWriteResult(tableId);

        if (committableWriteResults != null) {
            TablePath tablePath = tableBucketWriteResult.tablePath();
            try {
                // check if DV scan is needed before committing
                boolean scanTriggered =
                        mayTriggerRowPosScan(tableId, tablePath, committableWriteResults);
                if (scanTriggered) {
                    // DV scan pending — defer commit until scan results are collected,
                    // so the commit includes both write results and DV data
                    pendingTablePaths.put(tableId, tablePath);
                } else {
                    // no DV scan needed — commit and finish immediately (no index snapshot)
                    commitAndFinish(tableId, tablePath, committableWriteResults, null, null, 0L);
                }
            } catch (Exception e) {
                // if any exception happens, send to source coordinator to mark it as failed
                collectedTableBucketWriteResults.remove(tableId);
                operatorEventGateway.sendEventToCoordinator(
                        new SourceEventWrapper(
                                new FailedTieringEvent(
                                        tableId, ExceptionUtils.stringifyException(e))));
                LOG.warn(
                        "Fail to commit tiering write result, will try to tier again in next round.",
                        e);
            }
        }
    }

    /**
     * Commits the collected write results for one table to the lake and Fluss.
     *
     * <p>Always returns a non-null {@link CommitResult}. When all buckets produced no data (empty
     * commit), {@link CommitResult#committable} is {@code null} and stats are {@link
     * TieringStats#UNKNOWN}.
     */
    private CommitResult commitWriteResults(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults,
            @Nullable List<RowPosResult> rowPosResults,
            @Nullable Map<String, Map<Integer, List<String>>> deletedFiles,
            long indexSnapshotId)
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
            return new CommitResult(null, null);
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
                                tablePath, currentTableInfo, lakeTieringConfig, flussConfig))) {
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
            LakeSnapshot flussCurrentLakeSnapshot = getLatestLakeSnapshot(tablePath);
            checkFlussNotMissingLakeSnapshot(
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
            Map<String, String> snapshotProperties = new HashMap<>();
            snapshotProperties.put(
                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, lakeBucketTieredOffsetsFile);

            // persist nextFileId for DV tables so it survives Committer restarts
            Integer nextFileId = nextFileIdByTable.get(tableId);
            if (nextFileId != null) {
                snapshotProperties.put(
                        FLUSS_LAKE_SNAP_NEXT_FILE_ID_PROPERTY, String.valueOf(nextFileId));
            }
            LakeCommitResult lakeCommitResult =
                    lakeCommitter.commit(committable, snapshotProperties);

            // build DvPositionReport if DV scan results are available
            Map<TableBucket, FlussTableLakeSnapshotCommitter.DvBucketData> dvReport = null;
            if (rowPosResults != null && !rowPosResults.isEmpty()) {
                dvReport =
                        buildDvPositionReport(
                                tableId, rowPosResults, deletedFiles, lakeCommitResult);
            }

            // Ensure ALL buckets from ReadableSnapshot are present in dvReport.
            // This handles two cases:
            // 1. DV-only compaction: dvReport is null because no RowPos scan was
            //    triggered, but we still need DV orchestration for all buckets.
            // 2. Partial compaction: dvReport has entries for only some partitions
            //    (those with RowPos results), but ALL buckets must be switched
            //    because the readable snapshot is table-level.
            // 3. Iceberg (readable unknown): readers pin one table-level id, so every tiered
            //    bucket switches, not just those with RowPos results (-1 = Iceberg readable).
            LakeCommitResult.ReadableSnapshot readable = lakeCommitResult.getReadableSnapshot();
            Map<TableBucket, Long> switchBuckets = null;
            if (readable != null) {
                switchBuckets = readable.getReadableLogEndOffsets();
            } else if (dvReport != null) {
                switchBuckets = new HashMap<>();
                if (flussCurrentLakeSnapshot != null) {
                    for (TableBucket tb :
                            flussCurrentLakeSnapshot.getTableBucketsOffset().keySet()) {
                        switchBuckets.put(tb, -1L);
                    }
                }
                for (TableBucket tb : logEndOffsets.keySet()) {
                    switchBuckets.put(tb, -1L);
                }
            }
            if (switchBuckets != null) {
                if (dvReport == null) {
                    dvReport = new HashMap<>();
                }

                // Collect partitions that already have index.json from RowPos results
                Set<Long> partitionsWithIndex = new HashSet<>();
                if (rowPosResults != null) {
                    for (RowPosResult result : rowPosResults) {
                        Long pid = result.getPartitionId();
                        partitionsWithIndex.add(pid != null ? pid : -1L);
                    }
                }

                Set<Long> emptyIndexPartitions = new HashSet<>();
                for (Map.Entry<TableBucket, Long> entry : switchBuckets.entrySet()) {
                    dvReport.putIfAbsent(
                            entry.getKey(),
                            new FlussTableLakeSnapshotCommitter.DvBucketData(
                                    entry.getValue(),
                                    Collections.emptyMap(),
                                    Collections.emptyList()));

                    // Track partitions that need an empty index.json
                    Long partitionId = entry.getKey().getPartitionId();
                    long key = partitionId != null ? partitionId : -1L;
                    if (!partitionsWithIndex.contains(key)) {
                        emptyIndexPartitions.add(key);
                    }
                }

                // Upload empty index.json for partitions without RowPos results
                // so that DvPrepare can find the index file for every partition
                if (!emptyIndexPartitions.isEmpty()) {
                    String remoteDataDir = FlussConfigUtils.getDefaultRemoteDataDir(flussConfig);
                    FsPath remoteLakeTableSnapshotDir =
                            FlussPaths.remoteLakeTableSnapshotDir(
                                    remoteDataDir, tablePath, tableId);
                    RowPosSstUploader uploader = new RowPosSstUploader(remoteLakeTableSnapshotDir);
                    RowPosSstIndex emptyIndex = new RowPosSstIndex(Collections.emptyMap());
                    // empty index must key on the download snapshot (index snapshot when distinct)
                    long emptyIndexSnapshotId =
                            indexSnapshotId > 0
                                    ? indexSnapshotId
                                    : readable != null
                                            ? readable.getReadableSnapshotId()
                                            : lakeCommitResult.getCommittedSnapshotId();
                    for (Long partKey : emptyIndexPartitions) {
                        Long partitionId = partKey == -1L ? null : partKey;
                        uploader.writeIndex(emptyIndexSnapshotId, partitionId, emptyIndex);
                    }
                }
            }

            // commit to fluss
            flussTableLakeSnapshotCommitter.commit(
                    tableId,
                    tablePath,
                    lakeCommitResult,
                    lakeBucketTieredOffsetsFile,
                    logEndOffsets,
                    logMaxTieredTimestamps,
                    dvReport,
                    indexSnapshotId);
            return new CommitResult(committable, lakeCommitResult.getTieringStats());
        }
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

    private void checkFlussNotMissingLakeSnapshot(
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
                    LakeCommitResult.KEEP_ALL_PREVIOUS,
                    null,
                    0L);
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

    private void registerTableBucketWriteResult(
            long tableId, TableBucketWriteResult<WriteResult> tableBucketWriteResult) {
        collectedTableBucketWriteResults
                .computeIfAbsent(tableId, k -> new ArrayList<>())
                .add(tableBucketWriteResult);
    }

    @Nullable
    private List<TableBucketWriteResult<WriteResult>> collectTableAllBucketWriteResult(
            long tableId) {
        Set<TableBucket> collectedBuckets = new HashSet<>();
        Integer numberOfWriteResults = null;
        List<TableBucketWriteResult<WriteResult>> writeResults = new ArrayList<>();
        for (TableBucketWriteResult<WriteResult> tableBucketWriteResult :
                collectedTableBucketWriteResults.get(tableId)) {
            if (!collectedBuckets.add(tableBucketWriteResult.tableBucket())) {
                // it means the write results contain more than two write result
                // for same table, it shouldn't happen, let's throw exception to
                // avoid unexpected behavior
                throw new IllegalStateException(
                        String.format(
                                "Found duplicate write results for bucket %s of table %s.",
                                tableBucketWriteResult.tableBucket(), tableId));
            }
            if (numberOfWriteResults == null) {
                numberOfWriteResults = tableBucketWriteResult.numberOfWriteResults();
            } else {
                // the numberOfWriteResults must be same across tableBucketWriteResults
                checkState(
                        numberOfWriteResults == tableBucketWriteResult.numberOfWriteResults(),
                        "numberOfWriteResults is not same across TableBucketWriteResults for table %s, got %s and %s.",
                        tableId,
                        numberOfWriteResults,
                        tableBucketWriteResult.numberOfWriteResults());
            }
            writeResults.add(tableBucketWriteResult);
        }

        if (numberOfWriteResults != null && writeResults.size() == numberOfWriteResults) {
            return writeResults;
        } else {
            return null;
        }
    }

    /**
     * Commits write results to the lake and Fluss, emits downstream, sends FinishedTieringEvent,
     * and cleans up state. Used both for immediate commit (no DV scan) and deferred commit (after
     * DV scan completes).
     */
    private void commitAndFinish(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults,
            @Nullable List<RowPosResult> rowPosResults,
            @Nullable Map<String, Map<Integer, List<String>>> deletedFiles,
            long indexSnapshotId)
            throws Exception {
        CommitResult commitResult =
                commitWriteResults(
                        tableId,
                        tablePath,
                        committableWriteResults,
                        rowPosResults,
                        deletedFiles,
                        indexSnapshotId);
        if (commitResult.committable != null) {
            output.collect(new StreamRecord<>(new CommittableMessage<>(commitResult.committable)));
        }
        // Materialize the server's logical LakeDv into physical Iceberg Puffin DVs so $lake and
        // external engines mask too. Runs every round (incl. empty delete rounds) keyed off the
        // readable snapshot, since the readable-switch lags the data commit. Best-effort.
        maybeMaterializeDeletionVectors(tableId, tablePath);
        collectedTableBucketWriteResults.remove(tableId);
        operatorEventGateway.sendEventToCoordinator(
                new SourceEventWrapper(new FinishedTieringEvent(tableId, commitResult.stats)));
    }

    /**
     * Handles a RowPos scan result received from a Reader. Collects results by tableId and triggers
     * processing when all expected results have been received.
     */
    private void handleRowPosResult(TableBucketWriteResult<WriteResult> tableBucketWriteResult) {
        RowPosResult rowPosResult = tableBucketWriteResult.getRowPosResult();
        long tableId = tableBucketWriteResult.tableBucket().getTableId();
        pendingRowPosResults.computeIfAbsent(tableId, k -> new ArrayList<>()).add(rowPosResult);

        Integer expectedCount = expectedRowPosResultCounts.get(tableId);
        if (expectedCount != null && pendingRowPosResults.get(tableId).size() >= expectedCount) {
            processRowPosResults(tableId);
        }
    }

    /**
     * Processes all collected RowPos results for a table. SSTs have already been uploaded by
     * Readers. This method writes the cross-bucket index.json, then commits the deferred write
     * results to the lake.
     */
    private void processRowPosResults(long tableId) {
        List<RowPosResult> results = pendingRowPosResults.remove(tableId);
        Map<String, Map<Integer, List<String>>> deletedFiles = pendingDeletedFiles.remove(tableId);
        expectedRowPosResultCounts.remove(tableId);
        TablePath tablePath = pendingTablePaths.remove(tableId);
        Long compactSnapshotId = pendingCompactSnapshotIds.remove(tableId);

        LOG.info(
                "Received all {} RowPos scan results for table {} (snapshot={}). "
                        + "Deleted files: {}.",
                results != null ? results.size() : 0,
                tableId,
                compactSnapshotId,
                deletedFiles);

        try {
            // SSTs already uploaded by Readers. Write index.json.
            if (results != null && !results.isEmpty() && compactSnapshotId != null) {
                writeIndexJson(tableId, tablePath, compactSnapshotId, results);
            }

            // commit the deferred write results to the lake; the RowPos index was uploaded under
            // compactSnapshotId, so carry it so the server downloads from the matching path.
            List<TableBucketWriteResult<WriteResult>> writeResults =
                    collectedTableBucketWriteResults.get(tableId);
            long indexSnapshotId = compactSnapshotId != null ? compactSnapshotId : 0L;
            commitAndFinish(
                    tableId, tablePath, writeResults, results, deletedFiles, indexSnapshotId);
        } catch (Exception e) {
            collectedTableBucketWriteResults.remove(tableId);
            operatorEventGateway.sendEventToCoordinator(
                    new SourceEventWrapper(
                            new FailedTieringEvent(tableId, ExceptionUtils.stringifyException(e))));
            LOG.warn(
                    "Failed to commit after RowPos scan for table {} ({}). "
                            + "Will try to tier again in next round.",
                    tablePath,
                    tableId,
                    e);
        }
    }

    /**
     * Writes the index.json file after all Readers have uploaded their per-bucket SST files. The
     * index records which buckets participated in this snapshot and lists their SST files.
     */
    private void writeIndexJson(
            long tableId, TablePath tablePath, long compactSnapshotId, List<RowPosResult> results)
            throws IOException {
        String remoteDataDir = FlussConfigUtils.getDefaultRemoteDataDir(flussConfig);
        FsPath remoteLakeTableSnapshotDir =
                FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId);
        RowPosSstUploader uploader = new RowPosSstUploader(remoteLakeTableSnapshotDir);

        // Group results by partitionId for partition-aware index writing
        Map<Long, List<RowPosResult>> resultsByPartition = new HashMap<>();
        for (RowPosResult result : results) {
            Long partitionId = result.getPartitionId();
            // Use -1 as sentinel key for non-partitioned results
            long key = partitionId != null ? partitionId : -1L;
            resultsByPartition.computeIfAbsent(key, k -> new ArrayList<>()).add(result);
        }

        for (Map.Entry<Long, List<RowPosResult>> partitionEntry : resultsByPartition.entrySet()) {
            Long partitionId = partitionEntry.getKey() == -1L ? null : partitionEntry.getKey();
            Map<Integer, List<RowPosSstIndex.SstFileEntry>> bucketEntries = new HashMap<>();
            for (RowPosResult result : partitionEntry.getValue()) {
                List<RowPosSstIndex.SstFileEntry> entries = new ArrayList<>();
                for (RowPosResult.SstMeta meta : result.getSstMetas()) {
                    entries.add(
                            new RowPosSstIndex.SstFileEntry(
                                    meta.getFileName(), meta.getFileSize()));
                }
                bucketEntries.put(result.getBucketId(), entries);
            }
            RowPosSstIndex index = new RowPosSstIndex(bucketEntries);
            uploader.writeIndex(compactSnapshotId, partitionId, index);

            LOG.info(
                    "Wrote index.json for table {} (snapshot={}, partition={}) with {} buckets.",
                    tablePath,
                    compactSnapshotId,
                    partitionId,
                    bucketEntries.size());
        }
    }

    /**
     * Checks if DV is enabled for the table and triggers a row position scan if compaction is
     * detected. Called at the end of a successful commitWriteResults().
     *
     * @return true if a RowPos scan was triggered (FinishedTieringEvent should be deferred)
     */
    private boolean mayTriggerRowPosScan(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults) {
        try {
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            if (!tableInfo.isDeletionVectorsEnabled()) {
                return false;
            }

            int flussColumnCount = tableInfo.getSchema().getColumns().size();

            LakeSource<?> lakeSource = lakeTieringFactory.createLakeSource(tablePath);
            if (lakeSource == null) {
                return false;
            }

            // use the previous lake snapshot as fromSnapshotId
            LakeSnapshot flussCurrentLakeSnapshot = getLatestLakeSnapshot(tablePath);
            long fromSnapshotId =
                    flussCurrentLakeSnapshot != null ? flussCurrentLakeSnapshot.getSnapshotId() : 0;

            return triggerRowPosScanIfNeeded(
                    tableId, tablePath, lakeSource, fromSnapshotId, flussColumnCount);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to trigger row position scan for table {} ({}). "
                            + "Will retry in next tiering round.",
                    tablePath,
                    tableId,
                    e);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private <Split extends LakeSplit> boolean triggerRowPosScanIfNeeded(
            long tableId,
            TablePath tablePath,
            LakeSource<Split> lakeSource,
            long fromSnapshotId,
            int flussColumnCount)
            throws Exception {
        DataDeltaPlan<Split> deltaPlan = lakeSource.planDelta(fromSnapshotId);
        if (deltaPlan == null) {
            return false;
        }

        List<Split> splits = deltaPlan.getSplits();
        if (splits.isEmpty()) {
            return false;
        }

        SimpleVersionedSerializer<Split> splitSerializer = lakeSource.getSplitSerializer();

        // group serialized splits by partition and bucket
        Map<String, Map<Integer, List<byte[]>>> serializedByPartitionAndBucket = new HashMap<>();
        for (Split split : splits) {
            byte[] serialized = splitSerializer.serialize(split);
            // LakeSplit.partition() returns list of partition values; use joined form as key
            // For non-partitioned, partition() returns empty list → key is null
            List<String> partitionValues = split.partition();
            String partitionName =
                    (partitionValues == null || partitionValues.isEmpty())
                            ? null
                            : String.join(
                                    ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR,
                                    partitionValues);
            int bucketId = split.bucket();
            serializedByPartitionAndBucket
                    .computeIfAbsent(partitionName, k -> new HashMap<>())
                    .computeIfAbsent(bucketId, k -> new ArrayList<>())
                    .add(serialized);
        }

        // recover nextFileId from catalog if not cached locally
        int nextFileId;
        if (nextFileIdByTable.containsKey(tableId)) {
            nextFileId = nextFileIdByTable.get(tableId);
        } else {
            // first trigger for this table after restart — read from lake snapshot properties
            int recovered = 0;
            LakeStoragePlugin plugin =
                    LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat, null);
            LakeStorage lakeStorage = plugin.createLakeStorage(dataLakeConfig);
            try (LakeCatalog lakeCatalog = lakeStorage.createLakeCatalog()) {
                Map<String, String> snapshotProps = lakeCatalog.loadSnapshotProperties(tablePath);
                String storedFileId = snapshotProps.get(FLUSS_LAKE_SNAP_NEXT_FILE_ID_PROPERTY);
                if (storedFileId != null) {
                    recovered = Integer.parseInt(storedFileId);
                }
            }
            nextFileId = recovered;
            // cache it for subsequent triggers within this Committer lifecycle
            nextFileIdByTable.put(tableId, nextFileId);
        }
        int totalFileCount = splits.size();

        // expected results = number of batched splits (one per partition+bucket combo)
        int expectedBatchCount = 0;
        for (Map<Integer, List<byte[]>> bucketMap : serializedByPartitionAndBucket.values()) {
            expectedBatchCount += bucketMap.size();
        }

        long compactSnapshotId = deltaPlan.getCompactSnapshotId();

        // store expected count, deleted files, and compact snapshot id locally
        expectedRowPosResultCounts.put(tableId, expectedBatchCount);
        pendingDeletedFiles.put(tableId, deltaPlan.getDeletedFiles());
        pendingCompactSnapshotIds.put(tableId, compactSnapshotId);

        String remoteDataDir = FlussConfigUtils.getDefaultRemoteDataDir(flussConfig);
        String remoteUploadBasePath =
                FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId).toString();

        // send event to Enumerator
        RowPosRequestEvent event =
                new RowPosRequestEvent(
                        tableId,
                        tablePath,
                        nextFileId,
                        serializedByPartitionAndBucket,
                        compactSnapshotId,
                        remoteUploadBasePath,
                        flussColumnCount);
        operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));

        // update next file id counter (increments by total files, not batches)
        nextFileIdByTable.put(tableId, nextFileId + totalFileCount);

        LOG.info(
                "Triggered RowPos scan for table {} ({}): {} batched splits ({} files), "
                        + "nextFileId={}, deletedFiles={}",
                tablePath,
                tableId,
                expectedBatchCount,
                totalFileCount,
                nextFileId,
                deltaPlan.getDeletedFiles());

        return true;
    }

    /**
     * Builds a DvPositionReport from RowPos scan results and the lake commit result.
     *
     * <p>The readableOffset for each bucket comes from the {@link
     * LakeCommitResult.ReadableSnapshot}, which is only available after {@code
     * lakeCommitter.commit()} returns.
     */
    private Map<TableBucket, FlussTableLakeSnapshotCommitter.DvBucketData> buildDvPositionReport(
            long tableId,
            List<RowPosResult> rowPosResults,
            @Nullable Map<String, Map<Integer, List<String>>> deletedFiles,
            LakeCommitResult lakeCommitResult) {
        Map<TableBucket, FlussTableLakeSnapshotCommitter.DvBucketData> report = new HashMap<>();
        LakeCommitResult.ReadableSnapshot readable = lakeCommitResult.getReadableSnapshot();

        for (RowPosResult result : rowPosResults) {
            int bucketId = result.getBucketId();
            Long partitionId = result.getPartitionId();
            TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
            // readableOffset from ReadableSnapshot's readableLogEndOffsets
            long readableOffset = getReadableOffset(readable, tb);
            // deleted files from the deltaPlan
            List<String> oldFiles =
                    getDeletedFilesForBucket(deletedFiles, result.getPartitionName(), bucketId);

            report.put(
                    tb,
                    new FlussTableLakeSnapshotCommitter.DvBucketData(
                            readableOffset, result.getNewFileDictEntries(), oldFiles));
        }
        return report;
    }

    private long getReadableOffset(
            @Nullable LakeCommitResult.ReadableSnapshot readable, TableBucket tb) {
        if (readable == null) {
            return -1;
        }
        Long offset = readable.getReadableLogEndOffsets().get(tb);
        return offset != null ? offset : -1;
    }

    private List<String> getDeletedFilesForBucket(
            @Nullable Map<String, Map<Integer, List<String>>> deletedFiles,
            @Nullable String partitionName,
            int bucketId) {
        if (deletedFiles == null) {
            return Collections.emptyList();
        }
        Map<Integer, List<String>> bucketMap = deletedFiles.get(partitionName);
        if (bucketMap == null) {
            return Collections.emptyList();
        }
        List<String> files = bucketMap.get(bucketId);
        return files != null ? files : Collections.emptyList();
    }

    /**
     * Fetches the server's logical LakeDv (per-file deleted positions) for the previous readable
     * snapshot and asks the lake committer to materialize it into physical deletion vectors
     * (Iceberg v3 Puffin), so {@code $lake} and external engines mask too. Best-effort: any failure
     * is logged and skipped — union-read masking still works via the logical LakeDv, and the
     * cumulative LakeDv is re-materialized next round.
     */
    private void maybeMaterializeDeletionVectors(long tableId, TablePath tablePath) {
        try {
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            if (!tableInfo.isDeletionVectorsEnabled()) {
                return;
            }
            LOG.info("DV materialize: check for table {} ({}).", tablePath, tableId);
            // Materialize against the READABLE snapshot (the one union read masks against);
            // getDvSnapshot rejects any other id as stale. Absent early on -> nothing to do yet.
            LakeSnapshot readable;
            try {
                readable = admin.getReadableLakeSnapshot(tablePath).get();
            } catch (Exception noReadable) {
                LOG.info(
                        "DV materialize: no readable lake snapshot yet for table {} ({}), skipping.",
                        tablePath,
                        tableId);
                return;
            }
            long baseSnapshotId = readable.getSnapshotId();
            Map<String, byte[]> lakeDv = new HashMap<>();
            for (TableBucket tb : readable.getTableBucketsOffset().keySet()) {
                GetDvSnapshotResponse resp =
                        admin.getDvSnapshot(
                                        tablePath,
                                        tableId,
                                        tb.getPartitionId(),
                                        tb.getBucket(),
                                        baseSnapshotId)
                                .get();
                for (PbLakeDvEntry entry : resp.getLakeDvEntriesList()) {
                    lakeDv.put(entry.getFilePath(), entry.getDeletedPositionsBitmap());
                }
            }
            LOG.info(
                    "DV materialize: table {} ({}) readable snapshot {} has {} LakeDv file(s).",
                    tablePath,
                    tableId,
                    baseSnapshotId,
                    lakeDv.size());
            if (lakeDv.isEmpty()) {
                return;
            }
            LOG.info(
                    "DV materialize: writing Puffin DVs for {} file(s) on snapshot {} of table {}.",
                    lakeDv.size(),
                    baseSnapshotId,
                    tablePath);
            try (LakeCommitter<WriteResult, Committable> committer =
                    lakeTieringFactory.createLakeCommitter(
                            new TieringCommitterInitContext(
                                    tablePath, tableInfo, lakeTieringConfig, flussConfig))) {
                committer.materializeDeletionVectors(
                        lakeDv, baseSnapshotId, Collections.<String, String>emptyMap());
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to materialize deletion vectors for table {} ({}); union-read masking "
                            + "still works via logical LakeDv, will retry next round.",
                    tablePath,
                    tableId,
                    e);
        }
    }

    @Override
    public void close() throws Exception {
        flussTableLakeSnapshotCommitter.close();
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
