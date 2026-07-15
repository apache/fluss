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
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.source.TableBucketWriteResult;
import org.apache.fluss.flink.tiering.source.TieringSource;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.LakeTieringTableState;
import org.apache.fluss.lake.committer.PartitionDoneCandidate;
import org.apache.fluss.lake.committer.PartitionDoneHandler;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
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
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    private Connection connection;
    private Admin admin;

    // gateway to send event to flink source coordinator
    private final OperatorEventGateway operatorEventGateway;

    // tableid -> write results
    private final Map<Long, List<TableBucketWriteResult<WriteResult>>>
            collectedTableBucketWriteResults;

    // tableId -> cached PartitionDoneHandler; NO_OP_HANDLER means the table does not support
    // mark-done (checked once to avoid recreating the handler every round).
    private final Map<Long, PartitionDoneHandler> partitionDoneHandlers = new HashMap<>();

    // sentinel meaning the table has been checked and does not support partition mark-done
    private static final PartitionDoneHandler NO_OP_HANDLER =
            (candidates, currentTime) -> Collections.emptyList();

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
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        this.collectedTableBucketWriteResults = new HashMap<>();
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
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        long tableId = tableBucket.getTableId();
        registerTableBucketWriteResult(tableId, tableBucketWriteResult);

        // may collect all write results for the table
        List<TableBucketWriteResult<WriteResult>> committableWriteResults =
                collectTableAllBucketWriteResult(tableId);

        if (committableWriteResults != null) {
            try {
                CommitResult commitResult =
                        commitWriteResults(
                                tableId,
                                tableBucketWriteResult.tablePath(),
                                committableWriteResults);
                // only emit downstream when actual data was written
                if (commitResult.committable != null) {
                    output.collect(
                            new StreamRecord<>(new CommittableMessage<>(commitResult.committable)));
                }
                // notify that the table id has been finished tier
                operatorEventGateway.sendEventToCoordinator(
                        new SourceEventWrapper(
                                new FinishedTieringEvent(tableId, commitResult.stats)));
            } catch (Exception e) {
                // if any exception happens, send to source coordinator to mark it as failed
                operatorEventGateway.sendEventToCoordinator(
                        new SourceEventWrapper(
                                new FailedTieringEvent(
                                        tableId, ExceptionUtils.stringifyException(e))));
                LOG.warn(
                        "Fail to commit tiering write result, will try to tier again in next round.",
                        e);
            } finally {
                collectedTableBucketWriteResults.remove(tableId);
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
            List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        // filter down to buckets that actually produced data
        List<TableBucketWriteResult<WriteResult>> nonEmptyResults =
                committableWriteResults.stream()
                        .filter(r -> r.writeResult() != null)
                        .collect(Collectors.toList());

        // fetch current table info once; used both for the drop/recreate check and for deciding
        // whether partition mark-done is enabled.
        TableInfo currentTableInfo = admin.getTableInfo(tablePath).get();
        boolean tableIdMatches = currentTableInfo.getTableId() == tableId;

        // ========== Mark Done: read (getLakeSnapshot) -> judge -> execute action ==========
        // null handler means the lake has no partition mark-done for this table.
        PartitionDoneHandler handler =
                tableIdMatches ? getOrCreateHandler(tableId, tablePath, currentTableInfo) : null;
        boolean markDoneEnabled = handler != null;

        LakeSnapshot prevSnapshot = null;
        byte[] newTieringStateJson = null;
        boolean tieringStateChanged = false;
        if (markDoneEnabled) {
            prevSnapshot = getLatestLakeSnapshot(tablePath);
            long now = System.currentTimeMillis();
            Map<Long, String> partitionIdToName = listPartitionIdToName(tablePath);
            LakeTieringTableState prevState =
                    prevSnapshot != null ? prevSnapshot.getLakeTieringTableState() : null;

            // not-yet-done partitions' last update time: previous state MAX this round's per-bucket
            // maxTimestamp. Done partitions are absent (delete-on-done).
            Map<Long, Long> partitionUpdateTimes =
                    aggregatePartitionUpdateTimes(committableWriteResults, prevState);

            // Cold start (uninitialized): judge every existing partition by partitionEndTime;
            // otherwise only the not-yet-done partitions in the state.
            boolean initialized = prevState != null && prevState.isPartitionDoneInitialized();
            Set<Long> candidateIds =
                    initialized ? partitionUpdateTimes.keySet() : partitionIdToName.keySet();
            List<PartitionDoneCandidate> candidates =
                    buildCandidates(candidateIds, partitionUpdateTimes, partitionIdToName);

            // Safe to mark done before the data commit below: candidates exclude this round's
            // active partitions (their lastUpdateTime is ~now, never idle), so the two sets are
            // disjoint.
            List<String> newlyDone;
            boolean judged = true;
            try {
                newlyDone = handler.markDoneIfReady(candidates, now);
            } catch (Exception e) {
                // mark-done action failure must not fail the tiering main path; retry next round.
                LOG.warn(
                        "Mark done action failed for table {}, will retry next round.",
                        tablePath,
                        e);
                newlyDone = Collections.emptyList();
                judged = false;
            }

            // delete-on-done: drop the newly-done partitions from the state.
            if (!newlyDone.isEmpty()) {
                Map<String, Long> nameToId = new HashMap<>();
                for (Map.Entry<Long, String> e : partitionIdToName.entrySet()) {
                    nameToId.put(e.getValue(), e.getKey());
                }
                for (String name : newlyDone) {
                    Long id = nameToId.get(name);
                    if (id != null) {
                        partitionUpdateTimes.remove(id);
                    }
                }
            }

            // Keep initialized once set; on cold start only flip to true when the judgement ran
            // (not when the action threw), so a failed cold start retries next round.
            boolean newInitialized = initialized || judged;
            LakeTieringTableState newState =
                    new LakeTieringTableState(newInitialized, partitionUpdateTimes);
            tieringStateChanged = !newState.equals(prevState);
            newTieringStateJson = newState.toJsonBytes();
        }

        // all buckets were empty — nothing to commit to the lake
        if (nonEmptyResults.isEmpty()) {
            // empty round with a changed tiering state: persist it via a light-weight commit that
            // reuses the previous lake snapshot id (no lake data written).
            if (markDoneEnabled && prevSnapshot != null && tieringStateChanged) {
                flussTableLakeSnapshotCommitter.commitPartitionMarkDoneOnly(
                        tableId, tablePath, prevSnapshot.getSnapshotId(), newTieringStateJson);
            } else {
                LOG.info(
                        "Commit tiering write results is empty for table {}, table path {}",
                        tableId,
                        tablePath);
            }
            return new CommitResult(null, null);
        }

        // Check if the table was dropped and recreated during tiering.
        // If the current table id differs from the committable's table id, fail this commit
        // to avoid dirty commit to a newly created table.
        if (!tableIdMatches) {
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
            // reuse prevSnapshot fetched for mark-done to avoid an extra RPC when possible.
            LakeSnapshot flussCurrentLakeSnapshot =
                    markDoneEnabled ? prevSnapshot : getLatestLakeSnapshot(tablePath);
            checkFlussNotMissingLakeSnapshot(
                    tablePath,
                    tableId,
                    lakeCommitter,
                    committable,
                    flussCurrentLakeSnapshot == null
                            ? null
                            : flussCurrentLakeSnapshot.getSnapshotId());

            // get the lake bucket offsets file storing the log end offsets, carrying the opaque
            // tiering state so that it is persisted together with the offsets in this same commit.
            String lakeBucketTieredOffsetsFile =
                    flussTableLakeSnapshotCommitter.prepareLakeSnapshot(
                            tableId, tablePath, logEndOffsets, newTieringStateJson);

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
            return new CommitResult(committable, lakeCommitResult.getTieringStats());
        }
    }

    /**
     * Gets or creates the {@link PartitionDoneHandler} for the table, caching the result. Returns
     * {@code null} if the lake does not support partition mark-done for this table.
     */
    private PartitionDoneHandler getOrCreateHandler(
            long tableId, TablePath tablePath, TableInfo tableInfo) {
        PartitionDoneHandler cached = partitionDoneHandlers.get(tableId);
        if (cached != null) {
            return cached == NO_OP_HANDLER ? null : cached;
        }
        PartitionDoneHandler handler =
                lakeTieringFactory
                        .createPartitionDoneHandler(
                                new TieringCommitterInitContext(
                                        tablePath, tableInfo, lakeTieringConfig, flussConfig))
                        .orElse(null);
        partitionDoneHandlers.put(tableId, handler == null ? NO_OP_HANDLER : handler);
        return handler;
    }

    /** Lists the partitionId -> partitionName mapping for a partitioned table at runtime. */
    private Map<Long, String> listPartitionIdToName(TablePath tablePath) throws Exception {
        Map<Long, String> result = new HashMap<>();
        for (PartitionInfo partitionInfo : admin.listPartitionInfos(tablePath).get()) {
            result.put(partitionInfo.getPartitionId(), partitionInfo.getPartitionName());
        }
        return result;
    }

    /**
     * Aggregates the partition-level last update time (processing-time mode): starts from the
     * previous persisted tiering state and merges this round's per-bucket maxTimestamp by MAX per
     * partition. Buckets with maxTimestamp &lt;= 0 (empty / snapshot splits) are ignored.
     */
    private Map<Long, Long> aggregatePartitionUpdateTimes(
            List<TableBucketWriteResult<WriteResult>> writeResults,
            @Nullable LakeTieringTableState prevState) {
        Map<Long, Long> result = new HashMap<>();
        if (prevState != null) {
            result.putAll(prevState.getPartitionUpdateTimes());
        }
        for (TableBucketWriteResult<WriteResult> wr : writeResults) {
            Long partitionId = wr.tableBucket().getPartitionId();
            if (wr.maxTimestamp() > 0 && partitionId != null) {
                result.merge(partitionId, wr.maxTimestamp(), Math::max);
            }
        }
        return result;
    }

    /**
     * Builds the candidate partitions to be judged by the handler for the given partition ids. Each
     * candidate carries its last update time from {@code partitionUpdateTimes} (or {@code 0} when
     * absent, e.g. a cold-start partition that has no data this round, so the handler judges it
     * purely by its partitionEndTime). Partitions whose name cannot be resolved are skipped.
     */
    private List<PartitionDoneCandidate> buildCandidates(
            Set<Long> partitionIds,
            Map<Long, Long> partitionUpdateTimes,
            Map<Long, String> partitionIdToName) {
        List<PartitionDoneCandidate> candidates = new ArrayList<>();
        for (Long partitionId : partitionIds) {
            String partitionName = partitionIdToName.get(partitionId);
            if (partitionName == null) {
                LOG.warn(
                        "Cannot find partition name for partition id {}, skip mark-done check.",
                        partitionId);
                continue;
            }
            long lastUpdateTime = partitionUpdateTimes.getOrDefault(partitionId, 0L);
            candidates.add(new PartitionDoneCandidate(partitionName, lastUpdateTime));
        }
        return candidates;
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

    @Override
    public void close() throws Exception {
        for (PartitionDoneHandler handler : partitionDoneHandlers.values()) {
            if (handler != NO_OP_HANDLER) {
                try {
                    handler.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close partition done handler.", e);
                }
            }
        }
        partitionDoneHandlers.clear();
        flussTableLakeSnapshotCommitter.close();
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
