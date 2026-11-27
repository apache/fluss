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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.client.table.scanner.log.OffsetCommitCallback;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SingleThreadMultiplexSourceReaderBaseAdapter;
import org.apache.fluss.flink.lake.LakeSplitStateInitializer;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.state.LakeSnapshotAndFlussLogSplitState;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.reader.fetcher.FlinkSourceFetcherManager;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.KvBatchSplitState;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.GroupId;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static org.apache.fluss.config.ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED;

/** The source reader for Fluss. */
public class FlinkSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBaseAdapter<
                RecordAndPos, OUT, SourceSplitBase, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceReader.class);

    /** the tableBuckets ignore to send FinishedKvSnapshotConsumeEvent as it already sending. */
    private final Set<TableBucket> finishedKvSnapshotConsumeBuckets;

    /**
     * Pending offsets to commit, keyed by checkpoint id. The outer map is a synchronized sorted
     * map; the inner maps are ordinary HashMaps because the task thread is their only writer and
     * they are handed to the commit callback only after snapshotState completes.
     */
    private final SortedMap<Long, Map<TableBucket, OffsetMetadata>> offsetsToCommit;

    /**
     * Final offsets of eligible streaming splits that have finished but have not yet been included
     * in a successful checkpoint commit. This includes splits retired because their partition was
     * deleted. The map is reader-local and best-effort across failover: if the reader fails before
     * a successful commit, the final report can be lost. Server-side consumer offsets are not
     * deleted here; their expiry is left to the consumer-offset TTL.
     */
    private final ConcurrentMap<TableBucket, OffsetMetadata> offsetsOfFinishedSplits;

    private final boolean commitOffsetsOnCheckpoint;

    public FlinkSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
            Configuration flussConfig,
            boolean commitOffsetOnCheckpoint,
            boolean batchExecution,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics,
            FlinkRecordEmitter<OUT> recordEmitter,
            LakeSource<LakeSplit> lakeSource) {
        super(
                elementsQueue,
                new FlinkSourceFetcherManager(
                        elementsQueue,
                        () ->
                                new FlinkSourceSplitReader(
                                        maybeDisableAutoCommit(
                                                flussConfig,
                                                commitOffsetOnCheckpoint,
                                                batchExecution),
                                        tablePath,
                                        sourceOutputType,
                                        projectedFields,
                                        logRecordBatchFilter,
                                        lakeSource,
                                        flinkSourceReaderMetrics),
                        (ignore) -> {}),
                recordEmitter,
                context.getConfiguration(),
                context);
        this.finishedKvSnapshotConsumeBuckets = new HashSet<>();
        this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.offsetsOfFinishedSplits = new ConcurrentHashMap<>();
        this.commitOffsetsOnCheckpoint = commitOffsetOnCheckpoint && !batchExecution;
        if (commitOffsetOnCheckpoint) {
            GroupId.of(flussConfig.get(ConfigOptions.CLIENT_CONSUMER_GROUP_ID));
        } else if (!batchExecution
                && flussConfig.get(ConfigOptions.CLIENT_CONSUMER_GROUP_ID) != null) {
            LOG.warn(
                    "Offset commit on checkpoint is disabled. The reader will not report "
                            + "checkpointed offsets to the Fluss cluster; the scanner can still "
                            + "commit offsets periodically if auto-commit is enabled.");
        }
    }

    /**
     * Batch reads must not advance consumer group offsets. Streaming reads that commit on
     * checkpoints disable periodic auto-commit by default and honor an explicit client setting.
     */
    private static Configuration maybeDisableAutoCommit(
            Configuration flussConfig, boolean commitOffsetOnCheckpoint, boolean batchExecution) {
        if (batchExecution
                || (commitOffsetOnCheckpoint
                        && !flussConfig.contains(CLIENT_CONSUMER_AUTO_COMMIT_ENABLED))) {
            LOG.info(
                    "Disabling client-side auto-commit for {}.",
                    batchExecution ? "batch execution" : "checkpoint-based offset commits");
            return new Configuration(flussConfig).set(CLIENT_CONSUMER_AUTO_COMMIT_ENABLED, false);
        }
        return flussConfig;
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> map) {
        if (!commitOffsetsOnCheckpoint) {
            return;
        }

        // Record the final emitted position of finished splits, including splits retired because
        // their partition was deleted. The position remains pending until a completed checkpoint
        // successfully commits it; the partition unsubscription ack is not an offset commit.
        map.forEach(
                (ignored, splitState) -> {
                    if (splitState.isLogSplitState()
                            && splitState.asLogSplitState().getNextOffset() >= 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.toSourceSplit().getTableBucket(),
                                new OffsetMetadata(splitState.asLogSplitState().getNextOffset()));
                    } else if (splitState.isHybridSnapshotLogSplitState()
                            && splitState.asHybridSnapshotLogSplitState().isSnapshotFinished()
                            && splitState.asHybridSnapshotLogSplitState().getNextOffset() >= 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.toSourceSplit().getTableBucket(),
                                new OffsetMetadata(
                                        splitState
                                                .asHybridSnapshotLogSplitState()
                                                .getNextOffset()));
                    } else if (splitState instanceof LakeSnapshotAndFlussLogSplitState) {
                        LakeSnapshotAndFlussLogSplitState lakeState =
                                (LakeSnapshotAndFlussLogSplitState) splitState;
                        SourceSplitBase sourceSplit = lakeState.toSourceSplit();
                        if (sourceSplit instanceof LakeSnapshotAndFlussLogSplit
                                && ((LakeSnapshotAndFlussLogSplit) sourceSplit).isStreaming()
                                && lakeState.isLakeSplitFinished()
                                && lakeState.getNextLogOffset() >= 0) {
                            offsetsOfFinishedSplits.put(
                                    sourceSplit.getTableBucket(),
                                    new OffsetMetadata(lakeState.getNextLogOffset()));
                        }
                    }
                });
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        Set<TableBucket> bucketsFinishedConsumeKvSnapshot = new HashSet<>();

        // do not modify this state.
        List<SourceSplitBase> sourceSplitBases = super.snapshotState(checkpointId);
        for (SourceSplitBase sourceSplitBase : sourceSplitBases) {
            TableBucket tableBucket = sourceSplitBase.getTableBucket();
            if (finishedKvSnapshotConsumeBuckets.contains(tableBucket)) {
                continue;
            }

            if (sourceSplitBase.isHybridSnapshotLogSplit()) {
                HybridSnapshotLogSplit hybridSnapshotLogSplit =
                        sourceSplitBase.asHybridSnapshotLogSplit();
                if (hybridSnapshotLogSplit.isSnapshotFinished()) {
                    bucketsFinishedConsumeKvSnapshot.add(tableBucket);
                }
            }
        }

        // report finished kv snapshot consume event.
        if (!bucketsFinishedConsumeKvSnapshot.isEmpty()) {
            LOG.info(
                    "reader has finished kv snapshot read for bucket: {}, checkpoint id {}",
                    bucketsFinishedConsumeKvSnapshot,
                    checkpointId);

            // TODO Reduce the external IO operation, trace by
            // https://github.com/apache/fluss/issues/2597.
            context.sendSourceEventToCoordinator(
                    new FinishedKvSnapshotConsumeEvent(
                            checkpointId, bucketsFinishedConsumeKvSnapshot));
            // It won't be sent anymore in the future for this table bucket, but will be resent
            // after failover recovery as ignoreBuckets is cleared.
            finishedKvSnapshotConsumeBuckets.addAll(bucketsFinishedConsumeKvSnapshot);
        }

        // collect the offsets to commit for this checkpoint
        if (!commitOffsetsOnCheckpoint) {
            return sourceSplitBases;
        }

        if (sourceSplitBases.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
            offsetsToCommit.put(checkpointId, Collections.emptyMap());
        } else {
            Map<TableBucket, OffsetMetadata> offsetsMap =
                    offsetsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());

            // Put the offsets of the active splits.
            for (SourceSplitBase split : sourceSplitBases) {
                // If the checkpoint is triggered before the tableBucket starting offsets
                // is retrieved, do not commit the offsets for those tableBuckets.
                if (split.isLogSplit() && split.asLogSplit().getStartingOffset() >= 0) {
                    offsetsMap.put(
                            split.getTableBucket(),
                            new OffsetMetadata(split.asLogSplit().getStartingOffset()));
                } else if (split.isHybridSnapshotLogSplit()
                        && split.asHybridSnapshotLogSplit().isSnapshotFinished()
                        && split.asHybridSnapshotLogSplit().getLogStartingOffset() >= 0) {
                    offsetsMap.put(
                            split.getTableBucket(),
                            new OffsetMetadata(
                                    split.asHybridSnapshotLogSplit().getLogStartingOffset()));
                } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
                    LakeSnapshotAndFlussLogSplit lakeSplit = (LakeSnapshotAndFlussLogSplit) split;
                    if (lakeSplit.isStreaming()
                            && lakeSplit.isLakeSplitFinished()
                            && lakeSplit.getStartingOffset() >= 0) {
                        offsetsMap.put(
                                split.getTableBucket(),
                                new OffsetMetadata(lakeSplit.getStartingOffset()));
                    }
                }
            }

            // Put offsets of all the finished splits.
            offsetsMap.putAll(offsetsOfFinishedSplits);
        }

        return sourceSplitBases;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (!commitOffsetsOnCheckpoint) {
            return;
        }
        LOG.debug("Committing offsets for checkpoint {}", checkpointId);

        Map<TableBucket, OffsetMetadata> committedTableBuckets = offsetsToCommit.get(checkpointId);
        if (committedTableBuckets == null) {
            LOG.debug("Offsets for checkpoint {} have already been committed.", checkpointId);
            return;
        }

        if (committedTableBuckets.isEmpty()) {
            LOG.debug("There are no offsets to commit for checkpoint {}.", checkpointId);
            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
            return;
        }

        commitOffsetsAsync(
                committedTableBuckets,
                (offsets, e) -> {
                    // The offset commit here is needed by external monitoring. It won't break
                    // Flink job's correctness if we fail to commit the offset here.
                    if (e != null) {
                        LOG.warn(
                                "Failed to commit consumer offsets for checkpoint {}",
                                checkpointId,
                                e);
                        // Retaining the failed checkpoint keeps it in the pending map. A later
                        // successful commit for a newer checkpoint supersedes it and clears all
                        // entries up to that checkpoint. The failed request is not retried
                        // by itself.
                    } else {
                        LOG.debug("Successfully committed offsets for checkpoint {}", checkpointId);
                        // Remove a finished offset only when it is still the offset that was
                        // committed. A newer final offset may have been recorded after this
                        // checkpoint snapshot was taken and must remain pending.
                        offsets.forEach(
                                (bucket, committedOffset) ->
                                        offsetsOfFinishedSplits.remove(bucket, committedOffset));
                        removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
                    }
                });
    }

    private void removeAllOffsetsToCommitUpToCheckpoint(long checkpointId) {
        while (!offsetsToCommit.isEmpty() && offsetsToCommit.firstKey() <= checkpointId) {
            offsetsToCommit.remove(offsetsToCommit.firstKey());
        }
    }

    protected void commitOffsetsAsync(
            Map<TableBucket, OffsetMetadata> offsetsToCommit, OffsetCommitCallback callback) {
        ((FlinkSourceFetcherManager) splitFetcherManager).commitOffsets(offsetsToCommit, callback);
    }

    @VisibleForTesting
    SortedMap<Long, Map<TableBucket, OffsetMetadata>> getOffsetsToCommitForTest() {
        return offsetsToCommit;
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionsRemovedEvent) {
            PartitionsRemovedEvent partitionsRemovedEvent = (PartitionsRemovedEvent) sourceEvent;
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback =
                    (unsubscribedTableBuckets) -> {
                        // send remove partitions ack event to coordinator
                        // This ack reports unsubscription only; the final offset remains pending
                        // until a completed checkpoint commits it.
                        context.sendSourceEventToCoordinator(
                                new PartitionBucketsUnsubscribedEvent(unsubscribedTableBuckets));
                    };
            ((FlinkSourceFetcherManager) splitFetcherManager)
                    .removePartitions(
                            partitionsRemovedEvent.getRemovedPartitions(),
                            unsubscribeTableBucketsCallback);
        }
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split.isHybridSnapshotLogSplit()) {
            return new HybridSnapshotLogSplitState(split.asHybridSnapshotLogSplit());
        } else if (split.isLogSplit()) {
            return new LogSplitState(split.asLogSplit());
        } else if (split.isKvBatchSplit()) {
            return new KvBatchSplitState(split.asKvBatchSplit());
        } else if (split.isLakeSplit()) {
            return LakeSplitStateInitializer.initializedState(split);
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
