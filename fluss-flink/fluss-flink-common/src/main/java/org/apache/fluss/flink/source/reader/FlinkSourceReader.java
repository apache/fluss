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

import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.adapter.SingleThreadMultiplexSourceReaderBaseAdapter;
import org.apache.fluss.flink.lake.LakeSplitStateInitializer;
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

/** The source reader for Fluss. */
public class FlinkSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBaseAdapter<
                RecordAndPos, OUT, SourceSplitBase, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceReader.class);

    /**
     * Fluss-side view of {@link FlinkConnectorOptions#CLIENT_COMMIT_OFFSET_ON_CHECKPOINT}. The
     * option is declared with Flink's option API for SQL/table options, while this reader reads it
     * from the Fluss {@link Configuration}.
     */
    private static final org.apache.fluss.config.ConfigOption<Boolean> COMMIT_OFFSET_ON_CHECKPOINT =
            org.apache.fluss.config.ConfigBuilder.key(
                            FlinkConnectorOptions.CLIENT_COMMIT_OFFSET_ON_CHECKPOINT.key())
                    .booleanType()
                    .defaultValue(false);

    /** the tableBuckets ignore to send FinishedKvSnapshotConsumeEvent as it already sending. */
    private final Set<TableBucket> finishedKvSnapshotConsumeBuckets;

    /**
     * Pending offsets to commit, keyed by checkpoint id. The outer map is a synchronized sorted
     * map; the inner map is concurrent so that writes during snapshotState and reads/removals
     * during the commit callback don't trip over each other. Compound operations (firstKey/remove
     * loops, computeIfAbsent + put bursts) are guarded by synchronizing on offsetsToCommit
     * explicitly.
     */
    private final SortedMap<Long, Map<TableBucket, OffsetMetadata>> offsetsToCommit;

    private final ConcurrentMap<TableBucket, OffsetMetadata> offsetsOfFinishedSplits;

    private final boolean commitOffsetsOnCheckpoint;

    public FlinkSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
            Configuration flussConfig,
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
                                        maybeDisableAutoCommit(flussConfig),
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
        this.commitOffsetsOnCheckpoint = flussConfig.getBoolean(COMMIT_OFFSET_ON_CHECKPOINT);
        if (commitOffsetsOnCheckpoint) {
            GroupId.of(flussConfig.get(ConfigOptions.CLIENT_CONSUMER_GROUP_ID));
        }
    }

    /**
     * When checkpoint commits are requested, disable the Fluss client's periodic auto-commit so
     * that {@link #notifyCheckpointComplete(long)} is the only path that submits offsets. The
     * reader context does not necessarily contain the job's checkpointing interval.
     */
    private static Configuration maybeDisableAutoCommit(Configuration flussConfig) {
        if (!flussConfig.getBoolean(COMMIT_OFFSET_ON_CHECKPOINT)) {
            return flussConfig;
        }
        Configuration adjusted = new Configuration(flussConfig);
        adjusted.set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED, false);
        LOG.info(
                "Offset commits on checkpoint enabled; disabling client-side auto-commit."
                        + " Consumer offsets will be committed only after checkpoint completion.");
        return adjusted;
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> map) {
        if (!commitOffsetsOnCheckpoint) {
            return;
        }

        // record the offsets of finished splits so they can be committed on checkpoint completion
        map.forEach(
                (ignored, splitState) -> {
                    if (splitState.isLogSplitState()
                            && splitState.asLogSplitState().getNextOffset() >= 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.toSourceSplit().getTableBucket(),
                                new OffsetMetadata(splitState.asLogSplitState().getNextOffset()));
                    } else if (splitState.isHybridSnapshotLogSplitState()
                            && splitState.asHybridSnapshotLogSplitState().getNextOffset() >= 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.toSourceSplit().getTableBucket(),
                                new OffsetMetadata(
                                        splitState
                                                .asHybridSnapshotLogSplitState()
                                                .getNextOffset()));
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

        synchronized (offsetsToCommit) {
            if (sourceSplitBases.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
                offsetsToCommit.put(checkpointId, Collections.emptyMap());
            } else {
                Map<TableBucket, OffsetMetadata> offsetsMap =
                        offsetsToCommit.computeIfAbsent(
                                checkpointId, id -> new ConcurrentHashMap<>());

                // Put the offsets of the active splits.
                for (SourceSplitBase split : sourceSplitBases) {
                    // If the checkpoint is triggered before the tableBucket starting offsets
                    // is retrieved, do not commit the offsets for those tableBuckets.
                    if (split.isLogSplit() && split.asLogSplit().getStartingOffset() >= 0) {
                        offsetsMap.put(
                                split.getTableBucket(),
                                new OffsetMetadata(split.asLogSplit().getStartingOffset()));
                    } else if (split.isHybridSnapshotLogSplit()
                            && split.asHybridSnapshotLogSplit().getLogStartingOffset() >= 0) {
                        offsetsMap.put(
                                split.getTableBucket(),
                                new OffsetMetadata(
                                        split.asHybridSnapshotLogSplit().getLogStartingOffset()));
                    }
                }

                // Put offsets of all the finished splits.
                offsetsMap.putAll(offsetsOfFinishedSplits);
            }
        }

        return sourceSplitBases;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (!commitOffsetsOnCheckpoint) {
            return;
        }
        LOG.debug("Committing offsets for checkpoint {}", checkpointId);

        // Take a stable snapshot of the inner map under the outer monitor — by the time we hand
        // the map to the fetcher manager (and especially to its async callback), no concurrent
        // mutator can corrupt our view.
        Map<TableBucket, OffsetMetadata> committedTableBuckets;
        synchronized (offsetsToCommit) {
            Map<TableBucket, OffsetMetadata> raw = offsetsToCommit.get(checkpointId);
            if (raw == null) {
                LOG.debug("Offsets for checkpoint {} have already been committed.", checkpointId);
                return;
            }
            // Defensive copy so the async callback below operates on its own map.
            committedTableBuckets = new HashMap<>(raw);
        }

        if (committedTableBuckets.isEmpty()) {
            LOG.debug("There are no offsets to commit for checkpoint {}.", checkpointId);
            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
            return;
        }

        ((FlinkSourceFetcherManager) splitFetcherManager)
                .commitOffsets(
                        committedTableBuckets,
                        (offsets, e) -> {
                            // The offset commit here is needed by external monitoring. It won't
                            // break Flink job's correctness if we fail to commit the offset here.
                            if (e != null) {
                                LOG.warn(
                                        "Failed to commit consumer offsets for checkpoint {}",
                                        checkpointId,
                                        e);
                                removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
                            } else {
                                LOG.debug(
                                        "Successfully committed offsets for checkpoint {}",
                                        checkpointId);
                                // If the finished bucket has been committed, we remove it from
                                // the offsets of the finished splits map.
                                offsetsOfFinishedSplits
                                        .entrySet()
                                        .removeIf(entry -> offsets.containsKey(entry.getKey()));
                                removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
                            }
                        });
    }

    private void removeAllOffsetsToCommitUpToCheckpoint(long checkpointId) {
        synchronized (offsetsToCommit) {
            while (!offsetsToCommit.isEmpty() && offsetsToCommit.firstKey() <= checkpointId) {
                offsetsToCommit.remove(offsetsToCommit.firstKey());
            }
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionsRemovedEvent) {
            PartitionsRemovedEvent partitionsRemovedEvent = (PartitionsRemovedEvent) sourceEvent;
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback =
                    (unsubscribedTableBuckets) -> {
                        // send remove partitions ack event to coordinator
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
