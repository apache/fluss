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

package org.apache.fluss.flink.source.enumerator;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.client.initializer.NoStoppingOffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer.BucketOffsetsRetriever;
import org.apache.fluss.client.initializer.SnapshotOffsetsInitializer;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.lake.LakeSplitGenerator;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.FlinkSource;
import org.apache.fluss.flink.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.state.SourceEnumeratorState;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Lists;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.PartitionUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * An implementation of {@link SplitEnumerator} for the data of Fluss.
 *
 * <p>The enumerator is responsible for:
 *
 * <ul>
 *   <li>Get the all splits(lake split + kv snapshot split + log split) for a table of Fluss to be
 *       read.
 *   <li>Assign the splits to readers with the guarantee that the splits belong to the same bucket
 *       will be assigned to same reader.
 * </ul>
 */
@Internal
public class FlinkSourceEnumerator
        implements SplitEnumerator<SourceSplitBase, SourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceEnumerator.class);

    private final WorkerExecutor workerExecutor;
    private final TablePath tablePath;
    private final boolean hasPrimaryKey;
    private final boolean isPartitioned;
    private final Configuration flussConf;

    private final SplitEnumeratorContext<SourceSplitBase> context;

    private final Map<Integer, List<SourceSplitBase>> pendingSplitAssignment;

    /**
     * Partitions that have been assigned to readers, will be empty when the table is not
     * partitioned. Mapping from partition id to partition name.
     *
     * <p>It's mainly used to help enumerator to broadcast the partition removed event to the
     * readers when partitions is dropped.
     *
     * <p>If an assigned partition exists only in the lake and has already expired in Fluss, it will
     * remain here indefinitely and will not be removed. However, considering that only a small
     * number of such lake-only partitions might exist during the initial startup, and they consume
     * minimal memory, this issue is being ignored for now.
     */
    private final Map<Long, String> assignedPartitions;

    /** Buckets that have been assigned to readers. */
    private final Set<TableBucket> assignedTableBuckets;

    /**
     * Remaining lake snapshot and hybrid lake/Fluss splits to assign.
     *
     * <p>The field has three states:
     *
     * <ul>
     *   <li>{@code null}: lake split initialization has not run yet, or the source has no lake
     *       (non-lake table) so initialization will never run.
     *   <li>empty list: lake split initialization has run, or this enumerator was started in
     *       Fluss-only (non-lake) mode and must not initialize lake splits after restore.
     *   <li>non-empty list: lake split initialization has run and these splits still need to be
     *       assigned.
     * </ul>
     */
    @Nullable private List<SourceSplitBase> pendingHybridLakeFlussSplits;

    private final long scanPartitionDiscoveryIntervalMs;

    private final boolean streaming;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;

    /**
     * The offsets initializer used for partitions discovered after the initial startup. Following
     * FLIP-288 semantics, newly discovered partitions always start from earliest to prevent data
     * loss.
     */
    private final OffsetsInitializer newDiscoveryOffsetsInitializer;

    /**
     * Splits whose starting offsets have been initialized but that have not yet been assigned to
     * any reader. This map is persisted in checkpoint state (via {@link #snapshotState}) so that on
     * failover restore these splits are directly placed into {@link #pendingSplitAssignment}
     * without re-initialization, preserving the original offset strategy determined at first
     * discovery time (FLIP-288).
     */
    private final Map<TableBucket, SourceSplitBase> unassignedSplits;

    private final LeaseContext leaseContext;

    /** checkpointId -> tableBuckets who finished consume kv snapshots. */
    private final TreeMap<Long, Set<TableBucket>> consumedKvSnapshotMap = new TreeMap<>();

    // Lazily instantiated or mutable fields.
    private Connection connection;
    private Admin flussAdmin;
    private BucketOffsetsRetriever bucketOffsetsRetriever;
    private TableInfo tableInfo;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // split initializing has finished.
    private boolean noMoreNewSplits = false;

    /**
     * Whether the initial partition discovery has been completed. Following FLIP-288, this flag
     * alone determines offset strategy: partitions discovered before this flag is set to {@code
     * true} use the user-configured {@link #startingOffsetsInitializer}, while partitions
     * discovered after use {@link #newDiscoveryOffsetsInitializer} (earliest) to prevent data loss.
     */
    private boolean initialDiscoveryFinished;

    private boolean lakeEnabled = false;

    private volatile boolean closed = false;

    /**
     * Whether a checkpoint has been successfully completed before.
     *
     * <p>This flag is used in {@link #close()} to decide whether the kv snapshot lease should be
     * dropped:
     *
     * <ul>
     *   <li>If {@code false} (no checkpoint completed), the lease ID has not been persisted to
     *       checkpoint state, so it is safe to drop the lease on close — no future restore will
     *       reference it.
     *   <li>If {@code true} (at least one checkpoint completed), the lease ID has been persisted
     *       and may be restored via {@link FlinkSource#restoreEnumerator}. Dropping the lease on
     *       close would invalidate the restored lease, so it must be kept.
     * </ul>
     *
     * <p>This field is initialized to {@code true} when restoring from a checkpoint (i.e., {@code
     * assignedTableBuckets} is non-empty), and set to {@code true} in {@link
     * #notifyCheckpointComplete(long)} upon the first successful checkpoint.
     */
    private volatile boolean checkpointTriggeredBefore;

    @Nullable private final Predicate partitionFilters;

    @Nullable private final LakeSource<LakeSplit> lakeSource;

    private final int splitPerAssignmentBatchSize;

    public FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            LeaseContext leaseContext,
            boolean checkpointTriggeredBefore) {
        this(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                context,
                startingOffsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                streaming,
                partitionFilters,
                lakeSource,
                leaseContext,
                checkpointTriggeredBefore);
    }

    public FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            LeaseContext leaseContext,
            boolean checkpointTriggeredBefore) {
        this(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                context,
                Collections.emptySet(),
                Collections.emptyMap(),
                null,
                startingOffsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                splitPerAssignmentBatchSize,
                streaming,
                partitionFilters,
                lakeSource,
                leaseContext,
                checkpointTriggeredBefore,
                false,
                Collections.emptyList());
    }

    public FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            Set<TableBucket> assignedTableBuckets,
            Map<Long, String> assignedPartitions,
            List<SourceSplitBase> pendingHybridLakeFlussSplits,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            LeaseContext leaseContext,
            boolean checkpointTriggeredBefore,
            boolean initialDiscoveryFinished,
            List<SourceSplitBase> unassignedSplits) {
        this(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                context,
                assignedTableBuckets,
                assignedPartitions,
                pendingHybridLakeFlussSplits,
                startingOffsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                streaming,
                partitionFilters,
                lakeSource,
                leaseContext,
                checkpointTriggeredBefore,
                initialDiscoveryFinished,
                unassignedSplits);
    }

    public FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            Set<TableBucket> assignedTableBuckets,
            Map<Long, String> assignedPartitions,
            List<SourceSplitBase> pendingHybridLakeFlussSplits,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            LeaseContext leaseContext,
            boolean checkpointTriggeredBefore,
            boolean initialDiscoveryFinished,
            Collection<SourceSplitBase> unassignedSplits) {
        this(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                context,
                assignedTableBuckets,
                assignedPartitions,
                pendingHybridLakeFlussSplits,
                startingOffsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                splitPerAssignmentBatchSize,
                streaming,
                partitionFilters,
                lakeSource,
                new WorkerExecutor(context),
                leaseContext,
                checkpointTriggeredBefore,
                initialDiscoveryFinished,
                unassignedSplits);
    }

    FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            Set<TableBucket> assignedTableBuckets,
            Map<Long, String> assignedPartitions,
            List<SourceSplitBase> pendingHybridLakeFlussSplits,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            WorkerExecutor workerExecutor,
            LeaseContext leaseContext,
            boolean checkpointTriggeredBefore) {
        this(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                context,
                assignedTableBuckets,
                assignedPartitions,
                pendingHybridLakeFlussSplits,
                startingOffsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                streaming,
                partitionFilters,
                lakeSource,
                workerExecutor,
                leaseContext,
                checkpointTriggeredBefore,
                false,
                Collections.emptyList());
    }

    FlinkSourceEnumerator(
            TablePath tablePath,
            Configuration flussConf,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            SplitEnumeratorContext<SourceSplitBase> context,
            Set<TableBucket> assignedTableBuckets,
            Map<Long, String> assignedPartitions,
            List<SourceSplitBase> pendingHybridLakeFlussSplits,
            OffsetsInitializer startingOffsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            WorkerExecutor workerExecutor,
            LeaseContext leaseContext,
            boolean checkpointTriggeredBefore,
            boolean initialDiscoveryFinished,
            Collection<SourceSplitBase> unassignedSplits) {
        checkArgument(
                splitPerAssignmentBatchSize > 0,
                "Split assignment batch size must be positive, but was %s.",
                splitPerAssignmentBatchSize);
        this.tablePath = checkNotNull(tablePath);
        this.flussConf = checkNotNull(flussConf);
        this.hasPrimaryKey = hasPrimaryKey;
        this.isPartitioned = isPartitioned;
        this.context = checkNotNull(context);
        this.pendingSplitAssignment = new HashMap<>();
        this.assignedTableBuckets = new HashSet<>(assignedTableBuckets);
        this.assignedPartitions = new HashMap<>(assignedPartitions);
        this.pendingHybridLakeFlussSplits =
                pendingHybridLakeFlussSplits == null
                        ? null
                        : new LinkedList<>(pendingHybridLakeFlussSplits);
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.newDiscoveryOffsetsInitializer = OffsetsInitializer.earliest();
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.streaming = streaming;
        this.partitionFilters = partitionFilters;
        this.stoppingOffsetsInitializer =
                streaming ? new NoStoppingOffsetsInitializer() : OffsetsInitializer.latest();
        this.lakeSource = lakeSource;
        this.workerExecutor = workerExecutor;
        this.leaseContext = leaseContext;
        this.checkpointTriggeredBefore = checkpointTriggeredBefore;
        this.splitPerAssignmentBatchSize = splitPerAssignmentBatchSize;
        this.initialDiscoveryFinished = initialDiscoveryFinished;
        this.unassignedSplits = indexByPartition(unassignedSplits);
    }

    @Override
    public void start() {
        // init admin client
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(flussAdmin, tablePath);
        try {
            tableInfo = flussAdmin.getTableInfo(tablePath).get();
            lakeEnabled = tableInfo.getTableConfig().isDataLakeEnabled();
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to get table info for %s", tablePath),
                    ExceptionUtils.stripCompletionException(e));
        }

        // Find splits where the start offset has been initialized but not yet assigned to readers.
        // These splits must not be reinitialized to keep offsets consistent with first discovery.
        final Collection<SourceSplitBase> preinitializedSplits = unassignedSplits.values();
        if (!unassignedSplits.isEmpty()) {
            addSplitToPendingAssignments(preinitializedSplits);
        }

        if (isPartitioned) {
            if (streaming) {
                if (lakeSource != null) {
                    // we'll need to consider lake splits
                    List<SourceSplitBase> hybridLakeFlussSplits = generateHybridLakeFlussSplits();
                    if (hybridLakeFlussSplits != null) {
                        LOG.info(
                                "Generated {} hybrid lake splits for table {}.",
                                hybridLakeFlussSplits.size(),
                                tablePath);
                        // handle hybrid lake fluss splits firstly
                        handleSplitsAdd(hybridLakeFlussSplits, null);
                    }
                }

                if (scanPartitionDiscoveryIntervalMs > 0) {
                    // should do partition discovery
                    LOG.info(
                            "Starting the FlussSourceEnumerator for table {} "
                                    + "with new partition discovery interval of {} ms.",
                            tablePath,
                            scanPartitionDiscoveryIntervalMs);
                    // discover new partitions and handle new partitions at fixed delay.
                    workerExecutor.callAsyncAtFixedDelay(
                            this::listPartitions,
                            this::checkPartitionChanges,
                            0,
                            scanPartitionDiscoveryIntervalMs);
                } else {
                    // just call once
                    LOG.info(
                            "Starting the FlussSourceEnumerator for table {} without partition discovery.",
                            tablePath);
                    workerExecutor.callAsync(this::listPartitions, this::checkPartitionChanges);
                }
            } else {
                startInBatchMode();
            }
        } else {
            if (streaming) {
                startInStreamModeForNonPartitionedTable();
            } else {
                startInBatchMode();
            }
        }
    }

    private void startInBatchMode() {
        if (lakeEnabled) {
            if (lakeSource == null) {
                throw new IllegalStateException(
                        "The 'lakeSource' is null in batch mode. It should be set if lake is enabled.");
            }
            context.callAsync(
                    () -> {
                        List<SourceSplitBase> splits = generateHybridLakeFlussSplits();
                        // No lake snapshot exists, fall back to Fluss-only splits
                        if (splits == null) {
                            LOG.info(
                                    "No lake snapshot found for table {},"
                                            + " falling back to Fluss-only splits.",
                                    tablePath);
                            if (isPartitioned) {
                                Set<PartitionInfo> partitionInfos = listPartitions();
                                Collection<Partition> partitions =
                                        partitionInfos.stream()
                                                .map(
                                                        p ->
                                                                new Partition(
                                                                        p.getPartitionId(),
                                                                        p.getPartitionName()))
                                                .collect(Collectors.toList());
                                // Use log-only splits to avoid generating mixed split
                                // types (HybridSnapshotLogSplit + LogSplit) for
                                // primary-key tables, which is not supported.
                                splits =
                                        this.initLogTablePartitionSplits(
                                                partitions, startingOffsetsInitializer);
                            } else {
                                splits = this.getLogSplit(null, null);
                            }
                        }
                        return splits;
                    },
                    this::handleSplitsAdd);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Batch only supports when table option '%s' is set to true.",
                            ConfigOptions.TABLE_DATALAKE_ENABLED));
        }
    }

    private void startInStreamModeForNonPartitionedTable() {
        // If we have restored unassigned splits from checkpoint state, skip re-initialization.
        // These splits already have their offsets resolved and will be assigned to readers
        // when they register (via addReader -> assignPendingSplits).
        if (!pendingSplitAssignment.isEmpty()) {
            noMoreNewSplits = true;
            return;
        }

        if (lakeSource != null) {
            // Generate lake splits synchronously so that they are available before the
            // first checkpoint. This is consistent with the partitioned-table path in
            // start().
            List<SourceSplitBase> splits = generateHybridLakeFlussSplits();
            if (splits == null) {
                // no lake snapshot, fall back to normal Fluss splits
                splits = this.initNonPartitionedSplits();
            }
            handleSplitsAdd(splits, null);
        } else {
            // init bucket splits and assign
            context.callAsync(this::initNonPartitionedSplits, this::handleSplitsAdd);
        }
    }

    private List<SourceSplitBase> initNonPartitionedSplits() {
        if (hasPrimaryKey && startingOffsetsInitializer instanceof SnapshotOffsetsInitializer) {
            return getSnapshotAndLogSplits(getLatestKvSnapshotsAndRegister(null), null);
        } else {
            return getLogSplit(null, null);
        }
    }

    private Set<PartitionInfo> listPartitions() {
        if (closed) {
            return Collections.emptySet();
        }
        try {
            List<PartitionInfo> partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
            partitionInfos = applyPartitionFilter(partitionInfos);
            return new LinkedHashSet<>(partitionInfos);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to list partitions for %s", tablePath),
                    ExceptionUtils.stripCompletionException(e));
        }
    }

    /** Apply partition filter. */
    private List<PartitionInfo> applyPartitionFilter(List<PartitionInfo> partitionInfos) {
        if (partitionFilters == null) {
            return partitionInfos;
        } else {
            int originalSize = partitionInfos.size();
            RowType partitionRowType = PartitionUtils.partitionRowType(tableInfo);
            List<PartitionInfo> filteredPartitionInfos =
                    partitionInfos.stream()
                            .filter(
                                    partition ->
                                            partitionFilters.test(
                                                    PartitionUtils.toPartitionRow(
                                                            partition
                                                                    .getResolvedPartitionSpec()
                                                                    .getPartitionValues(),
                                                            partitionRowType)))
                            .collect(Collectors.toList());

            int filteredSize = filteredPartitionInfos.size();
            if (originalSize != filteredSize) {
                LOG.debug(
                        "Applied partition filter for table {}: {} partitions filtered down to {} "
                                + "matching partitions with predicate: {}. Matching partitions after filtering: {}",
                        tablePath,
                        originalSize,
                        filteredSize,
                        partitionFilters,
                        filteredPartitionInfos);
            } else {
                LOG.debug(
                        "Partition filter applied for table {}, but all {} partitions matched the predicate",
                        tablePath,
                        originalSize);
            }
            return filteredPartitionInfos;
        }
    }

    /** Init the splits for Fluss. */
    private void checkPartitionChanges(Set<PartitionInfo> partitionInfos, Throwable t) {
        if (closed) {
            // skip if the enumerator is closed to avoid unnecessary error logs
            return;
        }
        if (t != null) {
            LOG.error("Failed to list partitions for {}", tablePath, t);
            return;
        }

        LOG.debug(
                "Checking partition changes for table {}, found {} partitions",
                tablePath,
                partitionInfos.size());

        final PartitionChange partitionChange =
                getPartitionChange(partitionInfos, !initialDiscoveryFinished);

        if (partitionChange.isEmpty()) {
            // No partition changes found. For the empty-table case (no initial partitions
            // to track), mark initial discovery as finished immediately since there are
            // no splits that need to be persisted in state first.
            if (!initialDiscoveryFinished) {
                initialDiscoveryFinished = true;
            }
            LOG.debug("No partition changes detected for table {}", tablePath);
            return;
        }

        // handle removed partitions
        if (!partitionChange.removedPartitions.isEmpty()) {
            LOG.info(
                    "Handling {} removed partitions for table {}: {}",
                    partitionChange.removedPartitions.size(),
                    tablePath,
                    partitionChange.removedPartitions);
            handlePartitionsRemoved(partitionChange.removedPartitions);
        }

        // handle initial partitions and new partitions
        boolean hasNewOrInitialPartitions =
                !partitionChange.initialPartitions.isEmpty()
                        || !partitionChange.newPartitions.isEmpty();
        if (hasNewOrInitialPartitions) {
            Collection<Partition> allNewPartitions = new ArrayList<>();
            allNewPartitions.addAll(partitionChange.initialPartitions);
            allNewPartitions.addAll(partitionChange.newPartitions);
            LOG.info(
                    "Handling {} partitions for table {} (initial={}, new={}): {}",
                    allNewPartitions.size(),
                    tablePath,
                    partitionChange.initialPartitions.size(),
                    partitionChange.newPartitions.size(),
                    allNewPartitions);
            workerExecutor.callAsync(
                    () -> initPartitionedSplits(partitionChange),
                    (splits, throwable) -> {
                        handleSplitsAdd(splits, throwable);
                    });
        }
    }

    private PartitionChange getPartitionChange(
            Set<PartitionInfo> fetchedPartitionInfos, boolean initialDiscovery) {
        final Set<Partition> allNewPartitions =
                fetchedPartitionInfos.stream()
                        .map(p -> new Partition(p.getPartitionId(), p.getPartitionName()))
                        .collect(Collectors.toSet());
        final Set<Partition> removedPartitions = new HashSet<>();

        Set<Partition> assignedOrPendingPartitions = new HashSet<>();
        assignedPartitions.forEach(
                (partitionId, partitionName) ->
                        assignedOrPendingPartitions.add(new Partition(partitionId, partitionName)));

        pendingSplitAssignment.values().stream()
                .flatMap(Collection::stream)
                .forEach(
                        split -> {
                            long partitionId =
                                    checkNotNull(
                                            split.getTableBucket().getPartitionId(),
                                            "partition id shouldn't be null for the splits of partitioned table.");
                            String partitionName =
                                    checkNotNull(
                                            split.getPartitionName(),
                                            "partition name shouldn't be null for the splits of partitioned table.");
                            assignedOrPendingPartitions.add(
                                    new Partition(partitionId, partitionName));
                        });

        assignedOrPendingPartitions.forEach(
                p -> {
                    if (!allNewPartitions.remove(p)) {
                        removedPartitions.add(p);
                    }
                });

        if (!removedPartitions.isEmpty()) {
            LOG.info("Discovered removed partitions: {}", removedPartitions);
        }
        if (!allNewPartitions.isEmpty()) {
            LOG.info("Discovered new partitions: {}", allNewPartitions);
        }

        // Following Kafka's FLIP-288 pattern: if this is the initial discovery,
        // all new partitions are classified as "initial partitions" and will use
        // the user-configured offset initializer. After initial discovery is done,
        // all new partitions are classified as "new partitions" and will use earliest.
        Set<Partition> initialPartitions = new HashSet<>();
        Set<Partition> newPartitions;
        if (initialDiscovery) {
            initialPartitions.addAll(allNewPartitions);
            newPartitions = Collections.emptySet();
        } else {
            newPartitions = allNewPartitions;
        }

        return new PartitionChange(initialPartitions, newPartitions, removedPartitions);
    }

    private List<SourceSplitBase> initPartitionedSplits(PartitionChange partitionChange) {
        Collection<Partition> initialPartitions = partitionChange.initialPartitions;
        Collection<Partition> newPartitions = partitionChange.newPartitions;

        if (hasPrimaryKey && startingOffsetsInitializer instanceof SnapshotOffsetsInitializer) {
            // Snapshot mode for PK tables is already safe: it reads the snapshot or falls back
            // to earliest offsets when no snapshot is available.
            List<Partition> allPartitions = new ArrayList<>();
            allPartitions.addAll(initialPartitions);
            allPartitions.addAll(newPartitions);
            return initPrimaryKeyTablePartitionSplits(allPartitions);
        } else {
            // For log tables (or PK tables in non-snapshot mode), use FLIP-288 semantics:
            // - Initial partitions: use user-configured offset
            // - New partitions: use earliest to prevent data loss
            List<SourceSplitBase> splits = new ArrayList<>();
            if (!initialPartitions.isEmpty()) {
                splits.addAll(
                        initLogTablePartitionSplits(initialPartitions, startingOffsetsInitializer));
            }
            if (!newPartitions.isEmpty()) {
                splits.addAll(
                        initLogTablePartitionSplits(newPartitions, newDiscoveryOffsetsInitializer));
            }
            return splits;
        }
    }

    private List<SourceSplitBase> initLogTablePartitionSplits(
            Collection<Partition> newPartitions, OffsetsInitializer effectiveOffsetsInitializer) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (Partition partition : newPartitions) {
            splits.addAll(
                    getLogSplit(
                            partition.getPartitionId(),
                            partition.getPartitionName(),
                            effectiveOffsetsInitializer));
        }
        return splits;
    }

    private List<SourceSplitBase> initPrimaryKeyTablePartitionSplits(
            Collection<Partition> newPartitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (Partition partition : newPartitions) {
            String partitionName = partition.getPartitionName();
            splits.addAll(
                    getSnapshotAndLogSplits(
                            getLatestKvSnapshotsAndRegister(partitionName), partitionName));
        }
        return splits;
    }

    private KvSnapshots getLatestKvSnapshotsAndRegister(@Nullable String partitionName) {
        long tableId;
        Long partitionId;
        Map<Integer, Long> snapshotIds = new HashMap<>();
        Map<Integer, Long> logOffsets = new HashMap<>();

        // Get the latest kv snapshots and acquire kvSnapshot lease.
        try {
            KvSnapshots kvSnapshots = getLatestKvSnapshots(partitionName);

            tableId = kvSnapshots.getTableId();
            partitionId = kvSnapshots.getPartitionId();

            Map<TableBucket, Long> bucketsToLease = new HashMap<>();
            for (TableBucket tb : kvSnapshots.getTableBuckets()) {
                int bucket = tb.getBucket();
                OptionalLong snapshotIdOpt = kvSnapshots.getSnapshotId(bucket);
                OptionalLong logOffsetOpt = kvSnapshots.getLogOffset(bucket);
                if (snapshotIdOpt.isPresent() && !ignoreTableBucket(tb)) {
                    bucketsToLease.put(tb, snapshotIdOpt.getAsLong());
                }

                snapshotIds.put(
                        bucket, snapshotIdOpt.isPresent() ? snapshotIdOpt.getAsLong() : null);
                logOffsets.put(bucket, logOffsetOpt.isPresent() ? logOffsetOpt.getAsLong() : null);
            }

            if (!bucketsToLease.isEmpty()) {
                String kvSnapshotLeaseId = leaseContext.getKvSnapshotLeaseId();
                LOG.info(
                        "Try to acquire kv snapshot lease {} for table {}",
                        kvSnapshotLeaseId,
                        PhysicalTablePath.of(tablePath, partitionName));
                long kvSnapshotLeaseDurationMs = leaseContext.getKvSnapshotLeaseDurationMs();
                try {
                    Set<TableBucket> unavailableTableBucketSet =
                            flussAdmin
                                    .createKvSnapshotLease(
                                            kvSnapshotLeaseId, kvSnapshotLeaseDurationMs)
                                    .acquireSnapshots(bucketsToLease)
                                    .get()
                                    .getUnavailableTableBucketSet();
                    if (!unavailableTableBucketSet.isEmpty()) {
                        LOG.error(
                                "Failed to acquire kv snapshot lease for table {}: {}.",
                                tablePath,
                                unavailableTableBucketSet);
                    }
                } catch (Exception e) {
                    if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class)
                            .isPresent()) {
                        LOG.warn(
                                "Failed to acquire kv snapshot lease for table {} because the "
                                        + "server does not support kv snapshot lease API. "
                                        + "Snapshots may be cleaned up earlier than expected. "
                                        + "Please upgrade the Fluss server to version 0.9 or later.",
                                tablePath,
                                e);
                    } else {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to get table snapshot for %s", tablePath),
                    ExceptionUtils.stripCompletionException(e));
        }

        return new KvSnapshots(tableId, partitionId, snapshotIds, logOffsets);
    }

    private KvSnapshots getLatestKvSnapshots(@Nullable String partitionName) throws Exception {
        if (partitionName == null) {
            return flussAdmin.getLatestKvSnapshots(tablePath).get();
        } else {
            return flussAdmin.getLatestKvSnapshots(tablePath, partitionName).get();
        }
    }

    private List<SourceSplitBase> getSnapshotAndLogSplits(
            KvSnapshots snapshots, @Nullable String partitionName) {
        long tableId = snapshots.getTableId();
        Long partitionId = snapshots.getPartitionId();
        List<SourceSplitBase> splits = new ArrayList<>();
        List<Integer> bucketsNeedInitOffset = new ArrayList<>();
        for (Integer bucketId : snapshots.getBucketIds()) {
            TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
            // the ignore logic rely on the enumerator will always send splits for same bucket
            // in one batch; if we can ignore the bucket, we can skip all the splits(snapshot +
            // log) for the bucket
            if (ignoreTableBucket(tb)) {
                continue;
            }
            OptionalLong snapshotId = snapshots.getSnapshotId(bucketId);
            if (snapshotId.isPresent()) {
                // hybrid snapshot log split;
                OptionalLong logOffset = snapshots.getLogOffset(bucketId);
                checkState(
                        logOffset.isPresent(),
                        "Log offset should be present if snapshot id is present.");
                splits.add(
                        new HybridSnapshotLogSplit(
                                tb, partitionName, snapshotId.getAsLong(), logOffset.getAsLong()));
            } else {
                bucketsNeedInitOffset.add(bucketId);
            }
        }

        if (!bucketsNeedInitOffset.isEmpty()) {
            startingOffsetsInitializer
                    .getBucketOffsets(partitionName, bucketsNeedInitOffset, bucketOffsetsRetriever)
                    .forEach(
                            (bucketId, startingOffset) ->
                                    splits.add(
                                            new LogSplit(
                                                    new TableBucket(tableId, partitionId, bucketId),
                                                    partitionName,
                                                    startingOffset)));
        }

        return splits;
    }

    private List<SourceSplitBase> getLogSplit(
            @Nullable Long partitionId, @Nullable String partitionName) {
        return getLogSplit(partitionId, partitionName, startingOffsetsInitializer);
    }

    private List<SourceSplitBase> getLogSplit(
            @Nullable Long partitionId,
            @Nullable String partitionName,
            OffsetsInitializer effectiveStartingOffsetsInitializer) {
        // always assume the bucket is from 0 to bucket num
        List<SourceSplitBase> splits = new ArrayList<>();
        List<Integer> bucketsNeedInitOffset = new ArrayList<>();
        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
            if (ignoreTableBucket(tableBucket)) {
                continue;
            }
            bucketsNeedInitOffset.add(bucketId);
        }

        if (!bucketsNeedInitOffset.isEmpty()) {
            Map<Integer, Long> startingOffsets =
                    effectiveStartingOffsetsInitializer.getBucketOffsets(
                            partitionName, bucketsNeedInitOffset, bucketOffsetsRetriever);
            Map<Integer, Long> stoppingOffsets =
                    stoppingOffsetsInitializer.getBucketOffsets(
                            partitionName, bucketsNeedInitOffset, bucketOffsetsRetriever);
            startingOffsets.forEach(
                    (bucketId, startingOffset) ->
                            splits.add(
                                    new LogSplit(
                                            new TableBucket(
                                                    tableInfo.getTableId(), partitionId, bucketId),
                                            partitionName,
                                            startingOffset,
                                            stoppingOffsets.get(bucketId))));
        }
        return splits;
    }

    /** Return the hybrid lake and fluss splits. Return null if no lake snapshot. */
    @Nullable
    private List<SourceSplitBase> generateHybridLakeFlussSplits() {
        // Restored from checkpoint with pending lake splits — return them directly
        // without re-generating.
        if (pendingHybridLakeFlussSplits != null) {
            LOG.info("Still have pending lake fluss splits, shouldn't list splits again.");
            return pendingHybridLakeFlussSplits;
        }
        try {
            LakeSplitGenerator lakeSplitGenerator =
                    new LakeSplitGenerator(
                            tableInfo,
                            flussAdmin,
                            lakeSource,
                            bucketOffsetsRetriever,
                            stoppingOffsetsInitializer,
                            tableInfo.getNumBuckets(),
                            this::listPartitions);
            List<SourceSplitBase> generatedSplits =
                    lakeSplitGenerator.generateHybridLakeFlussSplits();
            if (generatedSplits == null) {
                // no hybrid lake splits, set the pending splits to empty list
                pendingHybridLakeFlussSplits = Collections.emptyList();
                return null;
            } else {
                pendingHybridLakeFlussSplits = generatedSplits;
                return generatedSplits;
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to generate hybrid lake fluss splits", e);
        }
    }

    private boolean ignoreTableBucket(TableBucket tableBucket) {
        // if the bucket has been assigned, we can ignore it
        // the bucket has been assigned, skip
        return assignedTableBuckets.contains(tableBucket);
    }

    private void handlePartitionsRemoved(Collection<Partition> removedPartitionInfo) {
        if (removedPartitionInfo.isEmpty()) {
            return;
        }

        Map<Long, String> removedPartitionsMap =
                removedPartitionInfo.stream()
                        .collect(
                                Collectors.toMap(
                                        Partition::getPartitionId, Partition::getPartitionName));

        // remove from the pending split assignment
        pendingSplitAssignment.forEach(
                (reader, splits) ->
                        splits.removeIf(
                                split -> {
                                    // Never remove LakeSnapshotSplit, because during union reads,
                                    // data from the lake must still be read even if the partition
                                    // has already expired in Fluss.
                                    if (split instanceof LakeSnapshotSplit) {
                                        return false;
                                    }

                                    // Similar to LakeSnapshotSplit, if it contains any lake split,
                                    // never remove it; otherwise, it can be removed when the Fluss
                                    // partition expires.
                                    if (split instanceof LakeSnapshotAndFlussLogSplit) {
                                        LakeSnapshotAndFlussLogSplit hybridSplit =
                                                (LakeSnapshotAndFlussLogSplit) split;
                                        if (!hybridSplit.isLakeSplitFinished()) {
                                            return false;
                                        }
                                    }

                                    return removedPartitionsMap.containsKey(
                                            split.getTableBucket().getPartitionId());
                                }));

        // send partition removed event to all readers
        PartitionsRemovedEvent event = new PartitionsRemovedEvent(removedPartitionsMap);
        for (int readerId : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(readerId, event);
        }
    }

    private void handleSplitsAdd(List<SourceSplitBase> splits, Throwable t) {
        if (t != null) {
            if (isPartitioned && streaming && scanPartitionDiscoveryIntervalMs > 0) {
                // it means continuously read new partition splits, not throw exception, temporally
                // warn it to avoid job fail. TODO: fix me in #288
                LOG.warn("Failed to list splits for {}.", tablePath, t);
                return;
            } else {
                throw new FlinkRuntimeException(
                        String.format("Failed to list splits for %s to read due to ", tablePath),
                        t);
            }
        }

        initialDiscoveryFinished = true;
        for (SourceSplitBase split : splits) {
            unassignedSplits.put(split.getTableBucket(), split);
        }

        if (isPartitioned) {
            if (!streaming || scanPartitionDiscoveryIntervalMs <= 0) {
                // if not streaming or partition discovery is disabled
                // should only add splits only once, no more new splits
                noMoreNewSplits = true;
            }
        } else {
            // if not partitioned, only will add splits only once,
            // so, noMoreNewPartitionSplits should be set to true
            noMoreNewSplits = true;
        }
        doHandleSplitsAdd(splits);
    }

    private void doHandleSplitsAdd(List<SourceSplitBase> splits) {
        addSplitToPendingAssignments(splits);
        assignPendingSplits(context.registeredReaders().keySet());
    }

    private void addSplitToPendingAssignments(Collection<SourceSplitBase> newSplits) {
        for (SourceSplitBase sourceSplit : newSplits) {
            int task = getSplitOwner(sourceSplit);
            pendingSplitAssignment.computeIfAbsent(task, k -> new LinkedList<>()).add(sourceSplit);
        }
    }

    private void assignPendingSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<SourceSplitBase>> incrementalAssignment = new HashMap<>();

        // Check if there's any pending splits for given readers
        for (int pendingReader : pendingReaders) {
            checkReaderRegistered(pendingReader);

            // Remove pending assignment for the reader
            final List<SourceSplitBase> pendingAssignmentForReader =
                    pendingSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Put pending assignment into incremental assignment
                incrementalAssignment
                        .computeIfAbsent(pendingReader, (ignored) -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);

                // Mark pending bucket assignment as already assigned
                pendingAssignmentForReader.forEach(
                        split -> {
                            TableBucket tableBucket = split.getTableBucket();
                            assignedTableBuckets.add(tableBucket);
                            unassignedSplits.remove(tableBucket);

                            if (isPartitioned) {
                                long partitionId =
                                        checkNotNull(
                                                tableBucket.getPartitionId(),
                                                "partition id shouldn't be null for the splits of partitioned table.");
                                String partitionName =
                                        checkNotNull(
                                                split.getPartitionName(),
                                                "partition name shouldn't be null for the splits of partitioned table.");
                                assignedPartitions.put(partitionId, partitionName);
                            }
                        });

                if (pendingHybridLakeFlussSplits != null) {
                    Set<String> splitIdsToRemove =
                            pendingAssignmentForReader.stream()
                                    .map(SourceSplitBase::splitId)
                                    .collect(Collectors.toSet());
                    // removed from the pendingHybridLakeFlussSplits
                    // since this split already be assigned
                    pendingHybridLakeFlussSplits.removeIf(
                            split -> splitIdsToRemove.contains(split.splitId()));
                }
            }
        }

        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            int totalSplits = incrementalAssignment.values().stream().mapToInt(List::size).sum();
            Map<Integer, Integer> batchesPerReader =
                    incrementalAssignment.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry ->
                                                    (entry.getValue().size()
                                                                    + splitPerAssignmentBatchSize
                                                                    - 1)
                                                            / splitPerAssignmentBatchSize));
            LOG.info(
                    "Assigning splits to {} readers: totalSplits={}, batchesPerReader={}",
                    incrementalAssignment.size(),
                    totalSplits,
                    batchesPerReader);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Assigning splits to readers {}", incrementalAssignment);
            }
            for (Map.Entry<Integer, List<SourceSplitBase>> entry :
                    incrementalAssignment.entrySet()) {
                int readerId = entry.getKey();
                List<SourceSplitBase> splits = entry.getValue();
                Lists.partition(splits, splitPerAssignmentBatchSize).stream()
                        .forEach(
                                batchSplits -> {
                                    context.assignSplits(
                                            new SplitsAssignment<>(
                                                    Collections.singletonMap(
                                                            readerId, batchSplits)));
                                });
            }
        }

        if (noMoreNewSplits) {
            LOG.info(
                    "No more FlussSplits to assign. Sending NoMoreSplitsEvent to reader {}",
                    pendingReaders);
            pendingReaders.forEach(context::signalNoMoreSplits);
        }
    }

    /**
     * Returns the index of the target subtask that a specific split should be assigned to.
     *
     * <p>The resulting distribution of splits of a single table has the following contract:
     *
     * <ul>
     *   <li>1. Splits in same bucket are assigned to same subtask
     *   <li>2. Uniformly distributed across subtasks
     *   <li>3. For partitioned table, the buckets in same partition are round-robin distributed
     *       (strictly clockwise w.r.t. ascending subtask indices) by using the partition id as the
     *       offset from a starting index. The starting index is the index of the subtask which
     *       bucket 0 of the partition will be assigned to, determined using the partition id to
     *       make sure the partitions' buckets of a table are distributed uniformly
     * </ul>
     *
     * @param split the split to assign.
     * @return the id of the subtask that owns the split.
     */
    @VisibleForTesting
    protected int getSplitOwner(SourceSplitBase split) {
        TableBucket tableBucket = split.getTableBucket();
        int startIndex =
                tableBucket.getPartitionId() == null
                        ? 0
                        : ((tableBucket.getPartitionId().hashCode() * 31) & 0x7FFFFFFF)
                                % context.currentParallelism();

        // super hack logic, if the bucket is -1, it means the split is
        // for bucket unaware, like paimon unaware bucket log table,
        // we use hash split id to get the split owner
        // todo: refactor the split assign logic
        if (split.isLakeSplit() && tableBucket.getBucket() == -1) {
            return (split.splitId().hashCode() & 0x7FFFFFFF) % context.currentParallelism();
        }

        return (startIndex + tableBucket.getBucket()) % context.currentParallelism();
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the fluss source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionBucketsUnsubscribedEvent) {
            PartitionBucketsUnsubscribedEvent removedEvent =
                    (PartitionBucketsUnsubscribedEvent) sourceEvent;

            Set<Long> partitionsPendingRemove = new HashSet<>();
            // remove from the assigned table buckets
            for (TableBucket tableBucket : removedEvent.getRemovedTableBuckets()) {
                assignedTableBuckets.remove(tableBucket);
                partitionsPendingRemove.add(tableBucket.getPartitionId());
            }

            for (TableBucket tableBucket : assignedTableBuckets) {
                Long partitionId = tableBucket.getPartitionId();
                if (partitionId != null) {
                    // we shouldn't remove the partition if still there is buckets assigned.
                    boolean removed = partitionsPendingRemove.remove(partitionId);
                    if (removed && partitionsPendingRemove.isEmpty()) {
                        // no need to check the rest of the buckets
                        break;
                    }
                }
            }

            // remove partitions if no assigned buckets belong to the partition
            for (Long partitionToRemove : partitionsPendingRemove) {
                assignedPartitions.remove(partitionToRemove);
            }
        } else if (sourceEvent instanceof FinishedKvSnapshotConsumeEvent) {
            FinishedKvSnapshotConsumeEvent event = (FinishedKvSnapshotConsumeEvent) sourceEvent;
            long checkpointId = event.getCheckpointId();
            Set<TableBucket> tableBuckets = event.getTableBuckets();
            if (!tableBuckets.isEmpty()) {
                LOG.info(
                        "Received finished kv snapshot consumer event for buckets: {}, checkpoint id: {}",
                        tableBuckets,
                        checkpointId);
            }

            tableBuckets.forEach(tableBucket -> addConsumedBucket(checkpointId, tableBucket));
        }
    }

    @VisibleForTesting
    Map<Long, String> getAssignedPartitions() {
        return assignedPartitions;
    }

    @VisibleForTesting
    Map<Integer, List<SourceSplitBase>> getPendingSplitAssignment() {
        return pendingSplitAssignment;
    }

    @Override
    public void addSplitsBack(List<SourceSplitBase> splits, int subtaskId) {
        LOG.debug("Flink Source Enumerator adds splits back: {}", splits);
        for (SourceSplitBase split : splits) {
            unassignedSplits.put(split.getTableBucket(), split);
            assignedTableBuckets.remove(split.getTableBucket());
            if (isPartitioned) {
                assignedPartitions.remove(split.getTableBucket().getPartitionId());
            }
        }
        addSplitToPendingAssignments(splits);

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader: {} to Flink Source enumerator.", subtaskId);
        assignPendingSplits(Collections.singleton(subtaskId));
    }

    @Override
    public SourceEnumeratorState snapshotState(long checkpointId) {
        final SourceEnumeratorState enumeratorState =
                new SourceEnumeratorState(
                        assignedTableBuckets,
                        assignedPartitions,
                        pendingHybridLakeFlussSplits,
                        leaseContext.getKvSnapshotLeaseId(),
                        initialDiscoveryFinished,
                        unassignedSplits.values());
        LOG.debug(
                "Source Checkpoint is {}, initialDiscoveryFinished={}",
                enumeratorState,
                initialDiscoveryFinished);
        return enumeratorState;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        checkpointTriggeredBefore = true;

        if (hasPrimaryKey) {
            // lower than this checkpoint id.
            Set<TableBucket> consumedKvSnapshots = getAndRemoveConsumedBucketsUpTo(checkpointId);

            if (!consumedKvSnapshots.isEmpty()) {
                LOG.info(
                        "kv snapshot has already consumed and try to release kv snapshot lease for: {}, checkpoint id: {}",
                        consumedKvSnapshots,
                        checkpointId);

                // send request to fluss to unregister the kv snapshot lease.
                try {
                    flussAdmin
                            .createKvSnapshotLease(
                                    leaseContext.getKvSnapshotLeaseId(),
                                    leaseContext.getKvSnapshotLeaseDurationMs())
                            .releaseSnapshots(consumedKvSnapshots)
                            .get();
                } catch (Exception e) {
                    if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class)
                            .isPresent()) {
                        LOG.warn(
                                "Failed to release kv snapshot lease because the server does not support "
                                        + "kv snapshot lease API. Snapshots may remain in storage longer "
                                        + "than necessary. Please upgrade the Fluss server to version 0.9 "
                                        + "or later.",
                                e);
                    } else {
                        LOG.error(
                                "Failed to release kv snapshot lease. These snapshots need to re-enqueue",
                                e);
                        // use the current checkpoint id to re-enqueue the buckets
                        consumedKvSnapshots.forEach(
                                tableBucket -> addConsumedBucket(checkpointId, tableBucket));
                    }
                }
            }
        }
    }

    /** Add bucket who has been consumed kv snapshot to the consumedKvSnapshotMap. */
    public void addConsumedBucket(long checkpointId, TableBucket tableBucket) {
        consumedKvSnapshotMap.computeIfAbsent(checkpointId, k -> new HashSet<>()).add(tableBucket);
    }

    /** Get and remove the buckets who have been consumed kv snapshot up to the checkpoint id. */
    public Set<TableBucket> getAndRemoveConsumedBucketsUpTo(long checkpointId) {
        NavigableMap<Long, Set<TableBucket>> toRemove =
                consumedKvSnapshotMap.headMap(checkpointId, false);
        Set<TableBucket> result = new HashSet<>();
        for (Set<TableBucket> snapshots : toRemove.values()) {
            result.addAll(snapshots);
        }
        toRemove.clear();
        return result;
    }

    @Override
    public void close() throws IOException {
        try {
            maybeDropKvSnapshotLease();

            closed = true;

            if (workerExecutor != null) {
                workerExecutor.close();
            }

            if (flussAdmin != null) {
                flussAdmin.close();
            }

            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close Flink Source enumerator.", e);
        }
    }

    private void maybeDropKvSnapshotLease() throws Exception {
        if (flussAdmin != null
                && hasPrimaryKey
                && startingOffsetsInitializer instanceof SnapshotOffsetsInitializer
                && !checkpointTriggeredBefore) {
            // 1. Drop the kv snapshot lease for the batch mode.
            // 2. For streaming mode, if no checkpoint was triggered, the lease ID
            // has not been persisted to state. It won't be restored on restart,
            // so it's safe to drop it now.
            LOG.info(
                    "Dropping kv snapshot lease {} when source enumerator close. isStreaming {}",
                    leaseContext.getKvSnapshotLeaseId(),
                    streaming);
            try {
                flussAdmin
                        .createKvSnapshotLease(
                                leaseContext.getKvSnapshotLeaseId(),
                                leaseContext.getKvSnapshotLeaseDurationMs())
                        .dropLease()
                        .get();
            } catch (Exception e) {
                if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class)
                        .isPresent()) {
                    LOG.warn(
                            "Failed to drop kv snapshot lease because the server does not support "
                                    + "kv snapshot lease API. Please upgrade the Fluss server to "
                                    + "version 0.9 or later.",
                            e);
                } else {
                    throw e;
                }
            }
        }
    }

    private static Map<TableBucket, SourceSplitBase> indexByPartition(
            Collection<SourceSplitBase> splits) {
        return splits.stream().collect(Collectors.toMap(SourceSplitBase::getTableBucket, e -> e));
    }

    // --------------- private class ---------------
    /** A container class to hold the newly added partitions and removed partitions. */
    private static class PartitionChange {
        private final Collection<Partition> initialPartitions;
        private final Collection<Partition> newPartitions;
        private final Collection<Partition> removedPartitions;

        PartitionChange(
                Collection<Partition> initialPartitions,
                Collection<Partition> newPartitions,
                Collection<Partition> removedPartitions) {
            this.initialPartitions = initialPartitions;
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public boolean isEmpty() {
            return initialPartitions.isEmpty()
                    && newPartitions.isEmpty()
                    && removedPartitions.isEmpty();
        }
    }

    /** A container class to hold the partition id and partition name. */
    private static class Partition {
        final long partitionId;
        final String partitionName;

        Partition(long partitionId, String partitionName) {
            this.partitionId = partitionId;
            this.partitionName = partitionName;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public String getPartitionName() {
            return partitionName;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Partition partition = (Partition) o;
            return partitionId == partition.partitionId
                    && Objects.equals(partitionName, partition.partitionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId, partitionName);
        }

        @Override
        public String toString() {
            return "Partition{" + "id=" + partitionId + ", name='" + partitionName + '\'' + '}';
        }
    }
}
