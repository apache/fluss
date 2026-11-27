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

package org.apache.fluss.flink.action.bulkload;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.BulkLoadBuildContext;
import org.apache.fluss.client.metadata.BulkLoadHandle;
import org.apache.fluss.client.metadata.BulkLoadInfo;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.BulkLoad;
import org.apache.fluss.client.table.writer.BulkLoadBucketWriter;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.BulkLoadTargetWithdrawnException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.flink.adapter.SinkAdapter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.TypeUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.utils.PartitionUtils.generateAutoPartitionTime;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Bounded initialization from one immutable source snapshot, resumable after old writers stop. */
final class LoadLakeToFluss {

    private static final Logger LOG = LoggerFactory.getLogger(LoadLakeToFluss.class);

    private LoadLakeToFluss() {}

    static <Split extends LakeSplit> JobClient submit(
            StreamExecutionEnvironment executionEnvironment,
            Configuration config,
            Admin admin,
            BulkLoad bulkLoad,
            TableInfo table,
            LakeSource<Split> source,
            @Nullable Long snapshotId,
            String bulkLoadId)
            throws Exception {
        LoadPlan plan = new LoadPlan(table, bulkLoadId, snapshotId);
        BulkLoadBuildContext context = null;
        List<LoadTask> tasks = new ArrayList<>();
        if (snapshotId == null) {
            LOG.info(
                    "Source lake table {} has no snapshot; the job will only enable lake access.",
                    table.getLakeTablePath());
        } else if (!table.getTableConfig().isDataLakeEnabled()) {
            BulkLoadInfo previous =
                    findLoad(bulkLoad, new BulkLoadHandle(table.getTableId(), bulkLoadId), config);
            Set<String> selectedPartitions = null;
            if (previous != null) {
                // The caller must preserve the original source snapshot. The SDK does not
                // bind the Bulk Load ID to Paimon, so a changed snapshot can mix source versions.
                checkState(
                        previous.getState() != BulkLoadInfo.State.ABORTED,
                        "Bulk Load %s was aborted and may contain partially imported data.",
                        bulkLoadId);
                List<String> originalNames = Collections.emptyList();
                if (table.isPartitioned()) {
                    selectedPartitions = restorePartitions(admin, table, previous, config);
                    originalNames = new ArrayList<>(selectedPartitions);
                }
                // An existing ID replays its frozen context without creating another load.
                context = await(bulkLoad.begin(bulkLoadId, originalNames), config);
            }
            if (previous != null && previous.getState() == BulkLoadInfo.State.COMMITTED) {
                LOG.info(
                        "Bulk Load {} is already committed; only finalizing lake initialization.",
                        bulkLoadId);
            } else {
                SimpleVersionedSerializer<Split> serializer = source.getSplitSerializer();
                Map<String, Map<Integer, List<Split>>> grouped =
                        planBucketSplits(table, source, snapshotId, selectedPartitions);
                if (context == null) {
                    context = beginLoad(bulkLoad, admin, table, plan, grouped.keySet(), config);
                }
                if (context != null) {
                    tasks = createLoadTasks(table, context, grouped, serializer);
                }
            }
        }
        return submitJob(executionEnvironment, config, source, plan, context, tasks);
    }

    private static Set<String> restorePartitions(
            Admin admin, TableInfo table, BulkLoadInfo previous, Configuration config)
            throws Exception {
        Set<Long> existingPartitionIds = new HashSet<>();
        for (PartitionInfo partition :
                await(admin.listPartitionInfos(table.getTablePath()), config)) {
            existingPartitionIds.add(partition.getPartitionId());
        }
        Set<String> selectedPartitions = new LinkedHashSet<>();
        for (BulkLoadInfo.TargetProgress target : previous.getTargets()) {
            String originalPartitionName = target.getPartitionName();
            checkState(
                    originalPartitionName != null,
                    "Cannot resume Bulk Load %s: the coordinator response is missing the original "
                            + "name of target partition %s.",
                    previous.getHandle().getBulkLoadId(),
                    target.getPartitionId());
            if (!existingPartitionIds.contains(target.getPartitionId())) {
                LOG.warn(
                        "Bulk Load {} target {} (partition ID {}) was deleted and will be skipped.",
                        previous.getHandle().getBulkLoadId(),
                        originalPartitionName,
                        target.getPartitionId());
            }
            // Replay Begin with the complete original names. The writer uses the frozen ID to
            // skip a withdrawn target, even if a new partition has since reused its name.
            selectedPartitions.add(originalPartitionName);
        }
        return selectedPartitions;
    }

    private static <Split extends LakeSplit>
            Map<String, Map<Integer, List<Split>>> planBucketSplits(
                    TableInfo table,
                    LakeSource<Split> source,
                    long snapshotId,
                    @Nullable Set<String> selectedPartitions)
                    throws IOException {
        Map<String, Map<Integer, List<Split>>> grouped = new LinkedHashMap<>();
        // Selected target names limit the scan; file splits still come from the source snapshot.
        for (Split split :
                planSplits(table, source, snapshotId, selectedPartitions, Instant.now())) {
            checkArgument(
                    split.bucket() >= 0,
                    "Lake snapshot contains unsupported bucket %s.",
                    split.bucket());
            String partition = table.isPartitioned() ? partitionName(table, split) : null;
            grouped.computeIfAbsent(partition, ignored -> new LinkedHashMap<>())
                    .computeIfAbsent(split.bucket(), ignored -> new ArrayList<>())
                    .add(split);
        }
        return grouped;
    }

    /**
     * Returns null when a partitioned table has no selected source partitions to load. A
     * non-partitioned table always has one physical target, even without source rows.
     */
    @Nullable
    private static BulkLoadBuildContext beginLoad(
            BulkLoad bulkLoad,
            Admin admin,
            TableInfo table,
            LoadPlan plan,
            Set<String> sourcePartitions,
            Configuration config)
            throws Exception {
        List<String> partitionNames =
                table.isPartitioned() ? new ArrayList<>(sourcePartitions) : Collections.emptyList();
        // A non-partitioned table is one physical target even when it has no source rows.
        boolean hasLoadTargets = !table.isPartitioned() || !partitionNames.isEmpty();
        if (!hasLoadTargets) {
            return null;
        }
        for (String name : partitionNames) {
            await(
                    admin.createPartition(
                            table.getTablePath(),
                            ResolvedPartitionSpec.fromPartitionName(table.getPartitionKeys(), name)
                                    .toPartitionSpec(),
                            true),
                    config);
        }
        LOG.info(
                "Preparing Bulk Load {} for table {} (table ID {}) from lake snapshot {}.",
                plan.bulkLoadId,
                plan.tablePath,
                plan.tableId,
                plan.sourceSnapshotId);
        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(3));
        ExecutionException lastFailure;
        do {
            try {
                return await(bulkLoad.begin(plan.bulkLoadId, partitionNames), config);
            } catch (ExecutionException e) {
                Throwable cause = ExceptionUtils.stripExecutionException(e);
                // Creation can complete before the coordinator's event thread sees the metadata.
                // Reusing the ID makes Begin safe to retry while that metadata becomes visible.
                if (!(cause instanceof TableNotExistException
                        || cause instanceof PartitionNotExistException)) {
                    throw e;
                }
                lastFailure = e;
                LOG.debug("Waiting for Bulk Load targets of {} to become visible.", plan.tablePath);
                TimeUnit.MILLISECONDS.sleep(Math.min(3000L, deadline.timeLeft().toMillis()));
            }
        } while (deadline.hasTimeLeft());
        throw lastFailure;
    }

    private static <Split extends LakeSplit> List<LoadTask> createLoadTasks(
            TableInfo table,
            BulkLoadBuildContext context,
            Map<String, Map<Integer, List<Split>>> grouped,
            SimpleVersionedSerializer<Split> serializer)
            throws IOException {
        checkState(
                context.getHandle().getTableId() == table.getTableId(),
                "Table was replaced while preparing the load.");
        List<LoadTask> tasks = new ArrayList<>();
        for (BulkLoadBuildContext.Target target : context.getTargets()) {
            Map<Integer, List<Split>> buckets =
                    grouped.getOrDefault(
                            target.getPath().getPartitionName(), Collections.emptyMap());
            for (int bucket : buckets.keySet()) {
                for (Split split : buckets.get(bucket)) {
                    Integer totalBuckets = split.totalBuckets();
                    checkArgument(
                            totalBuckets != null,
                            "Cannot load %s: source split does not report its physical bucket count.",
                            target.getPath());
                    // A valid bucket ID alone cannot detect old partitions with fewer buckets.
                    checkArgument(
                            Objects.equals(totalBuckets, target.getBucketCount()),
                            "Cannot load %s: source bucket count %s differs from frozen target bucket count %s. "
                                    + "Promotion of rescaled source partitions is not supported.",
                            target.getPath(),
                            totalBuckets,
                            target.getBucketCount());
                }
                checkArgument(
                        bucket < target.getBucketCount(),
                        "Source bucket ID %s is out of range for frozen target %s with %s buckets.",
                        bucket,
                        target.getPath(),
                        target.getBucketCount());
            }
            for (int bucket = 0; bucket < target.getBucketCount(); bucket++) {
                List<byte[]> splits = new ArrayList<>();
                for (Split split : buckets.getOrDefault(bucket, Collections.emptyList())) {
                    splits.add(serializer.serialize(split));
                }
                tasks.add(
                        new LoadTask(
                                target.getPath(),
                                target.getPartitionId(),
                                bucket,
                                serializer.getVersion(),
                                splits));
            }
        }
        return tasks;
    }

    private static <Split extends LakeSplit> JobClient submitJob(
            StreamExecutionEnvironment executionEnvironment,
            Configuration config,
            LakeSource<Split> source,
            LoadPlan plan,
            @Nullable BulkLoadBuildContext context,
            List<LoadTask> tasks)
            throws Exception {
        StreamExecutionEnvironment environment = createEnvironment(executionEnvironment);
        DataStream<TableBucket> completed;
        if (tasks.isEmpty()) {
            completed =
                    environment.fromCollection(
                            Collections.emptyList(), TypeInformation.of(TableBucket.class));
        } else {
            completed =
                    environment
                            .fromCollection(tasks, TypeInformation.of(LoadTask.class))
                            .rebalance()
                            .map(new BuildBucket<>(config, context, source))
                            .returns(TypeInformation.of(TableBucket.class))
                            .name("Build and commit lake bucket snapshots");
        }
        completed
                .sinkTo(new CommitSink(config, plan, context))
                .name("Commit and enable Fluss on lake table")
                .setParallelism(1)
                .setMaxParallelism(1);
        JobClient job = environment.executeAsync("load_lake_data_to_fluss: " + plan.tablePath);
        LOG.info(
                "Submitted lake load job {} with Bulk Load ID {} and source snapshot {}.",
                job.getJobID(),
                plan.bulkLoadId,
                plan.sourceSnapshotId);
        return job;
    }

    @Nullable
    private static BulkLoadInfo findLoad(
            BulkLoad bulkLoad, BulkLoadHandle handle, Configuration config) throws Exception {
        try {
            return await(bulkLoad.getInfo(handle), config);
        } catch (ExecutionException e) {
            if (ExceptionUtils.findThrowable(e, BulkLoadNotFoundException.class).isPresent()) {
                return null;
            }
            throw e;
        }
    }

    private static String partitionName(TableInfo table, LakeSplit split) {
        List<String> values = split.partition();
        List<String> keys = table.getPartitionKeys();
        checkArgument(
                values.size() == keys.size(),
                "Lake split for table %s has %s partition values %s, but the table has %s partition keys %s.",
                table.getTablePath(),
                values.size(),
                values,
                keys.size(),
                keys);
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            checkArgument(
                    !value.contains(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR),
                    "Partition value '%s' for key '%s' of table %s contains '%s', which Fluss uses to separate partition values.",
                    value,
                    keys.get(i),
                    table.getTablePath(),
                    ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR);
        }
        return String.join(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR, values);
    }

    static <Split extends LakeSplit> List<Split> planSplits(
            TableInfo tableInfo,
            LakeSource<Split> source,
            long snapshotId,
            @Nullable Set<String> selectedPartitions,
            Instant now)
            throws IOException {
        Predicate filter = buildPartitionFilter(tableInfo, selectedPartitions, now);
        if (filter != null) {
            // Prune partitions before planning; never silently load outside the requested scope.
            checkState(
                    source.withFilters(Collections.singletonList(filter))
                            .remainingPredicates()
                            .isEmpty(),
                    "The lake source cannot push down the load partition filter.");
        }
        return source.createPlanner(() -> snapshotId).plan();
    }

    @VisibleForTesting
    static StreamExecutionEnvironment createEnvironment(
            StreamExecutionEnvironment executionEnvironment) {
        org.apache.flink.configuration.Configuration jobConfig =
                org.apache.flink.configuration.Configuration.fromMap(
                        executionEnvironment.getConfiguration().toMap());
        // Flink 1.18 stores programmatic execution settings separately.
        jobConfig.addAll(executionEnvironment.getConfig().toConfiguration());
        jobConfig.set(BatchExecutionOptions.SPECULATIVE_ENABLED, false);
        if (!jobConfig.contains(RestartStrategyOptions.RESTART_STRATEGY)) {
            jobConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            if (!jobConfig.contains(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS)) {
                jobConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
            }
            if (!jobConfig.contains(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY)) {
                jobConfig.set(
                        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                        Duration.ofSeconds(10));
            }
        }
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment(jobConfig);
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return environment;
    }

    /**
     * Builds the source partition filter for the load.
     *
     * @param selectedPartitions target partition names recorded by an existing transaction; when
     *     non-null, use this set without reapplying retention. When {@code null}, apply the table's
     *     auto-partition retention policy, or scan all source partitions if the table is not
     *     auto-partitioned or retention is unlimited
     * @return a predicate matching the selected partitions or the automatic retention range, or
     *     {@code null} to scan all partitions in the source snapshot
     */
    @Nullable
    static Predicate buildPartitionFilter(
            TableInfo tableInfo, @Nullable Set<String> selectedPartitions, Instant now) {
        if (selectedPartitions != null) {
            List<Map<String, String>> specs = new ArrayList<>();
            for (String partition : selectedPartitions) {
                specs.add(
                        ResolvedPartitionSpec.fromPartitionName(
                                        tableInfo.getPartitionKeys(), partition)
                                .toPartitionSpec()
                                .getSpecMap());
            }
            return PredicateBuilder.partitions(specs, tableInfo.getRowType());
        }
        if (!tableInfo.isAutoPartitioned()) {
            return null;
        }
        AutoPartitionStrategy strategy = tableInfo.getTableConfig().getAutoPartitionStrategy();
        if (strategy.numToRetain() < 0) {
            return null;
        }
        ZonedDateTime current = now.atZone(strategy.timeZone().toZoneId());
        String earliestRetainedPartition =
                generateAutoPartitionTime(
                        current, -strategy.numToRetain(), strategy.timeUnit(), strategy);
        String key = strategy.key() == null ? tableInfo.getPartitionKeys().get(0) : strategy.key();
        int index = tableInfo.getRowType().getFieldIndex(key);
        // Retention includes the boundary, current and future source partitions. A range
        // filter preserves that scope without first enumerating all source partitions.
        return new PredicateBuilder(tableInfo.getRowType())
                .greaterOrEqual(
                        index,
                        TypeUtils.castFromString(
                                earliestRetainedPartition,
                                tableInfo.getRowType().getTypeAt(index)));
    }

    static <T> T await(CompletableFuture<T> future, Configuration config) throws Exception {
        try {
            return future.get(
                    Math.max(1L, config.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis()),
                    TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            // Cancellation does not roll back a possibly completed server-side operation.
            future.cancel(false);
            throw e;
        }
    }

    private static void completeLoad(
            LoadPlan plan, Map<TableBucket, Long> offsets, Configuration config) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {
            TableInfo current = await(admin.getTableInfo(plan.tablePath), config);
            checkState(
                    current.getTableId() == plan.tableId,
                    "Table %s was replaced during loading.",
                    plan.tablePath);
            if (current.getTableConfig().isDataLakeEnabled()) {
                return;
            }
            registerLakeSnapshot(admin, current, plan, offsets, config);
            await(
                    admin.alterTable(
                            plan.tablePath,
                            Collections.singletonList(
                                    TableChange.set(
                                            ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")),
                            false),
                    config);
        }
    }

    private static void registerLakeSnapshot(
            Admin admin,
            TableInfo table,
            LoadPlan plan,
            Map<TableBucket, Long> offsets,
            Configuration config)
            throws Exception {
        if (plan.sourceSnapshotId == null) {
            return;
        }
        // Begin freezes the targets, but a partition may be deleted before finalization.
        // Exclude its buckets from the initial lake snapshot's offsets.
        if (table.isPartitioned()) {
            Set<Long> existingPartitions = new LinkedHashSet<>();
            for (PartitionInfo partition :
                    await(admin.listPartitionInfos(plan.tablePath), config)) {
                existingPartitions.add(partition.getPartitionId());
            }
            offsets.keySet()
                    .removeIf(bucket -> !existingPartitions.contains(bucket.getPartitionId()));
        }
        try (FlussTableLakeSnapshotCommitter committer =
                new FlussTableLakeSnapshotCommitter(config)) {
            committer.open();
            // Re-register the fixed snapshot on retries, including when an earlier enable failed.
            committer.commitLakeSnapshot(
                    plan.tableId, plan.tablePath, plan.sourceSnapshotId, offsets);
        }
    }

    private static final class LoadTask implements Serializable {
        private static final long serialVersionUID = 1L;
        private final PhysicalTablePath path;
        @Nullable private final Long partitionId;
        private final int bucket;
        private final int splitVersion;
        private final List<byte[]> splits;

        private LoadTask(
                PhysicalTablePath path,
                @Nullable Long partitionId,
                int bucket,
                int splitVersion,
                List<byte[]> splits) {
            this.path = path;
            this.partitionId = partitionId;
            this.bucket = bucket;
            this.splitVersion = splitVersion;
            this.splits = splits;
        }
    }

    private static final class BuildBucket<Split extends LakeSplit>
            extends RichMapFunction<LoadTask, TableBucket> {
        private static final long serialVersionUID = 1L;
        private final Configuration config;
        private final BulkLoadHandle handle;
        private final List<String> partitionNames;
        private final long totalBucketsToLoad;
        private final LakeSource<Split> source;
        private transient BulkLoadBuildContext context;
        private transient Connection connection;
        private transient Table table;
        private transient BulkLoad bulkLoad;
        private transient boolean loadCommitted;
        private transient Counter rowsProcessed;
        private transient Counter completedBuckets;

        private BuildBucket(
                Configuration config, BulkLoadBuildContext context, LakeSource<Split> source) {
            this.config = config;
            this.handle = context.getHandle();
            this.partitionNames = new ArrayList<>();
            for (BulkLoadBuildContext.Target target : context.getTargets()) {
                if (target.getPath().getPartitionName() != null) {
                    partitionNames.add(target.getPath().getPartitionName());
                }
            }
            this.totalBucketsToLoad =
                    context.getTargets().stream()
                            .mapToLong(BulkLoadBuildContext.Target::getBucketCount)
                            .sum();
            this.source = source;
        }

        @Override
        public TableBucket map(LoadTask task) throws Exception {
            if (connection == null) {
                // The connection initializes Fluss filesystems with the client.fs.* options.
                connection = ConnectionFactory.createConnection(config);
                table = connection.getTable(task.path.getTablePath());
                bulkLoad = table.newBulkLoad();
                // Check once per operator attempt. Finalization can fail after commit, and a
                // withdrawn target may then have no individual bucket commit to replay.
                loadCommitted =
                        await(bulkLoad.getInfo(handle), config).getState()
                                == BulkLoadInfo.State.COMMITTED;
                if (!loadCommitted) {
                    // The JobGraph carries the original selection. Idempotent Begin retrieves
                    // the frozen context without serializing SDK implementation objects.
                    context = await(bulkLoad.begin(handle.getBulkLoadId(), partitionNames), config);
                }
                MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
                rowsProcessed = metricGroup.counter("rowsProcessed");
                completedBuckets = metricGroup.counter("completedBuckets");
                // Each subtask reports the same frozen total; aggregate this gauge with MAX.
                metricGroup.gauge("totalBucketsToLoad", () -> totalBucketsToLoad);
            }
            TableBucket result =
                    new TableBucket(handle.getTableId(), task.partitionId, task.bucket);
            if (loadCommitted) {
                LOG.info(
                        "Skipping Bulk Load {} bucket {} of {} because the transaction is already committed.",
                        handle.getBulkLoadId(),
                        task.bucket,
                        task.path);
                completedBuckets.inc();
                return result;
            }
            BulkLoadBuildContext.Target target = context.getTarget(task.path);
            checkState(
                    Objects.equals(target.getPartitionId(), task.partitionId),
                    "Load task no longer belongs to the original physical partition.");
            try {
                Optional<BulkLoadBucketWriter> opened =
                        bulkLoad.tryOpenBucketWriter(target, task.bucket);
                if (opened.isPresent()) {
                    try (BulkLoadBucketWriter writer = opened.get()) {
                        LOG.info(
                                "Starting Bulk Load {} for bucket {} of {}.",
                                context.getHandle().getBulkLoadId(),
                                task.bucket,
                                task.path);
                        for (byte[] bytes : task.splits) {
                            Split split =
                                    source.getSplitSerializer()
                                            .deserialize(task.splitVersion, bytes);
                            try (CloseableIterator<LogRecord> records =
                                    source.createRecordReader(() -> split).read()) {
                                while (records.hasNext()) {
                                    LogRecord record = records.next();
                                    checkState(
                                            record.getChangeType() == ChangeType.INSERT
                                                    || record.getChangeType()
                                                            == ChangeType.UPDATE_AFTER,
                                            "Lake snapshot reader must return the final primary-key"
                                                    + " state.");
                                    writer.write(record.getRow());
                                    rowsProcessed.inc();
                                }
                            }
                        }
                        writer.commit();
                    }
                    LOG.info(
                            "Finished Bulk Load {} for bucket {} of {}.",
                            context.getHandle().getBulkLoadId(),
                            task.bucket,
                            task.path);
                } else {
                    LOG.info(
                            "Skipping Bulk Load {} bucket {} of {} because its snapshot is already committed.",
                            context.getHandle().getBulkLoadId(),
                            task.bucket,
                            task.path);
                }
            } catch (Exception e) {
                if (!ExceptionUtils.findThrowable(e, BulkLoadTargetWithdrawnException.class)
                        .isPresent()) {
                    throw e;
                }
                LOG.info("Skipping deleted Bulk Load target {}.", target.getPath());
            }
            completedBuckets.inc();
            return result;
        }

        @Override
        public void close() throws Exception {
            IOUtils.closeAll(table, connection);
        }
    }

    private static final class CommitSink extends SinkAdapter<TableBucket> {
        private static final long serialVersionUID = 1L;
        private final Configuration config;
        private final LoadPlan plan;
        // Null when the table is already enabled or no physical partitions were selected.
        @Nullable private final BulkLoadHandle handle;
        private final Set<TableBucket> expected;

        private CommitSink(
                Configuration config, LoadPlan plan, @Nullable BulkLoadBuildContext context) {
            this.config = config;
            this.plan = plan;
            this.handle = context == null ? null : context.getHandle();
            this.expected = new LinkedHashSet<>();
            if (context != null) {
                for (BulkLoadBuildContext.Target target : context.getTargets()) {
                    for (int bucket = 0; bucket < target.getBucketCount(); bucket++) {
                        expected.add(
                                new TableBucket(plan.tableId, target.getPartitionId(), bucket));
                    }
                }
            }
        }

        @Override
        protected SinkWriter<TableBucket> createWriter(
                MailboxExecutor mailboxExecutor,
                SinkWriterMetricGroup metricGroup,
                int subtaskIndex) {
            return new SinkWriter<TableBucket>() {
                private final Set<TableBucket> completed = new LinkedHashSet<>();

                @Override
                public void write(TableBucket bucket, Context writerContext) {
                    checkState(completed.add(bucket), "Duplicate load result for %s.", bucket);
                }

                @Override
                public void flush(boolean endOfInput) throws IOException, InterruptedException {
                    if (!endOfInput) {
                        return;
                    }
                    try {
                        Map<TableBucket, Long> offsets = new LinkedHashMap<>();
                        if (handle != null) {
                            try (Connection connection =
                                            ConnectionFactory.createConnection(config);
                                    Table table = connection.getTable(plan.tablePath)) {
                                BulkLoad bulkLoad = table.newBulkLoad();
                                BulkLoadInfo info = await(bulkLoad.getInfo(handle), config);
                                checkState(
                                        info.getState() != BulkLoadInfo.State.ABORTED,
                                        "Bulk Load %s was aborted.",
                                        plan.bulkLoadId);
                                if (info.getState() == BulkLoadInfo.State.RUNNING) {
                                    checkState(
                                            completed.containsAll(expected),
                                            "Load results do not cover the prepared physical buckets.");
                                    await(bulkLoad.commit(handle), config);
                                }
                            }
                            // Bulk Load produces no WAL during new-table initialization.
                            // completeLoad removes deleted partitions before registering offsets.
                            for (TableBucket bucket : expected) {
                                offsets.put(bucket, 0L);
                            }
                        }
                        completeLoad(plan, offsets, config);
                    } catch (InterruptedException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new IOException("Failed to commit and enable the lake table.", e);
                    }
                }

                @Override
                public void close() {}
            };
        }
    }
}
