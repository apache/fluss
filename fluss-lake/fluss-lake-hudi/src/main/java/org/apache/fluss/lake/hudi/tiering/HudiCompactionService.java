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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.sink.compact.FlinkCompactionConfig;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategies;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.CompactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Coordinates Hudi compaction scheduling, execution, and commit for tiering writers. */
public class HudiCompactionService {

    private static final Logger LOG = LoggerFactory.getLogger(HudiCompactionService.class);

    private final HudiWriteTableInfo hudiTableInfo;
    private final HoodieFlinkWriteClient<?> writeClient;
    private final HoodieTableMetaClient metaClient;
    private final HoodieFlinkTable<?> table;
    private final Configuration conf;
    @Nullable private final TableBucket tableBucket;
    @Nullable private final String partition;

    private InternalSchemaManager internalSchemaManager;

    public static HudiCompactionService forScheduler(HudiWriteTableInfo hudiTableInfo) {
        return new HudiCompactionService(hudiTableInfo, null, null);
    }

    public static HudiCompactionService forExecutor(
            HudiWriteTableInfo hudiTableInfo, TableBucket tableBucket, @Nullable String partition) {
        return new HudiCompactionService(hudiTableInfo, tableBucket, partition);
    }

    private HudiCompactionService(
            HudiWriteTableInfo hudiTableInfo,
            @Nullable TableBucket tableBucket,
            @Nullable String partition) {
        this.hudiTableInfo = checkNotNull(hudiTableInfo, "Hudi write table info must not be null.");
        this.writeClient = hudiTableInfo.getWriteClient();
        this.metaClient = hudiTableInfo.getMetaClient();
        this.table = writeClient.getHoodieTable();
        this.conf = hudiTableInfo.getFlinkConfig();
        this.tableBucket = tableBucket;
        this.partition = partition;
    }

    public boolean scheduleCompaction() throws IOException {
        LOG.info("Scheduling Hudi compaction for table {}.", hudiTableInfo.getTablePath());
        metaClient.reloadActiveTimeline();
        try {
            boolean scheduled = writeClient.scheduleCompaction(Option.empty()).isPresent();
            metaClient.reloadActiveTimeline();
            if (!scheduled) {
                LOG.info(
                        "No Hudi compaction plan was scheduled for table {}.",
                        hudiTableInfo.getTablePath());
            }
            return scheduled;
        } catch (Exception e) {
            throw new IOException(
                    "Failed to schedule Hudi compaction for table "
                            + hudiTableInfo.getTablePath()
                            + ".",
                    e);
        }
    }

    public void markSelectedCompactionsInflight() {
        List<String> compactionInstantTimes = getSelectedCompactionInstantTimes();
        if (compactionInstantTimes.isEmpty()) {
            return;
        }
        compactionInstantTimes = validateSelectedCompactionInstantTimes(compactionInstantTimes);
        if (compactionInstantTimes.isEmpty()) {
            return;
        }

        HoodieTimeline pendingCompactionTimeline =
                table.getActiveTimeline().filterPendingCompactionTimeline();
        InstantGenerator instantGenerator = table.getInstantGenerator();

        for (String timestamp : compactionInstantTimes) {
            HoodieInstant inflightInstant =
                    instantGenerator.getCompactionInflightInstant(timestamp);
            if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
                LOG.info("Rollback stale inflight Hudi compaction instant {}.", timestamp);
                table.rollbackInflightCompaction(
                        inflightInstant, writeClient.getTransactionManager());
                metaClient.reloadActiveTimeline();
            }
        }

        pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
        for (String timestamp : compactionInstantTimes) {
            HoodieInstant requestedInstant =
                    instantGenerator.getCompactionRequestedInstant(timestamp);
            if (pendingCompactionTimeline.containsInstant(requestedInstant)) {
                // Each Fluss tiering table has a single lake committer, so requested->inflight
                // transitions are serialized by the committer task for this table.
                table.getActiveTimeline().transitionCompactionRequestedToInflight(requestedInstant);
                LOG.info("Marked Hudi compaction instant {} as inflight.", timestamp);
            }
        }
        metaClient.reloadActiveTimeline();
    }

    public List<String> getInflightCompactionInstantTimes() {
        List<String> compactionInstantTimes = getSelectedCompactionInstantTimes();
        if (compactionInstantTimes.isEmpty()) {
            return Collections.emptyList();
        }

        HoodieTimeline pendingCompactionTimeline =
                metaClient.getActiveTimeline().filterPendingCompactionTimeline();
        InstantGenerator instantGenerator = table.getInstantGenerator();

        List<String> inflightCompactionInstantTimes = new ArrayList<>();
        for (String timestamp : compactionInstantTimes) {
            HoodieInstant inflightInstant =
                    instantGenerator.getCompactionInflightInstant(timestamp);
            if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
                inflightCompactionInstantTimes.add(inflightInstant.requestedTime());
            }
        }
        LOG.info(
                "Found {} inflight Hudi compaction instants for table {}.",
                inflightCompactionInstantTimes.size(),
                hudiTableInfo.getTablePath());
        return inflightCompactionInstantTimes;
    }

    public List<Pair<String, HoodieCompactionPlan>> getCompactionPlans(
            List<String> compactionInstantTimes) {
        if (compactionInstantTimes == null || compactionInstantTimes.isEmpty()) {
            return Collections.emptyList();
        }

        List<Pair<String, HoodieCompactionPlan>> compactionPlans =
                compactionInstantTimes.stream()
                        .map(
                                timestamp -> {
                                    try {
                                        return Pair.of(
                                                timestamp,
                                                CompactionUtils.getCompactionPlan(
                                                        metaClient, timestamp));
                                    } catch (Exception e) {
                                        throw new HoodieException(
                                                "Failed to get Hudi compaction plan " + timestamp,
                                                e);
                                    }
                                })
                        .filter(pair -> isValidCompactionPlan(pair.getRight()))
                        .collect(Collectors.toList());

        LOG.info(
                "Loaded {} Hudi compaction plans for table {}.",
                compactionPlans.size(),
                hudiTableInfo.getTablePath());
        return compactionPlans;
    }

    public Map<String, List<WriteStatus>> executeCompaction(
            List<Pair<String, HoodieCompactionPlan>> compactionPlans) throws IOException {
        if (compactionPlans == null || compactionPlans.isEmpty()) {
            return Collections.emptyMap();
        }
        TableBucket currentBucket =
                checkNotNull(tableBucket, "Hudi compaction execution requires a table bucket.");

        Map<String, List<WriteStatus>> writeStatusesByInstant = new HashMap<>();
        HoodieWriteConfig writeConfig = writeClient.getConfig();
        String hudiPartitionPath = toHudiPartitionPath(partition);

        for (Pair<String, HoodieCompactionPlan> planPair : compactionPlans) {
            String instantTime = planPair.getLeft();
            HoodieCompactionPlan compactionPlan = planPair.getRight();
            metaClient.reload();
            try {
                CompactionUtil.setAvroSchema(writeConfig, metaClient);
                internalSchemaManager = null;
                HoodieReaderContext<?> readerContext = createReaderContext();
                for (HoodieCompactionOperation operation : compactionPlan.getOperations()) {
                    if (!belongsToCurrentWriter(operation, hudiPartitionPath, currentBucket)) {
                        continue;
                    }

                    HoodieFlinkMergeOnReadTableCompactor<?> compactor =
                            new HoodieFlinkMergeOnReadTableCompactor<>();
                    CompactionOperation compactionOperation =
                            CompactionOperation.convertFromAvroRecordInstance(operation);

                    List<WriteStatus> writeStatuses =
                            compactor.compact(
                                    writeConfig,
                                    compactionOperation,
                                    instantTime,
                                    table.getTaskContextSupplier(),
                                    readerContext,
                                    table);
                    writeStatusesByInstant
                            .computeIfAbsent(instantTime, ignored -> new ArrayList<>())
                            .addAll(writeStatuses);
                    LOG.info(
                            "Compacted Hudi file id {} for table {}, partition {}, bucket {}, instant {}.",
                            operation.getFileId(),
                            hudiTableInfo.getTablePath(),
                            hudiPartitionPath,
                            currentBucket.getBucket(),
                            instantTime);
                }
            } catch (Exception e) {
                writeStatusesByInstant.remove(instantTime);
                throw rollbackCompactionAfterFailure(
                        instantTime, hudiPartitionPath, currentBucket, e);
            }
        }
        return writeStatusesByInstant;
    }

    public String commitCompaction(
            Map<String, HudiWriteStats> compactionWriteStats,
            Map<String, String> snapshotProperties)
            throws IOException {
        if (compactionWriteStats == null || compactionWriteStats.isEmpty()) {
            return null;
        }

        List<String> compactionInstants = new ArrayList<>(compactionWriteStats.keySet());
        Collections.sort(compactionInstants);
        String latestInstant = compactionInstants.get(compactionInstants.size() - 1);
        for (String compactionInstant : compactionInstants) {
            HudiWriteStats writeStats = compactionWriteStats.get(compactionInstant);

            if (writeStats.getTotalErrorRecords() > 0 && !conf.get(FlinkOptions.IGNORE_FAILED)) {
                LOG.warn(
                        "Rollback Hudi compaction instant {} because it contains {} error records.",
                        compactionInstant,
                        writeStats.getTotalErrorRecords());
                CompactionUtil.rollbackCompaction(
                        table, compactionInstant, writeClient.getTransactionManager());
                throw new IOException(
                        "Failed to commit Hudi compaction instant "
                                + compactionInstant
                                + " because it contains "
                                + writeStats.getTotalErrorRecords()
                                + " error records.");
            }

            doCommitCompaction(compactionInstant, writeStats, snapshotProperties);
            LOG.info("Committed Hudi compaction instant {}.", compactionInstant);
        }
        return latestInstant;
    }

    private List<String> getSelectedCompactionInstantTimes() {
        metaClient.reloadActiveTimeline();
        HoodieTimeline pendingCompactionTimeline =
                table.getActiveTimeline().filterPendingCompactionTimeline();
        FlinkCompactionConfig compactionConfig = toFlinkCompactionConfig(conf);
        List<HoodieInstant> requestedInstants =
                CompactionPlanStrategies.getStrategy(compactionConfig)
                        .select(pendingCompactionTimeline);
        if (requestedInstants.isEmpty()) {
            LOG.info(
                    "No pending Hudi compaction plan found for table {}.",
                    hudiTableInfo.getTablePath());
            return Collections.emptyList();
        }
        return requestedInstants.stream()
                .map(HoodieInstant::requestedTime)
                .collect(Collectors.toList());
    }

    private List<String> validateSelectedCompactionInstantTimes(
            List<String> compactionInstantTimes) {
        HoodieTimeline pendingCompactionTimeline =
                table.getActiveTimeline().filterPendingCompactionTimeline();
        InstantGenerator instantGenerator = table.getInstantGenerator();
        List<String> validCompactionInstantTimes = new ArrayList<>();
        for (String timestamp : compactionInstantTimes) {
            HoodieCompactionPlan compactionPlan = loadCompactionPlan(timestamp);
            if (isValidCompactionPlan(compactionPlan)) {
                validCompactionInstantTimes.add(timestamp);
                continue;
            }

            HoodieInstant inflightInstant =
                    instantGenerator.getCompactionInflightInstant(timestamp);
            if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
                LOG.warn(
                        "Rollback invalid inflight Hudi compaction instant {} for table {}.",
                        timestamp,
                        hudiTableInfo.getTablePath());
                table.rollbackInflightCompaction(
                        inflightInstant, writeClient.getTransactionManager());
                metaClient.reloadActiveTimeline();
            } else {
                LOG.warn(
                        "Skip invalid Hudi compaction plan {} for table {}.",
                        timestamp,
                        hudiTableInfo.getTablePath());
            }
        }
        return validCompactionInstantTimes;
    }

    private HoodieCompactionPlan loadCompactionPlan(String timestamp) {
        try {
            return CompactionUtils.getCompactionPlan(metaClient, timestamp);
        } catch (Exception e) {
            throw new HoodieException("Failed to get Hudi compaction plan " + timestamp, e);
        }
    }

    private HoodieReaderContext<?> createReaderContext() {
        Supplier<InternalSchemaManager> internalSchemaManagerSupplier =
                () -> {
                    if (internalSchemaManager == null) {
                        internalSchemaManager =
                                InternalSchemaManager.get(metaClient.getStorageConf(), metaClient);
                    }
                    return internalSchemaManager;
                };
        StorageConfiguration<?> readerConf = writeClient.getEngineContext().getStorageConf();
        return new FlinkRowDataReaderContext(
                readerConf,
                internalSchemaManagerSupplier,
                Collections.emptyList(),
                metaClient.getTableConfig(),
                Option.empty());
    }

    private IOException rollbackCompactionAfterFailure(
            String instantTime,
            String hudiPartitionPath,
            TableBucket currentBucket,
            Exception cause) {
        IOException failure =
                new IOException(
                        String.format(
                                "Failed to execute Hudi compaction for table %s, partition %s, bucket %s, instant %s.",
                                hudiTableInfo.getTablePath(),
                                hudiPartitionPath,
                                currentBucket.getBucket(),
                                instantTime),
                        cause);
        try {
            CompactionUtil.rollbackCompaction(
                    table, instantTime, writeClient.getTransactionManager());
            metaClient.reloadActiveTimeline();
        } catch (Exception rollbackFailure) {
            failure.addSuppressed(
                    new IOException(
                            "Failed to rollback Hudi compaction instant " + instantTime + ".",
                            rollbackFailure));
        }
        return failure;
    }

    private void doCommitCompaction(
            String instant, HudiWriteStats writeStats, Map<String, String> snapshotProperties)
            throws IOException {
        HoodieCommitMetadata metadata =
                CompactHelpers.getInstance()
                        .createCompactionMetadata(
                                table,
                                instant,
                                HoodieListData.eager(toWriteStatuses(writeStats)),
                                writeClient.getConfig().getSchema());
        snapshotProperties.forEach(metadata::addMetadata);

        writeClient.completeCompaction(metadata, table, instant);
        if (!conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED)) {
            writeClient.clean();
        }
        metaClient.reloadActiveTimeline();
    }

    private static boolean belongsToCurrentWriter(
            HoodieCompactionOperation operation,
            String hudiPartitionPath,
            TableBucket currentBucket) {
        return belongsToCurrentPartition(operation, hudiPartitionPath)
                && belongsToCurrentBucket(operation, currentBucket);
    }

    private static boolean belongsToCurrentPartition(
            HoodieCompactionOperation operation, String hudiPartitionPath) {
        String operationPartitionPath = normalizePartitionPath(operation.getPartitionPath());
        return Objects.equals(operationPartitionPath, hudiPartitionPath);
    }

    private static boolean belongsToCurrentBucket(
            HoodieCompactionOperation operation, TableBucket currentBucket) {
        String fileId = operation.getFileId();
        if (fileId == null || fileId.isEmpty()) {
            return false;
        }
        try {
            return BucketIdentifier.bucketIdFromFileId(fileId) == currentBucket.getBucket();
        } catch (RuntimeException e) {
            LOG.warn("Failed to parse Hudi bucket id from file id {}.", fileId, e);
            return false;
        }
    }

    private String toHudiPartitionPath(@Nullable String partitionName) throws IOException {
        if (partitionName == null || partitionName.isEmpty()) {
            return "";
        }

        String partitionPath = partitionName.replace('$', '/');
        if (!conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING)) {
            return partitionPath;
        }

        String partitionFields = conf.get(FlinkOptions.PARTITION_PATH_FIELD);
        if (partitionFields == null || partitionFields.trim().isEmpty()) {
            return partitionPath;
        }

        String[] fields = partitionFields.split(",");
        String[] values = partitionName.split("\\$");
        if (fields.length != values.length) {
            throw new IOException(
                    String.format(
                            "Invalid Fluss partition name '%s' for Hudi hive-style partition fields '%s'. Expected %s values separated by '$' but got %s.",
                            partitionName, partitionFields, fields.length, values.length));
        }

        List<String> hiveStylePartitionSegments = new ArrayList<>(fields.length);
        for (int i = 0; i < fields.length; i++) {
            hiveStylePartitionSegments.add(fields[i].trim() + "=" + values[i]);
        }
        return String.join("/", hiveStylePartitionSegments);
    }

    private static String normalizePartitionPath(@Nullable String partitionPath) {
        return partitionPath == null ? "" : partitionPath;
    }

    private static List<WriteStatus> toWriteStatuses(HudiWriteStats writeStats) {
        List<HoodieWriteStat> stats = writeStats.getWriteStats();
        if (stats.isEmpty()) {
            return Collections.emptyList();
        }

        List<WriteStatus> writeStatuses = new ArrayList<>(stats.size());
        for (HoodieWriteStat stat : stats) {
            WriteStatus writeStatus = new WriteStatus();
            writeStatus.setStat(stat);
            writeStatus.setFileId(stat.getFileId());
            writeStatus.setPartitionPath(stat.getPartitionPath());
            writeStatuses.add(writeStatus);
        }
        return writeStatuses;
    }

    private static boolean isValidCompactionPlan(HoodieCompactionPlan plan) {
        return plan != null && plan.getOperations() != null && !plan.getOperations().isEmpty();
    }

    static FlinkCompactionConfig toFlinkCompactionConfig(Configuration config) {
        FlinkCompactionConfig compactionConfig = new FlinkCompactionConfig();

        // Hudi does not expose a reverse helper for FlinkCompactionConfig. Keep this mapping
        // focused on the scheduling and cleaning fields used by Fluss tiering; CLI/service-mode
        // fields intentionally keep Hudi defaults.
        compactionConfig.path = config.get(FlinkOptions.PATH);
        compactionConfig.compactionTriggerStrategy =
                config.get(FlinkOptions.COMPACTION_TRIGGER_STRATEGY);
        compactionConfig.archiveMaxCommits = config.get(FlinkOptions.ARCHIVE_MAX_COMMITS);
        compactionConfig.archiveMinCommits = config.get(FlinkOptions.ARCHIVE_MIN_COMMITS);
        compactionConfig.cleanPolicy = config.get(FlinkOptions.CLEAN_POLICY);
        compactionConfig.cleanRetainCommits = config.get(FlinkOptions.CLEAN_RETAIN_COMMITS);
        compactionConfig.cleanRetainHours = config.get(FlinkOptions.CLEAN_RETAIN_HOURS);
        compactionConfig.cleanRetainFileVersions =
                config.get(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS);
        compactionConfig.compactionDeltaCommits = config.get(FlinkOptions.COMPACTION_DELTA_COMMITS);
        compactionConfig.compactionDeltaSeconds = config.get(FlinkOptions.COMPACTION_DELTA_SECONDS);
        compactionConfig.compactionMaxMemory = config.get(FlinkOptions.COMPACTION_MAX_MEMORY);
        compactionConfig.compactionTargetIo = config.get(FlinkOptions.COMPACTION_TARGET_IO);
        compactionConfig.compactionTasks = config.get(FlinkOptions.COMPACTION_TASKS);
        compactionConfig.cleanAsyncEnable = config.get(FlinkOptions.CLEAN_ASYNC_ENABLED);
        compactionConfig.schedule = config.get(FlinkOptions.COMPACTION_SCHEDULE_ENABLED);
        return compactionConfig;
    }
}
