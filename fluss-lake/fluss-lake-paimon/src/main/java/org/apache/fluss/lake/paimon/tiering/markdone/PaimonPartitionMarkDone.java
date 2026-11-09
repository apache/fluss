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

package org.apache.fluss.lake.paimon.tiering.markdone;

import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.StringUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.apache.paimon.CoreOptions.PARTITION_TIMESTAMP_FORMATTER;
import static org.apache.paimon.CoreOptions.PARTITION_TIMESTAMP_PATTERN;

/**
 * Marks idle lake partitions done using Paimon table options and idempotent mark-done actions.
 *
 * <p>The trigger requires both the last update and partition end time to precede the idle window.
 * State is tracked in {@link PartitionMarkDoneState} and persisted by the lake committer.
 *
 * <p>Partition end times use Paimon's default or configured timestamp extractor and time interval.
 */
public class PaimonPartitionMarkDone implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartitionMarkDone.class);

    /** Snapshot property key storing the mark-done state JSON. */
    public static final String MARK_DONE_STATE_PROPERTY = "fluss.tiering.mark-done-state";

    // These options live in paimon-flink; mirror them here to avoid a Flink runtime dependency.
    private static final ConfigOption<Duration> PARTITION_IDLE_TIME_TO_DONE =
            ConfigOptions.key("partition.idle-time-to-done").durationType().noDefaultValue();

    private static final ConfigOption<Duration> PARTITION_TIME_INTERVAL =
            ConfigOptions.key("partition.time-interval").durationType().noDefaultValue();

    /** String adaptation of Paimon's enum-valued FlinkConnectorOptions#PARTITION_MARK_DONE_MODE. */
    private static final ConfigOption<String> PARTITION_MARK_DONE_MODE =
            ConfigOptions.key("partition.mark-done-action.mode")
                    .stringType()
                    .defaultValue("process-time");

    private final FileStoreTable fileStoreTable;
    private final TableInfo tableInfo;
    private final Clock clock;
    private final List<String> partitionKeys;
    private final long idleTimeToDoneMillis;
    private final long timeIntervalMillis;
    private final PartitionTimeExtractor partitionTimeExtractor;
    private final InternalRowPartitionComputer partitionComputer;
    private final List<PartitionMarkDoneAction> markDoneActions;

    /** Creates the configured actions and time rule for the lake table. */
    public PaimonPartitionMarkDone(FileStoreTable fileStoreTable, TableInfo tableInfo) {
        this(fileStoreTable, tableInfo, SystemClock.getInstance());
    }

    PaimonPartitionMarkDone(FileStoreTable fileStoreTable, TableInfo tableInfo, Clock clock) {
        checkState(
                isEnabled(fileStoreTable, tableInfo),
                "Partition mark-done is not enabled for table %s.",
                tableInfo.getTablePath());
        this.fileStoreTable = fileStoreTable;
        this.tableInfo = tableInfo;
        this.clock = clock;
        this.partitionKeys = tableInfo.getPartitionKeys();
        Options options = Options.fromMap(fileStoreTable.options());
        this.idleTimeToDoneMillis = options.get(PARTITION_IDLE_TIME_TO_DONE).toMillis();
        this.timeIntervalMillis = options.get(PARTITION_TIME_INTERVAL).toMillis();
        String timestampPattern = options.get(PARTITION_TIMESTAMP_PATTERN);
        String timestampFormatter = options.get(PARTITION_TIMESTAMP_FORMATTER);
        if (timestampFormatter != null) {
            // Reject invalid syntax before it can drain every partition from the tracked state.
            DateTimeFormatter.ofPattern(timestampFormatter);
        }
        this.partitionTimeExtractor =
                new PartitionTimeExtractor(timestampPattern, timestampFormatter);
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        fileStoreTable.coreOptions().partitionDefaultName(),
                        fileStoreTable.schema().logicalPartitionType(),
                        fileStoreTable.partitionKeys().toArray(new String[0]),
                        fileStoreTable.coreOptions().legacyPartitionName());
        this.markDoneActions =
                PartitionMarkDoneAction.createActions(
                        PaimonPartitionMarkDone.class.getClassLoader(),
                        fileStoreTable,
                        fileStoreTable.coreOptions());
    }

    /** Checks whether the lake table options enable process-time partition mark-done. */
    public static boolean isEnabled(FileStoreTable fileStoreTable, TableInfo tableInfo) {
        if (!tableInfo.isPartitioned()) {
            return false;
        }
        Options options = Options.fromMap(fileStoreTable.options());
        if (!options.containsKey(PARTITION_IDLE_TIME_TO_DONE.key())) {
            return false;
        }
        if (!options.containsKey(PARTITION_TIME_INTERVAL.key())) {
            LOG.warn(
                    "Option {} is missing for table {}, partition mark-done is disabled.",
                    PARTITION_TIME_INTERVAL.key(),
                    tableInfo.getTablePath());
            return false;
        }
        try {
            options.get(PARTITION_IDLE_TIME_TO_DONE);
            options.get(PARTITION_TIME_INTERVAL);
            CoreOptions coreOptions = new CoreOptions(options.toMap());
            if (coreOptions
                    .partitionMarkDoneActions()
                    .contains(CoreOptions.PartitionMarkDoneAction.CUSTOM)) {
                checkArgument(
                        !StringUtils.isNullOrWhitespaceOnly(
                                coreOptions.partitionMarkDoneCustomClass()),
                        "Option %s is required for the custom mark-done action.",
                        CoreOptions.PARTITION_MARK_DONE_CUSTOM_CLASS.key());
            }
        } catch (Exception e) {
            LOG.warn(
                    "Invalid mark-done configuration for table {}, "
                            + "partition mark-done is disabled.",
                    tableInfo.getTablePath(),
                    e);
            return false;
        }
        // Watermark mode requires a table watermark that tiering does not provide.
        String markDoneMode = options.get(PARTITION_MARK_DONE_MODE);
        if (!PARTITION_MARK_DONE_MODE.defaultValue().equalsIgnoreCase(markDoneMode)) {
            LOG.warn(
                    "Option {} is set to {} for table {} but only the process-time mode is "
                            + "supported, partition mark-done is disabled.",
                    PARTITION_MARK_DONE_MODE.key(),
                    markDoneMode,
                    tableInfo.getTablePath());
            return false;
        }
        return true;
    }

    /** Extracts the Fluss partition names of the given commit messages. */
    public Set<String> extractTieredPartitions(ManifestCommittable committable) {
        Set<String> tieredPartitions = new HashSet<>();
        for (CommitMessage commitMessage : committable.fileCommittables()) {
            tieredPartitions.add(toPartitionName(commitMessage.partition()));
        }
        return tieredPartitions;
    }

    /**
     * Backfills historical partitions, tracks tiered partitions and marks idle partitions done.
     * Returns a new state; dropped partitions remain tracked until their idle window expires.
     */
    public PartitionMarkDoneState markIdlePartitionsDone(
            PartitionMarkDoneState previousState, Set<String> tieredPartitions) {
        long now = clock.milliseconds();
        boolean initialized = previousState.isInitialized();
        PartitionMarkDoneTrigger trigger =
                new PartitionMarkDoneTrigger(
                        previousState.getTrackedPartitionLastUpdateTimes(),
                        this::extractPartitionEndTime,
                        idleTimeToDoneMillis);

        // Keep initialized=false on failure so historical partitions are backfilled again.
        if (!initialized) {
            try {
                for (Map.Entry<String, PartitionEntry> entry : listLivePartitions().entrySet()) {
                    if (!trigger.trackedPartitionLastUpdateTimes().containsKey(entry.getKey())) {
                        trigger.notifyPartition(
                                entry.getKey(), entry.getValue().lastFileCreationTime());
                    }
                }
                initialized = true;
            } catch (Exception e) {
                LOG.warn(
                        "Failed to backfill lake partitions of table {}, "
                                + "will retry the cold start in the next round.",
                        tableInfo.getTablePath(),
                        e);
            }
        }

        // track tiered partitions; this also re-adds a done partition on late data
        for (String tieredPartition : tieredPartitions) {
            trigger.notifyPartition(tieredPartition, now);
        }

        Map<String, Long> lastUpdateTimes =
                new HashMap<>(trigger.trackedPartitionLastUpdateTimes());
        List<String> donePartitions = trigger.donePartitions(now);

        // Tracked partitions may have no remaining files after deletion or expiration.
        // They still need the idempotent done actions.
        for (String partitionName : donePartitions) {
            try {
                markPartitionDone(partitionName);
            } catch (Exception e) {
                LOG.warn(
                        "Failed to mark partition {} of table {} as done, "
                                + "will retry in the next round.",
                        partitionName,
                        tableInfo.getTablePath(),
                        e);
                // keep the original last update time so the partition is judged done again
                trigger.notifyPartition(partitionName, lastUpdateTimes.get(partitionName));
            }
        }

        return new PartitionMarkDoneState(initialized, trigger.trackedPartitionLastUpdateTimes());
    }

    private void markPartitionDone(String partitionName) throws Exception {
        LinkedHashMap<String, String> partitionSpec = toPartitionSpec(partitionName);
        String partitionPath = PartitionPathUtils.generatePartitionPath(partitionSpec);
        LOG.info("Mark partition {} of table {} as done.", partitionPath, tableInfo.getTablePath());
        for (PartitionMarkDoneAction action : markDoneActions) {
            action.markDone(partitionPath);
        }
    }

    private Map<String, PartitionEntry> listLivePartitions() {
        Map<String, PartitionEntry> livePartitions = new HashMap<>();
        if (fileStoreTable.snapshotManager().latestSnapshotId() == null) {
            return livePartitions;
        }
        for (PartitionEntry partitionEntry :
                fileStoreTable.newSnapshotReader().partitionEntries()) {
            livePartitions.put(toPartitionName(partitionEntry.partition()), partitionEntry);
        }
        return livePartitions;
    }

    private String toPartitionName(BinaryRow partition) {
        return String.join(
                ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR,
                partitionComputer.generatePartValues(partition).values());
    }

    private LinkedHashMap<String, String> toPartitionSpec(String partitionName) {
        return new LinkedHashMap<>(
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName)
                        .toPartitionSpec()
                        .getSpecMap());
    }

    /** Returns the partition end time, or null when the partition value cannot be parsed. */
    @Nullable
    private Long extractPartitionEndTime(String partitionName) {
        try {
            List<String> partitionValues =
                    ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName)
                            .getPartitionValues();
            LocalDateTime startTime =
                    partitionTimeExtractor.extract(partitionKeys, partitionValues);
            return startTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                    + timeIntervalMillis;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to extract partition end time from partition {} of table {}, skipping mark-done.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
            return null;
        }
    }

    @Override
    public void close() {
        for (PartitionMarkDoneAction action : markDoneActions) {
            IOUtils.closeQuietly(action);
        }
    }
}
