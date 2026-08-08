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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.PartitionUtils;

import org.apache.paimon.data.BinaryRow;
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
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Judges and marks idle partitions of a Paimon lake table as done during tiering. The judgment
 * ({@code done <=> now - max(lastUpdateTime, partitionEndTime) > partition.idle-time-to-done}) is
 * delegated to {@link PartitionMarkDoneTrigger} copied from Paimon. It reuses Paimon's options
 * ({@code partition.idle-time-to-done}, {@code partition.time-interval}, {@code
 * partition.timestamp-pattern/formatter}, {@code partition.mark-done-action}) and executes the
 * idempotent Paimon mark-done actions. For auto-partitioned tables the partition end time is
 * derived from the auto-partition time unit instead of {@code partition.time-interval}. See {@link
 * MarkDoneState} for the state model.
 */
public class PaimonPartitionMarkDone implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartitionMarkDone.class);

    /** Snapshot property key storing the mark-done state JSON. */
    public static final String MARK_DONE_STATE_PROPERTY = "fluss.tiering.mark-done-state";

    /** Same option as Paimon's FlinkConnectorOptions#PARTITION_IDLE_TIME_TO_DONE. */
    private static final ConfigOption<Duration> PARTITION_IDLE_TIME_TO_DONE =
            ConfigOptions.key("partition.idle-time-to-done").durationType().noDefaultValue();

    /** Same option as Paimon's FlinkConnectorOptions#PARTITION_TIME_INTERVAL. */
    private static final ConfigOption<Duration> PARTITION_TIME_INTERVAL =
            ConfigOptions.key("partition.time-interval").durationType().noDefaultValue();

    private final FileStoreTable fileStoreTable;
    private final TableInfo tableInfo;
    private final List<String> partitionKeys;
    private final long idleTimeToDoneMillis;
    // partition end time interval for non auto-partitioned tables, null means idle judgment
    // is not possible (same as Paimon which requires partition.time-interval)
    @Nullable private final Long timeIntervalMillis;
    private final AutoPartitionStrategy autoPartitionStrategy;
    private final PartitionTimeExtractor partitionTimeExtractor;
    private final InternalRowPartitionComputer partitionComputer;
    private final List<PartitionMarkDoneAction> markDoneActions;

    public PaimonPartitionMarkDone(FileStoreTable fileStoreTable, TableInfo tableInfo) {
        checkState(
                isEnabled(tableInfo, fileStoreTable),
                "Partition mark-done is not enabled for table %s.",
                tableInfo.getTablePath());
        this.fileStoreTable = fileStoreTable;
        this.tableInfo = tableInfo;
        this.partitionKeys = tableInfo.getPartitionKeys();
        Options options = Options.fromMap(fileStoreTable.options());
        this.idleTimeToDoneMillis = options.get(PARTITION_IDLE_TIME_TO_DONE).toMillis();
        Duration timeInterval = options.get(PARTITION_TIME_INTERVAL);
        this.timeIntervalMillis = timeInterval == null ? null : timeInterval.toMillis();
        this.autoPartitionStrategy = tableInfo.getTableConfig().getAutoPartitionStrategy();
        // pattern/formatter may be null, then Paimon's default extraction rule applies
        this.partitionTimeExtractor =
                new PartitionTimeExtractor(
                        fileStoreTable.coreOptions().partitionTimestampPattern(),
                        fileStoreTable.coreOptions().partitionTimestampFormatter());
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

    /** Whether partition mark-done is enabled for the given table. */
    public static boolean isEnabled(TableInfo tableInfo, FileStoreTable fileStoreTable) {
        return tableInfo.isPartitioned()
                && fileStoreTable.options().containsKey(PARTITION_IDLE_TIME_TO_DONE.key());
    }

    /**
     * Cheap pre-check on Fluss custom properties (no lake access), used before opening the Paimon
     * table. The authoritative check is {@link #isEnabled(TableInfo, FileStoreTable)}.
     */
    public static boolean isEnabled(TableInfo tableInfo) {
        return tableInfo.isPartitioned()
                && tableInfo
                        .getCustomProperties()
                        .toMap()
                        .containsKey("paimon." + PARTITION_IDLE_TIME_TO_DONE.key());
    }

    /** Extracts the Fluss partition names of the given commit messages. */
    public Set<String> extractTieredPartitions(List<CommitMessage> commitMessages) {
        Set<String> tieredPartitions = new HashSet<>();
        for (CommitMessage commitMessage : commitMessages) {
            tieredPartitions.add(toPartitionName(commitMessage.partition()));
        }
        return tieredPartitions;
    }

    /**
     * Runs one round: cold-start backfill, tracks tiered partitions, marks idle partitions done and
     * prunes dropped ones. Returns the new state JSON, or null if unchanged.
     */
    @Nullable
    public String run(@Nullable String previousStateJson, Set<String> tieredPartitions)
            throws Exception {
        MarkDoneState previousState =
                previousStateJson == null
                        ? MarkDoneState.empty()
                        : MarkDoneStateJsonSerde.fromJson(previousStateJson);

        long now = System.currentTimeMillis();
        boolean initialized = previousState.isInitialized();
        Map<String, PartitionEntry> livePartitions = null;
        PartitionMarkDoneTrigger trigger =
                new PartitionMarkDoneTrigger(
                        previousState.getPendingPartitions(),
                        this::extractPartitionEndTime,
                        idleTimeToDoneMillis);

        // cold start: backfill all existing lake partitions into the pending set
        if (!initialized) {
            livePartitions = listLivePartitions();
            for (Map.Entry<String, PartitionEntry> entry : livePartitions.entrySet()) {
                if (!trigger.pendingPartitions().containsKey(entry.getKey())) {
                    trigger.notifyPartition(
                            entry.getKey(), entry.getValue().lastFileCreationTime());
                }
            }
            initialized = true;
        }

        // track tiered partitions; this also re-adds a done partition on late data
        for (String tieredPartition : tieredPartitions) {
            trigger.notifyPartition(tieredPartition, now);
        }

        // same as Paimon, idle judgment requires a derivable partition end time
        List<String> donePartitions =
                autoPartitionStrategy.isAutoPartitionEnabled() || timeIntervalMillis != null
                        ? trigger.donePartitions(now)
                        : Collections.emptyList();

        // done-is-delete: done partitions are already removed from the trigger's pending set
        if (!donePartitions.isEmpty()) {
            if (livePartitions == null) {
                livePartitions = listLivePartitions();
            }
            for (String partitionName : donePartitions) {
                if (livePartitions.containsKey(partitionName)) {
                    markPartitionDone(partitionName);
                }
            }
        }

        // prune pending partitions which no longer exist in the lake (dropped or expired)
        Map<String, Long> pending = trigger.pendingPartitions();
        if (livePartitions != null) {
            Map<String, PartitionEntry> alivePartitions = livePartitions;
            pending.keySet()
                    .removeIf(
                            partitionName ->
                                    !alivePartitions.containsKey(partitionName)
                                            && !tieredPartitions.contains(partitionName));
        }

        MarkDoneState newState = new MarkDoneState(initialized, pending);
        if (newState.equals(previousState)) {
            return null;
        }
        return MarkDoneStateJsonSerde.toJson(newState);
    }

    /** Executes the configured idempotent Paimon mark-done actions. */
    private void markPartitionDone(String partitionName) throws Exception {
        LinkedHashMap<String, String> partitionSpec = toPartitionSpec(partitionName);
        String partitionPath = PartitionPathUtils.generatePartitionPath(partitionSpec);
        LOG.info("Mark partition {} of table {} as done.", partitionPath, tableInfo.getTablePath());
        for (PartitionMarkDoneAction action : markDoneActions) {
            action.markDone(partitionPath);
        }
    }

    /** Lists the live partitions of the lake table, keyed by Fluss partition name. */
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
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        List<String> partitionValues = resolvedPartitionSpec.getPartitionValues();
        for (int i = 0; i < partitionKeys.size(); i++) {
            partitionSpec.put(partitionKeys.get(i), partitionValues.get(i));
        }
        return partitionSpec;
    }

    /**
     * Extracts the partition end time in epoch millis: partition start time (from the
     * auto-partition time unit, or Paimon's partition.timestamp-pattern/formatter) plus one
     * partition interval; null if it cannot be derived. Times are resolved in the zone that
     * generated the partition name: the table's auto-partition time zone for auto-partitioned
     * tables, or the JVM default zone otherwise (same as Paimon).
     */
    @Nullable
    private Long extractPartitionEndTime(String partitionName) {
        List<String> partitionValues =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName)
                        .getPartitionValues();
        try {
            if (autoPartitionStrategy.isAutoPartitionEnabled()) {
                String timeValue = partitionValues.get(autoPartitionTimeKeyIndex());
                LocalDateTime startTime = parseAutoPartitionTime(timeValue, autoPartitionStrategy);
                LocalDateTime endTime = plusOneTimeUnit(startTime, autoPartitionStrategy);
                return endTime.atZone(autoPartitionStrategy.timeZone().toZoneId())
                        .toInstant()
                        .toEpochMilli();
            } else {
                LocalDateTime startTime =
                        partitionTimeExtractor.extract(partitionKeys, partitionValues);
                return startTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                        + checkNotNull(timeIntervalMillis);
            }
        } catch (Exception e) {
            LOG.debug(
                    "Fail to extract partition end time from partition {} of table {}.",
                    partitionName,
                    tableInfo.getTablePath(),
                    e);
            return null;
        }
    }

    private int autoPartitionTimeKeyIndex() {
        if (partitionKeys.size() == 1) {
            return 0;
        }
        int index = partitionKeys.indexOf(autoPartitionStrategy.key());
        if (index < 0) {
            throw new IllegalStateException(
                    String.format(
                            "Auto partition time key %s is not found in partition keys %s.",
                            autoPartitionStrategy.key(), partitionKeys));
        }
        return index;
    }

    private static LocalDateTime parseAutoPartitionTime(
            String timeValue, AutoPartitionStrategy strategy) {
        AutoPartitionTimeUnit timeUnit = strategy.timeUnit();
        if (strategy.timeFormat() == null && timeUnit == AutoPartitionTimeUnit.QUARTER) {
            // default quarter format is 'yyyyQ' which can't be parsed field by field
            int year = Integer.parseInt(timeValue.substring(0, 4));
            int quarter = Integer.parseInt(timeValue.substring(4));
            return LocalDateTime.of(year, (quarter - 1) * 3 + 1, 1, 0, 0);
        }
        String format = PartitionUtils.getPartitionTimeFormat(timeUnit, strategy);
        DateTimeFormatter formatter =
                new DateTimeFormatterBuilder()
                        .appendPattern(format)
                        .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                        .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                        .toFormatter();
        return LocalDateTime.parse(timeValue, formatter);
    }

    private static LocalDateTime plusOneTimeUnit(
            LocalDateTime startTime, AutoPartitionStrategy strategy) {
        switch (strategy.timeUnit()) {
            case YEAR:
                return startTime.plusYears(1);
            case QUARTER:
                return startTime.plusMonths(3);
            case MONTH:
                return startTime.plusMonths(1);
            case DAY:
                return startTime.plusDays(1);
            case HOUR:
                return startTime.plusHours(1);
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + strategy.timeUnit());
        }
    }

    @Override
    public void close() {
        for (PartitionMarkDoneAction action : markDoneActions) {
            IOUtils.closeQuietly(action);
        }
    }
}
