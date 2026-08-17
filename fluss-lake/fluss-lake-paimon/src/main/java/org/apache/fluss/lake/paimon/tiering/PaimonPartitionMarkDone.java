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

import org.apache.paimon.CoreOptions;
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
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
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
 * delegated to {@link PartitionMarkDoneTrigger} copied from Paimon. It reuses the semantics of
 * Paimon's options ({@code partition.idle-time-to-done}, {@code partition.time-interval}, {@code
 * partition.timestamp-pattern/formatter}) but reads them exclusively from the {@code paimon.}
 * prefixed Fluss table custom properties as the single source of truth: options configured on the
 * Paimon table directly are deliberately not honored. It executes the idempotent Paimon mark-done
 * actions ({@code partition.mark-done-action}). For auto-partitioned tables the partition end time
 * is derived from the auto-partition time unit instead of {@code partition.time-interval}. See
 * {@link MarkDoneState} for the state model.
 */
public class PaimonPartitionMarkDone implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartitionMarkDone.class);

    /** Snapshot property key storing the mark-done state JSON. */
    public static final String MARK_DONE_STATE_PROPERTY = "fluss.tiering.mark-done-state";

    /** The prefix of Paimon options in the Fluss table custom properties. */
    private static final String PAIMON_PROPERTY_PREFIX = "paimon.";

    /** Same option as Paimon's FlinkConnectorOptions#PARTITION_IDLE_TIME_TO_DONE. */
    private static final ConfigOption<Duration> PARTITION_IDLE_TIME_TO_DONE =
            ConfigOptions.key("partition.idle-time-to-done").durationType().noDefaultValue();

    /** Same option as Paimon's FlinkConnectorOptions#PARTITION_TIME_INTERVAL. */
    private static final ConfigOption<Duration> PARTITION_TIME_INTERVAL =
            ConfigOptions.key("partition.time-interval").durationType().noDefaultValue();

    /** Same option as Paimon's FlinkConnectorOptions#PARTITION_MARK_DONE_MODE. */
    private static final ConfigOption<String> PARTITION_MARK_DONE_MODE =
            ConfigOptions.key("partition.mark-done-action.mode")
                    .stringType()
                    .defaultValue("process-time");

    /** Same option as Paimon's CoreOptions#PARTITION_TIMESTAMP_PATTERN. */
    private static final ConfigOption<String> PARTITION_TIMESTAMP_PATTERN =
            ConfigOptions.key("partition.timestamp-pattern").stringType().noDefaultValue();

    /** Same option as Paimon's CoreOptions#PARTITION_TIMESTAMP_FORMATTER. */
    private static final ConfigOption<String> PARTITION_TIMESTAMP_FORMATTER =
            ConfigOptions.key("partition.timestamp-formatter").stringType().noDefaultValue();

    private final FileStoreTable fileStoreTable;
    private final TableInfo tableInfo;
    private final List<String> partitionKeys;
    private final long idleTimeToDoneMillis;
    // partition end time interval for non auto-partitioned tables (required by isEnabled,
    // same as Paimon), null for auto-partitioned tables whose interval derives from the
    // auto-partition time unit
    @Nullable private final Long timeIntervalMillis;
    private final AutoPartitionStrategy autoPartitionStrategy;
    private final PartitionTimeExtractor partitionTimeExtractor;
    private final InternalRowPartitionComputer partitionComputer;
    private final List<PartitionMarkDoneAction> markDoneActions;

    public PaimonPartitionMarkDone(FileStoreTable fileStoreTable, TableInfo tableInfo) {
        checkState(
                isEnabled(tableInfo),
                "Partition mark-done is not enabled for table %s.",
                tableInfo.getTablePath());
        this.fileStoreTable = fileStoreTable;
        this.tableInfo = tableInfo;
        this.partitionKeys = tableInfo.getPartitionKeys();
        Options options = paimonOptionsInCustomProperties(tableInfo);
        this.idleTimeToDoneMillis = options.get(PARTITION_IDLE_TIME_TO_DONE).toMillis();
        Duration timeInterval = options.get(PARTITION_TIME_INTERVAL);
        this.timeIntervalMillis = timeInterval == null ? null : timeInterval.toMillis();
        this.autoPartitionStrategy = tableInfo.getTableConfig().getAutoPartitionStrategy();
        // pattern/formatter may be null, then Paimon's default extraction rule applies
        String timestampFormatter = options.get(PARTITION_TIMESTAMP_FORMATTER);
        if (timestampFormatter != null) {
            // an invalid formatter syntax must disable mark-done as a whole: otherwise every
            // partition would be treated as illegal and drained from the pending set
            DateTimeFormatter.ofPattern(timestampFormatter);
        }
        this.partitionTimeExtractor =
                new PartitionTimeExtractor(
                        options.get(PARTITION_TIMESTAMP_PATTERN), timestampFormatter);
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
                        // the action configuration also comes from the Fluss custom properties
                        // (the single source of truth), the table only provides the environment
                        new CoreOptions(options.toMap()));
    }

    /**
     * Whether partition mark-done is enabled for the given table according to the {@code paimon.}
     * prefixed Fluss table custom properties, the single source of truth (options configured on the
     * Paimon table directly are deliberately not honored). Besides {@code
     * partition.idle-time-to-done}, a derivable partition end time is required (auto-partitioning
     * or {@code partition.time-interval}, same as Paimon): otherwise no partition could ever be
     * judged done while the pending state would grow unbounded and keep triggering useless
     * maintenance rounds.
     */
    public static boolean isEnabled(TableInfo tableInfo) {
        if (!tableInfo.isPartitioned()) {
            return false;
        }
        Options options = paimonOptionsInCustomProperties(tableInfo);
        if (!options.containsKey(PARTITION_IDLE_TIME_TO_DONE.key())) {
            return false;
        }
        try {
            options.get(PARTITION_IDLE_TIME_TO_DONE);
            options.get(PARTITION_TIME_INTERVAL);
        } catch (Exception e) {
            LOG.warn(
                    "Invalid mark-done duration option for table {}, "
                            + "partition mark-done is disabled.",
                    tableInfo.getTablePath(),
                    e);
            return false;
        }
        if (!tableInfo.getTableConfig().getAutoPartitionStrategy().isAutoPartitionEnabled()
                && !options.containsKey(PARTITION_TIME_INTERVAL.key())) {
            LOG.warn(
                    "Option {} is set for table {} but the partition end time can't be derived "
                            + "(neither auto-partitioning nor option {} is set), "
                            + "partition mark-done is disabled.",
                    PARTITION_IDLE_TIME_TO_DONE.key(),
                    tableInfo.getTablePath(),
                    PARTITION_TIME_INTERVAL.key());
            return false;
        }
        // only the process-time mode is supported: the tiering service can't derive the
        // watermark of the table yet, silently degrading the watermark mode to process-time
        // would trigger the done actions ahead of the user-configured watermark boundary
        String markDoneMode = options.get(PARTITION_MARK_DONE_MODE);
        if (!"process-time".equalsIgnoreCase(markDoneMode)) {
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

    /** Extracts the Paimon options carried in the Fluss table custom properties. */
    private static Options paimonOptionsInCustomProperties(TableInfo tableInfo) {
        Map<String, String> paimonOptions = new HashMap<>();
        for (Map.Entry<String, String> entry : tableInfo.getCustomProperties().toMap().entrySet()) {
            if (entry.getKey().startsWith(PAIMON_PROPERTY_PREFIX)) {
                paimonOptions.put(
                        entry.getKey().substring(PAIMON_PROPERTY_PREFIX.length()),
                        entry.getValue());
            }
        }
        return Options.fromMap(paimonOptions);
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
     * Runs one round: cold-start backfill, tracks tiered partitions and marks idle partitions done
     * (done partitions are removed from the pending set). Dropped or expired partitions are not
     * pruned specially: same as Paimon's native listener, they stay pending until the idle time
     * elapses and are then marked done. Returns the new state JSON, or null if unchanged.
     */
    @Nullable
    public String run(@Nullable String previousStateJson, Set<String> tieredPartitions)
            throws Exception {
        MarkDoneState previousState = parsePreviousState(previousStateJson);

        long now = System.currentTimeMillis();
        boolean initialized = previousState.isInitialized();
        PartitionMarkDoneTrigger trigger =
                new PartitionMarkDoneTrigger(
                        previousState.getPendingPartitions(),
                        this::extractPartitionEndTime,
                        idleTimeToDoneMillis);

        // cold start: backfill all existing lake partitions into the pending set; on failure
        // keep initialized=false so the next round retries the backfill, while the tiered
        // partitions of this round are still tracked and persisted below
        if (!initialized) {
            try {
                for (Map.Entry<String, PartitionEntry> entry : listLivePartitions().entrySet()) {
                    if (!trigger.pendingPartitions().containsKey(entry.getKey())) {
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

        // done judgment: idle partitions are marked done and removed from the pending set
        Map<String, Long> lastUpdateTimes = new HashMap<>(trigger.pendingPartitions());
        List<String> donePartitions = trigger.donePartitions(now);

        // done-is-delete: done partitions are already removed from the trigger's pending set.
        // Execute the idempotent actions unconditionally (same as Paimon's native listener): a
        // done partition may legitimately hold zero files (e.g. a PK partition whose data was
        // fully deleted and compacted) and thus be invisible in the partition entries, but it
        // still deserves the done signal
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

        MarkDoneState newState = new MarkDoneState(initialized, trigger.pendingPartitions());
        if (newState.equals(previousState)) {
            return null;
        }
        return MarkDoneStateJsonSerde.toJson(newState);
    }

    /** Parses the restored state; a corrupt state falls back to a cold-start re-initialization. */
    private MarkDoneState parsePreviousState(@Nullable String previousStateJson) {
        if (previousStateJson == null) {
            return MarkDoneState.empty();
        }
        try {
            return MarkDoneStateJsonSerde.fromJson(previousStateJson);
        } catch (Exception e) {
            LOG.warn(
                    "Corrupt mark-done state of table {}, re-initializing via cold start.",
                    tableInfo.getTablePath(),
                    e);
            return MarkDoneState.empty();
        }
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
        try {
            List<String> partitionValues =
                    ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName)
                            .getPartitionValues();
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
        if (timeUnit == AutoPartitionTimeUnit.QUARTER) {
            return parseQuarterPartitionTime(timeValue, strategy);
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

    private static LocalDateTime parseQuarterPartitionTime(
            String timeValue, AutoPartitionStrategy strategy) {
        int year;
        int quarter;
        if (strategy.timeFormat() == null) {
            // default quarter format is 'yyyyQ' which can't be parsed field by field
            year = Integer.parseInt(timeValue.substring(0, 4));
            quarter = Integer.parseInt(timeValue.substring(4));
        } else {
            // resolving a custom quarter format (e.g. yyyy-'Q'Q) into a date would conflict
            // with month/day defaults for Q2-Q4, so extract the year and quarter directly
            // (via getLong since TemporalAccessor#get can't range-check the quarter field)
            TemporalAccessor accessor =
                    DateTimeFormatter.ofPattern(strategy.timeFormat()).parse(timeValue);
            year =
                    (int)
                            (accessor.isSupported(ChronoField.YEAR)
                                    ? accessor.getLong(ChronoField.YEAR)
                                    : accessor.getLong(ChronoField.YEAR_OF_ERA));
            quarter = (int) accessor.getLong(IsoFields.QUARTER_OF_YEAR);
        }
        return LocalDateTime.of(year, (quarter - 1) * 3 + 1, 1, 0, 0);
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
