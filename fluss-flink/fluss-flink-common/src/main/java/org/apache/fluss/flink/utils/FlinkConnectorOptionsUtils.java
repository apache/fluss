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

package org.apache.fluss.flink.utils;

import org.apache.fluss.client.initializer.NoStoppingOffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.FlinkConnectorOptions.ScanBoundedMode;
import org.apache.fluss.flink.FlinkConnectorOptions.ScanStartupMode;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.metadata.MergeEngineType;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR;
import static org.apache.fluss.config.ConfigOptions.LAKE_TIERING_IO_TMP_DIRS;
import static org.apache.fluss.flink.FlinkConnectorOptions.SCAN_BOUNDED_MODE;
import static org.apache.fluss.flink.FlinkConnectorOptions.SCAN_BOUNDED_TIMESTAMP;
import static org.apache.fluss.flink.FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE;
import static org.apache.fluss.flink.FlinkConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.fluss.flink.FlinkConnectorOptions.SCAN_STARTUP_TIMESTAMP;
import static org.apache.fluss.flink.FlinkConnectorOptions.ScanStartupMode.TIMESTAMP;

/** Utility class for {@link FlinkConnectorOptions}. */
public class FlinkConnectorOptionsUtils {

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static ZoneId getLocalTimeZone(String timeZone) {
        return TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(timeZone)
                ? ZoneId.systemDefault()
                : ZoneId.of(timeZone);
    }

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateScanStartupMode(tableOptions);
        validateScanBoundedMode(tableOptions);
        validateScanSplitAssignmentBatchSize(tableOptions);
    }

    /**
     * Validates that the distribution mode is compatible with merge engine tables.
     *
     * <p>For primary key tables with any merge engine, keyed shuffle must be enabled to ensure
     * correct data routing. This is required because:
     *
     * <ul>
     *   <li>AGGREGATION: aggregate state cannot be correctly redistributed on rescale
     *   <li>FIRST_ROW/VERSIONED: merge semantics require same-key records go to same task
     * </ul>
     *
     * @param mergeEngineType the merge engine type (can be null for non-merge-engine tables)
     * @param distributionMode the distribution mode configured for the sink
     * @throws IllegalArgumentException if distribution mode is incompatible with merge engine
     */
    public static void validateDistributionModeForMergeEngine(
            @Nullable MergeEngineType mergeEngineType, DistributionMode distributionMode) {
        if (mergeEngineType != null
                && distributionMode != DistributionMode.BUCKET
                && distributionMode != DistributionMode.AUTO) {
            throw new IllegalArgumentException(
                    String.format(
                            "For primary key tables with merge engine ('%s'), "
                                    + "'sink.distribution-mode' must be 'bucket' or 'auto' (default). "
                                    + "Disabling shuffle breaks merge semantics because records with the same key "
                                    + "must be processed by the same task. Current mode: %s",
                            mergeEngineType, distributionMode));
        }
    }

    public static StartupOptions getStartupOptions(ReadableConfig tableOptions, ZoneId timeZone) {
        ScanStartupMode scanStartupMode = tableOptions.get(SCAN_STARTUP_MODE);
        final StartupOptions options = new StartupOptions();
        options.startupMode = scanStartupMode;
        if (scanStartupMode == TIMESTAMP) {
            options.startupTimestampMs =
                    parseTimestamp(
                            tableOptions.get(SCAN_STARTUP_TIMESTAMP),
                            SCAN_STARTUP_TIMESTAMP.key(),
                            timeZone);
        }
        return options;
    }

    public static BoundedOptions getBoundedOptions(ReadableConfig tableOptions, ZoneId timeZone) {
        ScanBoundedMode scanBoundedMode = tableOptions.get(SCAN_BOUNDED_MODE);
        switch (scanBoundedMode) {
            case UNBOUNDED:
                return BoundedOptions.unbounded();
            case LATEST_OFFSET:
                return BoundedOptions.latestOffset();
            case TIMESTAMP:
                return BoundedOptions.timestamp(
                        parseTimestamp(
                                tableOptions.get(SCAN_BOUNDED_TIMESTAMP),
                                SCAN_BOUNDED_TIMESTAMP.key(),
                                timeZone));
            default:
                throw new IllegalArgumentException("Unsupported bounded mode: " + scanBoundedMode);
        }
    }

    /** Creates the stopping offsets initializer from the given bounded options. */
    public static OffsetsInitializer toStoppingOffsetsInitializer(BoundedOptions boundedOptions) {
        switch (boundedOptions.getBoundedMode()) {
            case UNBOUNDED:
                return new NoStoppingOffsetsInitializer();
            case LATEST_OFFSET:
                return OffsetsInitializer.latest();
            case TIMESTAMP:
                return OffsetsInitializer.timestamp(boundedOptions.getBoundedTimestampMs());
            default:
                throw new IllegalArgumentException(
                        "Unsupported bounded mode: " + boundedOptions.getBoundedMode());
        }
    }

    /**
     * Creates the stopping offsets initializer for the execution mode and bounded options.
     *
     * <p>Batch execution remains bounded by the latest offsets when no explicit bounded mode is
     * configured.
     */
    public static OffsetsInitializer toStoppingOffsetsInitializer(
            boolean streaming, BoundedOptions boundedOptions) {
        if (!streaming && boundedOptions.getBoundedMode() == ScanBoundedMode.UNBOUNDED) {
            return OffsetsInitializer.latest();
        }
        return toStoppingOffsetsInitializer(boundedOptions);
    }

    /** Returns the Flink source boundedness for the execution mode and bounded options. */
    public static Boundedness toBoundedness(boolean streaming, BoundedOptions boundedOptions) {
        return streaming && boundedOptions.getBoundedMode() == ScanBoundedMode.UNBOUNDED
                ? Boundedness.CONTINUOUS_UNBOUNDED
                : Boundedness.BOUNDED;
    }

    public static List<String> getBucketKeys(ReadableConfig tableOptions) {
        Optional<String> bucketKey = tableOptions.getOptional(FlinkConnectorOptions.BUCKET_KEY);
        if (!bucketKey.isPresent()) {
            // log tables don't have bucket key by default
            return new ArrayList<>();
        }

        String[] keys = bucketKey.get().split(",");
        return Arrays.stream(keys).collect(Collectors.toList());
    }

    public static int[] getBucketKeyIndexes(ReadableConfig tableOptions, RowType schema) {
        Optional<String> bucketKey = tableOptions.getOptional(FlinkConnectorOptions.BUCKET_KEY);
        if (!bucketKey.isPresent()) {
            // log tables don't have bucket key by default
            return new int[0];
        }

        String[] keys = bucketKey.get().split(",");
        int[] indexes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            int index = schema.getFieldIndex(keys[i].trim());
            if (index < 0) {
                throw new ValidationException(
                        String.format(
                                "Field '%s' not found in the schema. Available fields are: %s",
                                keys[i].trim(), schema.getFieldNames()));
            }
            indexes[i] = index;
        }
        return indexes;
    }

    // ----------------------------------------------------------------------------------------

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        ScanStartupMode scanStartupMode = tableOptions.get(SCAN_STARTUP_MODE);
        if (scanStartupMode == TIMESTAMP) {
            if (!tableOptions.getOptional(SCAN_STARTUP_TIMESTAMP).isPresent()) {
                throw new ValidationException(
                        String.format(
                                "'%s' is required int '%s' startup mode but missing.",
                                SCAN_STARTUP_TIMESTAMP.key(), TIMESTAMP));
            }
        }
    }

    private static void validateScanBoundedMode(ReadableConfig tableOptions) {
        ScanBoundedMode scanBoundedMode = tableOptions.get(SCAN_BOUNDED_MODE);
        if (scanBoundedMode == ScanBoundedMode.TIMESTAMP) {
            if (!tableOptions.getOptional(SCAN_BOUNDED_TIMESTAMP).isPresent()) {
                throw new ValidationException(
                        String.format(
                                "'%s' is required in '%s' bounded mode but missing.",
                                SCAN_BOUNDED_TIMESTAMP.key(), ScanBoundedMode.TIMESTAMP));
            }
        }
    }

    private static void validateScanSplitAssignmentBatchSize(ReadableConfig tableOptions) {
        int batchSize = tableOptions.get(SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE);
        if (batchSize <= 0) {
            throw new ValidationException(
                    String.format(
                            "'%s' must be positive, but was %s.",
                            SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.key(), batchSize));
        }
    }

    /**
     * Parses timestamp String to Long.
     *
     * <p>timestamp String format was given as following:
     *
     * <pre>
     *     scan.startup.timestamp = 1678883047356
     *     scan.startup.timestamp = 2023-12-09 23:09:12
     * </pre>
     *
     * @return timestamp as long value
     */
    public static long parseTimestamp(String timestampStr, String optionKey, ZoneId timeZone) {
        if (timestampStr.matches("\\d+")) {
            return Long.parseLong(timestampStr);
        }

        try {
            return LocalDateTime.parse(timestampStr, DATE_TIME_FORMATTER)
                    .atZone(timeZone)
                    .toInstant()
                    .toEpochMilli();
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Invalid properties '%s' should follow the format "
                                    + "'yyyy-MM-dd HH:mm:ss' or 'timestamp', but is '%s'. "
                                    + "You can config like: '2023-12-09 23:09:12' or '1678883047356'.",
                            optionKey, timestampStr),
                    e);
        }
    }

    public static String getClientScannerIoTmpDir(
            Configuration flussConf, org.apache.flink.configuration.Configuration flinkConfig) {
        return flussConf
                .getOptional(CLIENT_SCANNER_IO_TMP_DIR)
                .orElseGet(
                        () ->
                                new File(
                                                ConfigurationUtils.getRandomTempDirectory(
                                                        flinkConfig),
                                                "fluss")
                                        .getAbsolutePath());
    }

    /**
     * Returns the configured lake-tiering temporary directories, or Flink's temporary directories
     * with a {@code fluss} child directory when no tiering-specific value is configured.
     */
    public static String[] getLakeTieringIoTmpDirs(
            Configuration lakeTieringConfig,
            org.apache.flink.configuration.Configuration flinkConfig) {
        Optional<String> configuredTmpDirs =
                lakeTieringConfig.getOptional(LAKE_TIERING_IO_TMP_DIRS);
        if (configuredTmpDirs.isPresent()) {
            return ConfigurationUtils.splitPaths(configuredTmpDirs.get());
        }

        String[] flinkTmpDirs = ConfigurationUtils.parseTempDirectories(flinkConfig);
        String[] lakeTieringTmpDirs = new String[flinkTmpDirs.length];
        for (int i = 0; i < flinkTmpDirs.length; i++) {
            lakeTieringTmpDirs[i] = new File(flinkTmpDirs[i], "fluss").getAbsolutePath();
        }
        return lakeTieringTmpDirs;
    }

    /** Fluss startup options. * */
    public static class StartupOptions {
        public ScanStartupMode startupMode;
        public long startupTimestampMs;
    }

    /** Immutable Fluss bounded options. */
    public static final class BoundedOptions {
        private final ScanBoundedMode boundedMode;
        private final long boundedTimestampMs;

        private BoundedOptions(ScanBoundedMode boundedMode, long boundedTimestampMs) {
            this.boundedMode = boundedMode;
            this.boundedTimestampMs = boundedTimestampMs;
        }

        /** Returns the default bounded options, i.e. no user-specified stopping offsets. */
        public static BoundedOptions unbounded() {
            return new BoundedOptions(ScanBoundedMode.UNBOUNDED, 0L);
        }

        /** Returns bounded options that stop at the latest offsets resolved at startup. */
        public static BoundedOptions latestOffset() {
            return new BoundedOptions(ScanBoundedMode.LATEST_OFFSET, 0L);
        }

        /** Returns bounded options that stop at offsets resolved from the given timestamp. */
        public static BoundedOptions timestamp(long timestampMs) {
            return new BoundedOptions(ScanBoundedMode.TIMESTAMP, timestampMs);
        }

        /** Returns the configured bounded mode. */
        public ScanBoundedMode getBoundedMode() {
            return boundedMode;
        }

        /** Returns the timestamp used by timestamp bounded mode. */
        public long getBoundedTimestampMs() {
            return boundedTimestampMs;
        }
    }
}
