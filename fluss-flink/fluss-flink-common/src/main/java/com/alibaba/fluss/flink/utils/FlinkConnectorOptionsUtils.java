/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.flink.FlinkConnectorOptions;
import com.alibaba.fluss.flink.FlinkConnectorOptions.ScanStartupMode;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.types.logical.RowType;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static com.alibaba.fluss.flink.FlinkConnectorOptions.SCAN_STARTUP_MODE;
import static com.alibaba.fluss.flink.FlinkConnectorOptions.SCAN_STARTUP_TIMESTAMP;
import static com.alibaba.fluss.flink.FlinkConnectorOptions.ScanStartupMode.TIMESTAMP;

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

    /** Fluss startup options. * */
    public static class StartupOptions {
        public ScanStartupMode startupMode;
        public long startupTimestampMs;
    }
}
