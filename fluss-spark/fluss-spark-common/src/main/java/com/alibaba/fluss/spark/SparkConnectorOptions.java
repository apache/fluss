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

package com.alibaba.fluss.spark;

import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.FlussConfigUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.config.ConfigBuilder.key;

/** Options for spark connector. */
public class SparkConnectorOptions {

    public static final ConfigOption<Integer> BUCKET_NUMBER =
            key("bucket.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of buckets of a Fluss table.");

    public static final ConfigOption<String> BUCKET_KEY =
            key("bucket.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specific the distribution policy of the Fluss table. "
                                    + "Data will be distributed to each bucket according to the hash value of bucket-key. "
                                    + "If you specify multiple fields, delimiter is ','. "
                                    + "If the table is with primary key, you can't specific bucket key currently. "
                                    + "The bucket keys will always be the primary key. "
                                    + "If the table is not with primary key, you can specific bucket key, and when the bucket key is not specified, "
                                    + "the data will be distributed to each bucket randomly.");

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A list of host/port pairs to use for establishing the initial connection to the Fluss cluster. "
                                    + "The list should be in the form host1:port1,host2:port2,....");

    public static final ConfigOption<String> PRIMARY_KEY =
            key("primary.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the primary key of fluss table, such as key1,key2,...");

    public static final ConfigOption<String> TMP_DIRS =
            key("io.tmp.dirs")
                    .stringType()
                    .defaultValue(System.getProperty("java.io.tmpdir"))
                    .withDeprecatedKeys("taskmanager.tmp.dirs")
                    .withDescription(
                            "Directories for temporary files, separated by\",\", \"|\", or the system's java.io.File.pathSeparator.");

    // --------------------------------------------------------------------------------------------
    // Lookup specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            key("lookup.async")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to set async lookup. Default is true.");

    // --------------------------------------------------------------------------------------------
    // Scan specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<ScanStartupMode> SCAN_STARTUP_MODE =
            key("scan.startup.mode")
                    .enumType(ScanStartupMode.class)
                    .defaultValue(ScanStartupMode.INITIAL)
                    .withDescription(
                            "Optional startup mode for Fluss source. Default is 'initial'.");

    public static final ConfigOption<String> SCAN_STARTUP_TIMESTAMP =
            key("scan.startup.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp for Fluss source in case of startup mode is timestamp. "
                                    + "The format is 'timestamp' or 'yyyy-MM-dd HH:mm:ss'. "
                                    + "Like '1678883047356' or '2023-12-09 23:09:12'.");

    public static final ConfigOption<Duration> SCAN_PARTITION_DISCOVERY_INTERVAL =
            key("scan.partition.discovery.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The interval in milliseconds for the Fluss source to discover "
                                    + "the new partitions for partitioned table while scanning."
                                    + " A non-positive value disables the partition discovery.");

    // --------------------------------------------------------------------------------------------
    // table storage specific options
    // --------------------------------------------------------------------------------------------

    public static final List<ConfigOption<?>> TABLE_OPTIONS =
            new ArrayList<>(FlussConfigUtils.TABLE_OPTIONS.values());

    // --------------------------------------------------------------------------------------------
    // client specific options
    // --------------------------------------------------------------------------------------------

    public static final List<ConfigOption<?>> CLIENT_OPTIONS =
            new ArrayList<>(FlussConfigUtils.CLIENT_OPTIONS.values());

    // ------------------------------------------------------------------------------------------

    /** Startup mode for the fluss scanner, see {@link #SCAN_STARTUP_MODE}. */
    public enum ScanStartupMode {
        INITIAL(
                "initial",
                "Performs an initial snapshot n the table upon first startup, "
                        + "ans continue to read the latest changelog with exactly once guarantee. "
                        + "If the table to read is a log table, the initial snapshot means "
                        + "reading from earliest log offset. If the table to read is a primary key table, "
                        + "the initial snapshot means reading a latest snapshot which "
                        + "materializes all changes on the table."),
        EARLIEST("earliest", "Start reading logs from the earliest offset."),
        LATEST("latest", "Start reading logs from the latest offset."),
        TIMESTAMP("timestamp", "Start reading logs from user-supplied timestamp.");

        private final String value;
        private final String description;

        ScanStartupMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
