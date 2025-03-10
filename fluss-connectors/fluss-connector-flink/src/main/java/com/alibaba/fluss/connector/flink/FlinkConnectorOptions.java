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

package com.alibaba.fluss.connector.flink;

import com.alibaba.fluss.config.FlussConfigUtils;
import com.alibaba.fluss.connector.flink.utils.FlinkConversions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.configuration.description.TextElement.text;

/** Options for flink connector. */
public class FlinkConnectorOptions {

    public static final ConfigOption<Integer> BUCKET_NUMBER =
            ConfigOptions.key("bucket.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of buckets of a Fluss table.");

    public static final ConfigOption<String> BUCKET_KEY =
            ConfigOptions.key("bucket.key")
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
            ConfigOptions.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A list of host/port pairs to use for establishing the initial connection to the Fluss cluster. "
                                    + "The list should be in the form host1:port1,host2:port2,....");

    // --------------------------------------------------------------------------------------------
    // Lookup specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to set async lookup. Default is true.");

    // --------------------------------------------------------------------------------------------
    // Scan specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<ScanStartupMode> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(ScanStartupMode.class)
                    .defaultValue(ScanStartupMode.FULL)
                    .withDescription(
                            String.format(
                                    "Optional startup mode for Fluss source. Default is '%s'.",
                                    ScanStartupMode.FULL.value));

    public static final ConfigOption<String> SCAN_STARTUP_TIMESTAMP =
            ConfigOptions.key("scan.startup.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp for Fluss source in case of startup mode is timestamp. "
                                    + "The format is 'timestamp' or 'yyyy-MM-dd HH:mm:ss'. "
                                    + "Like '1678883047356' or '2023-12-09 23:09:12'.");

    public static final ConfigOption<Duration> SCAN_PARTITION_DISCOVERY_INTERVAL =
            ConfigOptions.key("scan.partition.discovery.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The time interval for the Fluss source to discover "
                                    + "the new partitions for partitioned table while scanning."
                                    + " A non-positive value disables the partition discovery.");

    public static final ConfigOption<Boolean> SINK_IGNORE_DELETE =
            ConfigOptions.key("sink.ignore-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore retract（-U/-D) record.");

    // --------------------------------------------------------------------------------------------
    // table storage specific options
    // --------------------------------------------------------------------------------------------

    public static final List<ConfigOption<?>> TABLE_OPTIONS =
            FlinkConversions.toFlinkOptions(FlussConfigUtils.TABLE_OPTIONS.values());

    // --------------------------------------------------------------------------------------------
    // client specific options
    // --------------------------------------------------------------------------------------------

    public static final List<ConfigOption<?>> CLIENT_OPTIONS =
            FlinkConversions.toFlinkOptions(FlussConfigUtils.CLIENT_OPTIONS.values());

    // ------------------------------------------------------------------------------------------

    /** Startup mode for the fluss scanner, see {@link #SCAN_STARTUP_MODE}. */
    public enum ScanStartupMode implements DescribedEnum {
        FULL(
                "full",
                text(
                        "Performs a full snapshot on the table upon first startup, "
                                + "and continue to read the latest changelog with exactly once guarantee. "
                                + "If the table to read is a log table, the full snapshot means "
                                + "reading from earliest log offset. If the table to read is a primary key table, "
                                + "the full snapshot means reading a latest snapshot which "
                                + "materializes all changes on the table.")),
        EARLIEST("earliest", text("Start reading logs from the earliest offset.")),
        LATEST("latest", text("Start reading logs from the latest offset.")),
        TIMESTAMP("timestamp", text("Start reading logs from user-supplied timestamp."));

        private final String value;
        private final InlineElement description;

        ScanStartupMode(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
