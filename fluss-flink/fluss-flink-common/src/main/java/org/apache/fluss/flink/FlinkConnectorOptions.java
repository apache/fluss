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

package org.apache.fluss.flink;

import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.utils.FlinkConversions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.IntervalFreshness;

import java.time.Duration;
import java.util.Arrays;
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
                                    + "Data will be distributed to each bucket according to the hash value of bucket-key (It must be a subset of the primary keys excluding partition keys of the primary key table). "
                                    + "If you specify multiple fields, delimiter is ','. "
                                    + "If the table has a primary key and a bucket key is not specified, the bucket key will be used as primary key(excluding the partition key). "
                                    + "If the table has no primary key and the bucket key is not specified, "
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
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The time interval for the Fluss source to discover "
                                    + "the new partitions for partitioned table while scanning."
                                    + " A non-positive value disables the partition discovery. The default value is 1 "
                                    + "minute. Currently, since Fluss Admin#listPartitions(TablePath tablePath) requires a large "
                                    + "number of requests to ZooKeeper in server, this option cannot be set too small, "
                                    + "as a small value would cause frequent requests and increase server load. In the future, "
                                    + "once list partitions is optimized, the default value of this parameter can be reduced.");

    public static final ConfigOption<Boolean> SINK_IGNORE_DELETE =
            ConfigOptions.key("sink.ignore-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore retract（-U/-D) record.");

    public static final ConfigOption<Boolean> SINK_BUCKET_SHUFFLE =
            ConfigOptions.key("sink.bucket-shuffle")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to shuffle by bucket id before write to sink. Shuffling the data with the same "
                                    + "bucket id to be processed by the same task can improve the efficiency of client "
                                    + "processing and reduce resource consumption. For Log Table, bucket shuffle will "
                                    + "only take effect when the '"
                                    + BUCKET_KEY.key()
                                    + "' is defined. For Primary Key table, it is enabled by default.");

    public static final ConfigOption<DistributionMode> SINK_DISTRIBUTION_MODE =
            ConfigOptions.key("sink.distribution-mode")
                    .enumType(DistributionMode.class)
                    .defaultValue(DistributionMode.BUCKET_SHUFFLE)
                    .withDescription(
                            "Defines the distribution mode for writing data to the sink. Available options are: \n"
                                    + "- NONE: No specific distribution strategy. Data is forwarded as is.\n"
                                    + "- BUCKET_SHUFFLE: Shuffle data by bucket ID before writing to sink. "
                                    + "Shuffling the data with the same bucket ID to be processed by the same task "
                                    + "can improve the efficiency of client processing and reduce resource consumption. "
                                    + "For Log Table, bucket shuffle will only take effect when the '"
                                    + BUCKET_KEY.key()
                                    + "' is defined. For Primary Key table, it is enabled by default.\n"
                                    + "- DYNAMIC_SHUFFLE: Dynamically adjust shuffle strategy based on partition key traffic patterns. "
                                    + "This mode monitors data distribution and adjusts the shuffle behavior to balance the load. "
                                    + "It is only supported for partitioned tables.");

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

    // --------------------------------------------------------------------------------------------
    // modification disallowed connector options
    // --------------------------------------------------------------------------------------------

    public static final List<String> ALTER_DISALLOW_OPTIONS =
            Arrays.asList(BUCKET_NUMBER.key(), BUCKET_KEY.key(), BOOTSTRAP_SERVERS.key());

    // -------------------------------------------------------------------------------------------
    // Only used internally to support materialized table
    // -------------------------------------------------------------------------------------------

    public static final String MATERIALIZED_TABLE_PREFIX = "materialized-table.";

    public static final ConfigOption<String> MATERIALIZED_TABLE_DEFINITION_QUERY =
            ConfigOptions.key("materialized-table.definition-query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The definition query text of materialized table, text is expanded in contrast to the original SQL.");
    public static final ConfigOption<String> MATERIALIZED_TABLE_INTERVAL_FRESHNESS =
            ConfigOptions.key("materialized-table.interval-freshness")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The freshness interval of materialized table which is used to determine the physical refresh mode.");
    public static final ConfigOption<IntervalFreshness.TimeUnit>
            MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT =
                    ConfigOptions.key("materialized-table.interval-freshness.time-unit")
                            .enumType(IntervalFreshness.TimeUnit.class)
                            .noDefaultValue()
                            .withDescription("The time unit of freshness interval.");
    public static final ConfigOption<CatalogMaterializedTable.LogicalRefreshMode>
            MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE =
                    ConfigOptions.key("materialized-table.logical-refresh-mode")
                            .enumType(CatalogMaterializedTable.LogicalRefreshMode.class)
                            .noDefaultValue()
                            .withDescription("The logical refresh mode of materialized table.");
    public static final ConfigOption<CatalogMaterializedTable.RefreshMode>
            MATERIALIZED_TABLE_REFRESH_MODE =
                    ConfigOptions.key("materialized-table.refresh-mode")
                            .enumType(CatalogMaterializedTable.RefreshMode.class)
                            .noDefaultValue()
                            .withDescription("The physical refresh mode of materialized table.");
    public static final ConfigOption<CatalogMaterializedTable.RefreshStatus>
            MATERIALIZED_TABLE_REFRESH_STATUS =
                    ConfigOptions.key("materialized-table.refresh-status")
                            .enumType(CatalogMaterializedTable.RefreshStatus.class)
                            .noDefaultValue()
                            .withDescription("The refresh status of materialized table.");
    public static final ConfigOption<String> MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION =
            ConfigOptions.key("materialized-table.refresh-handler-description")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The summary description of materialized table's refresh handler");
    public static final ConfigOption<String> MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES =
            ConfigOptions.key("materialized-table.refresh-handler-bytes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The serialized base64 bytes of refresh handler of materialized table.");

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
