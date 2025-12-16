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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.config.ConfigOption;

import java.time.Duration;

import static org.apache.fluss.config.ConfigBuilder.key;

/** Configuration options for the {@link TieringSource}. */
public class TieringSourceOptions {

    public static final String DATA_LAKE_CONFIG_PREFIX = "datalake.";

    public static final ConfigOption<Duration> POLL_TIERING_TABLE_INTERVAL =
            key("tiering.poll.table.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The fixed interval to request tiering table from Fluss cluster, by default 30 seconds.");

    public static final ConfigOption<Duration> TIERING_TABLE_DURATION_MAX =
            key("tiering.table.duration.max")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The maximum duration for tiering a single table. If tiering a table exceeds this duration, "
                                    + "it will be force completed: the tiering will be finalized and committed to the data lake "
                                    + "(e.g., Paimon) immediately, even if they haven't reached their desired stopping offsets.");

    public static final ConfigOption<Duration> TIERING_TABLE_DURATION_DETECT_INTERVAL =
            key("tiering.table.duration.detect-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The interval to check if a table tiering operation has reached the maximum duration. "
                                    + "The enumerator will periodically check tiering tables and force complete those that exceed the maximum duration.");
}
