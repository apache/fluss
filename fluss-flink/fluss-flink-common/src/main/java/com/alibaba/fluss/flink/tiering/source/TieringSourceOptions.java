/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.config.ConfigOption;

import java.time.Duration;

import static com.alibaba.fluss.config.ConfigBuilder.key;

/** Configuration options for the {@link TieringSource}. */
public class TieringSourceOptions {

    public static final String DATA_LAKE_CONFIG_PREFIX = "datalake.";

    public static final ConfigOption<Duration> POLL_TIERING_TABLE_INTERVAL =
            key("tiering.poll.table.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The fixed interval to request tiering table from Fluss cluster, by default 30 seconds.");
}
