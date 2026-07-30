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

package org.apache.fluss.spark.tiering

import org.apache.fluss.config.{ConfigBuilder, ConfigOption}

import java.time.Duration

/** Configuration options for the Spark tiering service. */
object SparkTieringOptions {

  val POLL_TIERING_TABLE_INTERVAL: ConfigOption[Duration] =
    ConfigBuilder
      .key("spark.tiering.poll.table.interval")
      .durationType()
      .defaultValue(Duration.ofSeconds(30))
      .withDescription(
        "The fixed interval to request tiering table from Fluss cluster, by default 30 seconds.")

  val HEARTBEAT_INTERVAL: ConfigOption[Duration] =
    ConfigBuilder
      .key("spark.tiering.heartbeat.interval")
      .durationType()
      .defaultValue(Duration.ofSeconds(10))
      .withDescription(
        "The interval for background heartbeat during RDD execution, by default 10 seconds.")

  val POLL_TIMEOUT: ConfigOption[Duration] =
    ConfigBuilder
      .key("spark.tiering.poll.timeout")
      .durationType()
      .defaultValue(Duration.ofSeconds(10))
      .withDescription(
        "The timeout for polling log records from Fluss on executors, by default 10 seconds.")

  val MAX_HEARTBEAT_FAILURES: ConfigOption[Integer] =
    ConfigBuilder
      .key("spark.tiering.heartbeat.max.failures")
      .intType()
      .defaultValue(10)
      .withDescription(
        "Maximum number of consecutive heartbeat failures before the Spark job fails.")
}
