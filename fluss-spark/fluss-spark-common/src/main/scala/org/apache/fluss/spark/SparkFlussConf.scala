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

package org.apache.fluss.spark

import org.apache.fluss.config.{ConfigBuilder, ConfigOption}
import org.apache.fluss.config.ConfigBuilder.key

import java.time.Duration

object SparkFlussConf {

  val SPARK_FLUSS_CONF_PREFIX = "spark.sql.fluss."

  val READ_OPTIMIZED_OPTION: ConfigOption[java.lang.Boolean] =
    key("read.optimized")
      .booleanType()
      .defaultValue(false)
      .withDescription(
        "If true, Spark will only read data from data lake snapshot or kv snapshot, not execute merge them with log changes. This is a temporary configuration that will be deprecated when read-optimized table(e.g. `mytbl$ro`) is supported.")

  object StartUpMode extends Enumeration {
    val FULL, EARLIEST, LATEST, TIMESTAMP = Value
  }

  val SCAN_START_UP_MODE: ConfigOption[String] =
    ConfigBuilder
      .key("scan.startup.mode")
      .stringType()
      .defaultValue(StartUpMode.FULL.toString)
      .withDescription("The start up mode when read Fluss table.")

  val SCAN_POLL_TIMEOUT: ConfigOption[Duration] =
    ConfigBuilder
      .key("scan.poll.timeout")
      .durationType()
      .defaultValue(Duration.ofMillis(10000L))
      .withDescription("The timeout for log scanner to poll records.")

  val MAX_OFFSETS_PER_TRIGGER: ConfigOption[java.lang.Long] =
    ConfigBuilder
      .key("scan.max.offsets.per.trigger")
      .longType()
      .noDefaultValue()
      .withDescription(
        "Maximum number of offsets processed per trigger interval, " +
          "proportionally split across table buckets.")

  val MIN_OFFSETS_PER_TRIGGER: ConfigOption[java.lang.Long] =
    ConfigBuilder
      .key("scan.min.offsets.per.trigger")
      .longType()
      .noDefaultValue()
      .withDescription(
        "Minimum number of offsets per trigger to ensure progress during data spikes. " +
          "Requires scan.max.offsets.per.trigger to be set.")

  val MAX_TRIGGER_DELAY: ConfigOption[Duration] =
    ConfigBuilder
      .key("scan.max.trigger.delay")
      .durationType()
      .defaultValue(Duration.ofMinutes(5))
      .withDescription(
        "Maximum time a trigger can be delayed waiting for scan.min.offsets.per.trigger " +
          "to be satisfied.")
}
