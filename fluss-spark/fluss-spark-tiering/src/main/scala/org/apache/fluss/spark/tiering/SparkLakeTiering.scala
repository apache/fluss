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

import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.utils.PropertiesUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Entry point for the Spark-based Fluss Lake Tiering Service.
 *
 * Parses CLI arguments, creates a SparkSession, and starts [[SparkTieringJobRunner]]
 * asynchronously. The main thread blocks until the runner's Future completes (either normally via
 * [[stop()]] or on failure).
 *
 * CLI arguments follow the same convention as the Flink entry point:
 *   - `fluss.*` -> Fluss client config (prefix stripped via `extractAndRemovePrefix`)
 *   - `datalake.format` -> data lake format identifier
 *   - `datalake.{format}.*` -> lake config (prefix stripped via `extractAndRemovePrefix`)
 *   - `lake.tiering.*` -> lake tiering config (prefix retained via `extractPrefix`)
 */
object SparkLakeTiering extends Logging {

  private val FLUSS_CONF_PREFIX = "fluss."
  private val DATA_LAKE_CONFIG_PREFIX = "datalake."
  private val LAKE_TIERING_CONFIG_PREFIX = "lake.tiering."

  def main(args: Array[String]): Unit = {
    val paramsMap = parseArgs(args)

    // Extract fluss config
    val flussConfigMap = PropertiesUtils.extractAndRemovePrefix(paramsMap.asJava, FLUSS_CONF_PREFIX)
    val bootstrapServers = flussConfigMap.get(ConfigOptions.BOOTSTRAP_SERVERS.key())
    if (bootstrapServers == null) {
      throw new IllegalArgumentException(
        s"The bootstrap server to Fluss is not configured, please configure " +
          s"$FLUSS_CONF_PREFIX${ConfigOptions.BOOTSTRAP_SERVERS.key()}")
    }
    val flussConfig = Configuration.fromMap(flussConfigMap)

    // Extract data lake format
    val dataLakeFormat = paramsMap.get("datalake.format").orNull
    if (dataLakeFormat == null) {
      throw new IllegalArgumentException(
        s"${ConfigOptions.DATALAKE_FORMAT.key()} is not configured")
    }

    // Extract lake config
    val lakeConfigMap = PropertiesUtils.extractAndRemovePrefix(
      paramsMap.asJava,
      s"$DATA_LAKE_CONFIG_PREFIX$dataLakeFormat.")
    val lakeConfig = Configuration.fromMap(lakeConfigMap)

    // Extract lake tiering config
    val lakeTieringConfigMap =
      PropertiesUtils.extractPrefix(paramsMap.asJava, LAKE_TIERING_CONFIG_PREFIX)
    val lakeTieringConfig = Configuration.fromMap(lakeTieringConfigMap)

    // Load lake storage plugin and create factory
    logInfo(s"Loading lake storage plugin for format: $dataLakeFormat")

    // Create SparkSession
    val spark = SparkSession
      .builder()
      .appName(s"Fluss Lake Tiering Service - $dataLakeFormat")
      .getOrCreate()

    logInfo(s"Starting Fluss Lake Tiering Service with Spark (format=$dataLakeFormat).")
    logInfo(s"Parsed CLI args: ${paramsMap.map { case (k, v) => s"$k=$v" }.mkString(", ")}")

    try {
      val runner = new SparkTieringJobRunner(
        spark,
        flussConfig,
        lakeTieringConfig,
        dataLakeFormat,
        lakeConfig
      )
      val future = runner.startAsync()
      Await.result(future, Duration.Inf)
    } finally {
      spark.stop()
    }
  }

  /**
   * Parses command line arguments in the format `--key value` or `--key=value` into a map of
   * key-value pairs.
   */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    var result = Map.empty[String, String]
    var i = 0
    while (i < args.length) {
      val arg = args(i)
      if (arg.startsWith("--")) {
        val keyValue = arg.substring(2)
        val eqIdx = keyValue.indexOf('=')
        if (eqIdx >= 0) {
          result = result + (keyValue.substring(0, eqIdx) -> keyValue.substring(eqIdx + 1))
        } else if (i + 1 < args.length) {
          result = result + (keyValue -> args(i + 1))
          i += 1
        }
      }
      i += 1
    }
    result
  }
}
