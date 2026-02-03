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

package org.apache.fluss.spark.read

import org.apache.fluss.client.initializer.OffsetsInitializer
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.spark.{SparkConnectorOptions, SparkConversions}

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** An interface that extends from Spark [[Scan]]. */
trait FlussScan extends Scan {
  def tableInfo: TableInfo

  def requiredSchema: Option[StructType]

  override def readSchema(): StructType = {
    requiredSchema.getOrElse(SparkConversions.toSparkDataType(tableInfo.getRowType))
  }
}

object FlussScan {
  def startOffsetsInitializer(options: CaseInsensitiveStringMap): OffsetsInitializer = {
    SparkConnectorOptions.StartUpMode.withName(
      options.getOrDefault(
        SparkConnectorOptions.SCAN_START_UP_MODE.key(),
        SparkConnectorOptions.SCAN_START_UP_MODE.defaultValue()).toUpperCase) match {
      case SparkConnectorOptions.StartUpMode.EARLIEST => OffsetsInitializer.earliest()
      case SparkConnectorOptions.StartUpMode.FULL => OffsetsInitializer.full()
      case SparkConnectorOptions.StartUpMode.LATEST => OffsetsInitializer.latest()
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported scan start up mode: ${options.get(SparkConnectorOptions.SCAN_START_UP_MODE.key())}")
    }
  }

  def stoppingOffsetsInitializer(
      isBatch: Boolean,
      options: CaseInsensitiveStringMap): OffsetsInitializer = {
    if (isBatch) {
      OffsetsInitializer.latest()
    } else {
      throw new UnsupportedOperationException("Stream read is not supported yet.")
    }
  }
}

/** Fluss Append Scan. */
case class FlussAppendScan(
    tablePath: TablePath,
    tableInfo: TableInfo,
    requiredSchema: Option[StructType],
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussScan {

  override def toBatch: Batch = {
    val startOffsetsInitializer = FlussScan.startOffsetsInitializer(options)
    val stoppingOffsetsInitializer = FlussScan.stoppingOffsetsInitializer(true, options)
    new FlussAppendBatch(
      tablePath,
      tableInfo,
      readSchema,
      startOffsetsInitializer,
      stoppingOffsetsInitializer,
      options,
      flussConfig)
  }
}

/** Fluss Upsert Scan. */
case class FlussUpsertScan(
    tablePath: TablePath,
    tableInfo: TableInfo,
    requiredSchema: Option[StructType],
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussScan {

  override def toBatch: Batch = {
    val startOffsetsInitializer = FlussScan.startOffsetsInitializer(options)
    val stoppingOffsetsInitializer = FlussScan.stoppingOffsetsInitializer(true, options)
    if (startOffsetsInitializer != OffsetsInitializer.latest()) {
      throw new UnsupportedOperationException("Upsert scan only support FULL startup mode.")
    }
    new FlussUpsertBatch(
      tablePath,
      tableInfo,
      readSchema,
      startOffsetsInitializer,
      stoppingOffsetsInitializer,
      options,
      flussConfig)
  }
}
