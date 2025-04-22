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

package com.alibaba.fluss.spark

import com.alibaba.fluss.config.Configuration
import com.alibaba.fluss.metadata.TableInfo
import com.alibaba.fluss.spark.SparkConnectorOptions.ScanStartupMode
import com.alibaba.fluss.spark.initializer.OffsetsInitializer
import com.alibaba.fluss.spark.utils.{SparkConversions, SparkTypeUtils}
import com.alibaba.fluss.types.RowType

import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import javax.annotation.Nullable

import java.time.ZoneId
import java.util.OptionalLong

class SparkScan(
    val flussConfig: Configuration,
    val tableInfo: TableInfo,
    val options: CaseInsensitiveStringMap,
    requiredSchema: StructType,
    @Nullable projectedFields: Array[Int]
) extends Scan {

  val hasPrimaryKey = tableInfo.hasPrimaryKey()
  val isPartitioned = tableInfo.isPartitioned()
  var tableId = tableInfo.getTableId()
  var bucketCount = tableInfo.getNumBuckets()
  var startingOffsetsInitializer: OffsetsInitializer = _
  override def description(): String = {
    // TODO add filters
    s"fluss(${tableInfo.getTablePath.getTableName})"
  }

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def toBatch(): Batch = null

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    val configuration = SparkConversions.toFlussConfig(options)
    val mode = configuration.get(SparkConnectorOptions.SCAN_STARTUP_MODE)
    mode match {
      case ScanStartupMode.EARLIEST =>
        startingOffsetsInitializer = OffsetsInitializer.earliest

      case ScanStartupMode.LATEST =>
        startingOffsetsInitializer = OffsetsInitializer.latest

      case ScanStartupMode.FULL =>
        startingOffsetsInitializer = OffsetsInitializer.initial

      case ScanStartupMode.TIMESTAMP =>
        val timestamp = configuration.get(SparkConnectorOptions.SCAN_STARTUP_TIMESTAMP)
        startingOffsetsInitializer = OffsetsInitializer.timestamp(
          SparkConversions.parseTimestamp(
            timestamp,
            SparkConnectorOptions.SCAN_STARTUP_TIMESTAMP.key(),
            ZoneId.systemDefault()))

      case _ =>
        throw new IllegalArgumentException(s"Unsupported startup mode: $mode")
    }
    new FlussMicroBatchStream(
      flussConfig,
      tableInfo.getTablePath,
      options,
      checkpointLocation,
      startingOffsetsInitializer,
      isPartitioned,
      hasPrimaryKey,
      bucketCount,
      tableId,
      SparkTypeUtils.toFlussType(requiredSchema.asInstanceOf[DataType]).asInstanceOf[RowType],
      projectedFields
    )
  }

}
