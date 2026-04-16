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

package org.apache.fluss.spark.read.lake

import org.apache.fluss.client.table.scanner.batch.LakeSnapshotAndLogSplitScanner
import org.apache.fluss.config.Configuration
import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.row.InternalRow
import org.apache.fluss.spark.read.FlussPartitionReader
import org.apache.fluss.types.RowType

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

/**
 * Partition reader that reads lake-enabled primary key table data.
 *
 * Reads lake snapshot and Fluss log tail, merging them using sort-merge algorithm. For rows with
 * the same primary key, log data takes precedence over lake snapshot data.
 */
class FlussLakeUpsertPartitionReader(
    tablePath: TablePath,
    lakeSource: LakeSource[LakeSplit],
    projection: Array[Int],
    flussPartition: FlussLakeUpsertInputPartition,
    flussConfig: Configuration)
  extends FlussPartitionReader(tablePath, flussConfig)
  with Logging {

  private val lakeSplits = if (flussPartition.lakeSplitBytes != null) {
    FlussLakeUtils.deserializeLakeSplits(
      flussPartition.lakeSplitBytes,
      lakeSource.getSplitSerializer)
  } else {
    null
  }
  private val flussLakeSnapshotAndLogSplitScanner = new LakeSnapshotAndLogSplitScanner(
    table,
    lakeSource,
    lakeSplits,
    flussPartition.tableBucket,
    flussPartition.logStartingOffset,
    flussPartition.logStoppingOffset,
    projection)
  private var mergedIterator: Iterator[InternalRow] = Iterator.empty
  private var scanFinished = false

  initialize()

  override lazy val projectedRowType: RowType = rowType.project(projection)

  override def next(): Boolean = {
    if (closed || scanFinished) {
      return false
    }

    // Loop pollBatch until fetch valid records or scan is complete
    while (true) {
      if (mergedIterator.hasNext) {
        currentRow = convertToSparkRow(mergedIterator.next())
        return true
      }

      val flussRecords = flussLakeSnapshotAndLogSplitScanner.pollBatch(POLL_TIMEOUT)
      if (flussRecords == null) {
        scanFinished = true
        return false
      }
      mergedIterator = flussRecords.asScala
    }
    false
  }

  private def initialize(): Unit = {
    val currentTs = System.currentTimeMillis()
    logInfo(s"Prepare read lake-enabled pk table $tablePath $flussPartition")

    // Loop pollBatch until fetch valid records or scan is complete
    var flussRecords = flussLakeSnapshotAndLogSplitScanner.pollBatch(POLL_TIMEOUT)
    while (flussRecords != null && !flussRecords.hasNext) {
      flussRecords = flussLakeSnapshotAndLogSplitScanner.pollBatch(POLL_TIMEOUT)
    }
    mergedIterator = if (flussRecords == null) {
      scanFinished = true
      Iterator.empty
    } else {
      flussRecords.asScala
    }
    val spend = (System.currentTimeMillis() - currentTs) / 1000
    logInfo(s"Initialize FlussLakeUpsertPartitionReader cost $spend(s)")
  }

  override def close0(): Unit = {
    if (mergedIterator != null) {
      mergedIterator match {
        case closeable: AutoCloseable => closeable.close()
        case _ => // Do nothing
      }
    }
    if (flussLakeSnapshotAndLogSplitScanner != null) {
      flussLakeSnapshotAndLogSplitScanner.close()
    }
  }
}
