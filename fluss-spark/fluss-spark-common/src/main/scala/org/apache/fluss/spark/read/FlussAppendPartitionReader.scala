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

import org.apache.fluss.client.table.scanner.ScanRecord
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableBucket, TablePath}

/** Partition reader that reads log data from a single Fluss table bucket. */
class FlussAppendPartitionReader(
    tablePath: TablePath,
    projection: Array[Int],
    flussPartition: FlussAppendInputPartition,
    flussConfig: Configuration)
  extends FlussPartitionReader(tablePath, flussConfig) {

  private val tableBucket: TableBucket = flussPartition.tableBucket
  private val partitionId = tableBucket.getPartitionId
  private val bucketId = tableBucket.getBucket
  private val logScanner = table.newScan().project(projection).createLogScanner()

  // Iterator for current batch of records
  private var currentRecords: java.util.Iterator[ScanRecord] = java.util.Collections.emptyIterator()

  // The latest offset of fluss is -2
  private var currentOffset: Long = flussPartition.startOffset.max(0L)

  // initialize log scanner
  initialize()

  private def pollMoreRecords(): Unit = {
    val scanRecords = logScanner.poll(POLL_TIMEOUT)
    if ((scanRecords == null || scanRecords.isEmpty) && currentOffset < flussPartition.stopOffset) {
      throw new IllegalStateException(s"No more data from fluss server," +
        s" but current offset $currentOffset not reach the stop offset ${flussPartition.stopOffset}")
    }
    currentRecords = scanRecords.records(tableBucket).iterator()
  }

  override def next(): Boolean = {
    if (closed || currentOffset >= flussPartition.stopOffset) {
      return false
    }

    if (!currentRecords.hasNext) {
      pollMoreRecords()
    }

    // If we have records in current batch, return next one
    if (currentRecords.hasNext) {
      val scanRecord = currentRecords.next()
      currentRow = convertToSparkRow(scanRecord)
      currentOffset = scanRecord.logOffset() + 1
      true
    } else if (currentOffset < flussPartition.stopOffset) {
      throw new IllegalStateException(s"No more data from fluss server," +
        s" but current offset $currentOffset not reach the stop offset ${flussPartition.stopOffset}")
    } else {
      false
    }
  }

  override def close0(): Unit = {
    if (logScanner != null) {
      logScanner.close()
    }
  }

  private def initialize(): Unit = {
    if (flussPartition.startOffset >= flussPartition.stopOffset) {
      throw new IllegalArgumentException(s"Invalid offset range $flussPartition")
    }
    logInfo(s"Prepare read table $tablePath partition $partitionId bucket $bucketId" +
      s" with start offset ${flussPartition.startOffset} stop offset ${flussPartition.stopOffset}")
    if (partitionId != null) {
      logScanner.subscribe(partitionId, bucketId, flussPartition.startOffset)
    } else {
      logScanner.subscribe(bucketId, flussPartition.startOffset)
    }
    pollMoreRecords()
  }
}
