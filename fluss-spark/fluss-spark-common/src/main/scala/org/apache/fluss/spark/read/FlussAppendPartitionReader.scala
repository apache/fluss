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
import org.apache.fluss.client.table.scanner.log.ScanRecords
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
  private var currentRecords: java.util.Iterator[ScanRecord] = _

  // initialize log scanner
  initialize()

  override def next(): Boolean = {
    if (closed) {
      return false
    }

    // If we have records in current batch, return next one
    if (currentRecords != null && currentRecords.hasNext) {
      val scanRecord = currentRecords.next()
      currentRow = convertToSparkRow(scanRecord)
      return true
    }

    // Poll for more records
    val scanRecords = logScanner.poll(POLL_TIMEOUT)

    if (scanRecords == null || scanRecords.isEmpty) {
      return false
    }

    // Get records for our bucket
    val bucketRecords = scanRecords.records(tableBucket)
    if (bucketRecords.isEmpty) {
      return false
    }

    currentRecords = bucketRecords.iterator()
    if (currentRecords.hasNext) {
      val scanRecord = currentRecords.next()
      currentRow = convertToSparkRow(scanRecord)
      true
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
    if (partitionId != null) {
      logScanner.subscribeFromBeginning(partitionId, bucketId)
    } else {
      logScanner.subscribeFromBeginning(bucketId)
    }
  }
}
