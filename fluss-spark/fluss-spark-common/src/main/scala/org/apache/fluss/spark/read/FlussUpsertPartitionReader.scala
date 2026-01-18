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
import org.apache.fluss.client.table.scanner.batch.BatchScanner
import org.apache.fluss.client.table.scanner.log.{LogScanner, ScanRecords}
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableBucket, TablePath}

import java.util

/** Partition reader that reads primary key table data. */
class FlussUpsertPartitionReader(
    tablePath: TablePath,
    projection: Array[Int],
    flussPartition: FlussUpsertInputPartition,
    flussConfig: Configuration)
  extends FlussPartitionReader(tablePath, flussConfig) {

  private val tableBucket: TableBucket = flussPartition.tableBucket
  private val partitionId = tableBucket.getPartitionId
  private val bucketId = tableBucket.getBucket
  private val snapshotId: Long = flussPartition.snapshotId
  private val logStartingOffset: Long = flussPartition.logStartingOffset

  // KV Snapshot Reader (if snapshot exists)
  private var snapshotScanner: BatchScanner = _
  private var snapshotIterator: util.Iterator[org.apache.fluss.row.InternalRow] = _
  private var snapshotFinished = false

  // Log Scanner for incremental data
  private var logScanner: LogScanner = _
  private var logRecords: util.Iterator[ScanRecord] = _

  // initialize scanners
  initialize()

  override def next(): Boolean = {
    if (closed) {
      return false
    }

    // Phase 1: Read snapshot if not finished
    if (!snapshotFinished) {
      if (snapshotIterator == null || !snapshotIterator.hasNext) {
        // Try to get next batch from snapshot scanner
        val batch = snapshotScanner.pollBatch(POLL_TIMEOUT)
        if (batch == null) {
          // Snapshot reading finished
          snapshotFinished = true
          if (snapshotScanner != null) {
            snapshotScanner.close()
            snapshotScanner = null
          }

          // Subscribe to log scanner for incremental data
          subscribeLogScanner()
          return next()
        } else {
          snapshotIterator = batch
          if (snapshotIterator.hasNext) {
            currentRow = convertToSparkRow(snapshotIterator.next())
            return true
          } else {
            return next()
          }
        }
      } else {
        // get data from current snapshot batch
        currentRow = convertToSparkRow(snapshotIterator.next())
        return true
      }
    }

    // Phase 2: Read incremental log
    if (logRecords != null && logRecords.hasNext) {
      val scanRecord = logRecords.next()
      currentRow = convertToSparkRow(scanRecord)
      return true
    }

    // Poll for more log records
    val scanRecords: ScanRecords = logScanner.poll(POLL_TIMEOUT)

    if (scanRecords == null || scanRecords.isEmpty) {
      return false
    }

    // Get records for our bucket
    val bucketRecords = scanRecords.records(tableBucket)
    if (bucketRecords.isEmpty) {
      return false
    }

    logRecords = bucketRecords.iterator()
    if (logRecords.hasNext) {
      val scanRecord = logRecords.next()
      currentRow = convertToSparkRow(scanRecord)
      true
    } else {
      false
    }
  }

  private def initialize(): Unit = {
    // Initialize Scanners
    if (snapshotId >= 0) {
      // Create batch scanner
      snapshotScanner =
        table.newScan().project(projection).createBatchScanner(tableBucket, snapshotId)
    } else {
      snapshotFinished = true
    }

    // Create log scanner
    logScanner = table.newScan().project(projection).createLogScanner()

    if (snapshotFinished) {
      // Subscribe to log scanner immediately
      subscribeLogScanner()
    }
  }

  private def subscribeLogScanner(): Unit = {
    if (partitionId != null) {
      logScanner.subscribe(partitionId, bucketId, logStartingOffset)
    } else {
      logScanner.subscribe(bucketId, logStartingOffset)
    }
  }

  override def close0(): Unit = {
    if (snapshotScanner != null) {
      snapshotScanner.close()
    }
    if (logScanner != null) {
      logScanner.close()
    }
  }
}
