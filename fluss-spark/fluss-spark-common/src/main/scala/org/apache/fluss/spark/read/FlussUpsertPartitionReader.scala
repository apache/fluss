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

import org.apache.fluss.client.table.scanner.batch.BatchScanner
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableBucket, TablePath}

import java.util

/**
 * Partition reader that reads primary key table data.
 *
 * For now, only data in snapshot can be read, without merging them with changes.
 */
class FlussUpsertPartitionReader(
    tablePath: TablePath,
    projection: Array[Int],
    flussPartition: FlussUpsertInputPartition,
    flussConfig: Configuration)
  extends FlussPartitionReader(tablePath, flussConfig) {

  private val tableBucket: TableBucket = flussPartition.tableBucket
  private val snapshotId: Long = flussPartition.snapshotId

  // KV Snapshot Reader (if snapshot exists)
  private var snapshotScanner: BatchScanner = _
  private var snapshotIterator: util.Iterator[org.apache.fluss.row.InternalRow] = _

  // initialize scanners
  initialize()

  override def next(): Boolean = {
    if (closed) {
      return false
    }

    if (snapshotIterator == null || !snapshotIterator.hasNext) {
      // Try to get next batch from snapshot scanner
      val batch = snapshotScanner.pollBatch(POLL_TIMEOUT)
      if (batch == null) {
        // No more data fetched.
        false
      } else {
        snapshotIterator = batch
        if (snapshotIterator.hasNext) {
          currentRow = convertToSparkRow(snapshotIterator.next())
          true
        } else {
          // Poll a new batch
          next()
        }
      }
    } else {
      // Get data from current snapshot batch
      currentRow = convertToSparkRow(snapshotIterator.next())
      true
    }
  }

  private def initialize(): Unit = {
    // Initialize Scanners
    if (snapshotId >= 0) {
      // Create batch scanner
      snapshotScanner =
        table.newScan().project(projection).createBatchScanner(tableBucket, snapshotId)
    }
  }

  override def close0(): Unit = {
    if (snapshotScanner != null) {
      snapshotScanner.close()
    }
  }
}
