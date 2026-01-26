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

import org.apache.fluss.client.table.scanner.{ScanRecord, SortMergeReader}
import org.apache.fluss.client.table.scanner.batch.BatchScanner
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableBucket, TablePath}
import org.apache.fluss.record.LogRecord
import org.apache.fluss.row.{encode, InternalRow, KeyValueRow}
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.spark.utils.LogChangesIterator
import org.apache.fluss.utils.CloseableIterator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Comparator

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Partition reader that reads primary key table data.
 *
 * Reads both snapshot and log data, merging them using sort-merge algorithm. For rows with the same
 * primary key, log data takes precedence over snapshot data.
 */
class FlussUpsertPartitionReader(
    tablePath: TablePath,
    projection: Array[Int],
    flussPartition: FlussUpsertInputPartition,
    flussConfig: Configuration)
  extends FlussPartitionReader(tablePath, flussConfig)
  with Logging {

  private val readOptimized = flussConfig.get(SparkFlussConf.READ_OPTIMIZED_OPTION)
  private val tableBucket: TableBucket = flussPartition.tableBucket
  private val snapshotId: Long = flussPartition.snapshotId
  private val logStartingOffset: Long = flussPartition.logStartingOffset
  private val logStoppingOffset: Long = flussPartition.logStoppingOffset
  private val logScanFinished = logStartingOffset >= logStoppingOffset || logStoppingOffset <= 0

  private val (projectionWithPks, pkProjection) = {
    val pkIndexes = tableInfo.getSchema.getPrimaryKeyIndexes
    var i = 0
    val _pkProjection = mutable.ArrayBuffer.empty[Int]
    val extraProjections = mutable.ArrayBuffer.empty[Int]
    extraProjections ++= projection
    pkIndexes.foreach {
      pkIndex =>
        projection.find(p => p == pkIndex) match {
          case Some(index) =>
            _pkProjection += index
          case _ =>
            extraProjections += pkIndex
            _pkProjection += projection.length + i
            i += 1
        }
    }
    (extraProjections.toArray, _pkProjection.toArray)
  }

  private var snapshotScanner: BatchScanner = _
  private var logScanner: LogScanner = _
  private var mergedIterator: Iterator[InternalRow] = _

  // initialize scanners
  initialize()

  override def next(): Boolean = {
    if (closed) {
      return false
    }

    if (mergedIterator.hasNext) {
      currentRow = convertToSparkRow(mergedIterator.next())
      true
    } else {
      false
    }
  }

  private def createSortMergeReader(): SortMergeReader = {
    // Create key encoder for primary keys
    val keyEncoder = encode.KeyEncoder.of(rowType, tableInfo.getPhysicalPrimaryKeys, null)

    // Create comparators based on primary key
    val comparator = new Comparator[InternalRow] {
      override def compare(o1: InternalRow, o2: InternalRow): Int = {
        val key1 = keyEncoder.encodeKey(o1)
        val key2 = keyEncoder.encodeKey(o2)
        java.util.Arrays.compareUnsigned(key1, key2)
      }
    }

    def createLogChangesIterator(): LogChangesIterator = {
      // Initialize the log scanner
      logScanner = table.newScan().project(projectionWithPks).createLogScanner()
      if (tableBucket.getPartitionId == null) {
        logScanner.subscribe(tableBucket.getBucket, logStartingOffset)
      } else {
        logScanner.subscribe(tableBucket.getPartitionId, tableBucket.getBucket, logStartingOffset)
      }

      // Collect all log records until logStoppingOffset
      val allLogRecords = mutable.ArrayBuffer[ScanRecord]()
      var continue = true

      while (continue) {
        val records = logScanner.poll(POLL_TIMEOUT)
        if (!records.isEmpty) {
          val flatRecords = records.asScala
          for (scanRecord <- flatRecords) {
            // Maybe data with logStoppingOffset doesn't exist.
            if (scanRecord.logOffset() < logStoppingOffset - 1) {
              allLogRecords += scanRecord
            } else if (scanRecord.logOffset() == logStoppingOffset - 1) {
              allLogRecords += scanRecord
              continue = false
            } else {
              continue = false // Stop if we reach the stopping offset
            }
          }
        }
      }

      LogChangesIterator(allLogRecords.toArray, pkProjection, comparator)
    }

    def createSnapshotIterator(): CloseableIterator[LogRecord] = {
      // Initialize the snapshot scanner
      snapshotScanner =
        table.newScan().project(projectionWithPks).createBatchScanner(tableBucket, snapshotId)

      // Convert snapshot iterator to LogRecord iterator for SortMergeReader
      new CloseableIterator[LogRecord] {
        private var currentBatch: java.util.Iterator[InternalRow] = _
        private var hasMoreBatches = true

        override def hasNext: Boolean = {
          while ((currentBatch == null || !currentBatch.hasNext) && hasMoreBatches) {
            val batch = snapshotScanner.pollBatch(POLL_TIMEOUT)
            if (batch == null) {
              hasMoreBatches = false
            } else {
              currentBatch = batch
            }
          }
          currentBatch != null && currentBatch.hasNext
        }

        override def next(): LogRecord = {
          // Convert InternalRow to LogRecord using GenericRecord
          new ScanRecord(currentBatch.next())
        }

        override def close(): Unit = {
          snapshotScanner.close()
        }
      }

    }

    // Get log iterator with proper filtering and sorting
    val logIterator = if (logScanFinished || readOptimized) {
      CloseableIterator.emptyIterator[KeyValueRow]()
    } else {
      createLogChangesIterator()
    }

    val snapshotIterators = if (snapshotId == -1) {
      null
    } else {
      createSnapshotIterator()
    }

    // Create the SortMergeReader
    val sortMergeReader = new SortMergeReader(
      projectionWithPks,
      pkProjection,
      snapshotIterators,
      comparator,
      logIterator
    )
    sortMergeReader
  }

  private def initialize(): Unit = {
    val currentTs = System.currentTimeMillis()

    val sortMergeReader = createSortMergeReader()

    // Get the merged result iterator
    val mergedResult = sortMergeReader.readBatch()

    // If merged result is null, return an empty iterator
    mergedIterator = if (mergedResult == null) {
      Iterator.empty
    } else {
      mergedResult.asScala
    }
    val spend = (System.currentTimeMillis() - currentTs) / 1000
    logInfo(s"Initialize FlussUpsertPartitionReader cost $spend(s)")
  }

  override def close0(): Unit = {
    if (mergedIterator != null) {
      mergedIterator match {
        case closeable: AutoCloseable => closeable.close()
        case _ => // Do nothing
      }
    }

    if (logScanner != null) {
      logScanner.close()
    }
    if (snapshotScanner != null) {
      snapshotScanner.close()
    }
  }
}
