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

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.log.{ArrowScanRecords, LogScannerImpl}
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.predicate.Predicate
import org.apache.fluss.record.ArrowBatchData
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.spark.row.FlussArrowColumnVector
import org.apache.fluss.types.RowType
import org.apache.fluss.utils.IOUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Factory used exclusively by bounded, Arrow-format log scans. */
class FlussAppendColumnarReaderFactory(
    tablePath: TablePath,
    rowType: RowType,
    projection: Array[Int],
    predicate: Option[Predicate],
    limit: Option[Int],
    conf: Configuration)
  extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("Use the columnar reader for Arrow batch scans")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val split = partition.asInstanceOf[FlussAppendInputPartition]
    var conn: Connection = null
    var table: Table = null
    var scanner: LogScannerImpl = null
    try {
      conn = ConnectionFactory.createConnection(conf)
      table = conn.getTable(tablePath)
      // Batch polling does not support server-side projection. Project the returned vectors instead.
      scanner = table
        .newScan()
        .filter(predicate.orNull)
        .createLogScanner()
        .asInstanceOf[LogScannerImpl]
      val bucket = split.tableBucket
      if (bucket.getPartitionId == null) {
        scanner.subscribe(bucket.getBucket, split.startOffset)
      } else {
        scanner.subscribe(bucket.getPartitionId, bucket.getBucket, split.startOffset)
      }
      new FlussAppendColumnarReader(
        rowType,
        projection,
        split,
        limit,
        () => scanner.pollRecordBatch(conf.get(SparkFlussConf.SCAN_POLL_TIMEOUT)),
        () => IOUtils.closeAll(scanner, table, conn))
    } catch {
      case error: Throwable =>
        try {
          IOUtils.closeAll(scanner, table, conn)
        } catch {
          case cleanup: Throwable => error.addSuppressed(cleanup)
        }
        throw error
    }
  }
}

/**
 * Reads bounded Arrow batches. Polling and cleanup are injected so boundary and ownership behavior
 * can be tested independently of a live cluster. The reader owns every polled batch, including
 * batches that are skipped or never returned to Spark.
 */
class FlussAppendColumnarReader(
    rowType: RowType,
    projection: Array[Int],
    split: FlussAppendInputPartition,
    limit: Option[Int],
    poll: () => ArrowScanRecords,
    closeResources: () => Unit)
  extends PartitionReader[ColumnarBatch] {

  private var records: ArrowScanRecords = ArrowScanRecords.EMPTY
  private var batches: java.util.Iterator[ArrowBatchData] = java.util.Collections.emptyIterator()
  private var currentBatch: ColumnarBatch = _
  private var offset = split.startOffset.max(0L)
  private var numRowsRead = 0L
  private var finished = false
  private var closed = false

  override def get(): ColumnarBatch = currentBatch

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    Array(FlussNumRowsReadTaskMetric(numRowsRead))

  override def next(): Boolean = {
    if (closed) {
      return false
    }
    try {
      if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }
      while (!finished && !limit.exists(numRowsRead >= _)) {
        if (!batches.hasNext) {
          // The poll watermark includes filtered batches, but must only be applied after all
          // returned batches have been consumed, otherwise buffered rows could be lost.
          Option(records.consumedUpToOffset(split.tableBucket)).foreach {
            consumed => offset = offset.max(consumed.longValue())
          }
          records.close()
          records = ArrowScanRecords.EMPTY
          if (offset >= split.stopOffset) {
            finished = true
          } else {
            records = poll()
            batches = records.records(split.tableBucket).iterator()
            if (
              !batches.hasNext &&
              !Option(records.consumedUpToOffset(split.tableBucket)).exists(_.longValue() > offset)
            ) {
              throw new IllegalStateException(
                s"No more data from fluss server, but current offset $offset " +
                  s"not reach the stop offset ${split.stopOffset}")
            }
          }
        } else {
          val batch = batches.next()
          val base = batch.getBaseLogOffset
          val end = base + batch.getRecordCount
          val start = offset.max(base)
          offset = offset.max(end)
          if (base >= split.stopOffset || split.timeRange.exists(_.isAfter(batch.getTimestamp))) {
            finished = true
            batch.close()
          } else if (split.timeRange.exists(!_.contains(batch.getTimestamp))) {
            batch.close()
          } else {
            val remaining = limit.map(_.toLong - numRowsRead).getOrElse(Long.MaxValue)
            val count = (end.min(split.stopOffset) - start).max(0L).min(remaining).toInt
            if (count > 0) {
              currentBatch = FlussArrowColumnVector.toBatch(
                batch,
                rowType,
                projection,
                (start - base).toInt,
                count)
              numRowsRead += count
              return true
            }
            batch.close()
          }
        }
      }
      close()
      false
    } catch {
      case error: Throwable =>
        try {
          close()
        } catch {
          case cleanup: Throwable => error.addSuppressed(cleanup)
        }
        throw error
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      val clientResources = new AutoCloseable {
        override def close(): Unit = closeResources()
      }
      try {
        IOUtils.closeAll(currentBatch, records, clientResources)
      } finally {
        currentBatch = null
        records = ArrowScanRecords.EMPTY
        batches = java.util.Collections.emptyIterator()
      }
    }
  }
}
