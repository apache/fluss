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

import com.alibaba.fluss.client.{Connection, ConnectionFactory}
import com.alibaba.fluss.client.table.Table
import com.alibaba.fluss.client.table.scanner.ScanRecord
import com.alibaba.fluss.config.Configuration
import com.alibaba.fluss.metadata.{TableBucket, TablePath}
import com.alibaba.fluss.row.{InternalRow => FlussInternalRow}
import com.alibaba.fluss.spark.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer}
import com.alibaba.fluss.types.RowType
import com.alibaba.fluss.utils.CloseableIterator

import org.apache.spark.internal.Logging
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import javax.annotation.Nullable

import java.io.IOException
import java.time.Duration

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.concurrent.TrieMap

case class FlussPartitionReader(
    partition: FlussInputPartition,
    row: SparkInternalRow,
    flussConf: Configuration,
    tablePath: TablePath,
    sourceOutputType: RowType,
    @Nullable projectedFields: Array[Int]
) extends PartitionReader[InternalRow]
  with Logging {
  private val POLL_TIMEOUT = Duration.ofMillis(10000L)
  private val connection: Connection = ConnectionFactory.createConnection(flussConf);
  private val bucketOffsetsRetrieverImpl =
    new BucketOffsetsRetrieverImpl(connection.getAdmin, tablePath)
  private val table: Table = connection.getTable(tablePath)
  private val latestOffsetsInitializer: OffsetsInitializer = OffsetsInitializer.latest()
  private val rangeToRead = resolveRange(partition.split)
  private var boundedSplitReader: FlussBoundedReader = createBoundReader()
  private val flussDataConsumer: FlussDataConsumer = FlussDataConsumer.acquire(
    rangeToRead.tableBucketInfo,
    table.newScan.project(projectedFields).createLogScanner())
  private var recordIterator: CloseableIterator[ScanRecord] = _
  private var nextOffset = rangeToRead.fromOffset

  private var currentRow: FlussInternalRow = _

  override def next(): Boolean = {
    if (boundedSplitReader != null) {
      advanceBatchIfNeeded()
      if (currentRow == null) {
        advanceStreamIfNeeded()
      }
      currentRow != null
    } else {
      advanceStreamIfNeeded()
      currentRow != null
    }
  }
  private def advanceBatchIfNeeded(): Unit = {
    var stop = false
    while (!stop) {
      if (recordIterator == null) {
        recordIterator = boundedSplitReader.readBatch()
      }
      if (recordIterator != null) {
        if (recordIterator.hasNext) {
          currentRow = recordIterator.next().getRow
          stop = true
        } else {
          recordIterator.close()
          recordIterator = null
        }
      } else {
        boundedSplitReader.close()
        boundedSplitReader = null
        currentRow = null
        stop = true
      }
    }

  }
  private def advanceStreamIfNeeded(): Unit = {
    if (nextOffset < rangeToRead.untilOffset) {
      val record = flussDataConsumer.get(nextOffset, rangeToRead.untilOffset, POLL_TIMEOUT.toMillis)
      if (record != null) {
        currentRow = record.getRow
        nextOffset = record.logOffset() + 1
      } else {
        currentRow = null
      }
    } else {
      currentRow = null
    }

  }

  override def get(): InternalRow = {
    if (currentRow != null) {
      row.replace(currentRow)
    } else {
      null
    }
  }

  private def createBoundReader(): FlussBoundedReader = {
    if (rangeToRead.tableBucketInfo.isBatch && !FlussPartitionReader.isInitialized(rangeToRead)) {
      FlussPartitionReader.setInitialized(rangeToRead)
      val scanner =
        table.newScan.createBatchScanner(rangeToRead.tableBucket, rangeToRead.snapshotId)
      new FlussBoundedReader(scanner, 0L)
    } else {
      null
    }
  }
  private def resolveRange(range: FlussOffsetRange): FlussOffsetRange = {
    if (range.untilOffset < 0) {
      var untilOffset = 0L
      while (untilOffset < 0) {
        val bucket = range.tableBucket.getBucket.asInstanceOf[Integer]
        val maybeString = Option(range.partitionName)
        val bucketOffsets = latestOffsetsInitializer.getBucketOffsets(
          maybeString.getOrElse(null),
          List(bucket).asJava,
          bucketOffsetsRetrieverImpl)
        if (!bucketOffsets.isEmpty) {
          untilOffset = bucketOffsets.get(bucket)
        }
      }
      FlussOffsetRange(range.tableBucketInfo, range.fromOffset, untilOffset)
    } else {
      range
    }
  }

  override def close(): Unit = {
    try {
      if (boundedSplitReader != null) {
        boundedSplitReader.close()
      }
      if (flussDataConsumer != null) {
        flussDataConsumer.close()
      }
      table.close()
      connection.close()
    } catch {
      case e: Exception =>
        throw new IOException(e)
    }
  }
}

object FlussPartitionReader {
  @volatile private[this] var initializedMap = TrieMap.empty[(TableBucket, Long), Boolean]

  def isInitialized(rangeToRead: FlussOffsetRange): Boolean = {
    initializedMap.contains((rangeToRead.tableBucket, rangeToRead.snapshotId))
  }

  def setInitialized(rangeToRead: FlussOffsetRange): Unit = {
    initializedMap += ((rangeToRead.tableBucket, rangeToRead.snapshotId) -> true)
  }
}
