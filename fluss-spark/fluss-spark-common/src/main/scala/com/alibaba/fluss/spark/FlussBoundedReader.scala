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

import com.alibaba.fluss.client.table.scanner.ScanRecord
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner
import com.alibaba.fluss.row.InternalRow
import com.alibaba.fluss.utils.CloseableIterator

import org.apache.spark.sql

import java.io.IOException
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue

/**
 * A bounded reader to reading Fluss's bounded kv snapshot data into [[ScanRecordBatch]]s.
 *
 * It wraps a [[BatchScanner]] to read data, skips the toSkip records while reading and produce
 * [[ScanRecordBatch]] with the current reading records count.
 *
 * In method readBatch, it'll first skip the toSkip records, and then return the
 * [[ScanRecordBatch]]s.
 */
private class FlussBoundedReader(splitScanner: BatchScanner, toSkip: Long) extends AutoCloseable {

  private val POLL_TIMEOUT = Duration.ofMillis(10000L)
  private var currentReadRecordsCount = 0L
  private var _toSkip = toSkip

  private val recordBatchPool =
    new ArrayBlockingQueue[RecordBatch](1)
  recordBatchPool.add(new RecordBatch())

  def readBatch(): CloseableIterator[ScanRecord] = {
    val recordBatch = pollRecordAndPosBatch()

    if (recordBatch == null) CloseableIterator.emptyIterator()
    else {
      val nextBatch = poll()

      if (nextBatch == null) {
        recordBatchPool.add(recordBatch)
        null
      } else recordBatch.replace(nextBatch)
    }
  }

  @throws(classOf[IOException])
  private def pollRecordAndPosBatch() = try
    recordBatchPool.poll(POLL_TIMEOUT.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
  catch {
    case e: InterruptedException =>
      Thread.currentThread.interrupt()
      throw new IOException("Interrupted")
  }

  @throws(classOf[IOException])
  private def poll() = {
    var nextBatch: CloseableIterator[ScanRecord] = null

    while (_toSkip > 0) {
      nextBatch = pollBatch()

      if (nextBatch == null)
        throw new RuntimeException(
          s"Skip more than the number of total records, has skipped $currentReadRecordsCount record(s), but remain ${_toSkip} record(s) to skip."
        )

      while (_toSkip > 0 && nextBatch.hasNext) {
        nextBatch.next()
        _toSkip -= 1
        currentReadRecordsCount += 1
      }
    }

    if (nextBatch != null && nextBatch.hasNext) nextBatch else pollBatch()
  }

  @throws(classOf[IOException])
  private def pollBatch() = {
    val records = splitScanner.pollBatch(POLL_TIMEOUT)

    if (records == null) null else new ScanRecordBatch(records)
  }

  override def close(): Unit = splitScanner.close()

  private class ScanRecordBatch(val rowIterator: CloseableIterator[InternalRow])
    extends CloseableIterator[ScanRecord] {
    override def hasNext: Boolean = rowIterator.hasNext

    override def next() = new ScanRecord(rowIterator.next())

    override def close(): Unit = rowIterator.close()
  }

  private class RecordBatch extends CloseableIterator[ScanRecord] {
    private var records: CloseableIterator[ScanRecord] = _

    def replace(records: CloseableIterator[ScanRecord]): this.type = {
      this.records = records
      this
    }

    override def hasNext: Boolean = records.hasNext

    override def next(): ScanRecord = records.next()

    override def close(): Unit = {
      records.close()
      recordBatchPool.add(this)
    }
  }
}
