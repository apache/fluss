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

package org.apache.fluss.spark.row

import org.apache.fluss.compression.ArrowCompressionInfo
import org.apache.fluss.memory.{ManagedPagedOutputView, TestingMemorySegmentPool}
import org.apache.fluss.record.{ArrowBatchData, ChangeType, DefaultLogRecordBatch, LogRecordBatch, LogRecordReadContext, MemoryLogRecords, MemoryLogRecordsArrowBuilder, TestingSchemaGetter}
import org.apache.fluss.row.InternalRow
import org.apache.fluss.row.arrow.ArrowWriterPool
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator
import org.apache.fluss.types.RowType

/** Real encoded Arrow batches with allocator checks on cleanup. */
class ArrowBatchTestUtils(rowType: RowType) extends AutoCloseable {
  private val allocator = new RootAllocator(Long.MaxValue)
  private val writers = new ArrowWriterPool(allocator)
  private val context =
    LogRecordReadContext.createArrowReadContext(rowType, 1, new TestingSchemaGetter(1, rowType))
  private val batches = scala.collection.mutable.ArrayBuffer.empty[ArrowBatchData]

  def batch(offset: Long, timestamp: Long, rows: InternalRow*): ArrowBatchData = {
    val writer =
      writers.getOrCreateWriter(1L, 1, 1024 * 1024, rowType, ArrowCompressionInfo.NO_COMPRESSION)
    val builder = MemoryLogRecordsArrowBuilder.builder(
      offset,
      LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
      1,
      writer,
      new ManagedPagedOutputView(new TestingMemorySegmentPool(32768)),
      true)
    rows.foreach(row => builder.append(ChangeType.APPEND_ONLY, row))
    builder.close()
    val record = MemoryLogRecords
      .pointToBytesView(builder.build())
      .batches()
      .iterator()
      .next()
      .asInstanceOf[DefaultLogRecordBatch]
    record.setCommitTimestamp(timestamp)
    val result = record.loadArrowBatch(context)
    batches += result
    result
  }

  override def close(): Unit = {
    batches.foreach(_.close())
    context.close()
    writers.close()
    allocator.close()
  }
}
