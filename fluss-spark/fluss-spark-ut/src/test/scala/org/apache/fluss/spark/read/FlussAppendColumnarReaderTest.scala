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

import org.apache.fluss.client.table.scanner.log.ArrowScanRecords
import org.apache.fluss.metadata.TableBucket
import org.apache.fluss.record.ArrowBatchData
import org.apache.fluss.row.GenericRow
import org.apache.fluss.spark.row.ArrowBatchTestUtils
import org.apache.fluss.types.{DataTypes, RowType}

import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

import scala.collection.JavaConverters._

/** Boundary, progress, and ownership tests using real Arrow memory. */
class FlussAppendColumnarReaderTest extends AnyFunSuite {
  private val rowType = RowType.of(DataTypes.INT())
  private val bucket = new TableBucket(1L, 0)

  private def records(watermark: Long, batches: ArrowBatchData*): ArrowScanRecords =
    new ArrowScanRecords(
      Collections.singletonMap(bucket, batches.toList.asJava),
      Collections.singletonMap(bucket, Long.box(watermark)))

  private def withBatches(testBody: ArrowBatchTestUtils => Unit): Unit = {
    val data = new ArrowBatchTestUtils(rowType)
    try testBody(data)
    finally data.close()
  }

  private def values(
      data: ArrowBatchTestUtils,
      offset: Long,
      timestamp: Long,
      count: Int): ArrowBatchData =
    data.batch(
      offset,
      timestamp,
      (0 until count).map(i => GenericRow.of(Int.box((offset + i).toInt))): _*)

  test("batch-internal start and stop offsets, buffered batches, and metrics") {
    withBatches {
      data =>
        val first = values(data, 0, 10, 4)
        val second = values(data, 4, 20, 4)
        val memory = first.getVectorSchemaRoot.getVector(0).getAllocator
        var polls = 0
        var closes = 0
        val reader = new FlussAppendColumnarReader(
          rowType,
          Array(0),
          FlussAppendInputPartition(bucket, 2, 7),
          None,
          () => { polls += 1; records(8, first, second) },
          () => closes += 1)
        try {
          assertThat(reader.next()).isTrue
          assertThat(reader.get().numRows()).isEqualTo(2)
          assertThat(reader.get().column(0).getInt(0)).isEqualTo(2)
          assertThat(reader.get().column(0).getInt(1)).isEqualTo(3)
          assertThat(reader.currentMetricsValues()(0).value()).isEqualTo(2L)
          assertThat(reader.next()).isTrue
          assertThat(reader.get().numRows()).isEqualTo(3)
          assertThat(reader.get().column(0).getInt(2)).isEqualTo(6)
          assertThat(reader.next()).isFalse
          assertThat(reader.currentMetricsValues()(0).value()).isEqualTo(5L)
          assertThat(polls).isEqualTo(1)
          assertThat(memory.getAllocatedMemory).isZero
          reader.close()
          assertThat(closes).isEqualTo(1)
          assertThat(reader.next()).isFalse
        } finally reader.close()
    }
  }

  test("filtered polls advance without rows and filtered tail ends the scan") {
    withBatches {
      data =>
        val batch = values(data, 4, 10, 2)
        val responses = Iterator(records(4), records(10, batch))
        val reader = new FlussAppendColumnarReader(
          rowType,
          Array(0),
          FlussAppendInputPartition(bucket, 0, 10),
          None,
          () => responses.next(),
          () => ())
        try {
          assertThat(reader.next()).isTrue
          assertThat(reader.get().numRows()).isEqualTo(2)
          assertThat(reader.get().column(0).getInt(0)).isEqualTo(4)
          assertThat(reader.next()).isFalse
          assertThat(responses.hasNext).isFalse
        } finally reader.close()
    }
  }

  test("all filtered and empty ranges finish without producing batches") {
    for (stop <- Seq(0L, 10L)) {
      var polls = 0
      val reader = new FlussAppendColumnarReader(
        rowType,
        Array(0),
        FlussAppendInputPartition(bucket, 0, stop),
        None,
        () => { polls += 1; records(10) },
        () => ())
      try {
        assertThat(reader.next()).isFalse
        assertThat(polls).isEqualTo(if (stop == 0) 0 else 1)
      } finally reader.close()
    }
  }

  test("time window includes start, excludes end, and releases skipped batches") {
    withBatches {
      data =>
        val batches = Seq(values(data, 0, 9, 2), values(data, 2, 10, 2), values(data, 4, 20, 2))
        val memory = batches.head.getVectorSchemaRoot.getVector(0).getAllocator
        val reader = new FlussAppendColumnarReader(
          rowType,
          Array(0),
          FlussAppendInputPartition(bucket, 0, 6, Some(FlussTimeRange(10, 20))),
          None,
          () => records(6, batches: _*),
          () => ())
        try {
          assertThat(reader.next()).isTrue
          assertThat(reader.get().column(0).getInt(0)).isEqualTo(2)
          assertThat(reader.get().numRows()).isEqualTo(2)
          assertThat(reader.next()).isFalse
          assertThat(memory.getAllocatedMemory).isZero
        } finally reader.close()
    }
  }

  test("limit truncates a batch and releases unread batches; zero limit does not poll") {
    withBatches {
      data =>
        val batches = Seq(values(data, 0, 10, 4), values(data, 4, 10, 4))
        val memory = batches.head.getVectorSchemaRoot.getVector(0).getAllocator
        val reader = new FlussAppendColumnarReader(
          rowType,
          Array(0),
          FlussAppendInputPartition(bucket, 0, 8),
          Some(2),
          () => records(8, batches: _*),
          () => ())
        try {
          assertThat(reader.next()).isTrue
          assertThat(reader.get().numRows()).isEqualTo(2)
          reader.get().close()
          reader.get().close()
          assertThat(reader.next()).isFalse
          assertThat(memory.getAllocatedMemory).isZero
        } finally reader.close()
    }
    val reader = new FlussAppendColumnarReader(
      rowType,
      Array(0),
      FlussAppendInputPartition(bucket, 0, 8),
      Some(0),
      () => throw new AssertionError("must not poll"),
      () => ())
    try assertThat(reader.next()).isFalse
    finally reader.close()
  }

  test("early close releases current and queued batches") {
    withBatches {
      data =>
        val first = values(data, 0, 10, 4)
        val second = values(data, 4, 10, 4)
        val memory = first.getVectorSchemaRoot.getVector(0).getAllocator
        val reader = new FlussAppendColumnarReader(
          rowType,
          Array(0),
          FlussAppendInputPartition(bucket, 0, 8),
          None,
          () => records(8, first, second),
          () => ())
        try {
          assertThat(reader.next()).isTrue
          reader.close()
          assertThat(memory.getAllocatedMemory).isZero
        } finally reader.close()
    }
  }

  test("empty poll without progress fails and closes resources") {
    var closes = 0
    val reader = new FlussAppendColumnarReader(
      rowType,
      Array(0),
      FlussAppendInputPartition(bucket, 0, 8),
      None,
      () => ArrowScanRecords.EMPTY,
      () => closes += 1)
    val error = intercept[IllegalStateException](reader.next())
    assertThat(error.getMessage).contains("stop offset 8")
    assertThat(closes).isEqualTo(1)
  }

  test("poll failure keeps its cause and suppresses cleanup failure") {
    val failure = new IllegalStateException("poll failed")
    val cleanup = new IllegalStateException("cleanup failed")
    val reader = new FlussAppendColumnarReader(
      rowType,
      Array(0),
      FlussAppendInputPartition(bucket, 0, 8),
      None,
      () => throw failure,
      () => throw cleanup)
    assertThat(intercept[IllegalStateException](reader.next())).isSameAs(failure)
    assertThat(failure.getSuppressed.length).isEqualTo(1)
    assertThat(failure.getSuppressed.head).isSameAs(cleanup)
    reader.close()
  }

  test("conversion failure releases all polled batches") {
    withBatches {
      data =>
        val batch = values(data, 0, 10, 4)
        val memory = batch.getVectorSchemaRoot.getVector(0).getAllocator
        val reader = new FlussAppendColumnarReader(
          rowType,
          Array(3),
          FlussAppendInputPartition(bucket, 0, 4),
          None,
          () => records(4, batch),
          () => ())
        try {
          intercept[IndexOutOfBoundsException](reader.next())
          assertThat(memory.getAllocatedMemory).isZero
        } finally reader.close()
    }
  }
}
