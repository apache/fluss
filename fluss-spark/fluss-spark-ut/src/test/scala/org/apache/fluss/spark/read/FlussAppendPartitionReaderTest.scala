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

import org.apache.fluss.client.lookup.Lookup
import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.{Scan, ScanRecord}
import org.apache.fluss.client.table.scanner.batch.BatchScanner
import org.apache.fluss.client.table.scanner.log.{LogScanner, ScanRecords, TypedLogScanner}
import org.apache.fluss.client.table.writer.{Append, Upsert}
import org.apache.fluss.config.Configuration
import org.apache.fluss.exception.FetchException
import org.apache.fluss.metadata.{Schema, TableBucket, TableDescriptor, TableInfo, TablePath}
import org.apache.fluss.predicate.Predicate
import org.apache.fluss.record.ChangeType
import org.apache.fluss.row.GenericRow
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.types.DataTypes
import org.apache.fluss.utils.clock.ManualClock

import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.{ArrayList, Collections, List => JList}

import scala.collection.JavaConverters._

/** Tests bounded log reads independently of the timing of network fetches. */
class FlussAppendPartitionReaderTest extends AnyFunSuite {

  private val path = TablePath.of("test", "append_reader")
  private val bucket = new TableBucket(1L, 0)
  private val testingInfo = TableInfo.of(
    path,
    1L,
    1,
    TableDescriptor
      .builder()
      .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
      .distributedBy(1)
      .build(),
    null,
    0L,
    0L)

  test("a progress-only poll can complete a bounded read") {
    val scanner = new TestingLogScanner(Seq(batch(Seq.empty, 5L)))

    assertThat(readValues(reader(scanner, 5L))).isEmpty()
    assertThat(scanner.closed).isTrue()
  }

  test("return every buffered record before applying the final poll progress") {
    val scanner = new TestingLogScanner(Seq(batch(Seq(record(0L, 10), record(1L, 20)), 2L)))

    assertThat(readValues(reader(scanner, 2L))).containsExactly(Int.box(10), Int.box(20))
  }

  test("trailing filtered records advance the scan past the last returned row") {
    val scanner = new TestingLogScanner(Seq(batch(Seq(record(0L, 10)), 5L)))

    assertThat(readValues(reader(scanner, 5L))).containsExactly(Int.box(10))
  }

  test("a progress-only poll before the stopping offset continues reading") {
    val scanner =
      new TestingLogScanner(Seq(batch(Seq.empty, 1L), batch(Seq(record(1L, 20)), 2L)))

    assertThat(readValues(reader(scanner, 2L))).containsExactly(Int.box(20))
  }

  test("repeated consumed offsets do not prevent a later batch from being read") {
    val scanner = new TestingLogScanner(
      Seq(batch(Seq.empty, 0L), batch(Seq.empty, 0L), batch(Seq(record(0L, 10)), 1L)))

    assertThat(readValues(reader(scanner, 1L))).containsExactly(Int.box(10))
    assertThat(scanner.closed).isTrue()
  }

  test("draining a returned batch may repeat its consumed offset before the next batch") {
    val scanner = new TestingLogScanner(
      Seq(batch(Seq(record(0L, 10)), 1L), batch(Seq.empty, 1L), batch(Seq(record(1L, 20)), 2L)))

    assertThat(readValues(reader(scanner, 2L))).containsExactly(Int.box(10), Int.box(20))
  }

  test("an empty poll fails after exhausting the no-progress budget") {
    val scanner =
      new TestingLogScanner(Seq(ScanRecords.EMPTY, batch(Seq(record(0L, 10)), 1L)))

    val failure = intercept[IllegalStateException] {
      readValues(reader(scanner, 1L))
    }

    assertThat(failure.getMessage).contains("made no progress")
    assertThat(scanner.closed).isTrue()
  }

  test("an unchanged consumed offset is not progress") {
    val scanner =
      new TestingLogScanner(
        Seq(batch(Seq.empty, 0L), batch(Seq(record(0L, 10)), 1L)),
        elapsedPolls = Seq(Duration.ofSeconds(10)))

    val failure = intercept[IllegalStateException] {
      readValues(reader(scanner, 1L))
    }

    assertThat(failure.getMessage).contains("made no progress")
    assertThat(scanner.closed).isTrue()
  }

  test("repeated offsets share one budget instead of renewing it") {
    val scanner = new TestingLogScanner(
      Seq.fill(3)(batch(Seq.empty, 0L)),
      elapsedPolls = Seq(Duration.ofSeconds(4), Duration.ofSeconds(4), Duration.ofSeconds(2)))

    val failure = intercept[IllegalStateException] {
      readValues(reader(scanner, 1L))
    }

    assertThat(failure.getMessage).contains("made no progress")
    assertThat(scanner.pollTimeouts.toArray)
      .containsExactly(Duration.ofSeconds(10), Duration.ofSeconds(6), Duration.ofSeconds(2))
    assertThat(scanner.closed).isTrue()
  }

  test("consumed-only advancement renews the no-progress budget") {
    val scanner = new TestingLogScanner(
      Seq(batch(Seq.empty, 0L), batch(Seq.empty, 1L), batch(Seq.empty, 1L), batch(Seq.empty, 2L)),
      elapsedPolls = Seq.fill(4)(Duration.ofSeconds(4)))

    assertThat(readValues(reader(scanner, 2L))).isEmpty()
    assertThat(scanner.pollTimeouts.toArray).containsExactly(
      Duration.ofSeconds(10),
      Duration.ofSeconds(6),
      Duration.ofSeconds(10),
      Duration.ofSeconds(6))
  }

  test("an empty earliest poll completes when retention covers the stopping offset") {
    val scanner = new TestingLogScanner(Seq(ScanRecords.EMPTY))

    assertThat(
      readValues(
        recoveringReader(
          Seq(scanner),
          5L,
          Seq(5L),
          startOffset = LogScanner.EARLIEST_OFFSET,
          allowExpiredOffsets = false))).isEmpty()
    assertThat(scanner.closed).isTrue()
  }

  test("an empty earliest poll fails when retention does not cover the stopping offset") {
    val scanner =
      new TestingLogScanner(Seq(ScanRecords.EMPTY, batch(Seq(record(3L, 30)), 5L)))

    val failure = intercept[IllegalStateException] {
      readValues(
        recoveringReader(
          Seq(scanner),
          5L,
          Seq(3L),
          startOffset = LogScanner.EARLIEST_OFFSET,
          allowExpiredOffsets = false))
    }

    assertThat(failure.getMessage).contains("made no progress")
    assertThat(scanner.closed).isTrue()
  }

  for (firstBatch <- Seq(batch(Seq(record(0L, 10)), 1L), batch(Seq.empty, 1L))) {
    test(
      s"a strict earliest scan cannot skip retention after progress with rows=${firstBatch.count()}") {
      val scanner = new TestingLogScanner(Seq(firstBatch, ScanRecords.EMPTY))

      val failure = intercept[IllegalStateException] {
        readValues(
          recoveringReader(
            Seq(scanner),
            5L,
            Seq(5L),
            startOffset = LogScanner.EARLIEST_OFFSET,
            allowExpiredOffsets = false))
      }

      assertThat(failure.getMessage).contains("made no progress")
      assertThat(scanner.closed).isTrue()
    }
  }

  test("never return a record at or beyond the fixed stopping offset") {
    val scanner = new TestingLogScanner(Seq(batch(Seq(record(0L, 10), record(2L, 99)), 3L)))

    assertThat(readValues(reader(scanner, 2L))).containsExactly(Int.box(10))
  }

  test("a zero-timeout poll without scanner progress fails") {
    val scanner = new TestingLogScanner(Seq(ScanRecords.EMPTY))

    val failure = intercept[IllegalStateException] {
      val boundedReader = reader(scanner, 1L, pollTimeout = Duration.ZERO)
      try {
        boundedReader.next()
      } finally {
        boundedReader.close()
      }
    }

    assertThat(failure.getMessage).contains("made no progress")
    assertThat(scanner.closed).isTrue()
  }

  test("apply the requested time window to buffered records before finishing") {
    val scanner = new TestingLogScanner(
      Seq(
        batch(
          Seq(record(0L, 10, 9L), record(1L, 11, 10L), record(2L, 12, 19L), record(3L, 13, 20L)),
          4L)))

    assertThat(readValues(reader(scanner, 4L, Some(FlussTimeRange(10L, 20L)))))
      .containsExactly(Int.box(11), Int.box(12))
  }

  test("record offsets remain usable when a poll has no separate progress metadata") {
    val scanner = new TestingLogScanner(
      Seq(
        new ScanRecords(
          Collections.singletonMap[TableBucket, JList[ScanRecord]](
            bucket,
            Collections.singletonList(record(0L, 10))))))

    assertThat(readValues(reader(scanner, 1L))).containsExactly(Int.box(10))
  }

  test("an expired explicit range can complete without recreating the scanner") {
    val scanner = new TestingLogScanner(Seq.empty, Map(1 -> expiredOffset()))

    assertThat(readValues(recoveringReader(Seq(scanner), 3L, Seq(3L), startOffset = 1L)))
      .isEmpty()
    assertThat(scanner.subscribedOffsets.toArray).containsExactly(1L)
    assertThat(scanner.closed).isTrue()
  }

  test("an empty explicit poll can complete an expired range") {
    val scanner = new TestingLogScanner(Seq(ScanRecords.EMPTY))

    assertThat(readValues(recoveringReader(Seq(scanner), 3L, Seq(3L), startOffset = 1L)))
      .isEmpty()
    assertThat(scanner.closed).isTrue()
  }

  test("an empty explicit poll resumes after retention advances") {
    val empty = new TestingLogScanner(Seq(ScanRecords.EMPTY))
    val remaining =
      new TestingLogScanner(Seq(batch(Seq(record(2L, 20), record(3L, 30)), 4L)))

    assertThat(readValues(recoveringReader(Seq(empty, remaining), 4L, Seq(2L))))
      .containsExactly(Int.box(20), Int.box(30))
    assertThat(remaining.subscribedOffsets.toArray).containsExactly(2L)
    assertThat(empty.closed).isTrue()
    assertThat(remaining.closed).isTrue()
  }

  test("retention recovery at the deadline gives the replacement scanner a fresh budget") {
    val empty = new TestingLogScanner(Seq(ScanRecords.EMPTY))
    val remaining = new TestingLogScanner(
      Seq(batch(Seq.empty, 2L), batch(Seq(record(2L, 20), record(3L, 30)), 4L)),
      elapsedPolls = Seq(Duration.ofSeconds(4), Duration.ofSeconds(4)))

    assertThat(readValues(recoveringReader(Seq(empty, remaining), 4L, Seq(2L))))
      .containsExactly(Int.box(20), Int.box(30))
    assertThat(remaining.pollTimeouts.toArray)
      .containsExactly(Duration.ofSeconds(10), Duration.ofSeconds(6))
  }

  test("an empty explicit poll fails when earliest has not advanced") {
    val scanner =
      new TestingLogScanner(Seq(ScanRecords.EMPTY, batch(Seq(record(0L, 10)), 1L)))

    val failure = intercept[IllegalStateException] {
      readValues(recoveringReader(Seq(scanner), 1L, Seq(0L)))
    }

    assertThat(failure.getMessage).contains("made no progress")
    assertThat(scanner.closed).isTrue()
  }

  test("a partially expired range resumes from earliest with a fresh scanner") {
    val expired = new TestingLogScanner(Seq.empty, Map(1 -> expiredOffset()))
    val remaining =
      new TestingLogScanner(Seq(batch(Seq(record(2L, 20), record(3L, 30), record(4L, 40)), 5L)))

    assertThat(readValues(recoveringReader(Seq(expired, remaining), 5L, Seq(2L))))
      .containsExactly(Int.box(20), Int.box(30), Int.box(40))
    assertThat(expired.subscribedOffsets.toArray).containsExactly(0L)
    assertThat(remaining.subscribedOffsets.toArray).containsExactly(2L)
    assertThat(expired.closed).isTrue()
    assertThat(remaining.closed).isTrue()
  }

  test("an expired-offset error at the deadline renews the recovered scanner budget") {
    val expired =
      new TestingLogScanner(Seq.empty, Map(1 -> expiredOffset()), Seq(Duration.ofSeconds(10)))
    val remaining = new TestingLogScanner(
      Seq(batch(Seq.empty, 2L), batch(Seq(record(2L, 20)), 3L)),
      elapsedPolls = Seq(Duration.ofSeconds(4), Duration.ofSeconds(4)))

    assertThat(readValues(recoveringReader(Seq(expired, remaining), 3L, Seq(2L))))
      .containsExactly(Int.box(20))
    assertThat(remaining.pollTimeouts.toArray)
      .containsExactly(Duration.ofSeconds(10), Duration.ofSeconds(6))
  }

  test("retention after the first batch never repeats previously returned records") {
    val expired =
      new TestingLogScanner(Seq(batch(Seq(record(0L, 10)), 1L)), Map(2 -> expiredOffset()))
    val remaining = new TestingLogScanner(Seq(batch(Seq(record(2L, 20), record(3L, 30)), 4L)))

    assertThat(readValues(recoveringReader(Seq(expired, remaining), 4L, Seq(2L))))
      .containsExactly(Int.box(10), Int.box(20), Int.box(30))
    assertThat(remaining.subscribedOffsets.toArray).containsExactly(2L)
    assertThat(expired.closed).isTrue()
    assertThat(remaining.closed).isTrue()
  }

  for (earliest <- Seq(0L, 5L)) {
    test(s"an out-of-range request is not rewound when earliest is $earliest") {
      val failure = expiredOffset()
      val scanner = new TestingLogScanner(Seq.empty, Map(1 -> failure))

      val thrown = intercept[FetchException] {
        readValues(recoveringReader(Seq(scanner), 10L, Seq(earliest), startOffset = 5L))
      }

      assertThat(thrown).isSameAs(failure)
      assertThat(scanner.subscribedOffsets.toArray).containsExactly(5L)
      assertThat(scanner.closed).isTrue()
    }
  }

  test("retention recovery never hides an unrelated fetch failure") {
    val failure = new FetchException("corrupt fetched data", new IllegalStateException("corrupt"))
    val scanner = new TestingLogScanner(Seq.empty, Map(1 -> failure))

    val thrown = intercept[FetchException] {
      readValues(recoveringReader(Seq(scanner), 5L, Seq(0L)))
    }

    assertThat(thrown).isSameAs(failure)
    assertThat(scanner.closed).isTrue()
  }

  test("a fetch failure can finish only a range confirmed to be fully expired") {
    val failure = new FetchException("failed fetch without a typed cause")
    val scanner = new TestingLogScanner(Seq.empty, Map(1 -> failure))

    assertThat(readValues(recoveringReader(Seq(scanner), 5L, Seq(5L)))).isEmpty()
    assertThat(scanner.closed).isTrue()
  }

  test("a persistent fetch failure is preserved once the position is still retained") {
    val firstFailure = new FetchException("first failed fetch")
    val nextFailure = new FetchException("persistent failed fetch")
    val first = new TestingLogScanner(Seq.empty, Map(1 -> firstFailure))
    val next = new TestingLogScanner(Seq.empty, Map(1 -> nextFailure))

    val thrown = intercept[FetchException] {
      readValues(recoveringReader(Seq(first, next), 5L, Seq(2L, 2L)))
    }

    assertThat(thrown).isSameAs(nextFailure)
    assertThat(next.subscribedOffsets.toArray).containsExactly(2L)
    assertThat(first.closed).isTrue()
    assertThat(next.closed).isTrue()
  }

  test("an earliest lookup failure is suppressed on the original fetch failure") {
    val failure = expiredOffset()
    val lookupFailure = new IllegalStateException("earliest lookup failed")
    val scanner = new TestingLogScanner(Seq.empty, Map(1 -> failure))

    val thrown = intercept[FetchException] {
      readValues(recoveringReader(Seq(scanner), 5L, Seq.empty, lookupFailure = Some(lookupFailure)))
    }

    assertThat(thrown).isSameAs(failure)
    assertThat(thrown.getSuppressed).containsExactly(lookupFailure)
    assertThat(scanner.closed).isTrue()
  }

  test("an interrupted retention lookup propagates interruption and preserves the fetch failure") {
    val failure = expiredOffset()
    val interrupted = new InterruptedException("interrupted earliest lookup")
    val scanner = new TestingLogScanner(Seq.empty, Map(1 -> failure))

    try {
      val thrown = intercept[InterruptedException] {
        readValues(recoveringReader(Seq(scanner), 5L, Seq.empty, lookupFailure = Some(interrupted)))
      }

      assertThat(thrown).isSameAs(interrupted)
      assertThat(Thread.currentThread().isInterrupted).isTrue()
      assertThat(thrown.getSuppressed).containsExactly(failure)
      assertThat(scanner.closed).isTrue()
    } finally {
      Thread.interrupted()
    }
  }

  test("strict readers preserve the original expired-offset failure") {
    val failure = expiredOffset()
    val scanner = new TestingLogScanner(Seq.empty, Map(1 -> failure))

    val thrown = intercept[FetchException] {
      readValues(recoveringReader(Seq(scanner), 5L, Seq.empty, allowExpiredOffsets = false))
    }

    assertThat(thrown).isSameAs(failure)
    assertThat(scanner.closed).isTrue()
  }

  test("a recovered scanner must make further progress") {
    val expired = new TestingLogScanner(Seq.empty, Map(1 -> expiredOffset()))
    val stalled = new TestingLogScanner(Seq(ScanRecords.EMPTY, batch(Seq(record(4L, 40)), 5L)))

    val failure = intercept[IllegalStateException] {
      readValues(recoveringReader(Seq(expired, stalled), 5L, Seq(1L, 1L)))
    }

    assertThat(failure.getMessage).contains("made no progress")
    assertThat(expired.closed).isTrue()
    assertThat(stalled.closed).isTrue()
  }

  private def expiredOffset(): FetchException =
    new FetchException("fetch offset expired")

  private def record(offset: Long, value: Int, timestamp: Long = 0L): ScanRecord =
    new ScanRecord(offset, timestamp, ChangeType.INSERT, GenericRow.of(Int.box(value)))

  private def batch(records: Seq[ScanRecord], consumedOffset: Long): ScanRecords =
    new ScanRecords(
      Collections.singletonMap[TableBucket, JList[ScanRecord]](bucket, records.asJava),
      Collections.singletonMap[TableBucket, java.lang.Long](bucket, Long.box(consumedOffset))
    )

  private def reader(
      scanner: TestingLogScanner,
      stopOffset: Long,
      timeRange: Option[FlussTimeRange] = None,
      pollTimeout: Duration = Duration.ofSeconds(10)): FlussAppendPartitionReader = {
    val config = new Configuration()
    config.set(SparkFlussConf.SCAN_POLL_TIMEOUT, pollTimeout)
    val clock = new ManualClock()
    scanner.clock = clock
    val testingTable = new TestingTable(new TestingScan(() => scanner))
    new FlussAppendPartitionReader(
      path,
      Array(0),
      None,
      None,
      FlussAppendInputPartition(bucket, 0L, stopOffset, timeRange),
      config) {
      override protected lazy val table: Table = testingTable
      override protected def monotonicTimeNanos(): Long = clock.nanoseconds()
    }
  }

  private def recoveringReader(
      scanners: Seq[TestingLogScanner],
      stopOffset: Long,
      earliestOffsets: Seq[Long],
      startOffset: Long = 0L,
      allowExpiredOffsets: Boolean = true,
      pollTimeout: Duration = Duration.ofSeconds(10),
      lookupFailure: Option[Throwable] = None): FlussAppendPartitionReader = {
    val config = new Configuration()
    config.set(SparkFlussConf.SCAN_POLL_TIMEOUT, pollTimeout)
    val clock = new ManualClock()
    scanners.foreach(_.clock = clock)
    val remainingScanners = scanners.iterator
    val remainingEarliestOffsets = earliestOffsets.iterator
    val testingTable = new TestingTable(
      new TestingScan(
        () => {
          require(remainingScanners.hasNext, "Unexpected scanner recreation")
          remainingScanners.next()
        }))
    new FlussAppendPartitionReader(
      path,
      Array(0),
      None,
      None,
      FlussAppendInputPartition(bucket, startOffset, stopOffset),
      config,
      allowExpiredOffsets) {
      override protected lazy val table: Table = testingTable
      override protected def monotonicTimeNanos(): Long = clock.nanoseconds()

      override protected def fetchEarliestOffset(): Long = {
        lookupFailure.foreach(error => throw error)
        require(remainingEarliestOffsets.hasNext, "Unexpected earliest-offset lookup")
        remainingEarliestOffsets.next()
      }
    }
  }

  private def readValues(reader: FlussAppendPartitionReader): Array[Integer] = {
    val result = new ArrayList[Integer]()
    try {
      while (reader.next()) {
        result.add(Int.box(reader.get().getInt(0)))
      }
      result.toArray(new Array[Integer](result.size()))
    } finally {
      reader.close()
    }
  }

  private class TestingTable(scan: Scan) extends Table {
    override def getTableInfo: TableInfo = testingInfo
    override def newScan(): Scan = scan
    override def newLookup(): Lookup = throw new UnsupportedOperationException
    override def newAppend(): Append = throw new UnsupportedOperationException
    override def newUpsert(): Upsert = throw new UnsupportedOperationException
    override def close(): Unit = {}
  }

  private class TestingScan(createScanner: () => LogScanner) extends Scan {
    override def project(columns: Array[Int]): Scan = this
    override def project(columns: JList[String]): Scan = this
    override def filter(predicate: Predicate): Scan = this
    override def createLogScanner(): LogScanner = createScanner()
    override def limit(rowNumber: Int): Scan = throw new UnsupportedOperationException
    override def createTypedLogScanner[T](pojoClass: Class[T]): TypedLogScanner[T] =
      throw new UnsupportedOperationException
    override def createBatchScanner(bucket: TableBucket): BatchScanner =
      throw new UnsupportedOperationException
    override def createBatchScanner(bucket: TableBucket, snapshotId: Long): BatchScanner =
      throw new UnsupportedOperationException
    override def createBatchScanner(): BatchScanner = throw new UnsupportedOperationException
  }

  private class TestingLogScanner(
      batches: Seq[ScanRecords],
      failures: Map[Int, RuntimeException] = Map.empty,
      elapsedPolls: Seq[Duration] = Seq.empty)
    extends LogScanner {
    private val remaining = batches.iterator
    private val elapsed = elapsedPolls.iterator
    private var pollCount = 0
    var clock = new ManualClock()
    var pollTimeouts = Vector.empty[Duration]
    var subscribedOffsets = Vector.empty[Long]
    var closed = false

    override def poll(timeout: Duration): ScanRecords = {
      pollCount += 1
      pollTimeouts :+= timeout
      val elapsedTime = if (elapsed.hasNext) Some(elapsed.next()) else None
      elapsedTime.foreach(clock.advanceTime)
      failures.get(pollCount).foreach(error => throw error)
      require(remaining.hasNext, "Unexpected log scanner poll")
      val records = remaining.next()
      if (elapsedTime.isEmpty && !records.hasProgress) {
        clock.advanceTime(timeout)
      }
      records
    }

    override def subscribe(bucket: Int, offset: Long): Unit = {
      subscribedOffsets :+= offset
    }
    override def subscribe(partitionId: Long, bucket: Int, offset: Long): Unit = {
      subscribedOffsets :+= offset
    }
    override def unsubscribe(bucket: Int): Unit = throw new UnsupportedOperationException
    override def unsubscribe(partitionId: Long, bucket: Int): Unit =
      throw new UnsupportedOperationException
    override def wakeup(): Unit = throw new UnsupportedOperationException
    override def close(): Unit = { closed = true }
  }
}
