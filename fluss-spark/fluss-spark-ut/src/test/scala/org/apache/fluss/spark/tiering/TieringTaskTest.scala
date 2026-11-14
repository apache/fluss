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

package org.apache.fluss.spark.tiering

import org.apache.fluss.client.lookup.Lookup
import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.{Scan, ScanRecord}
import org.apache.fluss.client.table.scanner.batch.BatchScanner
import org.apache.fluss.client.table.scanner.log.{LogScanner, ScanRecords, TypedLogScanner}
import org.apache.fluss.client.table.writer.{Append, Upsert}
import org.apache.fluss.client.tiering.{TieringLogSplit, TieringSnapshotSplit}
import org.apache.fluss.lake.writer.LakeWriter
import org.apache.fluss.metadata.{TableBucket, TableInfo, TablePath}
import org.apache.fluss.predicate.Predicate
import org.apache.fluss.record.{ChangeType, LogRecord}
import org.apache.fluss.row.{GenericRow, InternalRow}
import org.apache.fluss.utils.CloseableIterator

import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.{ArrayList, Arrays, Collections, List => JList}

/** Regression tests for executor-side Spark tiering. */
class TieringTaskTest extends AnyFunSuite {

  // Basic log and snapshot cases mirror Flink's TieringSplitReaderTest.
  test("log split writes changelog records only before its stopping offset") {
    val bucket = new TableBucket(1L, 0)
    val first = new ScanRecord(2L, 20L, ChangeType.INSERT, GenericRow.of(Int.box(1)))
    val second = new ScanRecord(3L, 30L, ChangeType.DELETE, GenericRow.of(Int.box(2)))
    val beyondEnd = new ScanRecord(4L, 99L, ChangeType.INSERT, GenericRow.of(Int.box(3)))
    val records = new ScanRecords(
      Collections.singletonMap(bucket, Arrays.asList(first, second, beyondEnd)),
      Collections.singletonMap[TableBucket, java.lang.Long](bucket, 5L))
    val scanner = new TestingLogScanner(Seq(records))
    val table = new TestingTable(new TestingScan(logScanner = scanner))
    val writeResult = new Object()
    val writer = new TestingLakeWriter(writeResult)
    val split = new TieringLogSplit(TablePath.of("db", "table"), bucket, null, 2L, 4L, 3)

    val result = TieringTask.processLogSplit(table, () => writer, split, 1L)

    assertThat[LogRecord](writer.records).containsExactly(first, second)
    assertThat(result.writeResult()).isSameAs(writeResult)
    assertThat(result.logEndOffset()).isEqualTo(4L)
    assertThat(result.maxTimestamp()).isEqualTo(30L)
    assertThat(result.numberOfWriteResults()).isEqualTo(3)
    assertThat(scanner.subscription.contains((0, 2L))).isTrue()
    assertThat(scanner.pollCount).isEqualTo(1)
    assertThat(scanner.closed).isTrue()
    assertThat(writer.closed).isTrue()
    assertThat(writer.completeCount).isEqualTo(1)
  }

  test("snapshot split writes snapshot rows as inserts and preserves its log offset") {
    val bucket = new TableBucket(1L, 0)
    val first = GenericRow.of(Int.box(1))
    val second = GenericRow.of(Int.box(2))
    val batch = new TestingRowIterator(Seq(first, second))
    val scanner = new TestingBatchScanner(Seq(batch))
    val scan = new TestingScan(batchScanner = scanner)
    val table = new TestingTable(scan)
    val writeResult = new Object()
    val writer = new TestingLakeWriter(writeResult)
    val split = new TieringSnapshotSplit(TablePath.of("db", "table"), bucket, null, 7L, 17L, 3)

    val result = TieringTask.processSnapshotSplit(table, () => writer, split, 1L)

    assertThat[LogRecord](writer.records).hasSize(2)
    assertThat(writer.records.get(0).getRow).isSameAs(first)
    assertThat(writer.records.get(1).getRow).isSameAs(second)
    writer.records.forEach(record => assertThat(record.getChangeType).isEqualTo(ChangeType.INSERT))
    assertThat(result.writeResult()).isSameAs(writeResult)
    assertThat(result.logEndOffset()).isEqualTo(17L)
    assertThat(result.maxTimestamp()).isEqualTo(-1L)
    assertThat(result.numberOfWriteResults()).isEqualTo(3)
    assertThat(scan.snapshotRequest.contains((bucket, 7L))).isTrue()
    assertThat(batch.closed).isTrue()
    assertThat(scanner.closed).isTrue()
    assertThat(writer.closed).isTrue()
    assertThat(writer.completeCount).isEqualTo(1)
  }

  test("log split finishes when empty WAL batches advance to the stopping offset") {
    val tablePath = TablePath.of("fluss", "empty_wal")
    val tableBucket = new TableBucket(1L, 0)
    val split = new TieringLogSplit(tablePath, tableBucket, null, 0L, 5L, 1)
    val progress = new ScanRecords(
      Collections.emptyMap[TableBucket, JList[ScanRecord]](),
      Collections.singletonMap[TableBucket, java.lang.Long](tableBucket, 5L))
    val scanner = new TestingLogScanner(Seq(progress))
    val table = new TestingTable(new TestingScan(logScanner = scanner))

    val result = TieringTask.processLogSplit(
      table,
      () => throw new AssertionError("Empty split must not create a writer"),
      split,
      1L)

    assertThat(result.logEndOffset()).isEqualTo(5L)
    assertThat(result.writeResult()).isNull()
    assertThat(result.numberOfWriteResults()).isEqualTo(1)
    assertThat(scanner.pollCount).isEqualTo(1)
    assertThat(scanner.closed).isTrue()
  }

  test("an empty log range finishes without polling") {
    val scanner = new TestingLogScanner(Seq.empty)
    val table = new TestingTable(new TestingScan(logScanner = scanner))
    val split =
      new TieringLogSplit(TablePath.of("db", "table"), new TableBucket(1L, 0), null, 5L, 5L, 1)

    val result = TieringTask.processLogSplit(
      table,
      () => throw new AssertionError("Empty split must not create a writer"),
      split,
      1L)

    assertThat(result.logEndOffset()).isEqualTo(5L)
    assertThat(scanner.pollCount).isZero()
    assertThat(scanner.closed).isTrue()
  }

  test("empty snapshot preserves its offset without creating a writer") {
    val scanner = new TestingBatchScanner(Seq.empty)
    val table = new TestingTable(new TestingScan(batchScanner = scanner))
    val split =
      new TieringSnapshotSplit(
        TablePath.of("db", "table"),
        new TableBucket(1L, 0),
        null,
        7L,
        17L,
        3)

    val result = TieringTask.processSnapshotSplit(
      table,
      () => throw new AssertionError("Empty split must not create a writer"),
      split,
      1L)

    assertThat(result.writeResult()).isNull()
    assertThat(result.logEndOffset()).isEqualTo(17L)
    assertThat(result.numberOfWriteResults()).isEqualTo(3)
    assertThat(scanner.closed).isTrue()
  }

  test("snapshot batch and scanner close when writing fails") {
    val bucket = new TableBucket(1L, 0)
    val batch = new TestingRowIterator(Seq(GenericRow.of(Int.box(1))))
    val scanner = new TestingBatchScanner(Seq(batch))
    val table = new TestingTable(new TestingScan(batchScanner = scanner))
    val writer = new TestingLakeWriter(writeFailure = new IllegalStateException("write failed"))
    val split = new TieringSnapshotSplit(TablePath.of("db", "table"), bucket, null, 7L, 10L, 1)

    val failure =
      intercept[IllegalStateException](
        TieringTask.processSnapshotSplit(table, () => writer, split, 1L))

    assertThat(failure).hasMessage("write failed")
    assertThat(batch.closed).isTrue()
    assertThat(scanner.closed).isTrue()
    assertThat(writer.closed).isTrue()
    assertThat(writer.completeCount).isZero()
  }

  private class TestingTable(scan: Scan) extends Table {
    override def newScan(): Scan = scan
    override def getTableInfo: TableInfo = throw new UnsupportedOperationException
    override def newLookup(): Lookup = throw new UnsupportedOperationException
    override def newAppend(): Append = throw new UnsupportedOperationException
    override def newUpsert(): Upsert = throw new UnsupportedOperationException
    override def close(): Unit = {}
  }

  private class TestingScan(logScanner: LogScanner = null, batchScanner: BatchScanner = null)
    extends Scan {
    var snapshotRequest: Option[(TableBucket, Long)] = None

    override def createLogScanner(): LogScanner = {
      require(logScanner != null, "No log scanner configured")
      logScanner
    }

    override def createBatchScanner(bucket: TableBucket, snapshotId: Long): BatchScanner = {
      require(batchScanner != null, "No batch scanner configured")
      snapshotRequest = Some((bucket, snapshotId))
      batchScanner
    }

    override def project(columns: Array[Int]): Scan = throw new UnsupportedOperationException
    override def project(columns: JList[String]): Scan = throw new UnsupportedOperationException
    override def limit(rowNumber: Int): Scan = throw new UnsupportedOperationException
    override def filter(predicate: Predicate): Scan = throw new UnsupportedOperationException
    override def createTypedLogScanner[T](pojoClass: Class[T]): TypedLogScanner[T] =
      throw new UnsupportedOperationException
    override def createBatchScanner(bucket: TableBucket): BatchScanner =
      throw new UnsupportedOperationException
    override def createBatchScanner(): BatchScanner = throw new UnsupportedOperationException
  }

  private class TestingLogScanner(batches: Seq[ScanRecords]) extends LogScanner {
    private val remaining = batches.iterator
    var subscription: Option[(Int, Long)] = None
    var pollCount = 0
    var closed = false

    override def subscribe(bucket: Int, offset: Long): Unit = {
      subscription = Some((bucket, offset))
    }

    override def poll(timeout: Duration): ScanRecords = {
      pollCount += 1
      require(remaining.hasNext, "Unexpected log scanner poll")
      remaining.next()
    }

    override def subscribe(partitionId: Long, bucket: Int, offset: Long): Unit =
      throw new UnsupportedOperationException
    override def unsubscribe(partitionId: Long, bucket: Int): Unit =
      throw new UnsupportedOperationException
    override def unsubscribe(bucket: Int): Unit = throw new UnsupportedOperationException
    override def wakeup(): Unit = throw new UnsupportedOperationException
    override def close(): Unit = { closed = true }
  }

  private class TestingBatchScanner(batches: Seq[CloseableIterator[InternalRow]])
    extends BatchScanner {
    private val remaining = batches.iterator
    private var finished = false
    var closed = false

    override def pollBatch(timeout: Duration): CloseableIterator[InternalRow] = {
      require(!finished, "Unexpected batch scanner poll after end of input")
      if (remaining.hasNext) {
        remaining.next()
      } else {
        finished = true
        null
      }
    }

    override def close(): Unit = { closed = true }
  }

  private class TestingRowIterator(rows: Seq[InternalRow]) extends CloseableIterator[InternalRow] {
    private val remaining = rows.iterator
    var closed = false

    override def hasNext: Boolean = remaining.hasNext
    override def next(): InternalRow = remaining.next()
    override def close(): Unit = { closed = true }
  }

  private class TestingLakeWriter(writeResult: AnyRef = null, writeFailure: RuntimeException = null)
    extends LakeWriter[AnyRef] {
    val records = new ArrayList[LogRecord]()
    var completeCount = 0
    var closed = false

    override def write(record: LogRecord): Unit = {
      if (writeFailure != null) {
        throw writeFailure
      }
      records.add(record)
    }

    override def complete(): AnyRef = {
      require(!records.isEmpty, "Cannot complete an empty writer")
      completeCount += 1
      writeResult
    }

    override def close(): Unit = { closed = true }
  }
}
