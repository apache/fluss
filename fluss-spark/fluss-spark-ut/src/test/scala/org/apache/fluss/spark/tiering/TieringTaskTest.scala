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

import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.{Scan, ScanRecord}
import org.apache.fluss.client.table.scanner.batch.BatchScanner
import org.apache.fluss.client.table.scanner.log.{LogScanner, ScanRecords}
import org.apache.fluss.client.tiering.{TieringLogSplit, TieringSnapshotSplit}
import org.apache.fluss.lake.writer.LakeWriter
import org.apache.fluss.metadata.{TableBucket, TablePath}
import org.apache.fluss.record.{ChangeType, LogRecord}
import org.apache.fluss.row.{GenericRow, InternalRow}
import org.apache.fluss.utils.CloseableIterator

import org.assertj.core.api.Assertions.assertThat
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doThrow, mock, never, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.{Arrays, Collections}

/** Regression tests for executor-side Spark tiering. */
class TieringTaskTest extends AnyFunSuite {

  // Basic log and snapshot cases mirror Flink's TieringSplitReaderTest.
  test("log split writes changelog records only before its stopping offset") {
    val table = mock(classOf[Table])
    val scan = mock(classOf[Scan])
    val scanner = mock(classOf[LogScanner])
    val writer = mock(classOf[LakeWriter[AnyRef]])
    val bucket = new TableBucket(1L, 0)
    val first = new ScanRecord(2L, 20L, ChangeType.INSERT, GenericRow.of(Int.box(1)))
    val second = new ScanRecord(3L, 30L, ChangeType.DELETE, GenericRow.of(Int.box(2)))
    val beyondEnd = new ScanRecord(4L, 99L, ChangeType.INSERT, GenericRow.of(Int.box(3)))
    val records = new ScanRecords(
      Collections.singletonMap(bucket, Arrays.asList(first, second, beyondEnd)),
      Collections.singletonMap[TableBucket, java.lang.Long](bucket, 5L))
    val writeResult = new Object()
    when(table.newScan()).thenReturn(scan)
    when(scan.createLogScanner()).thenReturn(scanner)
    when(scanner.poll(Duration.ofMillis(1))).thenReturn(records)
    when(writer.complete()).thenAnswer(_ => writeResult)
    val split = new TieringLogSplit(TablePath.of("db", "table"), bucket, null, 2L, 4L, 3)

    val result = TieringTask.processLogSplit(table, writer, split, 1L)

    val written = ArgumentCaptor.forClass(classOf[LogRecord])
    verify(writer, times(2)).write(written.capture())
    assertThat(written.getAllValues.get(0)).isSameAs(first)
    assertThat(written.getAllValues.get(1)).isSameAs(second)
    assertThat(result.writeResult()).isSameAs(writeResult)
    assertThat(result.logEndOffset()).isEqualTo(4L)
    assertThat(result.maxTimestamp()).isEqualTo(30L)
    assertThat(result.numberOfWriteResults()).isEqualTo(3)
    verify(scanner).subscribe(0, 2L)
    verify(scanner).close()
  }

  test("snapshot split writes snapshot rows as inserts and preserves its log offset") {
    val table = mock(classOf[Table])
    val scan = mock(classOf[Scan])
    val scanner = mock(classOf[BatchScanner])
    val batch = mock(classOf[CloseableIterator[InternalRow]])
    val writer = mock(classOf[LakeWriter[AnyRef]])
    val bucket = new TableBucket(1L, 0)
    val first = GenericRow.of(Int.box(1))
    val second = GenericRow.of(Int.box(2))
    val writeResult = new Object()
    when(table.newScan()).thenReturn(scan)
    when(scan.createBatchScanner(bucket, 7L)).thenReturn(scanner)
    when(scanner.pollBatch(Duration.ofMillis(1))).thenReturn(batch).thenReturn(null)
    when(batch.hasNext).thenReturn(true).thenReturn(true).thenReturn(false)
    when(batch.next()).thenReturn(first).thenReturn(second)
    when(writer.complete()).thenAnswer(_ => writeResult)
    val split = new TieringSnapshotSplit(TablePath.of("db", "table"), bucket, null, 7L, 17L, 3)

    val result = TieringTask.processSnapshotSplit(table, writer, split, 1L)

    val written = ArgumentCaptor.forClass(classOf[LogRecord])
    verify(writer, times(2)).write(written.capture())
    assertThat(written.getAllValues.get(0).getRow).isSameAs(first)
    assertThat(written.getAllValues.get(1).getRow).isSameAs(second)
    written.getAllValues.forEach(
      record => assertThat(record.getChangeType).isEqualTo(ChangeType.INSERT))
    assertThat(result.writeResult()).isSameAs(writeResult)
    assertThat(result.logEndOffset()).isEqualTo(17L)
    assertThat(result.maxTimestamp()).isEqualTo(-1L)
    assertThat(result.numberOfWriteResults()).isEqualTo(3)
    verify(batch).close()
    verify(scanner).close()
  }

  test("log split finishes when empty WAL batches advance to the stopping offset") {
    val table = mock(classOf[Table])
    val scan = mock(classOf[Scan])
    val scanner = mock(classOf[LogScanner])
    val writer = mock(classOf[LakeWriter[AnyRef]])
    val tablePath = TablePath.of("fluss", "empty_wal")
    val tableBucket = new TableBucket(1L, 0)
    val split = new TieringLogSplit(tablePath, tableBucket, null, 0L, 5L, 1)
    val timeout = Duration.ofMillis(1)
    val progress = new ScanRecords(
      Collections.emptyMap[TableBucket, java.util.List[ScanRecord]](),
      Collections.singletonMap[TableBucket, java.lang.Long](tableBucket, 5L))
    when(table.newScan()).thenReturn(scan)
    when(scan.createLogScanner()).thenReturn(scanner)
    when(scanner.poll(timeout))
      .thenReturn(progress)
      .thenThrow(new IllegalStateException("The split should finish after the first poll"))

    val result = TieringTask.processLogSplit(table, writer, split, timeout.toMillis)

    assertThat(result.logEndOffset()).isEqualTo(5L)
    assertThat(result.writeResult()).isNull()
    assertThat(result.numberOfWriteResults()).isEqualTo(1)
    verify(writer, never()).write(any(classOf[ScanRecord]))
    verify(writer).complete()
    verify(scanner).close()
  }

  test("an empty log range finishes without polling") {
    val table = mock(classOf[Table])
    val scan = mock(classOf[Scan])
    val scanner = mock(classOf[LogScanner])
    val writer = mock(classOf[LakeWriter[AnyRef]])
    when(table.newScan()).thenReturn(scan)
    when(scan.createLogScanner()).thenReturn(scanner)
    val split =
      new TieringLogSplit(TablePath.of("db", "table"), new TableBucket(1L, 0), null, 5L, 5L, 1)
    val result = TieringTask.processLogSplit(table, writer, split, 1L)
    assertThat(result.logEndOffset()).isEqualTo(5L)
    verify(scanner, never()).poll(any(classOf[Duration]))
    verify(writer).complete()
    verify(scanner).close()
  }

  test("snapshot batch and scanner close when writing fails") {
    val table = mock(classOf[Table])
    val scan = mock(classOf[Scan])
    val scanner = mock(classOf[BatchScanner])
    val batch = mock(classOf[CloseableIterator[InternalRow]])
    val writer = mock(classOf[LakeWriter[AnyRef]])
    val bucket = new TableBucket(1L, 0)
    when(table.newScan()).thenReturn(scan)
    when(scan.createBatchScanner(bucket, 7L)).thenReturn(scanner)
    when(scanner.pollBatch(Duration.ofMillis(1))).thenReturn(batch)
    when(batch.hasNext).thenReturn(true)
    doThrow(new IllegalStateException("write failed")).when(writer).write(any(classOf[ScanRecord]))
    val split = new TieringSnapshotSplit(TablePath.of("db", "table"), bucket, null, 7L, 10L, 1)
    intercept[IllegalStateException](TieringTask.processSnapshotSplit(table, writer, split, 1L))
    verify(batch).close()
    verify(scanner).close()
    verify(writer, never()).complete()
  }

}
