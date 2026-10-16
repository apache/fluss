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
import org.apache.fluss.client.table.scanner.log.{LogScanner, ScanRecords}
import org.apache.fluss.client.tiering.TieringLogSplit
import org.apache.fluss.lake.writer.LakeWriter
import org.apache.fluss.metadata.{TableBucket, TablePath}

import org.assertj.core.api.Assertions.assertThat
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.Collections

/** Regression tests for executor-side Spark tiering. */
class TieringTaskTest extends AnyFunSuite {

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
}
