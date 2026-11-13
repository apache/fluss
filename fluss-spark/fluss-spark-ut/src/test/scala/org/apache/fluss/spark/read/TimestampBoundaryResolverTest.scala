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

import org.apache.fluss.client.table.scanner.ScanRecord
import org.apache.fluss.client.table.scanner.log.{LogScanner, ScanRecords}
import org.apache.fluss.metadata.TableBucket
import org.apache.fluss.record.ChangeType
import org.apache.fluss.row.GenericRow

import org.assertj.core.api.Assertions.assertThat
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.Collections

import scala.collection.JavaConverters._

class TimestampBoundaryResolverTest extends AnyFunSuite {
  private val bucket = new TableBucket(1L, 0)

  private def record(offset: Long, timestamp: Long): ScanRecord =
    new ScanRecord(offset, timestamp, ChangeType.INSERT, GenericRow.of(Int.box(1)))

  test("sparse time-index seek is calibrated and all equal-T records are excluded") {
    val scanner = mock(classOf[LogScanner])
    when(scanner.poll(any(classOf[Duration]))).thenReturn(new ScanRecords(
      Map(
        bucket -> Seq(record(5, 99), record(6, 100), record(7, 100), record(8, 101)).asJava).asJava,
      Map(bucket -> Long.box(9)).asJava))
    val result = TimestampBoundaryResolver.calibrate(
      scanner,
      Map(bucket -> 0L),
      Map(bucket -> 9L),
      Map(bucket -> 5L),
      100L,
      Duration.ofSeconds(1))
    assertThat(Long.box(result(bucket))).isEqualTo(Long.box(6))
  }

  test("fixed committed high watermark excludes later uncommitted records") {
    val scanner = mock(classOf[LogScanner])
    when(scanner.poll(any(classOf[Duration]))).thenReturn(
      new ScanRecords(
        Map(bucket -> Seq(record(5, 99), record(7, 100)).asJava).asJava,
        Map(bucket -> Long.box(8)).asJava))
    assertThat(
      Long.box(
        TimestampBoundaryResolver.calibrate(
          scanner,
          Map(bucket -> 0L),
          Map(bucket -> 6L),
          Map(bucket -> 5L),
          100L,
          Duration.ofSeconds(1))(bucket)))
      .isEqualTo(Long.box(6))
  }

  test("progress-only batch reaches the fixed high watermark") {
    val scanner = mock(classOf[LogScanner])
    when(scanner.poll(any(classOf[Duration])))
      .thenReturn(new ScanRecords(Collections.emptyMap(), Map(bucket -> Long.box(9)).asJava))
    assertThat(
      Long.box(
        TimestampBoundaryResolver.calibrate(
          scanner,
          Map(bucket -> 0L),
          Map(bucket -> 9L),
          Map(bucket -> 5L),
          100L,
          Duration.ofSeconds(1))(bucket)))
      .isEqualTo(Long.box(9))
  }

  test("a deleted historical prefix cannot be relabeled as the exact timestamp boundary") {
    val scanner = mock(classOf[LogScanner])
    when(scanner.poll(any(classOf[Duration])))
      .thenReturn(new ScanRecords(Map(bucket -> Seq(record(5, 101)).asJava).asJava))
    val error = intercept[IllegalStateException] {
      TimestampBoundaryResolver.calibrate(
        scanner,
        Map(bucket -> 5L),
        Map(bucket -> 9L),
        Map(bucket -> 5L),
        100L,
        Duration.ofSeconds(1))
    }
    assertThat(error.getMessage).contains("Cannot prove historical boundary")
  }

  test("progress without retained timestamps cannot prove a deleted prefix's boundary") {
    val scanner = mock(classOf[LogScanner])
    when(scanner.poll(any(classOf[Duration])))
      .thenReturn(new ScanRecords(Collections.emptyMap(), Map(bucket -> Long.box(9)).asJava))
    val error = intercept[IllegalStateException] {
      TimestampBoundaryResolver.calibrate(
        scanner,
        Map(bucket -> 5L),
        Map(bucket -> 9L),
        Map(bucket -> 5L),
        100L,
        Duration.ofSeconds(1))
    }
    assertThat(error.getMessage).contains("Cannot prove historical boundary")
    val empty = intercept[IllegalArgumentException] {
      TimestampBoundaryResolver.calibrate(
        scanner,
        Map(bucket -> 9L),
        Map(bucket -> 9L),
        Map(bucket -> 9L),
        100L,
        Duration.ofSeconds(1))
    }
    assertThat(empty.getMessage).contains("no retained log prefix")
  }

  test("buckets resolve independently across polls and completed subscriptions are removed") {
    val second = new TableBucket(1L, 1)
    val scanner = mock(classOf[LogScanner])
    val firstPoll = new ScanRecords(
      Map(
        bucket -> Seq(record(0, 99), record(1, 100)).asJava,
        second -> Seq(record(5, 90)).asJava).asJava)
    val secondPoll = new ScanRecords(Map(second -> Seq(record(6, 100)).asJava).asJava)
    when(scanner.poll(any(classOf[Duration]))).thenReturn(firstPoll, secondPoll)
    val result = TimestampBoundaryResolver.calibrate(
      scanner,
      Map(bucket -> 0L, second -> 0L),
      Map(bucket -> 6L, second -> 10L),
      Map(bucket -> 0L, second -> 5L),
      100L,
      Duration.ofSeconds(1))
    assertThat(Long.box(result(bucket))).isEqualTo(Long.box(1))
    assertThat(Long.box(result(second))).isEqualTo(Long.box(6))
    verify(scanner).unsubscribe(0)
    verify(scanner).unsubscribe(1)
  }

  test("repeated non-advancing progress cannot keep boundary resolution alive forever") {
    val scanner = mock(classOf[LogScanner])
    when(scanner.poll(any(classOf[Duration])))
      .thenReturn(new ScanRecords(Collections.emptyMap(), Map(bucket -> Long.box(5)).asJava))
    val error = intercept[IllegalStateException] {
      TimestampBoundaryResolver.calibrate(
        scanner,
        Map(bucket -> 0L),
        Map(bucket -> 9L),
        Map(bucket -> 5L),
        100L,
        Duration.ofMillis(20))
    }
    assertThat(error.getMessage).contains("before timeout")
  }
}
