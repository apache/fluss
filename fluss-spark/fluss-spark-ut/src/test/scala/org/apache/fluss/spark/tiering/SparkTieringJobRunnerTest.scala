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

import org.apache.fluss.client.Connection
import org.apache.fluss.client.admin.{Admin, ListOffsetsResult, OffsetSpec}
import org.apache.fluss.client.tiering.{FlussTableLakeSnapshotCommitter, TieringSplitGenerator}
import org.apache.fluss.config.Configuration
import org.apache.fluss.exception.LakeTableSnapshotNotExistException
import org.apache.fluss.lake.committer.{CommitterInitContext, LakeCommitter}
import org.apache.fluss.lake.writer.LakeTieringFactory
import org.apache.fluss.metadata.{PartitionInfo, ResolvedPartitionSpec, Schema, TableDescriptor, TableInfo, TablePath}
import org.apache.fluss.types.DataTypes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.assertj.core.api.Assertions.assertThat
import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.{doAnswer, doThrow, mock, never, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util.Collections
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit}

import scala.concurrent.Await
import scala.concurrent.duration._

/** Regression tests for driver lifecycle and heartbeat failure handling. */
class SparkTieringJobRunnerTest extends AnyFunSuite {
  private class Fixture {
    val spark = mock(classOf[SparkSession])
    val sc = mock(classOf[SparkContext])
    val coordinator = mock(classOf[TieringCoordinator])
    val snapshotCommitter = mock(classOf[FlussTableLakeSnapshotCommitter])
    val connection = mock(classOf[Connection])
    val admin = mock(classOf[Admin])
    val generator = mock(classOf[TieringSplitGenerator])
    val conf = new Configuration()
    val lakeConf = new Configuration()
    val lakeFactory = mock(classOf[LakeTieringFactory[AnyRef, AnyRef]])
    when(spark.sparkContext).thenReturn(sc)
    when(connection.getAdmin).thenReturn(admin)
    def runner: SparkTieringJobRunner = new SparkTieringJobRunner(
      spark,
      conf,
      new Configuration(),
      "paimon",
      lakeConf,
      _ => coordinator,
      _ => snapshotCommitter,
      _ => connection,
      _ => generator,
      (_, _) => lakeFactory)
  }

  test("start is idempotent and stop releases resources") {
    val f = new Fixture
    val polled = new CountDownLatch(1)
    when(f.coordinator.heartbeatAndRequestTable()).thenAnswer {
      _ =>
        polled.countDown()
        None
    }
    val runner = f.runner
    try {
      val first = runner.startAsync()
      assertThat(runner.startAsync()).isSameAs(first)
      assertThat(polled.await(5, TimeUnit.SECONDS)).isTrue()
      runner.stop(5.seconds)
      assertThat(first.isCompleted).isTrue()
      verify(f.coordinator, times(1)).open()
      verify(f.coordinator, times(1)).close()
      verify(f.snapshotCommitter).close()
      verify(f.connection).close()
    } finally {
      runner.stop(5.seconds)
    }
  }

  test("stop before start completes without opening resources") {
    val f = new Fixture
    val runner = f.runner
    runner.stop(5.seconds)
    Await.result(runner.startAsync(), 5.seconds)
    verify(f.coordinator, never()).open()
  }

  test("partial startup failure closes resources and fails the termination future") {
    val f = new Fixture
    doThrow(new IllegalStateException("cannot open committer")).when(f.snapshotCommitter).open()
    val runner = f.runner
    val failure = intercept[IllegalStateException](Await.result(runner.startAsync(), 5.seconds))
    assertThat(failure.getMessage).isEqualTo("cannot open committer")
    runner.stop(5.seconds)
    verify(f.coordinator).close()
    verify(f.snapshotCommitter).close()
    verify(f.connection, never()).getAdmin
  }

  test("repeated background heartbeat failures cancel work and fail the service") {
    val f = new Fixture
    f.conf.set(SparkTieringOptions.HEARTBEAT_INTERVAL, Duration.ofMillis(10))
    f.conf.set(SparkTieringOptions.MAX_HEARTBEAT_FAILURES, Int.box(2))
    val tablePath = TablePath.of("db", "table")
    when(f.coordinator.heartbeatAndRequestTable()).thenReturn(Some((1L, 2L, tablePath)))
    val pendingInfo = new CompletableFuture[TableInfo]()
    when(f.admin.getTableInfo(tablePath)).thenReturn(pendingInfo)
    doThrow(new IllegalStateException("heartbeat unavailable"))
      .when(f.coordinator)
      .sendKeepAliveHeartbeat()
    doAnswer {
      _ =>
        pendingInfo.completeExceptionally(new IllegalStateException("cancelled"))
        null
    }.when(f.sc).cancelJobGroup(anyString())
    val runner = f.runner
    try {
      val failure = intercept[IllegalStateException](Await.result(runner.startAsync(), 5.seconds))
      assertThat(failure.getMessage).contains("background heartbeat failures")
      verify(f.sc).cancelJobGroup(anyString())
      verify(f.coordinator).markTableFailed(1L, 2L)
      verify(f.coordinator).close()
    } finally {
      pendingInfo.completeExceptionally(new IllegalStateException("test finished"))
      runner.stop(5.seconds)
    }
  }

  test("an empty split round still checks for missing lake snapshots") {
    val f = new Fixture
    val tablePath = TablePath.of("db", "empty_table")
    val descriptor = TableDescriptor
      .builder()
      .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
      .distributedBy(1, "id")
      .build()
    val tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, "remote", 0L, 0L)
    when(f.admin.getTableInfo(tablePath)).thenReturn(CompletableFuture.completedFuture(tableInfo))
    val noSnapshot = new CompletableFuture[org.apache.fluss.client.metadata.LakeSnapshot]()
    noSnapshot.completeExceptionally(new LakeTableSnapshotNotExistException("no lake snapshot"))
    when(f.admin.getLatestLakeSnapshot(tablePath)).thenReturn(noSnapshot)
    when(f.generator.generateTableSplits(tableInfo, Collections.emptyList[PartitionInfo]()))
      .thenReturn(Collections.emptyList[org.apache.fluss.client.tiering.TieringSplit]())
    val lakeCommitter = mock(classOf[LakeCommitter[AnyRef, AnyRef]])
    when(f.lakeFactory.createLakeCommitter(any(classOf[CommitterInitContext])))
      .thenReturn(lakeCommitter)
    when(f.coordinator.heartbeatAndRequestTable())
      .thenReturn(Some((1L, 2L, tablePath)))
      .thenReturn(None)
    val finished = new CountDownLatch(1)
    doAnswer {
      _ =>
        finished.countDown()
        null
    }.when(f.coordinator)
      .markTableFinished(
        org.mockito.ArgumentMatchers.eq(1L),
        org.mockito.ArgumentMatchers.eq(2L),
        any(classOf[org.apache.fluss.lake.committer.TieringStats]))
    val runner = f.runner
    try {
      runner.startAsync()
      assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue()
    } finally {
      runner.stop(5.seconds)
    }
    verify(lakeCommitter, org.mockito.Mockito.atLeastOnce()).getMissingLakeSnapshot(null)
    verify(lakeCommitter, org.mockito.Mockito.atLeastOnce()).close()
    verify(f.sc, never()).cancelJobGroup(anyString())
  }

  test("split generation reuses supplied partition metadata without querying it again") {
    val admin = mock(classOf[Admin])
    val tablePath = TablePath.of("db", "partitioned_table")
    val descriptor = TableDescriptor
      .builder()
      .schema(
        Schema.newBuilder().column("id", DataTypes.INT()).column("dt", DataTypes.STRING()).build())
      .distributedBy(2, "id")
      .partitionedBy("dt")
      .build()
    val tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, "remote", 0L, 0L)
    // The partition has one bucket even though the table default is two.
    val partition =
      new PartitionInfo(2L, ResolvedPartitionSpec.fromPartitionValue("dt", "2026-01-01"), null, 1)
    val partitionInfos = Collections.singletonList(partition)
    val noSnapshot = new CompletableFuture[org.apache.fluss.client.metadata.LakeSnapshot]()
    noSnapshot.completeExceptionally(new LakeTableSnapshotNotExistException("no lake snapshot"))
    when(admin.getLatestLakeSnapshot(tablePath)).thenReturn(noSnapshot)
    when(
      admin.listOffsets(
        eqTo(tablePath),
        eqTo(partition.getPartitionName),
        eqTo(Collections.singletonList(Int.box(0))),
        any(classOf[OffsetSpec.LatestSpec])))
      .thenReturn(new ListOffsetsResult(Collections
        .singletonMap(Int.box(0), CompletableFuture.completedFuture[java.lang.Long](5L))))

    val generator = new TieringSplitGenerator(admin)
    val splits = generator.generateTableSplits(tableInfo, partitionInfos)
    assertThat(splits.size()).isEqualTo(1)
    assertThat(splits.get(0).getTableBucket.getPartitionId).isEqualTo(Long.box(2L))
    assertThat(splits.get(0).asTieringLogSplit().getStoppingOffset).isEqualTo(5L)
    verify(admin, never()).listPartitionInfos(tablePath, true)

    when(admin.listPartitionInfos(tablePath, true))
      .thenReturn(CompletableFuture.completedFuture(partitionInfos))
    assertThat(generator.generateTableSplits(tableInfo) == splits).isTrue()
    verify(admin, times(1)).listPartitionInfos(tablePath, true)
  }

}
