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

import org.apache.fluss.client.admin.{Admin, ListOffsetsResult, OffsetSpec}
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{PartitionInfo, ResolvedPartitionSpec, Schema, TableBucket, TableDescriptor, TableInfo, TablePath}
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.types.DataTypes

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.Assertions.assertThat
import org.mockito.ArgumentMatchers.{any, anyCollection, anyString}
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, HashMap}
import java.util.concurrent.CompletableFuture

/** Tests for planning log buckets whose retained range starts at a nonzero offset. */
class AppendPlannerTest extends AnyFunSuite {

  for (partitioned <- Seq(false, true); maxRecords <- Seq(0L, 2L)) {
    test(s"skip retained-empty buckets: partitioned=$partitioned, maxRecords=$maxRecords") {
      val offsets = Map(0 -> 488L, 1 -> 565L)
      val planner = appendPlanner(partitioned, maxRecords, offsets, offsets)

      assertThat(planner.plan()).isEmpty()
    }

    test(s"retain nonempty ranges: partitioned=$partitioned, maxRecords=$maxRecords") {
      val planner =
        appendPlanner(partitioned, maxRecords, Map(0 -> 488L, 1 -> 565L), Map(0 -> 493L, 1 -> 565L))

      val partitions = planner.plan().map(_.asInstanceOf[FlussAppendInputPartition])
      val expectedRanges = if (maxRecords == 0L) {
        Seq((LogScanner.EARLIEST_OFFSET, 493L))
      } else {
        Seq((488L, 490L), (490L, 492L), (492L, 493L))
      }
      assertThat(partitions.length).isEqualTo(expectedRanges.size)
      val expectedBucket = if (partitioned) {
        new TableBucket(1L, 7L, 0)
      } else {
        new TableBucket(1L, 0)
      }
      partitions.zip(expectedRanges).foreach {
        case (partition, (start, stop)) =>
          assertThat(partition.tableBucket).isEqualTo(expectedBucket)
          assertThat(partition.startOffset).isEqualTo(start)
          assertThat(partition.stopOffset).isEqualTo(stop)
      }
    }
  }

  for (partitioned <- Seq(false, true)) {
    test(s"preserve incremental start offsets: partitioned=$partitioned") {
      val planner = appendPlanner(
        partitioned,
        0L,
        Map(0 -> 488L, 1 -> 565L),
        Map(0 -> 493L, 1 -> 565L),
        incremental = true)

      val partitions = planner.plan().map(_.asInstanceOf[FlussAppendInputPartition])
      assertThat(partitions.length).isEqualTo(1)
      assertThat(partitions.head.startOffset).isEqualTo(488L)
      assertThat(partitions.head.stopOffset).isEqualTo(493L)
    }
  }

  private def appendPlanner(
      partitioned: Boolean,
      maxRecords: Long,
      earliest: Map[Int, Long],
      latest: Map[Int, Long],
      incremental: Boolean = false): AppendPlanner = {
    val path = TablePath.of("test", "retained_log")
    val schema = Schema
      .newBuilder()
      .column("id", DataTypes.INT())
      .column("pt", DataTypes.STRING())
      .build()
    val descriptor = TableDescriptor.builder().schema(schema).distributedBy(2)
    if (partitioned) {
      descriptor.partitionedBy("pt")
    }
    val info = TableInfo.of(path, 1L, 1, descriptor.build(), null, 0L, 0L)
    val mockAdmin = mock(classOf[Admin])
    val answer = new Answer[ListOffsetsResult] {
      override def answer(invocation: InvocationOnMock): ListOffsetsResult = {
        val offsets = if (invocation.getArguments.last.isInstanceOf[OffsetSpec.LatestSpec]) {
          latest
        } else {
          earliest
        }
        val futures = new HashMap[Integer, CompletableFuture[java.lang.Long]]()
        offsets.foreach {
          case (bucket, offset) =>
            futures.put(bucket, CompletableFuture.completedFuture(java.lang.Long.valueOf(offset)))
        }
        new ListOffsetsResult(futures)
      }
    }
    when(
      mockAdmin
        .listOffsets(any(classOf[TablePath]), anyCollection[Integer](), any(classOf[OffsetSpec])))
      .thenAnswer(answer)
    when(
      mockAdmin.listOffsets(
        any(classOf[TablePath]),
        anyString(),
        anyCollection[Integer](),
        any(classOf[OffsetSpec]))).thenAnswer(answer)
    if (partitioned) {
      val partition =
        new PartitionInfo(7L, ResolvedPartitionSpec.fromPartitionValue("pt", "expired"), null, 2)
      when(mockAdmin.listPartitionInfos(path))
        .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(partition)))
    }
    val config = new Configuration()
    config.setLong(SparkFlussConf.SCAN_MAX_RECORDS_PER_PARTITION, maxRecords)
    val options = if (incremental) {
      Collections.singletonMap(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key(), "1000")
    } else {
      Collections.emptyMap[String, String]()
    }
    new AppendPlanner(
      path,
      info,
      None,
      None,
      Array(0, 1),
      new CaseInsensitiveStringMap(options),
      config) {
      override protected def admin: Admin = mockAdmin
    }
  }
}
