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

import org.apache.fluss.config.Configuration
import org.apache.fluss.lake.source.LakeSplit
import org.apache.fluss.metadata.{Schema, TableBucket, TableInfo, TablePath}
import org.apache.fluss.spark.read.lake.{FlussLakeInputPartition, FlussLakeUpsertInputPartition}
import org.apache.fluss.types.DataTypes

import org.apache.spark.sql.connector.expressions.{Expression, Expressions, Literal}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.Assertions.assertThat
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

import scala.collection.JavaConverters._

/** Verifies partition metadata, runtime filter semantics and the batch scan lifecycle. */
class FlussRuntimeFilteringTest extends AnyFunSuite {

  private val path = TablePath.of("db", "t")
  private val options = new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())
  private val partitionedInfo = tableInfo(Seq("dt", "region"))
  private val values =
    Map(10L -> Seq("a", "east"), 20L -> Seq("b", "west"), 30L -> Seq("a", "west"))
  private val window = Some(FlussTimeRange(100L, 200L))
  private val append = Array[InputPartition](
    FlussAppendInputPartition(bucket(10L), 4L, 9L, window),
    FlussAppendInputPartition(bucket(20L), 1L, 7L, window),
    FlussAppendInputPartition(bucket(30L), 2L, 8L, window),
    FlussAppendInputPartition(bucket(10L), 9L, 12L, window)
  )

  test("filter attributes use projected partition columns and quote special names") {
    val special = tableInfo(Seq("a.b", "a`b", "with space"))
    val scan = testScan(special, Array.empty[InputPartition], Map.empty)
    assertThat(scan.filterAttributes().map(_.fieldNames().toSeq))
      .containsExactly(Seq("a.b"), Seq("a`b"), Seq("with space"))
    val projected = new TestScan(
      special,
      Some(new StructType().add("a`b", StringType)),
      mock(classOf[AppendPlanner]),
      mock(classOf[Batch]))
    assertThat(projected.filterAttributes().map(_.fieldNames().toSeq))
      .containsExactly(Seq("a`b"))
    assertThat(testScan(tableInfo(Seq.empty), append, Map.empty).filterAttributes().length)
      .isZero()
    val emptyProjection =
      new TestScan(
        partitionedInfo,
        Some(new StructType()),
        mock(classOf[AppendPlanner]),
        mock(classOf[Batch]))
    assertThat(emptyProjection.filterAttributes().length).isZero()
    scan.filter(Array(in("`a.b`", "a")))
    assertThat(scan.description()).contains("RuntimePredicates")
  }

  test("runtime predicates intersect while retaining initial split order and boundaries") {
    val scan = testScan(partitionedInfo, append, values)
    val batch = scan.toBatch
    val initial = batch.planInputPartitions()
    // A caller must not be able to mutate the cached plan through the returned array.
    initial(0) = append(1)
    scan.filter(Array(in("dt", "a"), in("region", "east", "west")))
    assertPartitions(scan, Seq(append(0), append(2), append(3)))
    scan.filter(Array(in("region", "east")))
    assertPartitions(scan, Seq(append(0), append(3)))
    scan.filter(Array(in("dt", "b")))
    assertPartitions(scan, Seq.empty)
    scan.filter(Array(in("dt", "a")))
    assertPartitions(scan, Seq.empty)
    assertThat(scan.toBatch).isSameAs(batch)
    verify(scan.source, times(1)).planInputPartitions()
    verify(scan.planner, times(1)).partitionValuesById
  }

  test("filter before planning captures one plan and ignores subsequent planner changes") {
    val scan = testScan(partitionedInfo, append, values)
    scan.filter(Array(in("dt", "a")))
    when(scan.source.planInputPartitions()).thenReturn(Array(append(1)))
    when(scan.planner.partitionValuesById).thenReturn(Map(10L -> Seq("b", "east")))
    assertPartitions(scan, Seq(append(0), append(2), append(3)))
    scan.filter(Array(in("region", "east")))
    assertPartitions(scan, Seq(append(0), append(3)))
    verify(scan.source, times(1)).planInputPartitions()
  }

  test("empty and unsupported filters are no-ops, empty partition IN removes all splits") {
    val scan = testScan(partitionedInfo, append, values)
    scan.filter(Array.empty[Predicate])
    scan.filter(Array(in("id"), in("missing"), new Predicate("UNSUPPORTED", Array.empty)))
    verify(scan.source, times(0)).planInputPartitions()
    assertPartitions(scan, append.toSeq)
    scan.filter(Array(in("dt")))
    assertPartitions(scan, Seq.empty)
    val unpartitioned = testScan(tableInfo(Seq.empty), append, Map.empty)
    unpartitioned.filter(Array(in("id")))
    assertPartitions(unpartitioned, append.toSeq)
  }

  test("null IN literals follow SQL semantics and static pruning stays in effect") {
    val scan = testScan(partitionedInfo, Array(append(1), append(2)), values)
    scan.filter(Array(in("dt", "a", null)))
    assertPartitions(scan, Seq(append(2)))
    scan.filter(Array(in("dt", null)))
    assertPartitions(scan, Seq.empty)
    val nullPartition = testScan(partitionedInfo, Array(append(0)), Map(10L -> Seq(null, "east")))
    nullPartition.filter(
      Array(
        new Predicate("<=>", Array[Expression](Expressions.column("dt"), lit(null, StringType)))))
    assertPartitions(nullPartition, Seq(append(0)))
  }

  test("lake-only, Fluss-only and combined splits use their own partition metadata") {
    val lakeA = lakeSplit(Seq("a", "east"))
    val lakeB = lakeSplit(Seq("b", "west"))
    val unknown = lakeSplit(Seq("a"))
    val splits = Array[InputPartition](
      FlussLakeInputPartition(bucket(-1L), lakeA),
      FlussLakeInputPartition(bucket(-1L), lakeB),
      FlussLakeUpsertInputPartition(bucket(10L), Seq(lakeA).asJava, 5L, 9L),
      FlussLakeUpsertInputPartition(bucket(20L), null, 2L, 8L),
      FlussLakeUpsertInputPartition(bucket(-1L), Seq(lakeB).asJava, 5L, 5L),
      FlussLakeInputPartition(bucket(-1L), unknown),
      FlussUpsertInputPartition(bucket(30L), 42L, 6L, 11L, window),
      FlussAppendInputPartition(bucket(99L), 2L, 7L, window)
    )
    val scan = testScan(partitionedInfo, splits, values)
    scan.filter(Array(in("dt", "a")))
    assertPartitions(scan, Seq(splits(0), splits(2), splits(5), splits(6), splits(7)))
    // Even incomplete metadata cannot match an empty runtime key set.
    scan.filter(Array(in("region")))
    assertPartitions(scan, Seq.empty)
  }

  test("incomplete lake metadata and missing Fluss partition ids are retained conservatively") {
    val splits = Array[InputPartition](
      FlussLakeInputPartition(bucket(-1L), lakeSplit(null)),
      FlussLakeInputPartition(bucket(-1L), lakeSplit(Seq.empty)),
      FlussLakeInputPartition(bucket(-1L), lakeSplit(Seq("a", "east", "extra"))),
      FlussLakeUpsertInputPartition(bucket(99L), Seq(lakeSplit(Seq("b", "west"))).asJava, 1L, 2L),
      new InputPartition {}
    )
    val scan = testScan(partitionedInfo, splits, values)
    scan.filter(Array(in("dt", "a")))
    assertPartitions(scan, splits.toSeq)
    val numericInfo = tableInfo(Seq("dt"), numeric = true)
    val invalid = testScan(numericInfo, Array(append(0)), Map(10L -> Seq("invalid")))
    invalid.filter(
      Array(
        new Predicate(
          "IN",
          Array[Expression](Expressions.column("dt"), lit(Integer.valueOf(1), IntegerType)))))
    assertPartitions(invalid, Seq(append(0)))
  }

  test("append and upsert scans reuse their batches and original reader factories") {
    val appendPlanner = mock(classOf[AppendPlanner])
    when(appendPlanner.plan()).thenReturn(append)
    when(appendPlanner.partitionValuesById).thenReturn(values)
    val appendScan = FlussAppendScan(
      path,
      partitionedInfo,
      None,
      None,
      None,
      Seq.empty,
      None,
      options,
      new Configuration(),
      appendPlanner)
    val upsertPlanner = mock(classOf[UpsertPlanner])
    val upsert = FlussUpsertInputPartition(bucket(10L), 42L, 6L, 11L, window)
    when(upsertPlanner.plan()).thenReturn(Array[InputPartition](upsert))
    when(upsertPlanner.partitionValuesById).thenReturn(values)
    val upsertScan = FlussUpsertScan(
      path,
      partitionedInfo,
      None,
      None,
      None,
      Seq.empty,
      None,
      options,
      new Configuration(),
      upsertPlanner)
    Seq(appendScan, upsertScan).foreach {
      scan =>
        val batch = scan.toBatch
        val factory = batch.createReaderFactory()
        batch.planInputPartitions()
        scan.filter(Array(in("dt", "a")))
        assertThat(scan.toBatch).isSameAs(batch)
        assertThat(scan.toBatch.createReaderFactory()).isSameAs(factory)
        assertThat(scan.toBatch.planInputPartitions().length).isPositive()
    }
    assertThat(upsertScan.toBatch.planInputPartitions()(0)).isSameAs(upsert)
    verify(appendPlanner, times(1)).plan()
    verify(upsertPlanner, times(1)).plan()
  }

  private def tableInfo(keys: Seq[String], numeric: Boolean = false): TableInfo = {
    val schema = Schema.newBuilder().column("id", DataTypes.INT())
    keys.foreach(k => schema.column(k, if (numeric) DataTypes.INT() else DataTypes.STRING()))
    new TableInfo(
      path,
      1L,
      1,
      schema.build(),
      Collections.emptyList[String](),
      keys.asJava,
      1,
      new Configuration(),
      new Configuration(),
      null,
      null,
      0L,
      0L)
  }

  private def bucket(id: Long): TableBucket = new TableBucket(1L, id, 0)

  private def in(column: String, values: String*): Predicate =
    new Predicate(
      "IN",
      Array[Expression](Expressions.column(column)) ++
        values.map(v => lit(v, StringType)))

  private def lit[T](v: T, tpe: DataType): Literal[T] = new Literal[T] {
    override def value(): T = v
    override def dataType(): DataType = tpe
  }

  private def lakeSplit(values: Seq[String]): LakeSplit = {
    val split = mock(classOf[LakeSplit])
    when(split.partition()).thenReturn(if (values == null) null else values.asJava)
    split
  }

  private def testScan(
      partitionedInfo: TableInfo,
      partitions: Array[InputPartition],
      values: Map[Long, Seq[String]]): TestScan = {
    val planner = mock(classOf[AppendPlanner])
    when(planner.partitionValuesById).thenReturn(values)
    val source = mock(classOf[Batch])
    when(source.planInputPartitions()).thenReturn(partitions)
    when(source.createReaderFactory()).thenReturn(mock(classOf[PartitionReaderFactory]))
    new TestScan(partitionedInfo, None, planner, source)
  }

  private def assertPartitions(scan: FlussScan, expected: Seq[InputPartition]): Unit =
    assertThat(scan.toBatch.planInputPartitions())
      .containsExactlyElementsOf(expected.asJava)

  private class TestScan(
      val tableInfo: TableInfo,
      val requiredSchema: Option[StructType],
      val planner: AppendPlanner,
      val source: Batch)
    extends FlussScan {
    override def tablePath: TablePath = path
    override protected def scanType: String = "Test"
    override protected def createBatch(): Batch = source
  }
}
