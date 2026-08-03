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

package org.apache.fluss.spark.utils

import org.apache.fluss.metadata.{PartitionInfo, ResolvedPartitionSpec, Schema, TableDescriptor, TableInfo, TablePath}
import org.apache.fluss.predicate.{CompoundPredicate, LeafPredicate, PredicateBuilder}
import org.apache.fluss.row.BinaryString
import org.apache.fluss.types.DataTypes
import org.apache.fluss.utils.PartitionUtils

import org.apache.spark.sql.connector.expressions.{Expression, Expressions, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{Or, Predicate}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class SparkPartitionPredicateTest extends AnyFunSuite {

  // Log table partitioned by a single STRING column.
  private val singleKeyTable: TableInfo = tableInfo(
    columns =
      Seq(("orderId", DataTypes.BIGINT()), ("amount", DataTypes.INT()), ("dt", DataTypes.STRING())),
    partitionKeys = Seq("dt")
  )

  // Log table partitioned by two STRING columns, in declaration order dt, region.
  private val multiKeyTable: TableInfo = tableInfo(
    columns = Seq(
      ("orderId", DataTypes.BIGINT()),
      ("dt", DataTypes.STRING()),
      ("region", DataTypes.STRING())),
    partitionKeys = Seq("dt", "region")
  )

  // Primary key table partitioned by a non-STRING (INT) column. Fluss requires the partition key to
  // be a subset of the primary key for a partitioned PK table.
  private val pkIntKeyTable: TableInfo = tableInfo(
    columns =
      Seq(("id", DataTypes.BIGINT()), ("amount", DataTypes.INT()), ("day", DataTypes.INT())),
    partitionKeys = Seq("day"),
    primaryKeys = Seq("id", "day")
  )

  private val nonPartitionedTable: TableInfo = tableInfo(
    columns = Seq(("orderId", DataTypes.BIGINT()), ("amount", DataTypes.INT())),
    partitionKeys = Seq.empty
  )

  // -----------------------------------------------------------------------------------------------
  // matchesPartition — the guard protecting the lake-only split branch of the planners.
  // -----------------------------------------------------------------------------------------------

  test("matchesPartition accepts a partition whose values satisfy the predicate") {
    val predicate = partitionPredicateOf(singleKeyTable, dtEquals("2026-01-01"))
    assertThat(
      SparkPartitionPredicate
        .matchesPartition(singleKeyTable, Seq("2026-01-01"), predicate)).isTrue
  }

  test("matchesPartition rejects a partition whose values violate the predicate") {
    val predicate = partitionPredicateOf(singleKeyTable, dtEquals("2026-01-01"))
    assertThat(
      SparkPartitionPredicate
        .matchesPartition(singleKeyTable, Seq("2026-01-02"), predicate)).isFalse
  }

  // Regression guard for the lake-only split branch of AppendPlanner.planLakePartitionedTable /
  // UpsertPlanner.planLakePartitionedTable: a LakeSplit that reports no partition for a partitioned
  // table used to be admitted unconditionally. Because the scan builder removes the partition
  // predicate from the post-scan filters returned to Spark, such a split is never re-filtered, so
  // admitting it silently leaks rows from non-matching partitions.
  test("matchesPartition rejects empty partition values when a predicate is present") {
    val predicate = partitionPredicateOf(singleKeyTable, dtEquals("2026-01-01"))
    assertThatThrownBy(
      () =>
        SparkPartitionPredicate
          .matchesPartition(singleKeyTable, Seq.empty, predicate))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessageContaining("expected 1 partition value(s)")
      .hasMessageContaining("[dt]")
      .hasMessageContaining("reported 0")
  }

  test("matchesPartition rejects a partial partition value tuple on a multi-key table") {
    val predicate = partitionPredicateOf(multiKeyTable, dtEquals("2026-01-01"))
    assertThatThrownBy(
      () =>
        SparkPartitionPredicate
          .matchesPartition(multiKeyTable, Seq("2026-01-01"), predicate))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessageContaining("expected 2 partition value(s)")
      .hasMessageContaining("[dt, region]")
      .hasMessageContaining("reported 1")
  }

  test("matchesPartition rejects a partition value tuple longer than the partition keys") {
    val predicate = partitionPredicateOf(singleKeyTable, dtEquals("2026-01-01"))
    assertThatThrownBy(
      () =>
        SparkPartitionPredicate
          .matchesPartition(singleKeyTable, Seq("2026-01-01", "cn"), predicate))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessageContaining("reported 2")
  }

  // Without a predicate there is nothing to prune, so arity is irrelevant and the split must still
  // be admitted. This pins down that the guard above did not become over-strict.
  test("matchesPartition admits any partition when no predicate is present") {
    assertThat(SparkPartitionPredicate.matchesPartition(singleKeyTable, Seq.empty, None)).isTrue
    assertThat(
      SparkPartitionPredicate.matchesPartition(singleKeyTable, Seq("2026-01-01"), None)).isTrue
    assertThat(
      SparkPartitionPredicate.matchesPartition(multiKeyTable, Seq("only-one"), None)).isTrue
  }

  test("matchesPartition evaluates all keys of a multi-key partition") {
    val predicate = partitionPredicateOf(
      multiKeyTable,
      pred("=", ref("dt"), lit(UTF8String.fromString("2026-01-01"), StringType)),
      pred("=", ref("region"), lit(UTF8String.fromString("cn"), StringType))
    )
    assertThat(
      SparkPartitionPredicate
        .matchesPartition(multiKeyTable, Seq("2026-01-01", "cn"), predicate)).isTrue
    // second key differs
    assertThat(
      SparkPartitionPredicate
        .matchesPartition(multiKeyTable, Seq("2026-01-01", "us"), predicate)).isFalse
  }

  // Partition values always arrive as strings and are parsed against the partition row type, so a
  // non-STRING partition key must still compare correctly against the Spark literal.
  test("matchesPartition parses non-string partition values against the partition row type") {
    val predicate = partitionPredicateOf(
      pkIntKeyTable,
      pred(">", ref("day"), lit(Integer.valueOf(20260101), IntegerType)))
    assertThat(
      SparkPartitionPredicate.matchesPartition(pkIntKeyTable, Seq("20260102"), predicate)).isTrue
    assertThat(
      SparkPartitionPredicate.matchesPartition(pkIntKeyTable, Seq("20260101"), predicate)).isFalse
  }

  // -----------------------------------------------------------------------------------------------
  // extract — splitting partition-key predicates out of the pushed-down predicate list.
  // -----------------------------------------------------------------------------------------------

  test("extract returns all predicates as non-partition for a non-partitioned table") {
    val predicates = Seq(pred("=", ref("amount"), lit(Integer.valueOf(1), IntegerType)))
    val (nonPartition, partition) =
      SparkPartitionPredicate.extract(nonPartitionedTable, predicates)
    assert(nonPartition == predicates)
    assertThat(partition.isDefined).isFalse
  }

  test("extract separates a partition-key predicate from a data predicate") {
    val dtPred = dtEquals("2026-01-01")
    val amountPred = pred(">", ref("amount"), lit(Integer.valueOf(600), IntegerType))
    val (nonPartition, partition) =
      SparkPartitionPredicate.extract(singleKeyTable, Seq(dtPred, amountPred))

    assert(nonPartition == Seq(amountPred))
    assertThat(partition.isDefined).isTrue
    assertThat(partition.get).isInstanceOf(classOf[LeafPredicate])
    // Field index is relative to the partition row type, where dt is the only column.
    assertThat(partition.get)
      .isEqualTo(
        new PredicateBuilder(PartitionUtils.partitionRowType(singleKeyTable))
          .equal(0, BinaryString.fromString("2026-01-01")))
  }

  test("extract AND-combines multiple partition-key predicates") {
    val dtPred = dtEquals("2026-01-01")
    val regionPred = pred("=", ref("region"), lit(UTF8String.fromString("cn"), StringType))
    val (nonPartition, partition) =
      SparkPartitionPredicate.extract(multiKeyTable, Seq(dtPred, regionPred))

    assertThat(nonPartition.isEmpty).isTrue
    assertThat(partition.isDefined).isTrue
    assertThat(partition.get).isInstanceOf(classOf[CompoundPredicate])
  }

  // An OR spanning a partition key and a data column cannot be answered by partition pruning alone,
  // so it must stay in the non-partition list for Spark to re-apply.
  test("extract keeps an OR mixing partition and non-partition columns as non-partition") {
    val mixed: Predicate = new Or(
      dtEquals("2026-01-01"),
      pred(">", ref("amount"), lit(Integer.valueOf(600), IntegerType)))
    val (nonPartition, partition) = SparkPartitionPredicate.extract(singleKeyTable, Seq(mixed))

    assert(nonPartition == Seq(mixed))
    assertThat(partition.isDefined).isFalse
  }

  test("extract keeps an unconvertible predicate as non-partition") {
    // NOT on a string equality is not invertible by SparkPredicateConverter.
    val notEq: Predicate =
      new org.apache.spark.sql.connector.expressions.filter.Not(dtEquals("2026-01-01"))
    val (nonPartition, partition) = SparkPartitionPredicate.extract(singleKeyTable, Seq(notEq))

    assert(nonPartition == Seq(notEq))
    assertThat(partition.isDefined).isFalse
  }

  // -----------------------------------------------------------------------------------------------
  // filterPartitions — pruning the Fluss partition list at planning time.
  // -----------------------------------------------------------------------------------------------

  test("filterPartitions keeps only the partitions matching the predicate") {
    val partitions = Seq(
      partitionInfo(1L, singleKeyTable, Seq("2026-01-01")),
      partitionInfo(2L, singleKeyTable, Seq("2026-01-02")),
      partitionInfo(3L, singleKeyTable, Seq("2026-01-03"))
    )
    val predicate = partitionPredicateOf(singleKeyTable, dtEquals("2026-01-02"))

    val kept = SparkPartitionPredicate.filterPartitions(singleKeyTable, partitions, predicate)
    assert(kept.map(_.getPartitionId) == Seq(2L))
  }

  test("filterPartitions returns every partition when no predicate is present") {
    val partitions = Seq(
      partitionInfo(1L, singleKeyTable, Seq("2026-01-01")),
      partitionInfo(2L, singleKeyTable, Seq("2026-01-02")))
    val kept = SparkPartitionPredicate.filterPartitions(singleKeyTable, partitions, None)
    assert(kept.map(_.getPartitionId) == Seq(1L, 2L))
  }

  test("filterPartitions can prune everything away") {
    val partitions = Seq(partitionInfo(1L, singleKeyTable, Seq("2026-01-01")))
    val predicate = partitionPredicateOf(singleKeyTable, dtEquals("2099-01-01"))
    val kept = SparkPartitionPredicate.filterPartitions(singleKeyTable, partitions, predicate)
    assertThat(kept.isEmpty).isTrue
  }

  test("filterPartitions prunes a multi-key partition list") {
    val partitions = Seq(
      partitionInfo(1L, multiKeyTable, Seq("2026-01-01", "cn")),
      partitionInfo(2L, multiKeyTable, Seq("2026-01-01", "us")),
      partitionInfo(3L, multiKeyTable, Seq("2026-01-02", "cn"))
    )
    val predicate = partitionPredicateOf(
      multiKeyTable,
      dtEquals("2026-01-01"),
      pred("=", ref("region"), lit(UTF8String.fromString("cn"), StringType)))

    val kept = SparkPartitionPredicate.filterPartitions(multiKeyTable, partitions, predicate)
    assert(kept.map(_.getPartitionId) == Seq(1L))
  }

  // -----------------------------------------------------------------------------------------------
  // helpers
  // -----------------------------------------------------------------------------------------------

  /** Runs the predicates through `extract` and returns the partition predicate it produced. */
  private def partitionPredicateOf(
      table: TableInfo,
      predicates: Predicate*): Option[org.apache.fluss.predicate.Predicate] = {
    val (_, partition) = SparkPartitionPredicate.extract(table, predicates)
    assert(partition.isDefined, s"Expected $predicates to yield a partition predicate")
    partition
  }

  private def dtEquals(value: String): Predicate =
    pred("=", ref("dt"), lit(UTF8String.fromString(value), StringType))

  private def partitionInfo(
      partitionId: Long,
      table: TableInfo,
      partitionValues: Seq[String]): PartitionInfo =
    new PartitionInfo(
      partitionId,
      new ResolvedPartitionSpec(table.getPartitionKeys, partitionValues.asJava),
      null)

  private def tableInfo(
      columns: Seq[(String, org.apache.fluss.types.DataType)],
      partitionKeys: Seq[String],
      primaryKeys: Seq[String] = Seq.empty): TableInfo = {
    val schemaBuilder = Schema.newBuilder()
    columns.foreach { case (name, tpe) => schemaBuilder.column(name, tpe) }
    if (primaryKeys.nonEmpty) {
      schemaBuilder.primaryKey(primaryKeys.asJava)
    }
    val descriptorBuilder = TableDescriptor
      .builder()
      .schema(schemaBuilder.build())
      .distributedBy(1)
    if (partitionKeys.nonEmpty) {
      descriptorBuilder.partitionedBy(partitionKeys.asJava)
    }
    val now = System.currentTimeMillis()
    TableInfo.of(TablePath.of("db", "t"), 1L, 1, descriptorBuilder.build(), null, now, now)
  }

  private def ref(name: String): NamedReference = Expressions.column(name)

  private def lit[T](v: T, dt: DataType): Literal[T] = new Literal[T] {
    override def value(): T = v
    override def dataType(): DataType = dt
    override def children(): Array[Expression] = Array.empty
  }

  private def pred(name: String, children: Expression*): Predicate =
    new Predicate(name, children.toArray)
}
