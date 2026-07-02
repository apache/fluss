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
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.predicate.{Predicate => FlussPredicate}
import org.apache.fluss.spark.SparkConversions

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** An interface that extends from Spark [[Scan]]. */
trait FlussScan extends Scan {
  def tableInfo: TableInfo

  def tablePath: TablePath

  def requiredSchema: Option[StructType]

  /** Spark predicates that the scan reports as pushed down (used in [[description]]). */
  def pushedSparkPredicates: Seq[Predicate] = Seq.empty

  def partitionPredicate: Option[FlussPredicate] = None

  def limit: Option[Int] = None

  protected def scanType: String

  override def readSchema(): StructType = {
    requiredSchema.getOrElse(SparkConversions.toSparkDataType(tableInfo.getRowType))
  }

  override def description(): String = {
    val base = s"FlussScan: [$tablePath], Type: [$scanType]"
    val withPushed =
      if (pushedSparkPredicates.isEmpty) base
      else s"$base [PushedPredicates: ${pushedSparkPredicates.mkString("[", ", ", "]")}]"
    val withPartition = partitionPredicate match {
      case Some(p) => s"$withPushed [PartitionFilter: $p]"
      case None => withPushed
    }
    limit match {
      case Some(l) => s"$withPartition [Limit: $l]"
      case None => withPartition
    }
  }

  override def supportedCustomMetrics(): Array[CustomMetric] =
    Array(FlussNumRowsReadMetric())
}

/**
 * Fluss Append (log-table) scan. Whether the underlying batch reads from Fluss only or unions
 * Fluss with a lake snapshot is determined by the [[AppendSplitPlanner]] instance passed in from
 * the ScanBuilder. Description reflects the planner category.
 */
case class FlussAppendScan(
    tablePath: TablePath,
    tableInfo: TableInfo,
    requiredSchema: Option[StructType],
    pushedPredicate: Option[FlussPredicate],
    override val partitionPredicate: Option[FlussPredicate],
    override val pushedSparkPredicates: Seq[Predicate],
    override val limit: Option[Int],
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration,
    planner: AppendSplitPlanner)
  extends FlussScan {

  override protected val scanType: String =
    if (planner.hasLakeSnapshot) "LakeAppend" else "Append"

  override def toBatch: Batch = {
    new FlussAppendBatch(
      tablePath,
      tableInfo,
      readSchema,
      pushedPredicate,
      limit,
      options,
      flussConfig,
      planner)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new FlussAppendMicroBatchStream(
      tablePath,
      tableInfo,
      readSchema,
      options,
      flussConfig,
      checkpointLocation)
  }
}

/**
 * Fluss Upsert (primary-key table) scan. Whether the underlying batch reads from Fluss only or
 * unions Fluss with a lake snapshot is determined by the [[UpsertSplitPlanner]] instance passed
 * in from the ScanBuilder.
 */
case class FlussUpsertScan(
    tablePath: TablePath,
    tableInfo: TableInfo,
    requiredSchema: Option[StructType],
    pushedPredicate: Option[FlussPredicate],
    override val partitionPredicate: Option[FlussPredicate],
    override val pushedSparkPredicates: Seq[Predicate],
    override val limit: Option[Int],
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration,
    planner: UpsertSplitPlanner)
  extends FlussScan {

  override protected val scanType: String =
    if (planner.hasLakeSnapshot) "LakeUpsert" else "Upsert"

  override def toBatch: Batch = {
    new FlussUpsertBatch(
      tablePath,
      tableInfo,
      readSchema,
      pushedPredicate,
      limit,
      options,
      flussConfig,
      planner)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new FlussUpsertMicroBatchStream(
      tablePath,
      tableInfo,
      readSchema,
      options,
      flussConfig,
      checkpointLocation)
  }
}
