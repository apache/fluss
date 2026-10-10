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
import org.apache.fluss.metadata.{TableBucket, TableInfo, TablePath}
import org.apache.fluss.predicate.{Predicate => FlussPredicate}
import org.apache.fluss.spark.SparkConversions
import org.apache.fluss.spark.read.lake.{FlussLakeInputPartition, FlussLakeUpsertInputPartition}
import org.apache.fluss.spark.utils.SparkPartitionPredicate

import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** An interface that extends from Spark [[Scan]]. */
trait FlussScan extends Scan with SupportsRuntimeV2Filtering {
  def tableInfo: TableInfo

  def tablePath: TablePath

  def requiredSchema: Option[StructType]

  /** Spark predicates that the scan reports as pushed down (used in [[description]]). */
  def pushedSparkPredicates: Seq[Predicate] = Seq.empty

  def partitionPredicate: Option[FlussPredicate] = None

  def limit: Option[Int] = None

  def timeRange: Option[FlussTimeRange] = None

  /** The planner used to capture partition metadata together with the initial splits. */
  def planner: SplitPlanner

  /** Creates the batch whose original splits and reader factory are reused for runtime pruning. */
  protected def createBatch(): Batch

  protected def scanType: String

  private var runtimePredicates: Seq[Predicate] = Seq.empty
  private var filteredPartitions: Option[Array[InputPartition]] = None
  private var initialPartitionValues: Map[Long, Seq[String]] = Map.empty

  private lazy val originalBatch: Batch = createBatch()

  // Spark calls toBatch again after runtime filtering. Never re-plan offsets or snapshots.
  private lazy val initialPartitions: Array[InputPartition] = {
    val partitions = originalBatch.planInputPartitions()
    initialPartitionValues = planner.partitionValuesById
    partitions
  }

  private lazy val batch: Batch = new Batch {
    private lazy val readerFactory = originalBatch.createReaderFactory()

    override def planInputPartitions(): Array[InputPartition] =
      filteredPartitions.getOrElse(initialPartitions).clone()

    override def createReaderFactory(): PartitionReaderFactory = readerFactory
  }

  override def toBatch: Batch = batch

  override def filterAttributes(): Array[NamedReference] =
    tableInfo.getPartitionKeys.asScala
      .filter(key => readSchema().fields.count(_.name == key) == 1)
      .map(key => Expressions.column(s"`${key.replace("`", "``")}`"))
      .toArray

  override def filter(predicates: Array[Predicate]): Unit = {
    if (predicates.isEmpty || !tableInfo.isPartitioned) {
      return
    }
    val (unsupported, partitionFilter) =
      SparkPartitionPredicate.extract(tableInfo, predicates.toSeq)
    // The shared converter deliberately rejects empty IN. Runtime IN on a partition key,
    // however, means the join produced no keys, so even splits with unknown metadata can go.
    val emptyIn = predicates.filter {
      p =>
        p.name() == "IN" && (p.children() match {
          case Array(ref: NamedReference) =>
            ref.fieldNames().length == 1 &&
            tableInfo.getPartitionKeys.contains(ref.fieldNames()(0))
          case _ => false
        })
    }
    if (partitionFilter.isEmpty && emptyIn.isEmpty) {
      return
    }
    val current = filteredPartitions.getOrElse(initialPartitions)
    filteredPartitions = Some(
      if (emptyIn.nonEmpty) Array.empty[InputPartition]
      else current.filter(matchesPartition(_, partitionFilter)))
    runtimePredicates ++= predicates.filterNot(unsupported.contains) ++ emptyIn
  }

  private def matchesPartition(
      partition: InputPartition,
      predicate: Option[FlussPredicate]): Boolean = {
    def matches(values: Seq[String]): Boolean = {
      if (values.size != tableInfo.getPartitionKeys.size()) {
        true
      } else {
        try {
          SparkPartitionPredicate.matchesPartition(tableInfo, values, predicate)
        } catch {
          case NonFatal(_) => true
        }
      }
    }

    def matchesBucket(bucket: TableBucket): Boolean =
      Option(bucket.getPartitionId)
        .flatMap(id => initialPartitionValues.get(id.longValue()))
        .forall(matches)

    partition match {
      case p: FlussAppendInputPartition => matchesBucket(p.tableBucket)
      case p: FlussUpsertInputPartition => matchesBucket(p.tableBucket)
      case p: FlussLakeInputPartition =>
        matches(Option(p.lakeSplit.partition()).map(_.asScala.toSeq).getOrElse(Seq.empty))
      case p: FlussLakeUpsertInputPartition =>
        val lakeMatches = Option(p.lakeSplits).exists(_.asScala.exists {
          split => matches(Option(split.partition()).map(_.asScala.toSeq).getOrElse(Seq.empty))
        })
        val hasLogTail = p.logStartingOffset < p.logStoppingOffset
        lakeMatches || (hasLogTail && matchesBucket(p.tableBucket))
      case _ => true
    }
  }

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
    val withTimeRange = timeRange match {
      case Some(r) if r.endMs == Long.MaxValue =>
        s"$withPartition [TimeRange: [${r.startMs}, latest)]"
      case Some(r) => s"$withPartition [TimeRange: [${r.startMs}, ${r.endMs})]"
      case None => withPartition
    }
    val withRuntime =
      if (runtimePredicates.isEmpty) withTimeRange
      else s"$withTimeRange [RuntimePredicates: ${runtimePredicates.mkString("[", ", ", "]")}]"
    limit match {
      case Some(l) => s"$withRuntime [Limit: $l]"
      case None => withRuntime
    }
  }

  override def supportedCustomMetrics(): Array[CustomMetric] =
    Array(FlussNumRowsReadMetric())
}

/**
 * Fluss Append (log-table) scan. Whether the underlying batch reads from Fluss only or unions Fluss
 * with a lake snapshot is determined by the [[AppendSplitPlanner]] instance passed in from the
 * ScanBuilder. Description reflects the planner category.
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

  override protected lazy val scanType: String =
    if (planner.hasLakeSnapshot) "LakeAppend" else "Append"

  override def timeRange: Option[FlussTimeRange] = planner.timeRange

  override protected def createBatch(): Batch = {
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
 * unions Fluss with a lake snapshot is determined by the [[UpsertSplitPlanner]] instance passed in
 * from the ScanBuilder.
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

  override protected lazy val scanType: String =
    if (planner.hasLakeSnapshot) "LakeUpsert" else "Upsert"

  override def timeRange: Option[FlussTimeRange] = planner.timeRange

  override protected def createBatch(): Batch = {
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
