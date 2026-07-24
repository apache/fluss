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

import org.apache.fluss.config.{Configuration => FlussConfiguration}
import org.apache.fluss.metadata.{LakeTableUtil, LogFormat, TableInfo, TablePath}
import org.apache.fluss.predicate.{Predicate => FlussPredicate}
import org.apache.fluss.spark.read.lake.FlussLakeUtils
import org.apache.fluss.spark.utils.{SparkPartitionPredicate, SparkPredicateConverter}

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Collections, IdentityHashMap, Set => JSet}

import scala.collection.JavaConverters._

/** An interface that extends from Spark [[ScanBuilder]]. */
trait FlussScanBuilder
  extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with SupportsPushDownLimit {

  protected var requiredSchema: Option[StructType] = None
  protected var limit: Option[Int] = None

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = Some(requiredSchema)
  }

  override def pushLimit(limit: Int): Boolean = {
    this.limit = Some(limit)
    true
  }
}

/** Extracts a partition-key predicate so the scan can skip partitions that can't match. */
trait FlussSupportsPushDownPartitionFilters
  extends FlussScanBuilder
  with SupportsPushDownV2Filters {

  def tableInfo: TableInfo

  protected var partitionPredicate: Option[FlussPredicate] = None
  protected var pushedPredicate: Option[FlussPredicate] = None
  protected var acceptedPredicates: Array[Predicate] = Array.empty[Predicate]

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (nonPartitionPred, partitionPred) =
      SparkPartitionPredicate.extract(tableInfo, predicates.toSeq)
    partitionPredicate = partitionPred
    nonPartitionPred.toArray
  }

  override def pushedPredicates(): Array[Predicate] = acceptedPredicates
}

/**
 * Data-predicate push-down. Two branches:
 *   1. Non-PK log tables with ARROW format: converts predicates to server-side batch filters.
 *   2. Lake-enabled tables (including PK tables): probes the lake source for which predicates it
 *      accepts, reports those back to Spark, and passes the accepted predicate to the planner which
 *      applies it on the lake-union path.
 *
 * This consolidates the former `FlussSupportsPushDownV2Filters` (branch 1) and
 * `FlussLakeSupportsPushDownV2Filters` (branch 2) into a single trait.
 */
trait FlussSupportsPushDownV2Filters extends FlussSupportsPushDownPartitionFilters {

  def tablePath: TablePath

  def flussConfig: FlussConfiguration

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val nonPartition = super.pushPredicates(predicates)
    if (!tableInfo.hasPrimaryKey && tableInfo.getTableConfig.getLogFormat == LogFormat.ARROW) {
      // Server-side batch filter for log table only supports ARROW; other log formats reject it.
      val (predicate, accepted) =
        SparkPredicateConverter.convertPredicates(tableInfo.getRowType, nonPartition.toSeq)
      pushedPredicate = predicate
      acceptedPredicates = accepted.toArray
    } else if (
      tableInfo.getTableConfig.isDataLakeEnabled &&
      !LakeTableUtil.hasCustomLakePath(tableInfo.getProperties)
    ) {
      // TODO: Remove the custom lake path guard when Spark supports reading custom Paimon paths.
      // Predicate pushdown also runs for log-only fallback and streaming scans. Do not fail those
      // paths before we know that lake data is needed; actual lake reads are rejected when they
      // create the lake source.
      // Lake-enabled tables: probe the lake source for which predicates it accepts. All predicates
      // (including partition) are offered because the lake source handles both partition pruning
      // and data filtering internally. This recovers the former FlussLakeSupportsPushDownV2Filters
      // behavior that was split into a separate trait before consolidation.
      val pairs =
        SparkPredicateConverter.convertPerPredicate(tableInfo.getRowType, predicates.toSeq)
      if (pairs.nonEmpty) {
        val lakeSource = FlussLakeUtils.createLakeSource(
          flussConfig.toMap,
          tableInfo.getProperties.toMap,
          tablePath)
        val result = FlussLakeUtils.applyLakeFilters(lakeSource, pairs.map(_._2).asJava)
        val acceptedSet: JSet[FlussPredicate] =
          Collections.newSetFromMap(new IdentityHashMap[FlussPredicate, java.lang.Boolean]())
        acceptedSet.addAll(result.acceptedPredicates())
        val (acceptedSpark, acceptedFluss) =
          pairs.collect { case (sp, fp) if acceptedSet.contains(fp) => (sp, fp) }.unzip
        pushedPredicate = SparkPredicateConverter.combineAnd(acceptedFluss)
        acceptedPredicates = acceptedSpark.toArray
      }
    }
    nonPartition
  }
}

/**
 * Fluss Append (log-table) Scan Builder. The concrete [[AppendPlanner]] is materialized in
 * [[build]] once pushdown/prune state is settled. The planner probes the readable lake snapshot
 * itself at construction — the ScanBuilder is intentionally unaware of lake-union vs log-only
 * routing.
 */
class FlussAppendScanBuilder(
    val tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    val flussConfig: FlussConfiguration)
  extends FlussSupportsPushDownV2Filters {

  override def build(): Scan = {
    val projection = FlussScanBuilder.projectionOf(tableInfo, requiredSchema)
    val planner = new AppendPlanner(
      tablePath,
      tableInfo,
      partitionPredicate,
      pushedPredicate,
      projection,
      options,
      flussConfig)
    FlussAppendScan(
      tablePath,
      tableInfo,
      requiredSchema,
      pushedPredicate,
      partitionPredicate,
      acceptedPredicates.toSeq,
      limit,
      options,
      flussConfig,
      planner)
  }
}

/**
 * Fluss Upsert (primary-key table) Scan Builder. The concrete [[UpsertPlanner]] is materialized in
 * [[build]] once pushdown/prune state is settled. The planner probes the readable lake snapshot
 * itself at construction.
 */
class FlussUpsertScanBuilder(
    val tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    val flussConfig: FlussConfiguration)
  extends FlussSupportsPushDownV2Filters {

  override def build(): Scan = {
    val projection = FlussScanBuilder.projectionOf(tableInfo, requiredSchema)
    val planner = new UpsertPlanner(
      tablePath,
      tableInfo,
      partitionPredicate,
      pushedPredicate,
      projection,
      options,
      flussConfig)
    FlussUpsertScan(
      tablePath,
      tableInfo,
      requiredSchema,
      pushedPredicate,
      partitionPredicate,
      acceptedPredicates.toSeq,
      limit,
      options,
      flussConfig,
      planner)
  }
}

object FlussScanBuilder {

  /** Convert a Spark required-schema projection back to Fluss column indices. */
  def projectionOf(tableInfo: TableInfo, requiredSchema: Option[StructType]): Array[Int] = {
    val allFields = (0 until tableInfo.getRowType.getFieldCount).toArray
    val projected = requiredSchema match {
      case None => allFields
      case Some(schema) =>
        val columnNameToIndex =
          tableInfo.getSchema.getColumnNames.asScala.zipWithIndex.toMap
        schema.fields.map {
          f =>
            columnNameToIndex.getOrElse(
              f.name,
              throw new IllegalArgumentException(s"Invalid field name: ${f.name}"))
        }
    }
    // The Fluss server does not currently support empty Arrow projections. Fall back to projecting
    // the first column so the row count is preserved while still avoiding fetching unnecessary columns.
    if (projected.isEmpty) Array(0) else projected
  }
}
