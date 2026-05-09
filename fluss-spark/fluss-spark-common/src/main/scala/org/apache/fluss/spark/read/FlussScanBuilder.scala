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

import org.apache.fluss.client.ConnectionFactory
import org.apache.fluss.config.{Configuration => FlussConfiguration}
import org.apache.fluss.exception.UnsupportedVersionException
import org.apache.fluss.metadata.{LogFormat, TableInfo, TablePath}
import org.apache.fluss.predicate.{Predicate => FlussPredicate}
import org.apache.fluss.spark.read.lake.{FlussLakeBatch, FlussLakeUtils}
import org.apache.fluss.spark.utils.{SparkPartitionPredicate, SparkPredicateConverter}

import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
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

trait FlussSupportsPushDownV2Filters extends FlussSupportsPushDownPartitionFilters {

  protected def convertAndStorePredicates(predicates: Array[Predicate]): Unit = {
    val (predicate, accepted) =
      SparkPredicateConverter.convertPredicates(tableInfo.getRowType, predicates.toSeq)
    pushedPredicate = predicate
    acceptedPredicates = accepted.toArray
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val nonPartitionPredicates = super.pushPredicates(predicates)
    if (!tableInfo.hasPrimaryKey && tableInfo.getTableConfig.getLogFormat == LogFormat.ARROW) {
      // Server-side batch filter for log table only supports ARROW; other log formats reject it.
      convertAndStorePredicates(nonPartitionPredicates)
    }
    nonPartitionPredicates
  }
}

/**
 * Lake reads push to the lake source regardless of log format. Each convertible predicate is
 * offered to the lake source individually; only the lake-accepted subset is reported back to Spark
 * and combined into the predicate handed to the scan.
 */
trait FlussLakeSupportsPushDownV2Filters extends FlussSupportsPushDownPartitionFilters {

  def tablePath: TablePath

  def flussConfig: FlussConfiguration

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val nonPartitionPredicates = super.pushPredicates(predicates)

    // Pass ALL predicates to Lake Source (including partition predicates) for lake-side filtering
    val pairs =
      SparkPredicateConverter.convertPerPredicate(tableInfo.getRowType, predicates.toSeq)
    val (acceptedSpark, acceptedFluss) = if (pairs.isEmpty) {
      (Seq.empty[Predicate], Seq.empty[FlussPredicate])
    } else {
      val lakeSource =
        FlussLakeUtils.createLakeSource(flussConfig.toMap, tableInfo.getProperties.toMap, tablePath)
      val result = FlussLakeBatch.applyLakeFilters(lakeSource, pairs.map(_._2).asJava)
      // Identity-match: lake sources are expected to return the same instances they received.
      val acceptedSet: JSet[FlussPredicate] =
        Collections.newSetFromMap(new IdentityHashMap())
      acceptedSet.addAll(result.acceptedPredicates())
      pairs.collect { case (sp, fp) if acceptedSet.contains(fp) => (sp, fp) }.unzip
    }
    pushedPredicate = SparkPredicateConverter.combineAnd(acceptedFluss)
    acceptedPredicates = acceptedSpark.toArray
    nonPartitionPredicates
  }
}

/**
 * Aggregation pushdown mixin: converts a single `COUNT(*)` / `COUNT(1)` / `COUNT(non_null_col)`
 * without `GROUP BY` into a server-side row count. Only safe for PK (non-lake) tables, because KV
 * upsert writes make the server-side row count match a full scan; log / lake-tiered tables can
 * drift. Mirrors the Flink-side gating in `FlinkTableSource#applyAggregates`.
 */
trait FlussSupportsPushDownAggregates
  extends FlussSupportsPushDownPartitionFilters
  with SupportsPushDownAggregates {

  def tablePath: TablePath

  def flussConfig: FlussConfiguration

  protected var pushedRowCount: Option[Long] = None

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!isPushable(aggregation)) {
      return false
    }
    fetchRowCount() match {
      case Some(count) =>
        pushedRowCount = Some(count)
        true
      case None =>
        false
    }
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = isPushable(aggregation)

  private def isPushable(aggregation: Aggregation): Boolean = {
    if (aggregation.groupByExpressions().nonEmpty) {
      return false
    }
    // When a partition predicate is present, we cannot use the whole-table row count
    // from server-side stats because it would include rows from non-matching partitions.
    if (partitionPredicate.isDefined) {
      return false
    }
    val aggs = aggregation.aggregateExpressions()
    if (aggs.length != 1) {
      return false
    }
    aggs.head match {
      case _: CountStar => true
      case c: Count if !c.isDistinct => isCountOnNonNullableColumn(c)
      case _ => false
    }
  }

  private def isCountOnNonNullableColumn(count: Count): Boolean = {
    count.column() match {
      case ref: NamedReference =>
        val parts = ref.fieldNames()
        // Only top-level column references are pushable.
        if (parts.length != 1) {
          return false
        }
        val colName = parts(0)
        val rowType = tableInfo.getRowType
        if (rowType.getFieldIndex(colName) < 0) {
          return false
        }
        !rowType.getField(colName).getType.isNullable
      case _ => false
    }
  }

  // Probes server-side table stats; returns None on UnsupportedVersionException so Spark falls back.
  private def fetchRowCount(): Option[Long] = {
    val conn = ConnectionFactory.createConnection(flussConfig)
    try {
      val admin = conn.getAdmin
      try {
        Some(admin.getTableStats(tablePath).get().getRowCount)
      } catch {
        case e: Throwable =>
          if (isUnsupportedVersion(e)) {
            None
          } else {
            throw e
          }
      } finally {
        admin.close()
      }
    } finally {
      conn.close()
    }
  }

  private def isUnsupportedVersion(t: Throwable): Boolean = {
    var cur: Throwable = t
    while (cur != null) {
      if (cur.isInstanceOf[UnsupportedVersionException]) {
        return true
      }
      cur = cur.getCause
    }
    false
  }
}

/** Fluss Append Scan Builder. */
class FlussAppendScanBuilder(
    tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    val flussConfig: FlussConfiguration)
  extends FlussSupportsPushDownV2Filters {

  override def build(): Scan = {
    FlussAppendScan(
      tablePath,
      tableInfo,
      requiredSchema,
      pushedPredicate,
      partitionPredicate,
      acceptedPredicates.toSeq,
      limit,
      options,
      flussConfig)
  }
}

/** Fluss Lake Append Scan Builder. */
class FlussLakeAppendScanBuilder(
    val tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    val flussConfig: FlussConfiguration)
  extends FlussLakeSupportsPushDownV2Filters {

  override def build(): Scan = {
    FlussLakeAppendScan(
      tablePath,
      tableInfo,
      requiredSchema,
      pushedPredicate,
      partitionPredicate,
      acceptedPredicates.toSeq,
      limit,
      options,
      flussConfig)
  }
}

/** Fluss Upsert Scan Builder. */
class FlussUpsertScanBuilder(
    val tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    val flussConfig: FlussConfiguration)
  extends FlussSupportsPushDownAggregates
  with FlussSupportsPushDownV2Filters {

  override def build(): Scan = {
    pushedRowCount match {
      case Some(count) => FlussCountScan(tablePath, tableInfo, count, requiredSchema)
      case None =>
        FlussUpsertScan(
          tablePath,
          tableInfo,
          requiredSchema,
          partitionPredicate,
          limit,
          options,
          flussConfig)
    }
  }
}

/** Fluss Lake Upsert Scan Builder for lake-enabled primary key tables. */
class FlussLakeUpsertScanBuilder(
    val tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    val flussConfig: FlussConfiguration)
  extends FlussLakeSupportsPushDownV2Filters {

  override def build(): Scan = {
    FlussLakeUpsertScan(
      tablePath,
      tableInfo,
      requiredSchema,
      pushedPredicate,
      partitionPredicate,
      acceptedPredicates.toSeq,
      limit,
      options,
      flussConfig)
  }
}
