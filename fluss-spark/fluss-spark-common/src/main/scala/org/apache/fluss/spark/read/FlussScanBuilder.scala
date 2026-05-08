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
import org.apache.fluss.metadata.{LogFormat, TableInfo, TablePath}
import org.apache.fluss.predicate.{Predicate => FlussPredicate}
import org.apache.fluss.spark.read.lake.{FlussLakeBatch, FlussLakeUtils}
import org.apache.fluss.spark.utils.SparkPredicateConverter

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Collections, IdentityHashMap, Set => JSet}

import scala.collection.JavaConverters._

/** An interface that extends from Spark [[ScanBuilder]]. */
trait FlussScanBuilder extends ScanBuilder with SupportsPushDownRequiredColumns {

  protected var requiredSchema: Option[StructType] = None

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = Some(requiredSchema)
  }
}

/** Predicate pushdown mixin: converts what it can, returns all predicates as residual. */
trait FlussSupportsPushDownV2Filters extends FlussScanBuilder with SupportsPushDownV2Filters {

  def tableInfo: TableInfo

  protected var pushedPredicate: Option[FlussPredicate] = None
  protected var acceptedPredicates: Array[Predicate] = Array.empty[Predicate]

  protected def convertAndStorePredicates(predicates: Array[Predicate]): Unit = {
    val (predicate, accepted) =
      SparkPredicateConverter.convertPredicates(tableInfo.getRowType, predicates.toSeq)
    pushedPredicate = predicate
    acceptedPredicates = accepted.toArray
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    // Server-side batch filter only supports ARROW; other log formats reject it.
    if (tableInfo.getTableConfig.getLogFormat == LogFormat.ARROW) {
      convertAndStorePredicates(predicates)
    }
    predicates
  }

  override def pushedPredicates(): Array[Predicate] = acceptedPredicates
}

/**
 * Lake reads push to the lake source regardless of log format. Each convertible predicate is
 * offered to the lake source individually; only the lake-accepted subset is reported back to Spark
 * and combined into the predicate handed to the scan.
 */
trait FlussLakeSupportsPushDownV2Filters extends FlussSupportsPushDownV2Filters {

  def tablePath: TablePath

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val pairs =
      SparkPredicateConverter.convertPerPredicate(tableInfo.getRowType, predicates.toSeq)
    val (acceptedSpark, acceptedFluss) = if (pairs.isEmpty) {
      (Seq.empty[Predicate], Seq.empty[FlussPredicate])
    } else {
      val lakeSource =
        FlussLakeUtils.createLakeSource(tableInfo.getProperties.toMap, tablePath)
      val result = FlussLakeBatch.applyLakeFilters(lakeSource, pairs.map(_._2).asJava)
      // Identity-match: lake sources are expected to return the same instances they received.
      val acceptedSet: JSet[FlussPredicate] =
        Collections.newSetFromMap(new IdentityHashMap())
      acceptedSet.addAll(result.acceptedPredicates())
      pairs.collect { case (sp, fp) if acceptedSet.contains(fp) => (sp, fp) }.unzip
    }
    pushedPredicate = SparkPredicateConverter.combineAnd(acceptedFluss)
    acceptedPredicates = acceptedSpark.toArray
    predicates
  }
}

/** Fluss Append Scan Builder. */
class FlussAppendScanBuilder(
    tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    flussConfig: FlussConfiguration)
  extends FlussSupportsPushDownV2Filters {

  override def build(): Scan = {
    FlussAppendScan(
      tablePath,
      tableInfo,
      requiredSchema,
      pushedPredicate,
      acceptedPredicates.toSeq,
      options,
      flussConfig)
  }
}

/** Fluss Lake Append Scan Builder. */
class FlussLakeAppendScanBuilder(
    val tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    flussConfig: FlussConfiguration)
  extends FlussLakeSupportsPushDownV2Filters {

  override def build(): Scan = {
    FlussLakeAppendScan(
      tablePath,
      tableInfo,
      requiredSchema,
      pushedPredicate,
      acceptedPredicates.toSeq,
      options,
      flussConfig)
  }
}

/** Fluss Upsert Scan Builder. */
class FlussUpsertScanBuilder(
    tablePath: TablePath,
    tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    flussConfig: FlussConfiguration)
  extends FlussScanBuilder {

  override def build(): Scan = {
    FlussUpsertScan(tablePath, tableInfo, requiredSchema, options, flussConfig)
  }
}

/** Fluss Lake Upsert Scan Builder for lake-enabled primary key tables. */
class FlussLakeUpsertScanBuilder(
    val tablePath: TablePath,
    val tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    flussConfig: FlussConfiguration)
  extends FlussLakeSupportsPushDownV2Filters {

  override def build(): Scan = {
    FlussLakeUpsertScan(
      tablePath,
      tableInfo,
      requiredSchema,
      pushedPredicate,
      acceptedPredicates.toSeq,
      options,
      flussConfig)
  }
}
