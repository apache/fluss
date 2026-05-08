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
import org.apache.fluss.spark.utils.SparkPredicateConverter

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    // Server-side batch filter only supports ARROW; other log formats reject it.
    if (tableInfo.getTableConfig.getLogFormat == LogFormat.ARROW) {
      val (predicate, accepted) =
        SparkPredicateConverter.convertPredicates(tableInfo.getRowType, predicates.toSeq)
      pushedPredicate = predicate
      acceptedPredicates = accepted.toArray
    }
    // Server-side filter is batch-level only; Spark must re-apply for row-exact results.
    predicates
  }

  override def pushedPredicates(): Array[Predicate] = acceptedPredicates
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
    tablePath: TablePath,
    tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    flussConfig: FlussConfiguration)
  extends FlussScanBuilder {

  override def build(): Scan = {
    FlussLakeAppendScan(tablePath, tableInfo, requiredSchema, options, flussConfig)
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
    tablePath: TablePath,
    tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    flussConfig: FlussConfiguration)
  extends FlussScanBuilder {

  override def build(): Scan = {
    FlussLakeUpsertScan(tablePath, tableInfo, requiredSchema, options, flussConfig)
  }
}
