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
import org.apache.fluss.spark.SparkConversions

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** An interface that extends from Spark [[Scan]]. */
trait FlussScan extends Scan {
  def tableInfo: TableInfo

  def requiredSchema: Option[StructType]

  override def readSchema(): StructType = {
    requiredSchema.getOrElse(SparkConversions.toSparkDataType(tableInfo.getRowType))
  }
}

/** Fluss Append Scan. */
case class FlussAppendScan(
    tablePath: TablePath,
    tableInfo: TableInfo,
    requiredSchema: Option[StructType],
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussScan {

  override def toBatch: Batch = {
    new FlussAppendBatch(tablePath, tableInfo, readSchema, options, flussConfig)
  }
}

/** Fluss Upsert Scan. */
case class FlussUpsertScan(
    tablePath: TablePath,
    tableInfo: TableInfo,
    requiredSchema: Option[StructType],
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussScan {

  override def toBatch: Batch = {
    new FlussUpsertBatch(tablePath, tableInfo, readSchema, options, flussConfig)
  }
}
