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
import org.apache.fluss.metadata.{TableInfo, TablePath}

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** An interface that extends from Spark [[ScanBuilder]]. */
trait FlussScanBuilder extends ScanBuilder with SupportsPushDownRequiredColumns {

  protected var requiredSchema: Option[StructType] = _

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = Some(requiredSchema)
  }
}

/** Fluss Append Scan Builder. */
class FlussAppendScanBuilder(
    tablePath: TablePath,
    tableInfo: TableInfo,
    options: CaseInsensitiveStringMap,
    flussConfig: FlussConfiguration)
  extends FlussScanBuilder {

  override def build(): Scan = {
    FlussAppendScan(tablePath, tableInfo, requiredSchema, options, flussConfig)
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
