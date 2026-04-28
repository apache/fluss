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
import org.apache.fluss.spark.read.lake.FlussLakePartitionReaderFactory

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * Base class for planner-backed batch scans. The planner (constructed at ScanBuilder-time) opens
 * and releases the Fluss client Connection/Admin itself — scoped to its `plan()` call — and
 * produces the [[InputPartition]]s; the Batch is a thin adapter that hands the plan to Spark and
 * dispatches [[createReaderFactory]] based on whether the planner is unioning with a lake snapshot
 * (lake-union) or reading Fluss only (log-only).
 *
 * The Batch is intentionally not [[AutoCloseable]]: the Spark DSv2 Batch interface has no `close()`
 * hook that Spark invokes, so connection cleanup lives in the planner rather than here.
 */
abstract class FlussBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    limit: Option[Int],
    flussConfig: Configuration,
    planner: SplitPlanner)
  extends Batch {

  protected def projection: Array[Int] = {
    val columnNameToIndex = tableInfo.getSchema.getColumnNames.asScala.zipWithIndex.toMap
    val projected = readSchema.fields.map {
      field =>
        columnNameToIndex.getOrElse(
          field.name,
          throw new IllegalArgumentException(s"Invalid field name: ${field.name}"))
    }
    // The Fluss server does not currently support empty Arrow projections. Fall back to projecting
    // the first column so the row count is preserved while still avoiding fetching unnecessary columns.
    if (projected.isEmpty) Array(0) else projected
  }

  override def planInputPartitions(): Array[InputPartition] = planner.plan()
}

/** Batch for reading log table (append-only table). */
class FlussAppendBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    pushedPredicate: Option[FlussPredicate],
    limit: Option[Int],
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration,
    appendPlanner: AppendSplitPlanner)
  extends FlussBatch(tablePath, tableInfo, readSchema, limit, flussConfig, appendPlanner) {

  override def createReaderFactory(): PartitionReaderFactory = {
    if (appendPlanner.hasLakeSnapshot) {
      new FlussLakePartitionReaderFactory(
        tableInfo.getProperties.toMap,
        tablePath,
        projection,
        pushedPredicate,
        appendPlanner.logTailPredicate,
        limit,
        flussConfig)
    } else {
      new FlussAppendPartitionReaderFactory(
        tablePath,
        projection,
        pushedPredicate,
        limit,
        options,
        flussConfig)
    }
  }
}

/** Batch for reading primary key table (upsert table). */
class FlussUpsertBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    pushedPredicate: Option[FlussPredicate],
    limit: Option[Int],
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration,
    upsertPlanner: UpsertSplitPlanner)
  extends FlussBatch(tablePath, tableInfo, readSchema, limit, flussConfig, upsertPlanner) {

  override def createReaderFactory(): PartitionReaderFactory = {
    if (upsertPlanner.hasLakeSnapshot) {
      new FlussLakePartitionReaderFactory(
        tableInfo.getProperties.toMap,
        tablePath,
        projection,
        pushedPredicate,
        upsertPlanner.logTailPredicate,
        limit,
        flussConfig)
    } else {
      new FlussUpsertPartitionReaderFactory(tablePath, projection, limit, options, flussConfig)
    }
  }
}
