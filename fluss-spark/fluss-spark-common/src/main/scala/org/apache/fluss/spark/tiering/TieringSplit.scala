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

package org.apache.fluss.spark.tiering

import org.apache.fluss.metadata.{TableBucket, TablePath}

/** Sealed trait representing a tiering split for distributing work across Spark executors. */
sealed trait TieringSplit extends Serializable {
  def tablePath: TablePath
  def tableBucket: TableBucket
  def partitionName: Option[String]
  def numberOfSplits: Int
}

/**
 * A split for reading log data from Fluss.
 *
 * @param tablePath
 *   the table path
 * @param tableBucket
 *   the table bucket
 * @param partitionName
 *   optional partition name
 * @param startingOffset
 *   the starting log offset (inclusive)
 * @param stoppingOffset
 *   the stopping log offset (exclusive)
 * @param numberOfSplits
 *   total number of splits in this tiering round
 */
case class TieringLogSplit(
    tablePath: TablePath,
    tableBucket: TableBucket,
    partitionName: Option[String],
    startingOffset: Long,
    stoppingOffset: Long,
    numberOfSplits: Int)
  extends TieringSplit

/**
 * A split for reading a KV snapshot from Fluss (used for primary key tables on first tiering).
 *
 * @param tablePath
 *   the table path
 * @param tableBucket
 *   the table bucket
 * @param partitionName
 *   optional partition name
 * @param snapshotId
 *   the KV snapshot ID to read
 * @param logOffsetOfSnapshot
 *   the log offset corresponding to the snapshot
 * @param numberOfSplits
 *   total number of splits in this tiering round
 */
case class TieringSnapshotSplit(
    tablePath: TablePath,
    tableBucket: TableBucket,
    partitionName: Option[String],
    snapshotId: Long,
    logOffsetOfSnapshot: Long,
    numberOfSplits: Int)
  extends TieringSplit
