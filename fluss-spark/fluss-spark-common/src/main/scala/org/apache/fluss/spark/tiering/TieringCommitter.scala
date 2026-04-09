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

import org.apache.fluss.client.admin.Admin
import org.apache.fluss.client.metadata.LakeSnapshot
import org.apache.fluss.client.tiering.{FlussTableLakeSnapshotCommitter, TieringCommitterInitContext}
import org.apache.fluss.config.Configuration
import org.apache.fluss.exception.LakeTableSnapshotNotExistException
import org.apache.fluss.lake.committer.{LakeCommitResult, LakeCommitter, TieringStats}
import org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY
import org.apache.fluss.lake.writer.LakeTieringFactory
import org.apache.fluss.metadata.{TableBucket, TablePath}

import org.apache.spark.internal.Logging

import java.util.Collections

import scala.collection.JavaConverters._

// TODO: This logic is duplicated from
//  org.apache.fluss.flink.tiering.committer.TieringCommitOperator (commit logic).
//  Consider extracting to a shared module (e.g., fluss-tiering-common) in the future.

/**
 * Orchestrates the two-phase commit of tiering write results to the lake and Fluss coordinator.
 *
 * Runs on the Spark driver after all executor tasks complete. The commit pipeline:
 *   1. Deserializes [[SerializedTaskResult]] write results from executors.
 *   2. Converts them into a lake committable via [[LakeCommitter.toCommittable()]].
 *   3. Checks for missing lake snapshots (Fluss lags behind lake) and recovers if needed.
 *   4. Prepares lake snapshot metadata via
 *      [[FlussTableLakeSnapshotCommitter.prepareLakeSnapshot()]].
 *   5. Commits to the lake storage (e.g., Paimon/Iceberg) via [[LakeCommitter.commit()]].
 *   6. Commits the snapshot metadata to the Fluss coordinator via
 *      [[FlussTableLakeSnapshotCommitter.commit()]].
 */
object TieringCommitter extends Logging {

  def commitAll(
      tableId: Long,
      tablePath: TablePath,
      results: Seq[SerializedTaskResult],
      lakeTieringFactory: LakeTieringFactory[AnyRef, AnyRef],
      flussConfig: Configuration,
      lakeTieringConfig: Configuration,
      admin: Admin,
      snapshotCommitter: FlussTableLakeSnapshotCommitter): TieringStats = {
    // Filter to non-empty results
    val nonEmptyResults = results.filter(_.serializedWriteResult != null)

    if (nonEmptyResults.isEmpty) {
      logInfo(s"Commit tiering write results is empty for table $tableId, table path $tablePath")
      return TieringStats.UNKNOWN
    }

    // Verify table was not dropped and recreated during tiering
    val currentTableInfo = admin.getTableInfo(tablePath).get()
    if (currentTableInfo.getTableId != tableId) {
      throw new IllegalStateException(
        s"The current table id ${currentTableInfo.getTableId} for table path $tablePath" +
          s" is different from the table id $tableId in the committable." +
          " This usually happens when a table was dropped and recreated during tiering." +
          " Aborting commit to prevent dirty commit.")
    }

    val writeResultSerializer = lakeTieringFactory.getWriteResultSerializer

    val lakeCommitter: LakeCommitter[AnyRef, AnyRef] = lakeTieringFactory.createLakeCommitter(
      new TieringCommitterInitContext(tablePath, currentTableInfo, lakeTieringConfig, flussConfig))

    try {
      // Deserialize write results
      val writeResults = nonEmptyResults.map {
        result =>
          writeResultSerializer.deserialize(result.writeResultVersion, result.serializedWriteResult)
      }.asJava

      // Build offset maps
      val logEndOffsets = new java.util.HashMap[TableBucket, java.lang.Long]()
      val logMaxTieredTimestamps = new java.util.HashMap[TableBucket, java.lang.Long]()
      nonEmptyResults.foreach {
        result =>
          logEndOffsets.put(result.tableBucket, result.logEndOffset: java.lang.Long)
          logMaxTieredTimestamps.put(result.tableBucket, result.maxTimestamp: java.lang.Long)
      }

      // Convert to committable
      val committable = lakeCommitter.toCommittable(writeResults)

      // Check for missing lake snapshot before committing
      val flussCurrentLakeSnapshot = getLatestLakeSnapshot(admin, tablePath)
      checkFlussNotMissingLakeSnapshot(
        tablePath,
        tableId,
        lakeCommitter,
        committable,
        snapshotCommitter,
        if (flussCurrentLakeSnapshot != null) flussCurrentLakeSnapshot.getSnapshotId else null)

      // Prepare lake snapshot (get offsets file path)
      val lakeBucketTieredOffsetsFile =
        snapshotCommitter.prepareLakeSnapshot(tableId, tablePath, logEndOffsets)

      // Commit to lake with offsets file in snapshot properties
      val snapshotProperties = Collections.singletonMap(
        FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
        lakeBucketTieredOffsetsFile)
      val lakeCommitResult = lakeCommitter.commit(committable, snapshotProperties)

      // Commit to Fluss
      snapshotCommitter.commit(
        tableId,
        tablePath,
        lakeCommitResult,
        lakeBucketTieredOffsetsFile,
        logEndOffsets,
        logMaxTieredTimestamps)

      lakeCommitResult.getTieringStats
    } finally {
      lakeCommitter.close()
    }
  }

  private def getLatestLakeSnapshot(admin: Admin, tablePath: TablePath): LakeSnapshot = {
    try {
      admin.getLatestLakeSnapshot(tablePath).get()
    } catch {
      case e: Exception =>
        e.getCause match {
          case _: LakeTableSnapshotNotExistException => null
          case _ => throw e
        }
    }
  }

  private def checkFlussNotMissingLakeSnapshot(
      tablePath: TablePath,
      tableId: Long,
      lakeCommitter: LakeCommitter[AnyRef, AnyRef],
      committable: AnyRef,
      snapshotCommitter: FlussTableLakeSnapshotCommitter,
      flussCurrentLakeSnapshot: java.lang.Long): Unit = {
    val missingCommittedSnapshot =
      lakeCommitter.getMissingLakeSnapshot(flussCurrentLakeSnapshot)

    if (missingCommittedSnapshot != null) {
      val lakeSnapshotOffsetPath = missingCommittedSnapshot.getSnapshotProperties
        .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)

      if (lakeSnapshotOffsetPath == null) {
        throw new IllegalStateException(
          s"Can't find $FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY field from snapshot property.")
      }

      val trimmedPath = lakeSnapshotOffsetPath.trim
      if (trimmedPath.contains("{")) {
        throw new IllegalStateException(
          s"The $FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY field in snapshot property" +
            s" is a JSON string (tiered by v0.8), which is not supported to restore." +
            s" Snapshot ID: ${missingCommittedSnapshot.getLakeSnapshotId}," +
            s" Table: {tablePath=$tablePath, tableId=$tableId}.")
      }

      // Commit missing snapshot to Fluss
      snapshotCommitter.commit(
        tableId,
        missingCommittedSnapshot.getLakeSnapshotId,
        lakeSnapshotOffsetPath,
        null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        LakeCommitResult.KEEP_ALL_PREVIOUS
      )

      // Abort current committable
      lakeCommitter.abort(committable)

      throw new IllegalStateException(
        s"The current Fluss's lake snapshot $flussCurrentLakeSnapshot is less than" +
          s" lake actual snapshot ${missingCommittedSnapshot.getLakeSnapshotId}" +
          s" committed by Fluss for table: {tablePath=$tablePath, tableId=$tableId}," +
          s" missing snapshot: $missingCommittedSnapshot.")
    }
  }
}
