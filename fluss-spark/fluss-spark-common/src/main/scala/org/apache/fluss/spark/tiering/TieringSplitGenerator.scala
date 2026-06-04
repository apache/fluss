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
import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl
import org.apache.fluss.client.metadata.{KvSnapshots, LakeSnapshot}
import org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET
import org.apache.fluss.exception.LakeTableSnapshotNotExistException
import org.apache.fluss.metadata.{TableBucket, TableInfo, TablePath}
import org.apache.fluss.utils.ExceptionUtils

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

// TODO: This logic is duplicated from
//  org.apache.fluss.flink.tiering.source.split.TieringSplitGenerator.
//  Consider extracting to a shared module (e.g., fluss-tiering-common) in the future.

/**
 * Generates tiering splits for a table, producing one [[TieringSplit]] per bucket.
 *
 * For log tables, each split is a [[TieringLogSplit]] bounded by the last committed lake offset and
 * the current latest offset. For primary key tables that have never been tiered, a
 * [[TieringSnapshotSplit]] is generated to read the KV snapshot; subsequent rounds produce log
 * splits for incremental changes. Buckets with no new data are skipped.
 *
 * Supports both partitioned and non-partitioned tables.
 */
class TieringSplitGenerator(admin: Admin) extends Logging {

  def generateTableSplits(tableInfo: TableInfo): Seq[TieringSplit] = {
    val tablePath = tableInfo.getTablePath
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)

    // Get the latest lake snapshot (may be null if table was never tiered)
    val lakeSnapshot: LakeSnapshot =
      try {
        admin.getLatestLakeSnapshot(tablePath).get()
      } catch {
        case e: Exception =>
          ExceptionUtils.stripExecutionException(e) match {
            case _: LakeTableSnapshotNotExistException =>
              null
            case t =>
              throw new IllegalStateException(
                s"Failed to get table snapshot for table $tablePath",
                t)
          }
      }
    logInfo(s"Last committed lake table snapshot info is: $lakeSnapshot")

    if (tableInfo.isPartitioned) {
      val partitionInfos = admin.listPartitionInfos(tablePath).get().asScala
      val partitionNameById: Map[Long, String] =
        partitionInfos.map(p => (p.getPartitionId: Long) -> p.getPartitionName).toMap
      generatePartitionTableSplits(
        tableInfo,
        partitionNameById,
        bucketOffsetsRetriever,
        lakeSnapshot)
    } else {
      generateNonPartitionedTableSplits(tableInfo, bucketOffsetsRetriever, lakeSnapshot)
    }
  }

  private def generatePartitionTableSplits(
      tableInfo: TableInfo,
      partitionNameById: Map[Long, String],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl,
      lakeSnapshot: LakeSnapshot): Seq[TieringSplit] = {
    partitionNameById.flatMap {
      case (partitionId, partitionName) =>
        val bucketIds = (0 until tableInfo.getNumBuckets).map(Int.box).toList.asJava
        val latestBucketsOffset =
          bucketOffsetsRetriever.latestOffsets(partitionName, bucketIds).asScala.toMap

        val latestKvSnapshots: KvSnapshots = if (tableInfo.hasPrimaryKey) {
          try {
            admin.getLatestKvSnapshots(tableInfo.getTablePath, partitionName).get()
          } catch {
            case e: Exception =>
              throw new IllegalStateException(
                s"Failed to get table snapshot for table ${tableInfo.getTablePath}" +
                  s" and partition $partitionName",
                ExceptionUtils.stripCompletionException(e))
          }
        } else {
          null
        }

        generateTableSplits(
          tableInfo,
          Some(partitionId),
          Some(partitionName),
          lakeSnapshot,
          latestKvSnapshots,
          latestBucketsOffset)
    }.toSeq
  }

  private def generateNonPartitionedTableSplits(
      tableInfo: TableInfo,
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl,
      lakeSnapshot: LakeSnapshot): Seq[TieringSplit] = {
    val bucketIds = (0 until tableInfo.getNumBuckets).map(Int.box).toList.asJava
    val latestBucketsOffset =
      bucketOffsetsRetriever.latestOffsets(null, bucketIds).asScala.toMap

    val latestKvSnapshots: KvSnapshots = if (tableInfo.hasPrimaryKey) {
      try {
        admin.getLatestKvSnapshots(tableInfo.getTablePath).get()
      } catch {
        case e: Exception =>
          throw new IllegalStateException(
            s"Failed to get table snapshot for table ${tableInfo.getTablePath}",
            ExceptionUtils.stripCompletionException(e))
      }
    } else {
      null
    }

    generateTableSplits(tableInfo, None, None, lakeSnapshot, latestKvSnapshots, latestBucketsOffset)
  }

  private def generateTableSplits(
      tableInfo: TableInfo,
      partitionId: Option[Long],
      partitionName: Option[String],
      lakeSnapshot: LakeSnapshot,
      latestKvSnapshots: KvSnapshots,
      latestBucketsOffset: Map[Integer, java.lang.Long]): Seq[TieringSplit] = {
    val tablePath = tableInfo.getTablePath

    if (tableInfo.hasPrimaryKey) {
      require(latestKvSnapshots != null, "latestKvSnapshots must not be null for PK tables")
      (0 until tableInfo.getNumBuckets).flatMap {
        bucket =>
          val tableBucket =
            new TableBucket(tableInfo.getTableId, partitionId.map(Long.box).orNull, bucket)
          val lastCommittedBucketOffset: java.lang.Long =
            if (lakeSnapshot != null) lakeSnapshot.getTableBucketsOffset.get(tableBucket)
            else null
          val latestSnapshotId: java.lang.Long =
            if (latestKvSnapshots.getSnapshotId(bucket).isPresent)
              latestKvSnapshots.getSnapshotId(bucket).getAsLong
            else null
          val offsetOfLatestSnapshot: java.lang.Long =
            if (latestKvSnapshots.getSnapshotId(bucket).isPresent)
              latestKvSnapshots.getLogOffset(bucket).getAsLong
            else null
          val latestBucketOffset: Long = latestBucketsOffset(bucket)

          generateSplitForPrimaryKeyTableBucket(
            tablePath,
            tableBucket,
            partitionName,
            latestSnapshotId,
            offsetOfLatestSnapshot,
            lastCommittedBucketOffset,
            latestBucketOffset)
      }
    } else {
      (0 until tableInfo.getNumBuckets).flatMap {
        bucket =>
          val tableBucket =
            new TableBucket(tableInfo.getTableId, partitionId.map(Long.box).orNull, bucket)
          val lastCommittedOffset: java.lang.Long =
            if (lakeSnapshot != null) lakeSnapshot.getTableBucketsOffset.get(tableBucket)
            else null
          val latestBucketOffset: Long = latestBucketsOffset(bucket)

          generateSplitForLogTableBucket(
            tablePath,
            tableBucket,
            partitionName,
            lastCommittedOffset,
            latestBucketOffset)
      }
    }
  }

  private def generateSplitForPrimaryKeyTableBucket(
      tablePath: TablePath,
      tableBucket: TableBucket,
      partitionName: Option[String],
      latestSnapshotId: java.lang.Long,
      latestOffsetOfSnapshot: java.lang.Long,
      lastCommittedBucketOffset: java.lang.Long,
      latestBucketOffset: Long): Option[TieringSplit] = {
    if (latestBucketOffset <= 0) {
      logDebug(
        s"The latestBucketOffset $latestBucketOffset is equals or less than 0," +
          s" skip generating split for bucket $tableBucket")
      return None
    }

    if (lastCommittedBucketOffset == null) {
      // Never tiered
      if (latestSnapshotId == null) {
        // No snapshot, scan log from earliest
        Some(
          TieringLogSplit(
            tablePath,
            tableBucket,
            partitionName,
            EARLIEST_OFFSET,
            latestBucketOffset,
            0))
      } else {
        // Has snapshot, read KV snapshot
        require(
          latestOffsetOfSnapshot != null,
          "latestOffsetOfSnapshot must not be null when latestSnapshotId is present")
        Some(
          TieringSnapshotSplit(
            tablePath,
            tableBucket,
            partitionName,
            latestSnapshotId,
            latestOffsetOfSnapshot,
            0))
      }
    } else {
      // Previously tiered, read bounded log (snapshot is ignored)
      if (lastCommittedBucketOffset < latestBucketOffset) {
        Some(
          TieringLogSplit(
            tablePath,
            tableBucket,
            partitionName,
            lastCommittedBucketOffset,
            latestBucketOffset,
            0))
      } else {
        logDebug(
          s"The lastCommittedBucketOffset $lastCommittedBucketOffset is equals or" +
            s" bigger than latestBucketOffset $latestBucketOffset," +
            s" skip generating split for bucket $tableBucket")
        None
      }
    }
  }

  private def generateSplitForLogTableBucket(
      tablePath: TablePath,
      tableBucket: TableBucket,
      partitionName: Option[String],
      lastCommittedBucketOffset: java.lang.Long,
      latestBucketOffset: Long): Option[TieringSplit] = {
    if (latestBucketOffset <= 0) {
      logDebug(
        s"The latestBucketOffset $latestBucketOffset is equals or less than 0," +
          s" skip generating split for bucket $tableBucket")
      return None
    }

    if (lastCommittedBucketOffset == null) {
      // Never tiered, scan from earliest
      Some(
        TieringLogSplit(
          tablePath,
          tableBucket,
          partitionName,
          EARLIEST_OFFSET,
          latestBucketOffset,
          0))
    } else if (lastCommittedBucketOffset < latestBucketOffset) {
      // Previously tiered, scan remaining log
      Some(
        TieringLogSplit(
          tablePath,
          tableBucket,
          partitionName,
          lastCommittedBucketOffset,
          latestBucketOffset,
          0))
    } else {
      logDebug(
        s"The lastCommittedBucketOffset $lastCommittedBucketOffset is equals or" +
          s" bigger than latestBucketOffset $latestBucketOffset," +
          s" skip generating split for bucket $tableBucket")
      None
    }
  }
}
