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

package org.apache.fluss.spark.read.lake

import org.apache.fluss.client.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer}
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.Configuration
import org.apache.fluss.exception.LakeTableSnapshotNotExistException
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer
import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.{ResolvedPartitionSpec, TableBucket, TableInfo, TablePath}
import org.apache.fluss.spark.read._
import org.apache.fluss.utils.ExceptionUtils

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Batch for reading lake-enabled log table (append-only table with datalake). */
class FlussLakeAppendBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussLakeBatch(tablePath, tableInfo, readSchema, options, flussConfig) {

  // Required by FlussLakeBatch but unused — lake snapshot determines start offsets.
  override val startOffsetsInitializer: OffsetsInitializer = OffsetsInitializer.earliest()

  override def createReaderFactory(): PartitionReaderFactory = {
    if (isFallback) {
      new FlussAppendPartitionReaderFactory(tablePath, projection, options, flussConfig)
    } else {
      new FlussLakePartitionReaderFactory(
        tableInfo.getProperties.toMap,
        tablePath,
        projection,
        flussConfig)
    }
  }

  override def doPlan(): (Array[InputPartition], Boolean) = {
    val lakeSnapshot =
      try {
        admin.getReadableLakeSnapshot(tablePath).get()
      } catch {
        case e: Exception =>
          if (
            ExceptionUtils
              .stripExecutionException(e)
              .isInstanceOf[LakeTableSnapshotNotExistException]
          ) {
            return (planFallbackPartitions(), true)
          }
          throw e
      }

    val lakeSource = FlussLakeUtils.createLakeSource(tableInfo.getProperties.toMap, tablePath)
    lakeSource.withProject(FlussLakeUtils.lakeProjection(projection))

    val lakeSplits = lakeSource
      .createPlanner(new LakeSource.PlannerContext {
        override def snapshotId(): Long = lakeSnapshot.getSnapshotId
      })
      .plan()

    val splitSerializer = lakeSource.getSplitSerializer
    val tableBucketsOffset = lakeSnapshot.getTableBucketsOffset
    val buckets = (0 until tableInfo.getNumBuckets).toSeq
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)

    val partitions = if (tableInfo.isPartitioned) {
      planPartitionedTable(
        lakeSplits.asScala.toSeq,
        splitSerializer,
        tableBucketsOffset,
        buckets,
        bucketOffsetsRetriever)
    } else {
      planNonPartitionedTable(
        lakeSplits.asScala.toSeq,
        splitSerializer,
        tableBucketsOffset,
        buckets,
        bucketOffsetsRetriever)
    }

    (partitions, false)
  }

  private def planNonPartitionedTable(
      lakeSplits: Seq[LakeSplit],
      splitSerializer: SimpleVersionedSerializer[LakeSplit],
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      buckets: Seq[Int],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = tableInfo.getTableId

    val lakePartitions =
      createLakePartitions(lakeSplits, splitSerializer, tableId, partitionId = None)

    val stoppingOffsets =
      getBucketOffsets(stoppingOffsetsInitializer, null, buckets, bucketOffsetsRetriever)
    val logPartitions = buckets.flatMap {
      bucketId =>
        val tableBucket = new TableBucket(tableId, bucketId)
        createLogTailPartition(tableBucket, tableBucketsOffset, stoppingOffsets(bucketId))
    }

    (lakePartitions ++ logPartitions).toArray
  }

  private def planPartitionedTable(
      lakeSplits: Seq[LakeSplit],
      splitSerializer: SimpleVersionedSerializer[LakeSplit],
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      buckets: Seq[Int],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = tableInfo.getTableId

    val flussPartitionIdByName = mutable.LinkedHashMap.empty[String, Long]
    partitionInfos.asScala.foreach {
      pi => flussPartitionIdByName(pi.getPartitionName) = pi.getPartitionId
    }

    val lakeSplitsByPartition = groupLakeSplitsByPartition(lakeSplits)
    var lakeSplitPartitionId = -1L

    val lakeAndLogPartitions = lakeSplitsByPartition.flatMap {
      case (partitionName, splits) =>
        flussPartitionIdByName.remove(partitionName) match {
          case Some(partitionId) =>
            // Partition in both lake and Fluss — lake splits + log tail
            val lakePartitions =
              createLakePartitions(splits.toSeq, splitSerializer, tableId, Some(partitionId))

            val stoppingOffsets = getBucketOffsets(
              stoppingOffsetsInitializer,
              partitionName,
              buckets,
              bucketOffsetsRetriever)
            val logPartitions = buckets.flatMap {
              bucketId =>
                val tableBucket = new TableBucket(tableId, partitionId, bucketId)
                createLogTailPartition(tableBucket, tableBucketsOffset, stoppingOffsets(bucketId))
            }

            lakePartitions ++ logPartitions

          case None =>
            // Partition only in lake (expired in Fluss) — lake splits only
            val pid = lakeSplitPartitionId
            lakeSplitPartitionId -= 1
            createLakePartitions(splits.toSeq, splitSerializer, tableId, Some(pid))
        }
    }.toSeq

    // Partitions only in Fluss (not yet tiered) — log from earliest
    val flussOnlyPartitions = flussPartitionIdByName.flatMap {
      case (partitionName, partitionId) =>
        val stoppingOffsets = getBucketOffsets(
          stoppingOffsetsInitializer,
          partitionName,
          buckets,
          bucketOffsetsRetriever)
        buckets.flatMap {
          bucketId =>
            val stoppingOffset = stoppingOffsets(bucketId)
            if (stoppingOffset > 0) {
              val tableBucket = new TableBucket(tableId, partitionId, bucketId)
              Some(
                FlussAppendInputPartition(
                  tableBucket,
                  LogScanner.EARLIEST_OFFSET,
                  stoppingOffset): InputPartition)
            } else {
              None
            }
        }
    }.toSeq

    (lakeAndLogPartitions ++ flussOnlyPartitions).toArray
  }

  private def groupLakeSplitsByPartition(
      lakeSplits: Seq[LakeSplit]): mutable.LinkedHashMap[String, mutable.ArrayBuffer[LakeSplit]] = {
    val grouped = mutable.LinkedHashMap.empty[String, mutable.ArrayBuffer[LakeSplit]]
    lakeSplits.foreach {
      split =>
        val partitionName = if (split.partition() == null || split.partition().isEmpty) {
          ""
        } else {
          split.partition().asScala.mkString(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR)
        }
        grouped.getOrElseUpdate(partitionName, mutable.ArrayBuffer.empty) += split
    }
    grouped
  }

  private def createLakePartitions(
      splits: Seq[LakeSplit],
      splitSerializer: SimpleVersionedSerializer[LakeSplit],
      tableId: Long,
      partitionId: Option[Long]): Seq[InputPartition] = {
    splits.map {
      split =>
        val tableBucket = partitionId match {
          case Some(pid) => new TableBucket(tableId, pid, split.bucket())
          case None => new TableBucket(tableId, split.bucket())
        }
        FlussLakeInputPartition(tableBucket, splitSerializer.serialize(split))
    }
  }

  private def createLogTailPartition(
      tableBucket: TableBucket,
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      stoppingOffset: Long): Option[InputPartition] = {
    val snapshotLogOffset = tableBucketsOffset.get(tableBucket)
    if (snapshotLogOffset != null) {
      if (snapshotLogOffset.longValue() < stoppingOffset) {
        Some(FlussAppendInputPartition(tableBucket, snapshotLogOffset.longValue(), stoppingOffset))
      } else {
        None
      }
    } else if (stoppingOffset > 0) {
      Some(FlussAppendInputPartition(tableBucket, LogScanner.EARLIEST_OFFSET, stoppingOffset))
    } else {
      None
    }
  }

  private def planFallbackPartitions(): Array[InputPartition] = {
    val fallbackStartInit = FlussOffsetInitializers.startOffsetsInitializer(options, flussConfig)
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)
    val buckets = (0 until tableInfo.getNumBuckets).toSeq
    val tableId = tableInfo.getTableId

    def createPartitions(
        partitionId: Option[Long],
        partitionName: String): Array[InputPartition] = {
      val startOffsets =
        getBucketOffsets(fallbackStartInit, partitionName, buckets, bucketOffsetsRetriever)
      val stoppingOffsets =
        getBucketOffsets(stoppingOffsetsInitializer, partitionName, buckets, bucketOffsetsRetriever)

      buckets.map {
        bucketId =>
          val tableBucket = partitionId match {
            case Some(pid) => new TableBucket(tableId, pid, bucketId)
            case None => new TableBucket(tableId, bucketId)
          }
          FlussAppendInputPartition(
            tableBucket,
            startOffsets(bucketId),
            stoppingOffsets(bucketId)
          ): InputPartition
      }.toArray
    }

    if (tableInfo.isPartitioned) {
      partitionInfos.asScala.flatMap {
        pi => createPartitions(Some(pi.getPartitionId), pi.getPartitionName)
      }.toArray
    } else {
      createPartitions(None, null)
    }
  }
}
