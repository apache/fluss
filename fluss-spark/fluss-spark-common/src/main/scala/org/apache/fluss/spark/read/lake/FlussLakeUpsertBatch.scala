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

import org.apache.fluss.client.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer, SnapshotOffsetsInitializer}
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.Configuration
import org.apache.fluss.exception.LakeTableSnapshotNotExistException
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer
import org.apache.fluss.lake.source.LakeSplit
import org.apache.fluss.metadata.{ResolvedPartitionSpec, TableBucket, TableInfo, TablePath}
import org.apache.fluss.spark.read._
import org.apache.fluss.utils.ExceptionUtils

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Batch for reading lake-enabled primary key tables. Combines lake snapshot data with Fluss kv
 * tail, merging them using sort-merge algorithm.
 */
class FlussLakeUpsertBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussLakeBatch(tablePath, tableInfo, readSchema, options, flussConfig) {

  override val startOffsetsInitializer: OffsetsInitializer = {
    val offsetsInitializer = FlussOffsetInitializers.startOffsetsInitializer(options, flussConfig)
    if (!offsetsInitializer.isInstanceOf[SnapshotOffsetsInitializer]) {
      throw new UnsupportedOperationException("Upsert scan only supports FULL startup mode.")
    }
    offsetsInitializer
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    if (isFallback) {
      new FlussUpsertPartitionReaderFactory(tablePath, projection, options, flussConfig)
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
      .createPlanner(() => lakeSnapshot.getSnapshotId)
      .plan()

    val splitSerializer = lakeSource.getSplitSerializer
    val tableBucketsOffset = lakeSnapshot.getTableBucketsOffset
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)

    val partitions = if (tableInfo.isPartitioned) {
      planPartitionedTable(
        lakeSplits.asScala.toSeq,
        splitSerializer,
        tableBucketsOffset,
        bucketOffsetsRetriever)
    } else {
      planNonPartitionedTable(
        lakeSplits.asScala.toSeq,
        splitSerializer,
        tableBucketsOffset,
        bucketOffsetsRetriever)
    }

    (partitions, false)
  }

  private def planNonPartitionedTable(
      lakeSplits: Seq[LakeSplit],
      splitSerializer: SimpleVersionedSerializer[LakeSplit],
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = tableInfo.getTableId
    val buckets = (0 until tableInfo.getNumBuckets).toSeq

    val stoppingOffsets =
      getBucketOffsets(stoppingOffsetsInitializer, null, buckets, bucketOffsetsRetriever)

    // Group lake splits by bucket
    val lakeSplitsByBucket = lakeSplits.groupBy(_.bucket()).mapValues(_.toSeq).toMap

    buckets.flatMap {
      bucketId =>
        val tableBucket = new TableBucket(tableId, bucketId)
        val snapshotLogOffset = tableBucketsOffset.get(tableBucket)
        val stoppingOffset = stoppingOffsets(bucketId)

        createLakeUpsertPartition(
          tableBucket,
          lakeSplitsByBucket.get(bucketId),
          splitSerializer,
          snapshotLogOffset,
          stoppingOffset)
    }.toArray
  }

  private def planPartitionedTable(
      lakeSplits: Seq[LakeSplit],
      splitSerializer: SimpleVersionedSerializer[LakeSplit],
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = tableInfo.getTableId
    val buckets = (0 until tableInfo.getNumBuckets).toSeq

    val flussPartitionIdByName = mutable.LinkedHashMap.empty[String, Long]
    partitionInfos.asScala.foreach {
      pi => flussPartitionIdByName(pi.getPartitionName) = pi.getPartitionId
    }

    val lakeSplitsByPartition = groupLakeSplitsByPartition(lakeSplits)

    val lakePartitions = lakeSplitsByPartition.flatMap {
      case (partitionName, splitsByBucket) =>
        flussPartitionIdByName.remove(partitionName) match {
          case Some(partitionId) =>
            // Partition in both lake and Fluss
            val stoppingOffsets = getBucketOffsets(
              stoppingOffsetsInitializer,
              partitionName,
              buckets,
              bucketOffsetsRetriever)

            buckets.flatMap {
              bucketId =>
                val tableBucket = new TableBucket(tableId, partitionId, bucketId)
                val snapshotLogOffset = tableBucketsOffset.get(tableBucket)
                val stoppingOffset = stoppingOffsets(bucketId)

                createLakeUpsertPartition(
                  tableBucket,
                  splitsByBucket.get(bucketId),
                  splitSerializer,
                  snapshotLogOffset,
                  stoppingOffset)
            }

          case None =>
            // Partition only in lake (expired in Fluss)
            buckets.flatMap {
              bucketId =>
                val tableBucket = new TableBucket(tableId, -1, bucketId)
                splitsByBucket.getOrElse(bucketId, Seq.empty).map {
                  lakeSplit =>
                    FlussLakeInputPartition(tableBucket, splitSerializer.serialize(lakeSplit))
                }
            }
        }
    }

    // Partitions only in Fluss (not yet tiered) - read from earliest
    val flussOnlyPartitions = flussPartitionIdByName.flatMap {
      case (partitionName, partitionId) =>
        val stoppingOffsets = getBucketOffsets(
          stoppingOffsetsInitializer,
          partitionName,
          buckets,
          bucketOffsetsRetriever)

        buckets.flatMap {
          bucketId =>
            val tableBucket = new TableBucket(tableId, partitionId, bucketId)
            val stoppingOffset = stoppingOffsets(bucketId)

            // No lake snapshot for this bucket, skip if there's no data to read
            if (stoppingOffset > 0) {
              Some(
                FlussLakeUpsertInputPartition(
                  tableBucket,
                  null, // no lake splits
                  LogScanner.EARLIEST_OFFSET,
                  stoppingOffset
                ))
            } else {
              None
            }
        }
    }

    (lakePartitions ++ flussOnlyPartitions).toArray
  }

  private def groupLakeSplitsByPartition(
      lakeSplits: Seq[LakeSplit]): Map[String, mutable.Map[Int, Seq[LakeSplit]]] = {
    val grouped = mutable.LinkedHashMap.empty[String, mutable.Map[Int, Seq[LakeSplit]]]
    lakeSplits.foreach {
      split =>
        val partitionName = if (split.partition() == null || split.partition().isEmpty) {
          ""
        } else {
          split.partition().asScala.mkString(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR)
        }
        val bucketId = split.bucket()
        val bucketMap = grouped.getOrElseUpdate(partitionName, mutable.Map.empty)
        val splits = bucketMap.getOrElse(bucketId, Seq.empty)
        bucketMap(bucketId) = splits :+ split
    }
    grouped.toMap
  }

  private def createLakeUpsertPartition(
      tableBucket: TableBucket,
      lakeSplits: Option[Seq[LakeSplit]],
      splitSerializer: SimpleVersionedSerializer[LakeSplit],
      snapshotLogOffset: java.lang.Long,
      stoppingOffset: Long): Option[InputPartition] = {
    val needLogSplit = if (snapshotLogOffset == null) {
      stoppingOffset > 0
    } else {
      snapshotLogOffset < stoppingOffset.longValue()
    }
    val needLakeSplit = lakeSplits.isDefined && lakeSplits.get.nonEmpty
    if (!needLogSplit && !needLakeSplit) {
      return None
    }

    val lakeSplitBytes =
      if (lakeSplits.isDefined && lakeSplits.get.nonEmpty) {
        // Serialize all lake splits for this bucket into a single byte array
        FlussLakeUtils.serializeLakeSplits(lakeSplits.get, splitSerializer)
      } else null

    val logStartingOffset =
      if (snapshotLogOffset != null) snapshotLogOffset.longValue()
      else LogScanner.EARLIEST_OFFSET

    Some(
      FlussLakeUpsertInputPartition(
        tableBucket,
        lakeSplitBytes,
        logStartingOffset,
        stoppingOffset
      ))
  }

  private def planFallbackPartitions(): Array[InputPartition] = {
    // Fallback to pure Fluss kv reading when no lake snapshot exists
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)
    val buckets = (0 until tableInfo.getNumBuckets).toSeq

    def createPartitions(
        partitionId: Option[Long],
        partitionName: String): Array[InputPartition] = {
      val stoppingOffsets =
        getBucketOffsets(stoppingOffsetsInitializer, partitionName, buckets, bucketOffsetsRetriever)

      buckets.map {
        bucketId =>
          val tableBucket = partitionId match {
            case Some(pid) => new TableBucket(tableInfo.getTableId, pid, bucketId)
            case None => new TableBucket(tableInfo.getTableId, bucketId)
          }
          // Use FlussUpsertInputPartition for fallback (reads from Fluss kv snapshot)
          FlussUpsertInputPartition(
            tableBucket,
            -1L, // no snapshot
            LogScanner.EARLIEST_OFFSET,
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
