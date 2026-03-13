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

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.admin.Admin
import org.apache.fluss.client.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer}
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{PartitionInfo, TableBucket, TableInfo, TablePath}
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.utils.json.TableBucketOffsets

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.Optional

import scala.collection.JavaConverters._

case class FlussSourceOffset(tableBucketOffsets: TableBucketOffsets) extends Offset {
  override val json: String = new String(tableBucketOffsets.toJsonBytes, "utf-8")
}

abstract class FlussMicroBatchStream(
    val tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration,
    checkpointLocation: String)
  extends SupportsTriggerAvailableNow
  with ReportsSourceMetrics
  with MicroBatchStream
  with Logging
  with AutoCloseable {

  lazy val conn: Connection = ConnectionFactory.createConnection(flussConfig)

  lazy val admin: Admin = conn.getAdmin

  lazy val bucketOffsetsRetriever: BucketOffsetsRetrieverImpl =
    new BucketOffsetsRetrieverImpl(admin, tableInfo.getTablePath)

  lazy val partitionInfos: util.List[PartitionInfo] = admin.listPartitionInfos(tablePath).get()

  private var allDataForTriggerAvailableNow: Option[TableBucketOffsets] = None

  val startOffsetsInitializer: OffsetsInitializer =
    FlussOffsetInitializers.startOffsetsInitializer(options, flussConfig)

  val stoppingOffsetsInitializer: OffsetsInitializer =
    FlussOffsetInitializers.stoppingOffsetsInitializer(false, options, flussConfig)

  private val maxOffsetsPerTrigger: Option[Long] = {
    val optVal = Option(options.get(SparkFlussConf.MAX_OFFSETS_PER_TRIGGER.key()))
      .map(_.toLong)
    optVal.orElse(Option(flussConfig.get(SparkFlussConf.MAX_OFFSETS_PER_TRIGGER)).map(_.toLong))
  }

  private val minOffsetsPerTrigger: Option[Long] = {
    val optVal = Option(options.get(SparkFlussConf.MIN_OFFSETS_PER_TRIGGER.key()))
      .map(_.toLong)
    optVal.orElse(Option(flussConfig.get(SparkFlussConf.MIN_OFFSETS_PER_TRIGGER)).map(_.toLong))
  }

  private val maxTriggerDelayMs: Long = {
    Option(options.get(SparkFlussConf.MAX_TRIGGER_DELAY.key()))
      .map(_.toLong) // accept milliseconds from DataFrameReader options
      .getOrElse(flussConfig.get(SparkFlussConf.MAX_TRIGGER_DELAY).toMillis)
  }

  // Validate that minOffsetsPerTrigger is not used without maxOffsetsPerTrigger
  if (minOffsetsPerTrigger.isDefined && maxOffsetsPerTrigger.isEmpty) {
    throw new IllegalArgumentException(
      "minOffsetsPerTrigger requires maxOffsetsPerTrigger to be set")
  }

  // Validate that minOffsetsPerTrigger <= maxOffsetsPerTrigger
  if (
    minOffsetsPerTrigger.isDefined && maxOffsetsPerTrigger.isDefined &&
    minOffsetsPerTrigger.get > maxOffsetsPerTrigger.get
  ) {
    throw new IllegalArgumentException(
      s"minOffsetsPerTrigger (${minOffsetsPerTrigger.get}) must not be greater than " +
        s"maxOffsetsPerTrigger (${maxOffsetsPerTrigger.get})")
  }

  protected def projection: Array[Int] = {
    val columnNameToIndex = tableInfo.getSchema.getColumnNames.asScala.zipWithIndex.toMap
    readSchema.fields.map {
      field =>
        columnNameToIndex.getOrElse(
          field.name,
          throw new IllegalArgumentException(s"Invalid field name: ${field.name}"))
    }
  }

  override def close(): Unit = {
    if (admin != null) {
      admin.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def getDefaultReadLimit: ReadLimit = {
    (minOffsetsPerTrigger, maxOffsetsPerTrigger) match {
      case (Some(minOffsets), Some(maxOffsets)) =>
        ReadLimit.compositeLimit(
          Array(ReadLimit.minRows(minOffsets, maxTriggerDelayMs), ReadLimit.maxRows(maxOffsets)))
      case (_, Some(maxOffsets)) =>
        ReadLimit.maxRows(maxOffsets)
      case _ =>
        ReadLimit.allAvailable()
    }
  }

  override def initialOffset(): Offset = {
    val initialTableBucketOffsets = getOrCreateInitialPartitionOffsets()
    FlussSourceOffset(initialTableBucketOffsets)
  }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    val latestTableBucketOffsets = if (allDataForTriggerAvailableNow.isDefined) {
      allDataForTriggerAvailableNow.get
    } else {
      fetchLatestOffsets().get
    }

    // Use the more restrictive of readLimit and maxOffsetsPerTrigger
    val limitFromReadLimit = extractMaxOffsets(readLimit)
    val effectiveLimit = (limitFromReadLimit, maxOffsetsPerTrigger) match {
      case (Some(a), Some(b)) => Some(Math.min(a, b))
      case (a @ Some(_), None) => a
      case (None, b @ Some(_)) => b
      case _ => None
    }

    val cappedOffsets = effectiveLimit match {
      case Some(limit) if start != null =>
        val startOffsets = start.asInstanceOf[FlussSourceOffset].tableBucketOffsets
        capOffsets(startOffsets, latestTableBucketOffsets, limit)
      case _ =>
        latestTableBucketOffsets
    }
    FlussSourceOffset(cappedOffsets)
  }

  /**
   * Extracts the maximum number of offsets from the given ReadLimit. Returns None if all available
   * data should be read (no capping).
   */
  private def extractMaxOffsets(readLimit: ReadLimit): Option[Long] = {
    readLimit match {
      case _: ReadAllAvailable => None
      case maxRows: ReadMaxRows => Some(maxRows.maxRows())
      case composite: CompositeReadLimit =>
        composite.getReadLimits
          .collectFirst { case maxRows: ReadMaxRows => maxRows.maxRows() }
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported ReadLimit type: ${readLimit.getClass.getSimpleName}")
    }
  }

  /**
   * Caps the latest offsets so that the total number of offsets (records) across all buckets does
   * not exceed `maxOffsets`. The limit is distributed proportionally across buckets based on their
   * available data.
   */
  private def capOffsets(
      startOffsets: TableBucketOffsets,
      latestOffsets: TableBucketOffsets,
      maxOffsets: Long): TableBucketOffsets = {
    val bucketAvailableMap = latestOffsets.getOffsets.asScala.map {
      case (bucket, latestOffset) =>
        val startOffset = Option(startOffsets.getOffsets.get(bucket))
          .map(_.toLong)
          .getOrElse(0L)
        bucket -> Math.max(0L, latestOffset.toLong - startOffset)
    }

    val totalAvailable = bucketAvailableMap.values.sum

    if (totalAvailable <= maxOffsets) {
      // All available data fits within the limit, no capping needed
      latestOffsets
    } else {
      // Proportionally distribute maxOffsets across buckets
      var remainingOffsets = maxOffsets
      val sortedBuckets = bucketAvailableMap.toSeq.sortBy(_._2)

      val allocations = scala.collection.mutable.Map[TableBucket, Long]()
      var bucketsLeft = sortedBuckets.size

      for ((bucket, available) <- sortedBuckets) {
        // Fair share for this bucket
        val fairShare = remainingOffsets / bucketsLeft
        val allocated = Math.min(available, fairShare)
        allocations(bucket) = allocated
        remainingOffsets -= allocated
        bucketsLeft -= 1
      }

      val cappedOffsets = latestOffsets.getOffsets.asScala.map {
        case (bucket, _) =>
          val startOffset = Option(startOffsets.getOffsets.get(bucket))
            .map(_.toLong)
            .getOrElse(0L)
          val allocated = allocations.getOrElse(bucket, 0L)
          bucket -> java.lang.Long.valueOf(startOffset + allocated)
      }
      new TableBucketOffsets(latestOffsets.getTableId, cappedOffsets.asJava)
    }
  }

  override def prepareForTriggerAvailableNow(): Unit = {
    allDataForTriggerAvailableNow = fetchLatestOffsets()
  }

  private def fetchLatestOffsets(): Option[TableBucketOffsets] = {
    val buckets = (0 until tableInfo.getNumBuckets).toSeq
    val offsetsInitializer = OffsetsInitializer.latest()
    if (tableInfo.isPartitioned) {
      val partitionOffsets = partitionInfos.asScala.map(
        partitionInfo =>
          FlussMicroBatchStream.getLatestOffsets(
            tableInfo,
            offsetsInitializer,
            bucketOffsetsRetriever,
            buckets,
            Some(partitionInfo)))
      val mergedOffsets = partitionOffsets
        .map(_.getOffsets)
        .reduce((l, r) => (l.asScala ++ r.asScala).asJava)
      Some(new TableBucketOffsets(tableInfo.getTableId, mergedOffsets))
    } else {
      Some(
        FlussMicroBatchStream
          .getLatestOffsets(tableInfo, offsetsInitializer, bucketOffsetsRetriever, buckets, None))
    }
  }

  // No need to notify fluss server
  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = close()

  override def deserializeOffset(json: String): Offset = {
    FlussSourceOffset(TableBucketOffsets.fromJsonBytes(json.getBytes("utf-8")))
  }

  override def metrics(latestConsumedOffset: Optional[Offset]): util.Map[String, String] = {
    // TODO add metrics
    Map.empty[String, String].asJava
  }

  private def getOrCreateInitialPartitionOffsets(): TableBucketOffsets = {
    if (tableInfo.isPartitioned) {
      initPartitionedSplits()
    } else {
      initNonPartitionedSplits()
    }
  }

  private def initPartitionedSplits(): TableBucketOffsets = {
    val partitionOffsets = partitionInfos.asScala.map {
      partitionInfo =>
        if (tableInfo.hasPrimaryKey) {
          getSnapshotAndLogSplits(Some(partitionInfo))
        } else {
          getLogSplit(Some(partitionInfo))
        }
    }

    val mergedOffsets = partitionOffsets
      .map(_.getOffsets)
      .reduce((l, r) => (l.asScala ++ r.asScala).asJava)

    new TableBucketOffsets(tableInfo.getTableId, mergedOffsets)
  }

  private def initNonPartitionedSplits(): TableBucketOffsets = {
    if (tableInfo.hasPrimaryKey) {
      getSnapshotAndLogSplits(None)
    } else {
      getLogSplit(None)
    }
  }

  private def getSnapshotAndLogSplits(partitionInfo: Option[PartitionInfo]): TableBucketOffsets = {
    // TODO read snapshot when more startup mode supported
    getLogSplit(partitionInfo)
  }

  private def getLogSplit(partitionInfo: Option[PartitionInfo]): TableBucketOffsets = {
    val buckets = (0 until tableInfo.getNumBuckets).toSeq
    FlussMicroBatchStream.getLatestOffsets(
      tableInfo,
      startOffsetsInitializer,
      bucketOffsetsRetriever,
      buckets,
      partitionInfo)
  }
}

object FlussMicroBatchStream {
  def getLatestOffsets(
      tableInfo: TableInfo,
      offsetsInitializer: OffsetsInitializer,
      bucketOffsetsRetrieverImpl: BucketOffsetsRetrieverImpl,
      buckets: Seq[Int],
      partitionInfo: Option[PartitionInfo]): TableBucketOffsets = {
    val latestOffsets = partitionInfo match {
      case Some(partitionInfo) =>
        offsetsInitializer
          .getBucketOffsets(
            partitionInfo.getPartitionName,
            buckets.map(Integer.valueOf).asJava,
            bucketOffsetsRetrieverImpl)
          .asScala
          .map {
            case (bucket, offset) =>
              val tableBucket =
                new TableBucket(tableInfo.getTableId, partitionInfo.getPartitionId, bucket)
              tableBucket -> offset
          }

      case None =>
        offsetsInitializer
          .getBucketOffsets(null, buckets.map(Integer.valueOf).asJava, bucketOffsetsRetrieverImpl)
          .asScala
          .map {
            case (bucket, offset) =>
              val tableBucket = new TableBucket(tableInfo.getTableId, bucket)
              tableBucket -> offset
          }
    }
    new TableBucketOffsets(
      tableInfo.getTableId,
      latestOffsets.map(e => (e._1, long2Long(e._2))).asJava)
  }
}

/** Batch for reading log table (append-only table). */
class FlussAppendMicroBatchStream(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration,
    checkpointLocation: String)
  extends FlussMicroBatchStream(
    tablePath,
    tableInfo,
    readSchema,
    options,
    flussConfig,
    checkpointLocation) {

  override def createReaderFactory(): PartitionReaderFactory = {
    new FlussAppendPartitionReaderFactory(tablePath, projection, options, flussConfig)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    // TODO process new partition and deleted partition
    val startOffsets = start.asInstanceOf[FlussSourceOffset].tableBucketOffsets
    val stopOffsets = end.asInstanceOf[FlussSourceOffset].tableBucketOffsets

    if (
      startOffsets.getOffsets
        .keySet()
        .asScala
        .diff(stopOffsets.getOffsets.keySet().asScala)
        .nonEmpty
    ) {
      throw new IllegalArgumentException(
        "start and end offset must have the same table bucket info")
    }

    val inputPartitions = startOffsets.getOffsets
      .keySet()
      .asScala
      .map {
        tableBucket =>
          val startOffset = startOffsets.getOffsets.get(tableBucket)
          val stopOffset = stopOffsets.getOffsets.get(tableBucket)
          FlussAppendInputPartition(tableBucket, startOffset, stopOffset)
      }
      .filter(e => e.startOffset < e.stopOffset)
      .toArray
    inputPartitions.map(_.asInstanceOf[InputPartition])
  }
}

class FlussUpsertMicroBatchStream(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration,
    checkpointLocation: String)
  extends FlussMicroBatchStream(
    tablePath,
    tableInfo,
    readSchema,
    options,
    flussConfig,
    checkpointLocation) {

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    // TODO process new partition and deleted partition
    val startOffsets = start.asInstanceOf[FlussSourceOffset].tableBucketOffsets
    val stopOffsets = end.asInstanceOf[FlussSourceOffset].tableBucketOffsets

    if (
      startOffsets.getOffsets
        .keySet()
        .asScala
        .diff(stopOffsets.getOffsets.keySet().asScala)
        .nonEmpty
    ) {
      throw new IllegalArgumentException(
        "start and end offset must have the same table bucket info")
    }

    val inputPartitions = startOffsets.getOffsets
      .keySet()
      .asScala
      .map {
        tableBucket =>
          val startOffset = startOffsets.getOffsets.get(tableBucket)
          val stopOffset = stopOffsets.getOffsets.get(tableBucket)
          // TODO read snapshot with startup mode.
          FlussUpsertInputPartition(tableBucket, -1, startOffset, stopOffset)
      }
      .filter(e => e.logStartingOffset < e.logStoppingOffset)
      .toArray
    inputPartitions.map(_.asInstanceOf[InputPartition])
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new FlussUpsertPartitionReaderFactory(tablePath, projection, options, flussConfig)
  }
}
