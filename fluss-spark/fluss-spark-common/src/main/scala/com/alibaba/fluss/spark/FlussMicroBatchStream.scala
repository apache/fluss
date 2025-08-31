/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.spark

import com.alibaba.fluss.client.ConnectionFactory
import com.alibaba.fluss.client.metadata.KvSnapshots
import com.alibaba.fluss.config.Configuration
import com.alibaba.fluss.metadata.{PartitionInfo, TableBucket, TablePath}
import com.alibaba.fluss.spark.FlussMicroBatchStream.emptyKvSnapshots
import com.alibaba.fluss.spark.exception.SparkRuntimeException
import com.alibaba.fluss.spark.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer, SnapshotOffsetsInitializer}
import com.alibaba.fluss.types.RowType
import com.alibaba.fluss.utils.ExceptionUtils
import com.alibaba.fluss.utils.Preconditions.checkState
import com.alibaba.fluss.utils.clock.{Clock, ManualClock, SystemClock}

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.fluss.{BucketOffsetMap, MockedSystemClock}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import javax.annotation.Nullable

import java.{util => ju}
import java.util.Optional

import scala.collection.JavaConverters._

/**
 * A [[MicroBatchStream]] that reads data from Fluss.
 *
 * The [[FlussSourceOffset]] is the custom [[Offset]] defined for this source that contains
 */
private class FlussMicroBatchStream(
    flussConf: Configuration,
    tablePath: TablePath,
    options: CaseInsensitiveStringMap,
    checkpointLocation: String,
    startingOffsetsInitializer: OffsetsInitializer,
    isPartitioned: Boolean,
    hasPrimaryKey: Boolean,
    bucketCount: Integer,
    tableId: Long,
    sourceOutputType: RowType,
    @Nullable projectedFields: Array[Int])
  extends SupportsTriggerAvailableNow
  with ReportsSourceMetrics
  with MicroBatchStream
  with Logging {

  private var lastTriggerMillis = 0L

  private val maxOffsetsPerTrigger =
    Option(options.get(SparkConnectorOptions.MAX_OFFSET_PER_TRIGGER.key())).map(_.toLong)

  private val minOffsetPerTrigger =
    Option(options.get(SparkConnectorOptions.MIN_OFFSET_PER_TRIGGER.key())).map(_.toLong)
  val maxTriggerDelayMs =
    JavaUtils.timeStringAsMs(
      Option(options.get(SparkConnectorOptions.MAX_TRIGGER_DELAY.key())).getOrElse("15m"))
  private var latestPartitionOffsets: BucketOffsetMap = _

  private var allDataForTriggerAvailableNow: BucketOffsetMap = _

  private val stoppingOffsetsInitializer: OffsetsInitializer = OffsetsInitializer.latest
  private val connection = ConnectionFactory.createConnection(flussConf)
  private val flussAdmin = connection.getAdmin
  private val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(flussAdmin, tablePath)
  private var cachedKvSnapshots: Map[(TablePath, Option[String]), KvSnapshots] = Map.empty

  // this allows us to mock system clock for testing purposes
  private val clock: Clock = if (options.containsKey(SparkConnectorOptions.MOCK_SYSTEM_TIME)) {
    MockedSystemClock.manualClock
  } else {
    SystemClock.getInstance()
  }

  /** Lazily initialize initialPartitionOffsets */
  override def initialOffset(): Offset = {
    FlussSourceOffset(getOrCreateInitialPartitionOffsets())
  }

  override def getDefaultReadLimit: ReadLimit = {
    if (minOffsetPerTrigger.isDefined && maxOffsetsPerTrigger.isDefined) {
      ReadLimit.compositeLimit(
        Array(
          ReadLimit.minRows(minOffsetPerTrigger.get, maxTriggerDelayMs),
          ReadLimit.maxRows(maxOffsetsPerTrigger.get)))
    } else if (minOffsetPerTrigger.isDefined) {
      ReadLimit.minRows(minOffsetPerTrigger.get, maxTriggerDelayMs)
    } else {
      maxOffsetsPerTrigger.map(ReadLimit.maxRows).getOrElse(ReadLimit.allAvailable())
    }
  }

  override def reportLatestOffset(): Offset = {
    Option(FlussSourceOffset(latestPartitionOffsets))
      .filterNot(_.bucketToOffsets.isEmpty)
      .getOrElse(null)
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    val startPartitionOffsets = start.asInstanceOf[FlussSourceOffset].bucketToOffsets

    // Use the pre-fetched list of partition offsets when Trigger.AvailableNow is enabled.
    latestPartitionOffsets = if (allDataForTriggerAvailableNow != null) {
      allDataForTriggerAvailableNow
    } else {
      fetchLatestOffsets(Some(startPartitionOffsets))
    }

    val limits: Seq[ReadLimit] = readLimit match {
      case rows: CompositeReadLimit => rows.getReadLimits
      case rows => Seq(rows)
    }

    val offsets = if (limits.exists(_.isInstanceOf[ReadAllAvailable])) {
      // ReadAllAvailable has the highest priority
      latestPartitionOffsets
    } else {
      val lowerLimit = limits.find(_.isInstanceOf[ReadMinRows]).map(_.asInstanceOf[ReadMinRows])
      val upperLimit = limits.find(_.isInstanceOf[ReadMaxRows]).map(_.asInstanceOf[ReadMaxRows])

      lowerLimit
        .flatMap {
          limit =>
            // checking if we need to skip batch based on minOffsetPerTrigger criteria
            val skipBatch = delayBatch(
              limit.minRows,
              latestPartitionOffsets,
              startPartitionOffsets,
              limit.maxTriggerDelayMs)
            if (skipBatch) {
              logDebug(
                s"Delaying batch as number of records available is less than minOffsetsPerTrigger")
              Some(startPartitionOffsets)
            } else {
              None
            }
        }
        .orElse {
          // checking if we need to adjust a range of offsets based on maxOffsetPerTrigger criteria
          upperLimit.map {
            limit => rateLimit(limit.maxRows(), startPartitionOffsets, latestPartitionOffsets)
          }
        }
        .getOrElse(latestPartitionOffsets)
    }

    Option(FlussSourceOffset(offsets)).filterNot(_.bucketToOffsets.isEmpty).getOrElse(null)
  }

  /** Checks if we need to skip this trigger based on minOffsetsPerTrigger & maxTriggerDelay */
  private def delayBatch(
      minLimit: Long,
      latestOffsets: Map[TableBucketInfo, Long],
      currentOffsets: Map[TableBucketInfo, Long],
      maxTriggerDelayMs: Long): Boolean = {
    // Checking first if the maxbatchDelay time has passed
    if ((clock.milliseconds() - lastTriggerMillis) >= maxTriggerDelayMs) {
      logDebug("Maximum wait time is passed, triggering batch")
      lastTriggerMillis = clock.milliseconds()
      false
    } else {
      val newRecords = latestOffsets
        .flatMap {
          case (topic, offset) =>
            Some(topic -> (offset - currentOffsets.getOrElse(topic, 0L)))
        }
        .values
        .sum
        .toDouble
      if (newRecords < minLimit) true
      else {
        lastTriggerMillis = clock.milliseconds()
        false
      }
    }
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startPartitionOffsets = start.asInstanceOf[FlussSourceOffset].bucketToOffsets
    val endPartitionOffsets = end.asInstanceOf[FlussSourceOffset].bucketToOffsets

    val offsetRanges =
      getOffsetRangesFromResolvedOffsets(startPartitionOffsets, endPartitionOffsets)
    offsetRanges.map(FlussInputPartition(_)).toArray
  }

  def getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets: BucketOffsetMap,
      untilPartitionOffsets: BucketOffsetMap): Seq[FlussOffsetRange] = {

    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
    val newPartitionInitialOffsets = fetchEarliestOffsets(newPartitions.toSeq)

    val bucketPartitions = untilPartitionOffsets.keySet.filter {
      tp =>
//         Ignore partitions that we don't know the from offsets.
        newPartitionInitialOffsets.contains(tp) ||
        fromPartitionOffsets.contains(tp)
    }.toSeq

    val fromOffsets = fromPartitionOffsets ++ newPartitionInitialOffsets
    val untilOffsets = untilPartitionOffsets
    var ranges = bucketPartitions.map {
      bp =>
        var fromOffset = fromOffsets(bp)
        if (fromOffset < 0) {
          fromOffset = 0;
        }
        val untilOffset = untilOffsets(bp)
        if (untilOffset < fromOffset) {
          logWarning(
            s"Bucket $bp's offset was changed from " +
              s"$fromOffset to $untilOffset, some data may have been missed")
        }
        FlussOffsetRange(bp, fromOffset, untilOffset)
    }
    ranges
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    FlussPartitionReaderFactory(flussConf, tablePath, sourceOutputType, projectedFields)
  }

  override def deserializeOffset(json: String): Offset = {
    FlussSourceOffset(json)
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    flussAdmin.close()
  }

  override def metrics(latestConsumedOffset: ju.Optional[Offset]): ju.Map[String, String] = {
    FlussMicroBatchStream.metrics(latestConsumedOffset, latestPartitionOffsets)
  }
  def fetchLatestOffsets(optionalOffsetMap: Option[BucketOffsetMap]): BucketOffsetMap = {
    initLatestTableSplits()
  }
  def fetchEarliestOffsets(): BucketOffsetMap = {
    if (isPartitioned) {
      val partitions = flussAdmin.listPartitionInfos(tablePath).get
      initPartitionedSplits(partitions)
    } else {
      initNonPartitionedSplits()
    }
  }
  def fetchEarliestOffsets(newTableBuckets: Seq[TableBucketInfo]): BucketOffsetMap = {
    var allAvailable = fetchEarliestOffsets()
    allAvailable = allAvailable.filter(entry => newTableBuckets.contains(entry._1))
    allAvailable
  }

  private def initPartitionedSplits(
      newPartitions: ju.Collection[PartitionInfo]): BucketOffsetMap = {
    if (hasPrimaryKey && startingOffsetsInitializer.isInstanceOf[SnapshotOffsetsInitializer]) {
      initPrimaryKeyTablePartitionSplits(newPartitions)
    } else {
      initLogTablePartitionSplits(newPartitions)
    }
  }

  private def initLogTablePartitionSplits(
      newPartitions: ju.Collection[PartitionInfo]): BucketOffsetMap = {
    import scala.collection.JavaConversions._
    var partitionOffsetMap: Map[TableBucketInfo, Long] = Map.empty
    for (partition <- newPartitions) {
      partitionOffsetMap = partitionOffsetMap ++ getLogSplit(
        Some(partition.getPartitionId),
        Some(partition.getPartitionName))
    }
    partitionOffsetMap
  }

  private def initLatestTableSplits(): BucketOffsetMap = {
    var partitionOffsetMap: Map[TableBucketInfo, Long] = Map.empty
    if (isPartitioned) {
      val partitions = flussAdmin.listPartitionInfos(tablePath).get
      for (partition <- partitions.asScala) {
        partitionOffsetMap = partitionOffsetMap ++ getLatestLogSplit(
          Some(partition.getPartitionId),
          Some(partition.getPartitionName))
      }
      partitionOffsetMap
    } else {
      getLatestLogSplit(Option.empty, Option.empty)
    }

  }

  private def initPrimaryKeyTablePartitionSplits(
      newPartitions: ju.Collection[PartitionInfo]): BucketOffsetMap = {
    var partitionOffsetMap: Map[TableBucketInfo, Long] = Map.empty
    import scala.collection.JavaConversions._
    for (partitionInfo <- newPartitions) {
      val partitionName = partitionInfo.getPartitionName
      // get the table snapshot info
      var kvSnapshots: KvSnapshots = null
      kvSnapshots = getLatestKvSnapshots(tablePath, Option.apply(partitionName))
      partitionOffsetMap =
        partitionOffsetMap ++ getSnapshotAndLogSplits(kvSnapshots, Some(partitionName))
    }
    partitionOffsetMap
  }

  private def initNonPartitionedSplits(): BucketOffsetMap = {
    if (hasPrimaryKey && startingOffsetsInitializer.isInstanceOf[SnapshotOffsetsInitializer]) {
      // get the table snapshot info
      var kvSnapshots: KvSnapshots = null
      kvSnapshots = getLatestKvSnapshots(tablePath, Option.empty)

      getSnapshotAndLogSplits(kvSnapshots, Option.empty)
    } else {
      getLogSplit(Option.empty, Option.empty)
    }
  }

  private def getLogSplit(
      partitionId: Option[Long],
      partitionName: Option[String]): BucketOffsetMap = {
    // always assume the bucket is from 0 to bucket num
    var partitionOffsetMap: Map[TableBucketInfo, Long] = Map.empty
    val bucketsNeedInitOffset = new java.util.ArrayList[Integer]
    for (bucketId <- 0 until bucketCount) {
      bucketsNeedInitOffset.add(bucketId)
    }

    import scala.collection.JavaConversions._
    if (!bucketsNeedInitOffset.isEmpty) {
      startingOffsetsInitializer
        .getBucketOffsets(
          partitionName.getOrElse(null),
          bucketsNeedInitOffset,
          bucketOffsetsRetriever)
        .forEach(
          (bucketId, startingOffset) =>
            partitionOffsetMap += ((partitionId match {
              case Some(id) =>
                new TableBucketInfo(
                  new TableBucket(tableId, id, bucketId),
                  partitionName.getOrElse(null))
              case None =>
                new TableBucketInfo(
                  new TableBucket(tableId, bucketId),
                  partitionName.getOrElse(null))
            }) -> startingOffset))
    }
    partitionOffsetMap
  }

  private def getLatestLogSplit(
      partitionId: Option[Long],
      partitionName: Option[String]): BucketOffsetMap = {
    // always assume the bucket is from 0 to bucket num
    var partitionOffsetMap: Map[TableBucketInfo, Long] = Map.empty
    val bucketsNeedInitOffset = new java.util.ArrayList[Integer]
    for (bucketId <- 0 until bucketCount) {
      bucketsNeedInitOffset.add(bucketId)
    }

    if (!bucketsNeedInitOffset.isEmpty) {
      stoppingOffsetsInitializer
        .getBucketOffsets(
          partitionName.getOrElse(null),
          bucketsNeedInitOffset,
          bucketOffsetsRetriever)
        .forEach(
          (bucketId, startingOffset) =>
            partitionOffsetMap += ((partitionId match {
              case Some(id) =>
                new TableBucketInfo(
                  new TableBucket(tableId, id, bucketId),
                  partitionName.getOrElse(null))
              case None =>
                new TableBucketInfo(
                  new TableBucket(tableId, bucketId),
                  partitionName.getOrElse(null))
            }) -> startingOffset))
    }
    if (hasPrimaryKey && startingOffsetsInitializer.isInstanceOf[SnapshotOffsetsInitializer]) {
      var kvSnapshots: KvSnapshots = null
      kvSnapshots = getLatestKvSnapshots(tablePath, partitionName)
      partitionOffsetMap.foreach {
        case (tbInfo, offset) =>
          val optionalLong = kvSnapshots
            .getSnapshotId(tbInfo.getTableBucket.getBucket)
          if (optionalLong.isPresent) {
            tbInfo.setSnapshotId(optionalLong.getAsLong)
          }

      }
    }

    partitionOffsetMap
  }

  private def getSnapshotAndLogSplits(
      snapshots: KvSnapshots,
      partitionName: Option[String]): BucketOffsetMap = {
    val tableId = snapshots.getTableId
    val partitionId = snapshots.getPartitionId
    val bucketsNeedInitOffset = new ju.ArrayList[Integer]
    var partitionOffsetMap: Map[TableBucketInfo, Long] = Map.empty

    import scala.collection.JavaConversions._
    for (bucketId <- snapshots.getBucketIds) {
      val tb = new TableBucket(tableId, partitionId, bucketId)
      val snapshotId = snapshots.getSnapshotId(bucketId)
      if (snapshotId.isPresent) {
        // hybrid snapshot log split;
        val logOffset = snapshots.getLogOffset(bucketId)
        checkState(logOffset.isPresent, s"Log offset should be present if snapshot id is.", "")
        partitionOffsetMap += (new TableBucketInfo(
          tb,
          partitionName.getOrElse(null),
          snapshotId.getAsLong) -> logOffset.getAsLong)
      } else bucketsNeedInitOffset.add(bucketId)
    }
    if (!bucketsNeedInitOffset.isEmpty) {
      startingOffsetsInitializer
        .getBucketOffsets(
          partitionName.getOrElse(null),
          bucketsNeedInitOffset,
          bucketOffsetsRetriever)
        .forEach(
          (bucketId, startingOffset) =>
            partitionOffsetMap += (new TableBucketInfo(
              new TableBucket(tableId, partitionId, bucketId),
              partitionName.getOrElse(null)) -> startingOffset))
    }
    partitionOffsetMap

  }

  /**
   * Read initial partition offsets from the checkpoint, or decide the offsets and write them to the
   * checkpoint.
   */
  private def getOrCreateInitialPartitionOffsets(): BucketOffsetMap = {
    assert(SparkSession.getActiveSession.nonEmpty)
    val map = fetchEarliestOffsets()
    // Ensure all values are non-negative by replacing negative values with 0
    val adjustedMap = map.mapValues(offset => math.max(0, offset))
    adjustedMap
  }

  def getLatestKvSnapshots(tablePath: TablePath, partitionName: Option[String]): KvSnapshots = {
    val key = (tablePath, partitionName)

    cachedKvSnapshots.get(key) match {
      case Some(snapshot) => snapshot
      case None =>
        try {
          var kvSnapshots = partitionName match {
            case Some(partition) =>
              flussAdmin.getLatestKvSnapshots(tablePath, partition).get
            case None => flussAdmin.getLatestKvSnapshots(tablePath).get
          }
          if (kvSnapshots == null) {
            kvSnapshots = emptyKvSnapshots()
          }
          cachedKvSnapshots += (key -> kvSnapshots)
          kvSnapshots
        } catch {
          case e: Exception =>
            throw new SparkRuntimeException(
              s"Failed to get table snapshot for table $tablePath",
              ExceptionUtils.stripCompletionException(e))
        }

    }
  }

  /** Proportionally distribute limit number of offsets among RichTableBuckets */
  private def rateLimit(
      limit: Long,
      from: BucketOffsetMap,
      until: BucketOffsetMap): BucketOffsetMap = {
    lazy val fromNew = fetchEarliestOffsets(until.keySet.diff(from.keySet).toSeq)
    val sizes = until.flatMap {
      case (tp, end) =>
        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
        from.get(tp).orElse(fromNew.get(tp)).flatMap {
          begin =>
            val size = end - begin
            logDebug(s"rateLimit $tp size is $size")
            if (size > 0) Some(tp -> size) else None
        }
    }
    val total = sizes.values.sum.toDouble
    if (total < 1) {
      until
    } else {
      until.map {
        case (tp, end) =>
          tp -> sizes
            .get(tp)
            .map {
              size =>
                val begin = from.getOrElse(tp, fromNew(tp))
                val prorate = limit * (size / total)
                // Don't completely starve small RichTableBuckets
                val prorateLong =
                  (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
                // need to be careful of integer overflow
                // therefore added canary checks where to see if off variable could be overflowed
                // refer to [https://issues.apache.org/jira/browse/SPARK-26718]
                val off = if (prorateLong > Long.MaxValue - begin) {
                  Long.MaxValue
                } else {
                  begin + prorateLong
                }
                // Paranoia, make sure not to return an offset that's past end
                Math.min(end, off)
            }
            .getOrElse(end)
      }
    }
  }

  override def prepareForTriggerAvailableNow(): Unit = {
    allDataForTriggerAvailableNow = fetchLatestOffsets(Some(getOrCreateInitialPartitionOffsets()))
  }

}

object FlussMicroBatchStream extends Logging {

  /**
   * Compute the difference of offset per partition between latestAvailablePartitionOffsets and
   * partition offsets in the latestConsumedOffset. Report min/max/avg offsets behind the latest for
   * all the partitions in the Fluss stream.
   *
   * Because of rate limit, latest consumed offset per partition can be smaller than the latest
   * available offset per partition.
   * @param latestConsumedOffset
   *   latest consumed offset
   * @param latestAvailablePartitionOffsets
   *   latest available offset per partition
   * @return
   *   the generated metrics map
   */
  def metrics(
      latestConsumedOffset: Optional[Offset],
      latestAvailablePartitionOffsets: BucketOffsetMap): ju.Map[String, String] = {
    val offset = Option(latestConsumedOffset.orElse(null))

    if (offset.nonEmpty && latestAvailablePartitionOffsets != null) {
      val consumedPartitionOffsets = offset.map(FlussSourceOffset(_)).get.bucketToOffsets
      val offsetsBehindLatest = latestAvailablePartitionOffsets
        .map(partitionOffset => partitionOffset._2 - consumedPartitionOffsets(partitionOffset._1))
      if (offsetsBehindLatest.nonEmpty) {
        val avgOffsetBehindLatest = offsetsBehindLatest.sum.toDouble / offsetsBehindLatest.size
        return Map[String, String](
          "minOffsetsBehindLatest" -> offsetsBehindLatest.min.toString,
          "maxOffsetsBehindLatest" -> offsetsBehindLatest.max.toString,
          "avgOffsetsBehindLatest" -> avgOffsetBehindLatest.toString
        ).asJava
      }
    }
    ju.Collections.emptyMap()
  }

  def emptyKvSnapshots(): KvSnapshots = {
    new KvSnapshots(
      0,
      null,
      Map.empty[Integer, java.lang.Long].asJava,
      Map.empty[Integer, java.lang.Long].asJava)
  }
}
