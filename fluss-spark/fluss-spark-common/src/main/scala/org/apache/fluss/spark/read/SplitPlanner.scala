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
import org.apache.fluss.client.metadata.{KvSnapshots, LakeSnapshot}
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.Configuration
import org.apache.fluss.exception.LakeTableSnapshotNotExistException
import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.{LogFormat, PartitionInfo, ResolvedPartitionSpec, TableBucket, TableInfo, TablePath}
import org.apache.fluss.predicate.{Predicate => FlussPredicate}
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.spark.read.lake.{FlussLakeInputPartition, FlussLakeUpsertInputPartition, FlussLakeUtils}
import org.apache.fluss.spark.utils.SparkPartitionPredicate
import org.apache.fluss.utils.ExceptionUtils

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Plans Spark [[InputPartition]]s for a Fluss batch scan. The planner owns a Fluss client
 * connection for the lifetime of the scan and is closed by the enclosing batch.
 *
 * Concrete planners probe for a readable lake snapshot at construction time and branch inside
 * `plan()`:
 *   - Lake-union branch (snapshot present): unions lake splits with a Fluss log-tail (append) or
 *     kv+log-tail (upsert).
 *   - Log-only branch (snapshot absent): the plan is derived exclusively from Fluss metadata —
 *     the full Fluss log (append) or Fluss kv snapshots + log tail (upsert).
 *
 * The probe is snapshot-isolated: it is performed exactly once per planner instance (i.e. once
 * per Spark scan build). There is no runtime fallback path — presence/absence is decided
 * deterministically at construction and `plan()` picks a branch accordingly.
 */
sealed trait SplitPlanner extends AutoCloseable {

  def tablePath: TablePath

  def tableInfo: TableInfo

  def flussConfig: Configuration

  def plan(): Array[InputPartition]

  /**
   * Whether this planner will union its Fluss plan with a lake snapshot. Determined once at
   * construction; deterministic across `plan()` invocations.
   */
  def hasLakeSnapshot: Boolean

  /**
   * Server-side batch filter to attach to the Fluss log-tail reader. Only ARROW-formatted log
   * tables accept a server filter; other formats must return None to avoid a server-side reject.
   * Upsert planners always return None because the reader must reconcile the full log tail with
   * kv snapshots.
   */
  def logTailPredicate: Option[FlussPredicate]
}

/** Marker: planner yields partitions consumable by an append (log-table) reader factory. */
sealed trait AppendSplitPlanner extends SplitPlanner

/** Marker: planner yields partitions consumable by an upsert (primary-key) reader factory. */
sealed trait UpsertSplitPlanner extends SplitPlanner

/**
 * Base implementation: owns the shared Fluss client Connection + Admin so concrete planners can
 * issue metadata requests. [[close]] tears down the whole chain.
 *
 * Also centralizes the lake-snapshot probe: if data lake is enabled at the table level, try
 * loading the readable snapshot; treat [[LakeTableSnapshotNotExistException]] as "no snapshot",
 * all other exceptions propagate.
 */
abstract class AbstractSplitPlanner(
    override val tablePath: TablePath,
    override val tableInfo: TableInfo,
    override val flussConfig: Configuration)
  extends SplitPlanner {

  protected lazy val conn: Connection = ConnectionFactory.createConnection(flussConfig)

  protected lazy val admin: Admin = conn.getAdmin

  protected lazy val partitionInfos: util.List[PartitionInfo] =
    admin.listPartitionInfos(tablePath).get()

  protected def stoppingOffsetsInitializer: OffsetsInitializer

  /**
   * Probes the readable lake snapshot exactly once. Absent = data-lake disabled at the table
   * level, OR the readable snapshot admin call reported no snapshot; Present = snapshot to union
   * with the Fluss tail. Other exceptions propagate.
   */
  protected def probeLakeSnapshot(): Option[LakeSnapshot] = {
    if (!tableInfo.getTableConfig.isDataLakeEnabled) {
      None
    } else {
      try {
        Some(admin.getReadableLakeSnapshot(tablePath).get())
      } catch {
        case e: Exception =>
          if (
            ExceptionUtils
              .stripExecutionException(e)
              .isInstanceOf[LakeTableSnapshotNotExistException]
          ) {
            None
          } else {
            throw e
          }
      }
    }
  }

  protected def getBucketOffsets(
      initializer: OffsetsInitializer,
      partitionName: String,
      buckets: Seq[Int],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Map[Int, Long] = {
    initializer
      .getBucketOffsets(partitionName, buckets.map(Integer.valueOf).asJava, bucketOffsetsRetriever)
      .asScala
      .map(e => (e._1.intValue(), Long2long(e._2)))
      .toMap
  }

  override def close(): Unit = {
    if (admin != null) admin.close()
    if (conn != null) conn.close()
  }
}

/**
 * Single append (log-table) planner. Probes a readable lake snapshot at construction; if
 * present, the plan is a union of lake splits and the Fluss log-tail (from each bucket's
 * snapshotLogOffset to committed). If absent, the plan is a pure Fluss log scan from earliest
 * to committed (SCAN_START_UP_MODE deliberately ignored — see class scaladoc note below).
 *
 * Batch semantics note: start offset is hardcoded to [[OffsetsInitializer.full]] instead of
 * consuming the user-facing SCAN_START_UP_MODE. Rationale — batch reads semantically mean "the
 * full table". Letting SCAN_START_UP_MODE (a streaming concept) also gate batch planning creates
 * two problems: (a) the same config key would carry different semantics on append vs upsert
 * tables (KV snapshot has no partial-read semantics), which is confusing; (b) with mode=latest
 * and no writes since planning time, start==stop==tail — an empty range that trips the
 * reader-side `Invalid offset range` guard. Symmetric "batch = earliest → committed" closes both
 * concerns and keeps append/upsert planners aligned. Time-range batch reads should be expressed
 * via predicate pushdown on the timestamp column, not startup mode.
 *
 * `OffsetsInitializer.full()` is chosen over `OffsetsInitializer.earliest()` intentionally: for
 * a log table the two are semantically equivalent (see OffsetsInitializer.full javadoc), but
 * full() resolves each bucket's start offset to a concrete numeric value via RPC, whereas
 * earliest() returns the `LogScanner.EARLIEST_OFFSET` (-2) sentinel — the split-by-max-records
 * logic below requires concrete numeric bounds to compute range partitions.
 */
class AppendPlanner(
    override val tablePath: TablePath,
    override val tableInfo: TableInfo,
    partitionPredicate: Option[FlussPredicate],
    pushedPredicate: Option[FlussPredicate],
    projection: Array[Int],
    options: CaseInsensitiveStringMap,
    override val flussConfig: Configuration)
  extends AbstractSplitPlanner(tablePath, tableInfo, flussConfig)
  with AppendSplitPlanner {

  private val readableLakeSnapshot: Option[LakeSnapshot] = probeLakeSnapshot()

  override val hasLakeSnapshot: Boolean = readableLakeSnapshot.isDefined

  private val startOffsetsInitializer: OffsetsInitializer = OffsetsInitializer.full()

  override protected val stoppingOffsetsInitializer: OffsetsInitializer =
    FlussOffsetInitializers.stoppingOffsetsInitializer(true, options, flussConfig)

  // Server-side log filter requires ARROW format. Pushdown already gates this on the log-only
  // path (never sets pushedPredicate for non-ARROW), but re-checking here keeps the planner
  // self-consistent regardless of upstream pushdown behavior and applies uniformly to the
  // lake-union path (whose log-tail component reads from the same Fluss reader).
  override val logTailPredicate: Option[FlussPredicate] =
    if (tableInfo.getTableConfig.getLogFormat == LogFormat.ARROW) pushedPredicate else None

  override def plan(): Array[InputPartition] = readableLakeSnapshot match {
    case Some(snap) => planLakeUnion(snap)
    case None => planLogOnly()
  }

  // ---------------------------------------------------------------------------------------------
  // Log-only branch: pure Fluss log scan from earliest → committed with optional range splitting.
  // ---------------------------------------------------------------------------------------------

  private def planLogOnly(): Array[InputPartition] = {
    val maxRecordsPerPartition: Option[Long] = {
      val value = flussConfig.getLong(SparkFlussConf.SCAN_MAX_RECORDS_PER_PARTITION, 0)
      if (value > 0) Some(value) else None
    }

    val bucketOffsetsRetrieverImpl = maxRecordsPerPartition match {
      case Some(_) => new BucketOffsetsRetrieverImpl(admin, tablePath, true)
      case _ => new BucketOffsetsRetrieverImpl(admin, tablePath)
    }
    val buckets = (0 until tableInfo.getNumBuckets).toSeq

    def splitOffsetRange(
        tableBucket: TableBucket,
        startOffset: Long,
        stopOffset: Long,
        maxRecords: Long): Seq[InputPartition] = {
      if (
        startOffset < 0 || stopOffset <= startOffset || stopOffset <= (startOffset + maxRecords)
      ) {
        return Seq(FlussAppendInputPartition(tableBucket, startOffset, stopOffset))
      }
      val rangeSize = stopOffset - startOffset
      val numSplits = ((rangeSize + maxRecords - 1) / maxRecords).toInt
      val step = (rangeSize + numSplits - 1) / numSplits

      Iterator
        .from(0)
        .take(numSplits)
        .map(i => startOffset + i * step)
        .map {
          from => FlussAppendInputPartition(tableBucket, from, math.min(from + step, stopOffset))
        }
        .toSeq
    }

    def createPartitions(
        partitionId: Option[Long],
        startBucketOffsets: Map[Integer, Long],
        stoppingBucketOffsets: Map[Integer, Long]): Array[InputPartition] = {
      buckets.flatMap {
        bucketId =>
          val (startOffset, stopOffset) =
            (startBucketOffsets(bucketId), stoppingBucketOffsets(bucketId))
          val tableBucket = partitionId match {
            case Some(pid) => new TableBucket(tableInfo.getTableId, pid, bucketId)
            case None => new TableBucket(tableInfo.getTableId, bucketId)
          }
          maxRecordsPerPartition match {
            case Some(maxRecs) => splitOffsetRange(tableBucket, startOffset, stopOffset, maxRecs)
            case _ => Seq(FlussAppendInputPartition(tableBucket, startOffset, stopOffset))
          }
      }.toArray
    }

    if (tableInfo.isPartitioned) {
      val matching = SparkPartitionPredicate.filterPartitions(
        tableInfo,
        partitionInfos.asScala.toSeq,
        partitionPredicate)
      matching
        .map {
          partitionInfo =>
            val startBucketOffsets = startOffsetsInitializer.getBucketOffsets(
              partitionInfo.getPartitionName,
              buckets.map(Integer.valueOf).asJava,
              bucketOffsetsRetrieverImpl)
            val stoppingBucketOffsets = stoppingOffsetsInitializer.getBucketOffsets(
              partitionInfo.getPartitionName,
              buckets.map(Integer.valueOf).asJava,
              bucketOffsetsRetrieverImpl)
            (
              partitionInfo.getPartitionId,
              startBucketOffsets.asScala.map(e => (e._1, Long2long(e._2))),
              stoppingBucketOffsets.asScala.map(e => (e._1, Long2long(e._2))))
        }
        .flatMap {
          case (partitionId, startBucketOffsets, stoppingBucketOffsets) =>
            createPartitions(Some(partitionId), startBucketOffsets.toMap, stoppingBucketOffsets.toMap)
        }
        .toArray
    } else {
      val startBucketOffsets = startOffsetsInitializer.getBucketOffsets(
        null,
        buckets.map(Integer.valueOf).asJava,
        bucketOffsetsRetrieverImpl)
      val stoppingBucketOffsets = stoppingOffsetsInitializer.getBucketOffsets(
        null,
        buckets.map(Integer.valueOf).asJava,
        bucketOffsetsRetrieverImpl)
      createPartitions(
        None,
        startBucketOffsets.asScala.map(e => (e._1, Long2long(e._2))).toMap,
        stoppingBucketOffsets.asScala.map(e => (e._1, Long2long(e._2))).toMap)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Lake-union branch: lake splits + Fluss log tail from each bucket's snapshotLogOffset onward.
  // ---------------------------------------------------------------------------------------------

  private def planLakeUnion(snap: LakeSnapshot): Array[InputPartition] = {
    val lakeSource =
      FlussLakeUtils.createLakeSource(flussConfig.toMap, tableInfo.getProperties.toMap, tablePath)
    lakeSource.withProject(FlussLakeUtils.lakeProjection(projection))
    pushedPredicate.foreach(FlussLakeUtils.applyLakeFilters(lakeSource, _))

    val lakeSplits = lakeSource
      .createPlanner(new LakeSource.PlannerContext {
        override def snapshotId(): Long = snap.getSnapshotId
      })
      .plan()

    val tableBucketsOffset = snap.getTableBucketsOffset
    val buckets = (0 until tableInfo.getNumBuckets).toSeq
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)

    if (tableInfo.isPartitioned) {
      planLakePartitionedTable(
        lakeSplits.asScala.toSeq,
        tableBucketsOffset,
        buckets,
        bucketOffsetsRetriever)
    } else {
      planLakeNonPartitionedTable(
        lakeSplits.asScala.toSeq,
        tableBucketsOffset,
        buckets,
        bucketOffsetsRetriever)
    }
  }

  private def planLakeNonPartitionedTable(
      lakeSplits: Seq[LakeSplit],
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      buckets: Seq[Int],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = tableInfo.getTableId

    val lakePartitions = createLakePartitions(lakeSplits, tableId, partitionId = None)

    val stoppingOffsets =
      getBucketOffsets(stoppingOffsetsInitializer, null, buckets, bucketOffsetsRetriever)
    val logPartitions = buckets.flatMap {
      bucketId =>
        val tableBucket = new TableBucket(tableId, bucketId)
        createLogTailPartition(tableBucket, tableBucketsOffset, stoppingOffsets(bucketId))
    }

    (lakePartitions ++ logPartitions).toArray
  }

  private def planLakePartitionedTable(
      lakeSplits: Seq[LakeSplit],
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      buckets: Seq[Int],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = tableInfo.getTableId

    val filteredPartitionInfos = SparkPartitionPredicate.filterPartitions(
      tableInfo,
      partitionInfos.asScala.toSeq,
      partitionPredicate)

    val flussPartitionIdByName = mutable.LinkedHashMap.empty[String, Long]
    filteredPartitionInfos.foreach {
      pi => flussPartitionIdByName(pi.getPartitionName) = pi.getPartitionId
    }

    val lakeSplitsByPartition = groupLakeSplitsByPartitionBuffered(lakeSplits)
    var lakeSplitPartitionId = -1L

    val lakeAndLogPartitions = lakeSplitsByPartition.flatMap {
      case (partitionName, (partitionValues, splits)) =>
        flussPartitionIdByName.remove(partitionName) match {
          case Some(partitionId) =>
            val lakePartitions =
              createLakePartitions(splits.toSeq, tableId, Some(partitionId))

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
            if (
              SparkPartitionPredicate
                .matchesPartition(tableInfo, partitionValues, partitionPredicate)
            ) {
              val pid = lakeSplitPartitionId
              lakeSplitPartitionId -= 1
              createLakePartitions(splits.toSeq, tableId, Some(pid))
            } else {
              Seq.empty
            }
        }
    }.toSeq

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

  private def groupLakeSplitsByPartitionBuffered(lakeSplits: Seq[LakeSplit])
      : mutable.LinkedHashMap[String, (Seq[String], mutable.ArrayBuffer[LakeSplit])] = {
    val grouped =
      mutable.LinkedHashMap.empty[String, (Seq[String], mutable.ArrayBuffer[LakeSplit])]
    lakeSplits.foreach {
      split =>
        val partitionValues =
          if (split.partition() == null) Seq.empty[String] else split.partition().asScala.toSeq
        val partitionName =
          if (partitionValues.isEmpty) ""
          else partitionValues.mkString(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR)
        val (_, buf) =
          grouped.getOrElseUpdate(partitionName, (partitionValues, mutable.ArrayBuffer.empty))
        buf += split
    }
    grouped
  }

  private def createLakePartitions(
      splits: Seq[LakeSplit],
      tableId: Long,
      partitionId: Option[Long]): Seq[InputPartition] = {
    splits.map {
      split =>
        val tableBucket = partitionId match {
          case Some(pid) => new TableBucket(tableId, pid, split.bucket())
          case None => new TableBucket(tableId, split.bucket())
        }
        FlussLakeInputPartition(tableBucket, split)
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
}

/**
 * Single upsert (primary-key table) planner. Probes a readable lake snapshot at construction; if
 * present, the plan is a union of lake splits and per-bucket Fluss (kv-snapshot + log-tail)
 * partitions. If absent, the plan is a pure Fluss upsert scan derived from kv snapshots + log
 * tail.
 *
 * Startup-mode gating has been removed: a batch upsert scan is always full-table regardless of
 * the user-facing SCAN_START_UP_MODE setting — same rationale as [[AppendPlanner]].
 */
class UpsertPlanner(
    override val tablePath: TablePath,
    override val tableInfo: TableInfo,
    partitionPredicate: Option[FlussPredicate],
    pushedPredicate: Option[FlussPredicate],
    projection: Array[Int],
    options: CaseInsensitiveStringMap,
    override val flussConfig: Configuration)
  extends AbstractSplitPlanner(tablePath, tableInfo, flussConfig)
  with UpsertSplitPlanner {

  private val readableLakeSnapshot: Option[LakeSnapshot] = probeLakeSnapshot()

  override val hasLakeSnapshot: Boolean = readableLakeSnapshot.isDefined

  override protected val stoppingOffsetsInitializer: OffsetsInitializer =
    FlussOffsetInitializers.stoppingOffsetsInitializer(true, options, flussConfig)

  // Upsert never pushes a server-side log filter (kv+log union semantics require full log tail
  // to be reconciled with kv snapshots — see FlussUpsertPartitionReader).
  override val logTailPredicate: Option[FlussPredicate] = None

  override def plan(): Array[InputPartition] = readableLakeSnapshot match {
    case Some(snap) => planLakeUnion(snap)
    case None => planLogOnly()
  }

  // ---------------------------------------------------------------------------------------------
  // Log-only branch: Fluss kv snapshots + log tail (no lake involvement).
  // ---------------------------------------------------------------------------------------------

  private def planLogOnly(): Array[InputPartition] = {
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)

    if (tableInfo.isPartitioned) {
      val matching = SparkPartitionPredicate.filterPartitions(
        tableInfo,
        partitionInfos.asScala.toSeq,
        partitionPredicate)
      matching.flatMap {
        partitionInfo =>
          val partitionName = partitionInfo.getPartitionName
          val kvSnapshots = admin.getLatestKvSnapshots(tablePath, partitionName).get()
          createUpsertPartitions(partitionName, kvSnapshots, bucketOffsetsRetriever)
      }.toArray
    } else {
      val kvSnapshots = admin.getLatestKvSnapshots(tablePath).get()
      createUpsertPartitions(null, kvSnapshots, bucketOffsetsRetriever)
    }
  }

  private def createUpsertPartitions(
      partitionName: String,
      kvSnapshots: KvSnapshots,
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = kvSnapshots.getTableId
    val partitionId = kvSnapshots.getPartitionId
    val bucketIds = kvSnapshots.getBucketIds
    val bucketIdToLogOffset =
      stoppingOffsetsInitializer.getBucketOffsets(partitionName, bucketIds, bucketOffsetsRetriever)
    bucketIds.asScala
      .map {
        bucketId =>
          val tableBucket = new TableBucket(tableId, partitionId, bucketId)
          val snapshotIdOpt = kvSnapshots.getSnapshotId(bucketId)
          val logStartingOffsetOpt = kvSnapshots.getLogOffset(bucketId)
          val logEndingOffset = bucketIdToLogOffset.get(bucketId)

          if (snapshotIdOpt.isPresent) {
            assert(
              logStartingOffsetOpt.isPresent,
              "Log offset must be present when snapshot id is present")
            FlussUpsertInputPartition(
              tableBucket,
              snapshotIdOpt.getAsLong,
              logStartingOffsetOpt.getAsLong,
              logEndingOffset)
          } else {
            FlussUpsertInputPartition(tableBucket, -1L, LogScanner.EARLIEST_OFFSET, logEndingOffset)
          }
      }
      .map(_.asInstanceOf[InputPartition])
      .toArray
  }

  // ---------------------------------------------------------------------------------------------
  // Lake-union branch: lake splits (upsert view) + Fluss log tail after snapshotLogOffset.
  // ---------------------------------------------------------------------------------------------

  private def planLakeUnion(snap: LakeSnapshot): Array[InputPartition] = {
    val lakeSource =
      FlussLakeUtils.createLakeSource(flussConfig.toMap, tableInfo.getProperties.toMap, tablePath)
    lakeSource.withProject(FlussLakeUtils.lakeProjection(projection))
    pushedPredicate.foreach(FlussLakeUtils.applyLakeFilters(lakeSource, _))

    val lakeSplits = lakeSource
      .createPlanner(new LakeSource.PlannerContext {
        override def snapshotId(): Long = snap.getSnapshotId
      })
      .plan()

    val tableBucketsOffset = snap.getTableBucketsOffset
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)

    if (tableInfo.isPartitioned) {
      planLakePartitionedTable(lakeSplits.asScala.toSeq, tableBucketsOffset, bucketOffsetsRetriever)
    } else {
      planLakeNonPartitionedTable(lakeSplits.asScala.toSeq, tableBucketsOffset, bucketOffsetsRetriever)
    }
  }

  private def planLakeNonPartitionedTable(
      lakeSplits: Seq[LakeSplit],
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = tableInfo.getTableId
    val buckets = (0 until tableInfo.getNumBuckets).toSeq

    val stoppingOffsets =
      getBucketOffsets(stoppingOffsetsInitializer, null, buckets, bucketOffsetsRetriever)

    val lakeSplitsByBucket = lakeSplits.groupBy(_.bucket()).mapValues(_.toSeq).toMap

    buckets.flatMap {
      bucketId =>
        val tableBucket = new TableBucket(tableId, bucketId)
        val snapshotLogOffset = tableBucketsOffset.get(tableBucket)
        val stoppingOffset = stoppingOffsets(bucketId)

        createLakeUpsertPartition(
          tableBucket,
          lakeSplitsByBucket.get(bucketId),
          snapshotLogOffset,
          stoppingOffset)
    }.toArray
  }

  private def planLakePartitionedTable(
      lakeSplits: Seq[LakeSplit],
      tableBucketsOffset: java.util.Map[TableBucket, java.lang.Long],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Array[InputPartition] = {
    val tableId = tableInfo.getTableId
    val buckets = (0 until tableInfo.getNumBuckets).toSeq

    val filteredPartitionInfos = SparkPartitionPredicate.filterPartitions(
      tableInfo,
      partitionInfos.asScala.toSeq,
      partitionPredicate)

    val flussPartitionIdByName = mutable.LinkedHashMap.empty[String, Long]
    filteredPartitionInfos.foreach {
      pi => flussPartitionIdByName(pi.getPartitionName) = pi.getPartitionId
    }

    val lakeSplitsByPartition = groupLakeSplitsByPartition(lakeSplits)

    val lakePartitions = lakeSplitsByPartition.flatMap {
      case (partitionName, (partitionValues, splitsByBucket)) =>
        flussPartitionIdByName.remove(partitionName) match {
          case Some(partitionId) =>
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
                  snapshotLogOffset,
                  stoppingOffset)
            }

          case None =>
            if (
              SparkPartitionPredicate
                .matchesPartition(tableInfo, partitionValues, partitionPredicate)
            ) {
              buckets.flatMap {
                bucketId =>
                  val tableBucket = new TableBucket(tableId, -1, bucketId)
                  splitsByBucket.getOrElse(bucketId, Seq.empty).map {
                    lakeSplit => FlussLakeInputPartition(tableBucket, lakeSplit)
                  }
              }
            } else {
              Seq.empty
            }
        }
    }

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

            if (stoppingOffset > 0) {
              Some(
                FlussLakeUpsertInputPartition(
                  tableBucket,
                  null,
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
      lakeSplits: Seq[LakeSplit]): Map[String, (Seq[String], mutable.Map[Int, Seq[LakeSplit]])] = {
    val grouped =
      mutable.LinkedHashMap.empty[String, (Seq[String], mutable.Map[Int, Seq[LakeSplit]])]
    lakeSplits.foreach {
      split =>
        val partitionValues =
          if (split.partition() == null) Seq.empty[String] else split.partition().asScala.toSeq
        val partitionName =
          if (partitionValues.isEmpty) ""
          else partitionValues.mkString(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR)
        val (_, bucketMap) =
          grouped.getOrElseUpdate(partitionName, (partitionValues, mutable.Map.empty))
        val bucketId = split.bucket()
        val splits = bucketMap.getOrElse(bucketId, Seq.empty)
        bucketMap(bucketId) = splits :+ split
    }
    grouped.toMap
  }

  private def createLakeUpsertPartition(
      tableBucket: TableBucket,
      lakeSplits: Option[Seq[LakeSplit]],
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

    val lakeSplitList =
      if (lakeSplits.isDefined && lakeSplits.get.nonEmpty) {
        new java.util.ArrayList[LakeSplit](lakeSplits.get.asJava)
      } else null

    val logStartingOffset =
      if (snapshotLogOffset != null) snapshotLogOffset.longValue()
      else LogScanner.EARLIEST_OFFSET

    Some(
      FlussLakeUpsertInputPartition(
        tableBucket,
        lakeSplitList,
        logStartingOffset,
        stoppingOffset
      ))
  }
}
