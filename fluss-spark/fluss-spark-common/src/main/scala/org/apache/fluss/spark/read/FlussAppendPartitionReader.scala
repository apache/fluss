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

import org.apache.fluss.client.admin.OffsetSpec
import org.apache.fluss.client.table.scanner.ScanRecord
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.exception.FetchException
import org.apache.fluss.metadata.{TableBucket, TablePath}
import org.apache.fluss.predicate.Predicate
import org.apache.fluss.spark.utils.FlussConnectionCache
import org.apache.fluss.types.RowType

import java.time.Duration
import java.util.Collections
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Partition reader that reads log data from a single Fluss table bucket.
 *
 * @param allowExpiredOffsets
 *   whether a log-table batch scan may skip records removed by retention. Checkpoint positions and
 *   lake snapshot log tails require contiguous logs and must keep the default strict policy.
 */
class FlussAppendPartitionReader(
    tablePath: TablePath,
    projection: Array[Int],
    pushedPredicate: Option[Predicate],
    limit: Option[Int],
    flussPartition: FlussAppendInputPartition,
    flussConfig: Configuration,
    allowExpiredOffsets: Boolean = false)
  extends FlussPartitionReader(tablePath, flussConfig, limit) {

  override protected lazy val projectedRowType: RowType = rowType.project(projection)

  private val tableBucket: TableBucket = flussPartition.tableBucket
  private val partitionId = tableBucket.getPartitionId
  private val bucketId = tableBucket.getBucket
  private var logScanner = createLogScanner()

  // Iterator for current batch of records
  private var currentRecords: java.util.Iterator[ScanRecord] = java.util.Collections.emptyIterator()

  private var currentOffset: Long = flussPartition.startOffset.max(0L)

  // A poll can advance past filtered records while also returning rows. Apply its progress only
  // after those rows have been consumed, otherwise the final batch could be skipped.
  private var polledOffset: Long = currentOffset

  private val timeRange: Option[FlussTimeRange] = flussPartition.timeRange

  // Set once a record at or after the end of the requested window is seen; commit timestamps are
  // non-decreasing within a bucket, so no later record can belong to the window either.
  private var reachedWindowEnd = false

  // initialize log scanner
  initializeResources(initialize())

  private def pollMoreRecords(): Unit = {
    val timeoutNanos = POLL_TIMEOUT.toNanos
    var lastProgressNanos = monotonicTimeNanos()
    while (!closed && !currentRecords.hasNext && currentOffset < flussPartition.stopOffset) {
      val previousOffset = currentOffset
      val remainingNanos =
        math.max(0L, timeoutNanos - (monotonicTimeNanos() - lastProgressNanos))
      val scanRecords =
        try {
          logScanner.poll(Duration.ofNanos(remainingNanos))
        } catch {
          case error: FetchException if allowExpiredOffsets =>
            recoverExpiredOffset(error)
            null
        }
      if (scanRecords != null) {
        currentRecords = scanRecords.records(tableBucket).iterator()
        val consumedOffset = scanRecords.consumedUpToOffset(tableBucket)
        polledOffset = if (consumedOffset == null) currentOffset else consumedOffset.longValue()
      }
      if (!currentRecords.hasNext) {
        currentOffset = math.max(currentOffset, polledOffset)
        // The client may repeat the last consumed offset while draining a returned batch.
        // Only a real advance renews the budget; an empty poll alone does not prove completion.
        if (currentOffset > previousOffset) {
          lastProgressNanos = monotonicTimeNanos()
        } else if (monotonicTimeNanos() - lastProgressNanos >= timeoutNanos) {
          // At the deadline, confirm retention before completing or recovering a batch range.
          // A strict EARLIEST scan starts at zero until it advances; later gaps must remain errors.
          if (
            allowExpiredOffsets ||
            (flussPartition.startOffset == LogScanner.EARLIEST_OFFSET && currentOffset == 0L)
          ) {
            val earliestOffset = fetchEarliestOffset()
            if (earliestOffset >= flussPartition.stopOffset) {
              currentOffset = earliestOffset
            } else if (allowExpiredOffsets && earliestOffset > currentOffset) {
              restartFromOffset(earliestOffset)
            }
          }
          if (currentOffset <= previousOffset) {
            throw new IllegalStateException(
              s"Log scan made no progress for $tableBucket at offset $currentOffset " +
                s"before the stop offset ${flussPartition.stopOffset}")
          }
          lastProgressNanos = monotonicTimeNanos()
        }
      }
    }
  }

  /** Supplies monotonic time for the no-progress budget. */
  protected def monotonicTimeNanos(): Long = System.nanoTime()

  override def next0(): Boolean = {
    while (!closed && !reachedWindowEnd && currentOffset < flussPartition.stopOffset) {
      if (!currentRecords.hasNext) {
        currentOffset = math.max(currentOffset, polledOffset)
        if (currentOffset >= flussPartition.stopOffset) {
          return false
        }
        pollMoreRecords()
      }
      if (currentRecords.hasNext) {
        val scanRecord = currentRecords.next()
        if (scanRecord.logOffset() >= flussPartition.stopOffset) {
          currentOffset = flussPartition.stopOffset
          return false
        }
        currentOffset = scanRecord.logOffset() + 1
        timeRange match {
          case Some(range) if range.isAfter(scanRecord.timestamp()) => reachedWindowEnd = true
          // The record precedes the requested window: the start offset resolved from the start
          // timestamp is only time-index accurate on tiered segments, so it can undershoot.
          case Some(range) if !range.contains(scanRecord.timestamp()) => // skip
          case _ =>
            currentRow = convertToSparkRow(scanRecord)
            return true
        }
      }
    }
    false
  }

  override def close0(): Unit = {
    if (logScanner != null) {
      logScanner.close()
    }
  }

  private def createLogScanner(): LogScanner = initializeResources {
    table.newScan().project(projection).filter(pushedPredicate.orNull).createLogScanner()
  }

  private def subscribe(offset: Long): Unit = {
    if (partitionId != null) {
      logScanner.subscribe(partitionId, bucketId, offset)
    } else {
      logScanner.subscribe(bucketId, offset)
    }
  }

  private def recoverExpiredOffset(error: FetchException): Unit = {
    val earliestOffset =
      try {
        fetchEarliestOffset()
      } catch {
        case interrupted: InterruptedException =>
          Thread.currentThread().interrupt()
          interrupted.addSuppressed(error)
          throw interrupted
        case NonFatal(lookupError) =>
          if (lookupError ne error) {
            error.addSuppressed(lookupError)
          }
          throw error
      }
    if (earliestOffset <= currentOffset) {
      // A failed fetch alone does not justify skipping data. Recover only a proven retention gap.
      throw error
    }
    restartFromOffset(earliestOffset)
  }

  private def restartFromOffset(offset: Long): Unit = {
    currentOffset = offset
    polledOffset = currentOffset
    logScanner.close()
    logScanner = null
    if (currentOffset < flussPartition.stopOffset) {
      // Replacing the scanner discards in-flight fetches and errors for its old subscription.
      logScanner = createLogScanner()
      initializeResources(subscribe(currentOffset))
    }
  }

  /** Loads the retained start after checking that the original table and partition still exist. */
  protected def fetchEarliestOffset(): Long = {
    val lease = FlussConnectionCache.acquire(flussConfig)
    try {
      val admin = lease.connection.getAdmin
      val timeoutMs = flussConfig.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis
      def checkTableIdentity(): Unit = {
        val currentTableId =
          admin.getTableInfo(tablePath).get(timeoutMs, TimeUnit.MILLISECONDS).getTableId
        if (currentTableId != tableBucket.getTableId) {
          throw new IllegalStateException(
            s"Table $tablePath was recreated while reading $tableBucket")
        }
      }
      def currentPartitionName(): String = {
        admin
          .listPartitionInfos(tablePath, true)
          .get(timeoutMs, TimeUnit.MILLISECONDS)
          .asScala
          .find(_.getPartitionId == partitionId.longValue())
          .getOrElse(throw new IllegalStateException(
            s"Partition $partitionId no longer exists in $tablePath"))
          .getPartitionName
      }
      checkTableIdentity()
      val buckets = Collections.singletonList(Integer.valueOf(bucketId))
      val spec = new OffsetSpec.EarliestSpec()
      val partitionName = if (partitionId == null) null else currentPartitionName()
      val offsets = if (partitionId == null) {
        admin.listOffsets(tablePath, buckets, spec)
      } else {
        admin.listOffsets(tablePath, partitionName, buckets, spec)
      }
      val earliestOffset =
        offsets.bucketResult(bucketId).get(timeoutMs, TimeUnit.MILLISECONDS).longValue()
      // listOffsets resolves names independently. Reject a replacement created during that lookup.
      checkTableIdentity()
      if (partitionId != null && currentPartitionName() != partitionName) {
        throw new IllegalStateException(s"Partition $partitionId changed while reading $tablePath")
      }
      earliestOffset
    } catch {
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        throw interrupted
    } finally {
      lease.close()
    }
  }

  private def initialize(): Unit = {
    if (flussPartition.startOffset >= flussPartition.stopOffset) {
      throw new IllegalArgumentException(s"Invalid offset range $flussPartition")
    }
    logInfo(s"Prepare read table $tablePath $flussPartition")
    subscribe(flussPartition.startOffset)
    pollMoreRecords()
  }
}
