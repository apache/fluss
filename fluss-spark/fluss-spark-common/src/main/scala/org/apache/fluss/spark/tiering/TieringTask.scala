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

import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.ScanRecord
import org.apache.fluss.client.tiering.{TableBucketWriteResult, TieringLogSplit, TieringSnapshotSplit, TieringSplit, TieringWriterInitContext}
import org.apache.fluss.config.Configuration
import org.apache.fluss.lake.writer.LakeWriter
import org.apache.fluss.spark.tiering.SparkTieringJobRunner.createLakeTieringFactory
import org.apache.fluss.spark.utils.FlussConnectionCache
import org.apache.fluss.utils.IOUtils.closeQuietly

import org.apache.spark.internal.Logging

import java.time.Duration

import scala.collection.JavaConverters._

/**
 * Executor-side processing logic for a single tiering split.
 *
 * Each Spark task processes one {@link TieringSplit}: reads data from Fluss (log or KV snapshot)
 * and writes it to the lake via [[LakeWriter]].
 */
object TieringTask extends Logging {

  private val UNKNOWN_BUCKET_TIMESTAMP = -1L

  private def checkInterrupted(): Unit = {
    if (Thread.currentThread().isInterrupted) {
      throw new InterruptedException("Spark tiering task was cancelled")
    }
  }

  def process(
      split: TieringSplit,
      flussConfig: Configuration,
      dataLakeFormat: String,
      lakeConfig: Configuration,
      pollTimeoutMs: Long,
      partitionBucketCount: java.lang.Integer,
      ioTmpDirs: Array[String] = null): SerializedTaskResult = {
    val tablePath = split.getTablePath
    val tableBucket = split.getTableBucket
    val partitionName = Option(split.getPartitionName)

    var lease: FlussConnectionCache.Lease = null
    var table: Table = null

    try {
      val (acquiredLease, acquiredTable) =
        FlussConnectionCache.acquireWithTable(flussConfig, tablePath)
      lease = acquiredLease
      table = acquiredTable

      // Verify table ID hasn't changed (table not dropped/recreated)
      val currentTableId = table.getTableInfo.getTableId
      if (currentTableId != tableBucket.getTableId) {
        throw new IllegalStateException(
          s"Table ID mismatch for $tablePath: expected ${tableBucket.getTableId}" +
            s" but got $currentTableId. Table may have been dropped and recreated.")
      }

      // Initialize LakeTieringFactory on executor
      val lakeTieringFactory = createLakeTieringFactory(dataLakeFormat, lakeConfig)

      val writerInitContext =
        new TieringWriterInitContext(
          tablePath,
          tableBucket,
          partitionName.orNull,
          table.getTableInfo,
          split.getSplitIndex,
          split.getTieringRoundTimestamp,
          partitionBucketCount,
          ioTmpDirs)
      val createWriter = () => lakeTieringFactory.createLakeWriter(writerInitContext)

      val result = if (split.isTieringLogSplit) {
        processLogSplit(table, createWriter, split.asTieringLogSplit(), pollTimeoutMs)
      } else {
        processSnapshotSplit(table, createWriter, split.asTieringSnapshotSplit(), pollTimeoutMs)
      }
      SerializedTaskResult.fromWriteResult(result, lakeTieringFactory.getWriteResultSerializer)
    } finally {
      closeQuietly(table, "Table")
      closeQuietly(lease, "Connection lease")
    }
  }

  private[tiering] def processLogSplit(
      table: Table,
      createWriter: () => LakeWriter[AnyRef],
      split: TieringLogSplit,
      pollTimeoutMs: Long): TableBucketWriteResult[AnyRef] = {
    val logScanner = table.newScan().createLogScanner()
    var lakeWriter: LakeWriter[AnyRef] = null
    try {
      val tableBucket = split.getTableBucket
      if (tableBucket.getPartitionId != null) {
        logScanner.subscribe(
          tableBucket.getPartitionId,
          tableBucket.getBucket,
          split.getStartingOffset)
      } else {
        logScanner.subscribe(tableBucket.getBucket, split.getStartingOffset)
      }

      val stoppingOffset = split.getStoppingOffset
      val pollTimeout = Duration.ofMillis(pollTimeoutMs)
      var maxTimestamp = UNKNOWN_BUCKET_TIMESTAMP
      var finished = split.getStartingOffset >= stoppingOffset

      while (!finished) {
        checkInterrupted()
        val scanRecords = logScanner.poll(pollTimeout)
        val records = scanRecords.records(tableBucket)
        if (records != null) {
          records.asScala.foreach {
            record =>
              if (record.logOffset() < stoppingOffset) {
                if (lakeWriter == null) {
                  lakeWriter = createWriter()
                }
                lakeWriter.write(record)
                if (record.timestamp() > maxTimestamp) {
                  maxTimestamp = record.timestamp()
                }
              }
              if (record.logOffset() >= stoppingOffset - 1) {
                finished = true
              }
          }
        }
        // Empty WAL batches can advance offsets without returning any records.
        val consumedUpToOffset = scanRecords.consumedUpToOffset(tableBucket)
        if (consumedUpToOffset != null && consumedUpToOffset >= stoppingOffset) {
          finished = true
        }
      }

      val writeResult = if (lakeWriter == null) null else lakeWriter.complete()

      logInfo(
        s"Finished tiering log split for bucket $tableBucket," +
          s" logEndOffset=$stoppingOffset, maxTimestamp=$maxTimestamp")

      new TableBucketWriteResult(
        split.getTablePath,
        tableBucket,
        split.getPartitionName,
        writeResult,
        stoppingOffset,
        maxTimestamp,
        split.getNumberOfSplits)
    } finally {
      closeQuietly(lakeWriter, "LakeWriter")
      closeQuietly(logScanner, "LogScanner")
    }
  }

  private[tiering] def processSnapshotSplit(
      table: Table,
      createWriter: () => LakeWriter[AnyRef],
      split: TieringSnapshotSplit,
      pollTimeoutMs: Long): TableBucketWriteResult[AnyRef] = {
    val tableBucket = split.getTableBucket
    val batchScanner = table
      .newScan()
      .createBatchScanner(tableBucket, split.getSnapshotId)
    var lakeWriter: LakeWriter[AnyRef] = null

    try {
      val pollTimeout = Duration.ofMillis(pollTimeoutMs)
      var batch = batchScanner.pollBatch(pollTimeout)
      while (batch != null) {
        try {
          while (batch.hasNext) {
            checkInterrupted()
            val row = batch.next()
            // Snapshot rows represent inserts at the snapshot's log offset.
            if (lakeWriter == null) {
              lakeWriter = createWriter()
            }
            lakeWriter.write(new ScanRecord(row))
          }
        } finally {
          batch.close()
        }
        batch = batchScanner.pollBatch(pollTimeout)
      }

      val writeResult = if (lakeWriter == null) null else lakeWriter.complete()

      val logEndOffset = split.getLogOffsetOfSnapshot

      logInfo(
        s"Finished tiering snapshot split for bucket $tableBucket," +
          s" snapshotId=${split.getSnapshotId}, logEndOffset=$logEndOffset")

      new TableBucketWriteResult(
        split.getTablePath,
        tableBucket,
        split.getPartitionName,
        writeResult,
        logEndOffset,
        UNKNOWN_BUCKET_TIMESTAMP,
        split.getNumberOfSplits
      )
    } finally {
      closeQuietly(lakeWriter, "LakeWriter")
      closeQuietly(batchScanner, "BatchScanner")
    }
  }
}
