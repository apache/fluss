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

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.ScanRecord
import org.apache.fluss.client.tiering.TieringWriterInitContext
import org.apache.fluss.config.Configuration
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer
import org.apache.fluss.lake.writer.{LakeTieringFactory, LakeWriter}

import org.apache.spark.internal.Logging

import java.time.Duration

import scala.collection.JavaConverters._

/**
 * Executor-side processing logic for a single tiering split.
 *
 * Each Spark task processes one [[TieringSplit]]: reads data from Fluss (log or KV snapshot) and
 * writes it to the lake via [[LakeWriter]].
 */
object TieringTask extends Logging {

  private val UNKNOWN_BUCKET_TIMESTAMP = -1L

  def process(
      split: TieringSplit,
      flussConfig: Configuration,
      dataLakeFormat: String,
      lakeConfig: Configuration,
      pollTimeoutMs: Long): SerializedTaskResult = {
    val tablePath = split.tablePath
    val tableBucket = split.tableBucket
    val partitionName = split.partitionName

    var connection: Connection = null
    var table: Table = null
    var lakeWriter: LakeWriter[AnyRef] = null

    try {
      connection = ConnectionFactory.createConnection(flussConfig)
      table = connection.getTable(tablePath)

      // Verify table ID hasn't changed (table not dropped/recreated)
      val currentTableId = table.getTableInfo.getTableId
      if (currentTableId != tableBucket.getTableId) {
        throw new IllegalStateException(
          s"Table ID mismatch for $tablePath: expected ${tableBucket.getTableId}" +
            s" but got $currentTableId. Table may have been dropped and recreated.")
      }

      // Initialize LakeTieringFactory on executor
      val lakeTieringFactory = createLakeTieringFactory(dataLakeFormat, lakeConfig)
      val writeResultSerializer = lakeTieringFactory.getWriteResultSerializer

      val writerInitContext =
        new TieringWriterInitContext(
          tablePath,
          tableBucket,
          partitionName.orNull,
          table.getTableInfo)
      lakeWriter = lakeTieringFactory.createLakeWriter(writerInitContext)

      split match {
        case logSplit: TieringLogSplit =>
          processLogSplit(table, lakeWriter, logSplit, pollTimeoutMs, writeResultSerializer)
        case snapshotSplit: TieringSnapshotSplit =>
          processSnapshotSplit(
            table,
            lakeWriter,
            snapshotSplit,
            pollTimeoutMs,
            writeResultSerializer)
      }
    } finally {
      closeQuietly(lakeWriter, "LakeWriter")
      closeQuietly(table, "Table")
      closeQuietly(connection, "Connection")
    }
  }

  /** Creates a [[LakeTieringFactory]] from the data lake format and config. */
  private def createLakeTieringFactory(
      dataLakeFormat: String,
      lakeConfig: Configuration): LakeTieringFactory[AnyRef, AnyRef] = {
    val lakeStoragePlugin = LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat, null)
    val lakeStorage = lakeStoragePlugin.createLakeStorage(lakeConfig)
    lakeStorage.createLakeTieringFactory().asInstanceOf[LakeTieringFactory[AnyRef, AnyRef]]
  }

  private def processLogSplit(
      table: Table,
      lakeWriter: LakeWriter[AnyRef],
      split: TieringLogSplit,
      pollTimeoutMs: Long,
      writeResultSerializer: SimpleVersionedSerializer[AnyRef]): SerializedTaskResult = {
    val logScanner = table.newScan().createLogScanner()
    try {
      val tableBucket = split.tableBucket
      if (tableBucket.getPartitionId != null) {
        logScanner.subscribe(
          tableBucket.getPartitionId,
          tableBucket.getBucket,
          split.startingOffset)
      } else {
        logScanner.subscribe(tableBucket.getBucket, split.startingOffset)
      }

      val stoppingOffset = split.stoppingOffset
      val pollTimeout = Duration.ofMillis(pollTimeoutMs)
      var lastOffset = -1L
      var maxTimestamp = UNKNOWN_BUCKET_TIMESTAMP
      var finished = false

      while (!finished) {
        val scanRecords = logScanner.poll(pollTimeout)
        val records = scanRecords.records(tableBucket)
        if (records != null) {
          records.asScala.foreach {
            record =>
              if (record.logOffset() < stoppingOffset) {
                lakeWriter.write(record)
                lastOffset = record.logOffset()
                if (record.timestamp() > maxTimestamp) {
                  maxTimestamp = record.timestamp()
                }
              }
              if (record.logOffset() >= stoppingOffset - 1) {
                finished = true
              }
          }
        }
      }

      val writeResult = lakeWriter.complete()
      val serializedBytes = if (writeResult != null) {
        writeResultSerializer.serialize(writeResult)
      } else {
        null
      }

      logInfo(
        s"Finished tiering log split for bucket $tableBucket," +
          s" logEndOffset=$stoppingOffset, maxTimestamp=$maxTimestamp")

      SerializedTaskResult(
        split.tablePath,
        tableBucket,
        split.partitionName,
        serializedBytes,
        writeResultSerializer.getVersion,
        stoppingOffset,
        maxTimestamp,
        split.numberOfSplits)
    } finally {
      closeQuietly(logScanner, "LogScanner")
    }
  }

  private def processSnapshotSplit(
      table: Table,
      lakeWriter: LakeWriter[AnyRef],
      split: TieringSnapshotSplit,
      pollTimeoutMs: Long,
      writeResultSerializer: SimpleVersionedSerializer[AnyRef]): SerializedTaskResult = {
    val tableBucket = split.tableBucket
    val batchScanner = table
      .newScan()
      .createBatchScanner(tableBucket, split.snapshotId)

    try {
      val pollTimeout = Duration.ofMillis(pollTimeoutMs)
      var batch = batchScanner.pollBatch(pollTimeout)
      while (batch != null) {
        while (batch.hasNext) {
          val row = batch.next()
          // Wrap InternalRow as ScanRecord with INSERT change type for snapshot data
          val scanRecord = new ScanRecord(row)
          lakeWriter.write(scanRecord)
        }
        batch.close()
        batch = batchScanner.pollBatch(pollTimeout)
      }

      val writeResult = lakeWriter.complete()
      val serializedBytes = if (writeResult != null) {
        writeResultSerializer.serialize(writeResult)
      } else {
        null
      }

      val logEndOffset = split.logOffsetOfSnapshot

      logInfo(
        s"Finished tiering snapshot split for bucket $tableBucket," +
          s" snapshotId=${split.snapshotId}, logEndOffset=$logEndOffset")

      SerializedTaskResult(
        split.tablePath,
        tableBucket,
        split.partitionName,
        serializedBytes,
        writeResultSerializer.getVersion,
        logEndOffset,
        UNKNOWN_BUCKET_TIMESTAMP,
        split.numberOfSplits
      )
    } finally {
      closeQuietly(batchScanner, "BatchScanner")
    }
  }
}
