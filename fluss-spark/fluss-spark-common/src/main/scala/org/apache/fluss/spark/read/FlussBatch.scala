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
import org.apache.fluss.client.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer, SnapshotOffsetsInitializer}
import org.apache.fluss.client.metadata.KvSnapshots
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{PartitionInfo, TableBucket, TableInfo, TablePath}

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters._

abstract class FlussBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    flussConfig: Configuration)
  extends Batch
  with AutoCloseable {

  lazy val conn: Connection = ConnectionFactory.createConnection(flussConfig)

  lazy val admin: Admin = conn.getAdmin

  lazy val partitionInfos: util.List[PartitionInfo] = admin.listPartitionInfos(tablePath).get()

  def startOffsetsInitializer: OffsetsInitializer

  def stoppingOffsetsInitializer: OffsetsInitializer

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
}

/** Batch for reading log table (append-only table). */
class FlussAppendBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    startOffsetsInitializer: OffsetsInitializer,
    stoppingOffsetsInitializer: OffsetsInitializer,
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussBatch(tablePath, tableInfo, readSchema, flussConfig) {

  override val startOffsetsInitializer: OffsetsInitializer = {
    FlussOffsetInitializers.startOffsetsInitializer(options, flussConfig)
  }

  override val stoppingOffsetsInitializer: OffsetsInitializer = {
    FlussOffsetInitializers.stoppingOffsetsInitializer(true, options, flussConfig)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val bucketOffsetsRetrieverImpl = new BucketOffsetsRetrieverImpl(admin, tablePath)
    val buckets = (0 until tableInfo.getNumBuckets).toSeq

    def createPartitions(
        partitionId: Option[Long],
        startBucketOffsets: Map[Integer, Long],
        stoppingBucketOffsets: Map[Integer, Long]): Array[InputPartition] = {
      buckets.map {
        bucketId =>
          val (startBucketOffset, stoppingBucketOffset) =
            (startBucketOffsets(bucketId), stoppingBucketOffsets(bucketId))
          partitionId match {
            case Some(partitionId) =>
              val tableBucket = new TableBucket(tableInfo.getTableId, partitionId, bucketId)
              FlussAppendInputPartition(tableBucket, startBucketOffset, stoppingBucketOffset)
                .asInstanceOf[InputPartition]
            case None =>
              val tableBucket = new TableBucket(tableInfo.getTableId, bucketId)
              FlussAppendInputPartition(tableBucket, startBucketOffset, stoppingBucketOffset)
                .asInstanceOf[InputPartition]
          }
      }.toArray
    }

    if (tableInfo.isPartitioned) {
      partitionInfos.asScala
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
            createPartitions(
              Some(partitionId),
              startBucketOffsets.toMap,
              stoppingBucketOffsets.toMap)
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

  override def createReaderFactory(): PartitionReaderFactory = {
    new FlussAppendPartitionReaderFactory(tablePath, projection, options, flussConfig)
  }

}

/** Batch for reading primary key table (upsert table). */
class FlussUpsertBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussBatch(tablePath, tableInfo, readSchema, flussConfig) {

  override val startOffsetsInitializer: OffsetsInitializer = {
    val offsetsInitializer = FlussOffsetInitializers.startOffsetsInitializer(options, flussConfig)
    if (!offsetsInitializer.isInstanceOf[SnapshotOffsetsInitializer]) {
      throw new UnsupportedOperationException("Upsert scan only support FULL startup mode.")
    }
    offsetsInitializer
  }

  override val stoppingOffsetsInitializer: OffsetsInitializer = {
    FlussOffsetInitializers.stoppingOffsetsInitializer(true, options, flussConfig)
  }

  private val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)

  override def planInputPartitions(): Array[InputPartition] = {
    def createPartitions(partitionName: String, kvSnapshots: KvSnapshots): Array[InputPartition] = {
      val tableId = kvSnapshots.getTableId
      val partitionId = kvSnapshots.getPartitionId
      val bucketIds = kvSnapshots.getBucketIds
      val bucketIdToLogOffset =
        stoppingOffsetsInitializer.getBucketOffsets(
          partitionName,
          bucketIds,
          bucketOffsetsRetriever)
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

              // Create hybrid partition
              FlussUpsertInputPartition(
                tableBucket,
                snapshotIdOpt.getAsLong,
                logStartingOffsetOpt.getAsLong,
                logEndingOffset
              )
            } else {
              // No snapshot yet, only read log from beginning
              FlussUpsertInputPartition(
                tableBucket,
                -1L,
                LogScanner.EARLIEST_OFFSET,
                logEndingOffset)
            }
        }
        .map(_.asInstanceOf[InputPartition])
        .toArray
    }

    if (tableInfo.isPartitioned) {
      partitionInfos.asScala.flatMap {
        partitionInfo =>
          val partitionName = partitionInfo.getPartitionName
          val kvSnapshots =
            admin.getLatestKvSnapshots(tablePath, partitionName).get()
          createPartitions(partitionName, kvSnapshots)
      }.toArray
    } else {
      val kvSnapshots = admin.getLatestKvSnapshots(tablePath).get()
      createPartitions(null, kvSnapshots)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new FlussUpsertPartitionReaderFactory(tablePath, projection, options, flussConfig)
  }
}
