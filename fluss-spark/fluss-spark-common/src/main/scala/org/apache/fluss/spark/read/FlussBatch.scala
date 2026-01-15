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
import org.apache.fluss.client.metadata.KvSnapshots
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
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussBatch(tablePath, tableInfo, readSchema, flussConfig) {

  override def planInputPartitions(): Array[InputPartition] = {
    def createPartitions(partitionId: Option[Long]): Array[InputPartition] = {
      (0 until tableInfo.getNumBuckets).map {
        bucketId =>
          val tableBucket = partitionId match {
            case Some(partitionId) =>
              new TableBucket(tableInfo.getTableId, partitionId, bucketId)
            case None =>
              new TableBucket(tableInfo.getTableId, bucketId)
          }
          FlussAppendInputPartition(tableBucket).asInstanceOf[InputPartition]
      }.toArray
    }

    if (tableInfo.isPartitioned) {
      partitionInfos.asScala.flatMap {
        partitionInfo => createPartitions(Some(partitionInfo.getPartitionId))
      }.toArray
    } else {
      createPartitions(None)
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

  override def planInputPartitions(): Array[InputPartition] = {
    def createPartitions(kvSnapshots: KvSnapshots): Array[InputPartition] = {
      val tableId = kvSnapshots.getTableId
      val partitionId = kvSnapshots.getPartitionId
      kvSnapshots.getBucketIds.asScala
        .map {
          bucketId =>
            val tableBucket = new TableBucket(tableId, partitionId, bucketId)
            val snapshotIdOpt = kvSnapshots.getSnapshotId(bucketId)
            val logOffsetOpt = kvSnapshots.getLogOffset(bucketId)

            if (snapshotIdOpt.isPresent) {
              assert(
                logOffsetOpt.isPresent,
                "Log offset must be present when snapshot id is present")

              // Create hybrid partition
              FlussUpsertInputPartition(
                tableBucket,
                snapshotIdOpt.getAsLong,
                logOffsetOpt.getAsLong
              )
            } else {
              // No snapshot yet, only read log from beginning
              FlussUpsertInputPartition(tableBucket, -1L, 0L)
            }
        }
        .map(_.asInstanceOf[InputPartition])
        .toArray
    }

    if (tableInfo.isPartitioned) {
      partitionInfos.asScala.flatMap {
        partitionInfo =>
          val kvSnapshots =
            admin.getLatestKvSnapshots(tablePath, partitionInfo.getPartitionName).get()
          createPartitions(kvSnapshots)
      }.toArray
    } else {
      val kvSnapshots = admin.getLatestKvSnapshots(tablePath).get()
      createPartitions(kvSnapshots)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new FlussUpsertPartitionReaderFactory(tablePath, projection, options, flussConfig)
  }
}
