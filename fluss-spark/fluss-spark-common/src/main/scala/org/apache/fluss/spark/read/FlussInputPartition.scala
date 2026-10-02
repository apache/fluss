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

import org.apache.fluss.metadata.TableBucket

import org.apache.spark.sql.connector.read.InputPartition

trait FlussInputPartition extends InputPartition {

  override def preferredLocations(): Array[String] = {
    // Could return tablet server locations for data locality
    Array.empty[String]
  }

}

/**
 * The `[startMs, endMs)` window a time-range batch read was asked for, on the record commit
 * timestamp. The reader applies it because offsets resolved from timestamps are only as accurate as
 * the server-side time index. Timestamps are non-decreasing within a bucket, so the first record at
 * or after `endMs` also ends the partition.
 */
case class FlussTimeRange(startMs: Long, endMs: Long) {

  def contains(timestampMs: Long): Boolean = timestampMs >= startMs && timestampMs < endMs

  def isAfter(timestampMs: Long): Boolean = timestampMs >= endMs

  override def toString: String = s"TimeRange[$startMs - $endMs]"
}

/**
 * Represents an input partition for reading data from a Fluss table bucket.
 *
 * @param tableBucket
 *   the table bucket to read from
 * @param timeRange
 *   the requested time window, set for a time-range batch read only
 */
case class FlussAppendInputPartition(
    tableBucket: TableBucket,
    startOffset: Long,
    stopOffset: Long,
    timeRange: Option[FlussTimeRange] = None)
  extends FlussInputPartition {
  override def toString: String = {
    s"FlussAppendInputPartition{tableId=${tableBucket.getTableId}, bucketId=${tableBucket.getBucket}," +
      s" partitionId=${tableBucket.getPartitionId}" +
      s" logStartOffset=$startOffset, logStopOffset=$stopOffset, timeRange=$timeRange"
  }
}

/**
 * Represents an input partition for reading data from a primary key table bucket. This partition
 * includes snapshot information for hybrid snapshot-log reading.
 *
 * @param tableBucket
 *   the table bucket to read from
 * @param snapshotId
 *   the snapshot ID to read from, [[org.apache.fluss.metadata.TableBucketSnapshot.NO_SNAPSHOT_ID]]
 *   if no snapshot
 * @param logStartingOffset
 *   the log offset where incremental reading should start
 * @param logStoppingOffset
 *   the log offset where incremental reading should end
 * @param timeRange
 *   the requested time window, set for a time-range batch read only
 */
case class FlussUpsertInputPartition(
    tableBucket: TableBucket,
    snapshotId: Long,
    logStartingOffset: Long,
    logStoppingOffset: Long,
    timeRange: Option[FlussTimeRange] = None)
  extends FlussInputPartition {
  override def toString: String = {
    s"FlussUpsertInputPartition{tableId=${tableBucket.getTableId}, bucketId=${tableBucket.getBucket}," +
      s" partitionId=${tableBucket.getPartitionId}, snapshotId=$snapshotId," +
      s" logStartOffset=$logStartingOffset, logStopOffset=$logStoppingOffset," +
      s" timeRange=$timeRange}"
  }
}
