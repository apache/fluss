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

package com.alibaba.fluss.connector.spark

import com.alibaba.fluss.metadata.TableBucket
import com.alibaba.fluss.spark.TableBucketInfo

import org.apache.spark.sql.connector.read.InputPartition

/** A [[InputPartition]] for reading Fluss data in a batch based streaming/batch query. */
trait FlussInputPartition extends InputPartition {
  def splits: Seq[FlussOffsetRange]
}

case class SimpleFlussInputPartition(splits: Seq[FlussOffsetRange]) extends FlussInputPartition
object FlussInputPartition {
  def apply(split: FlussOffsetRange): FlussInputPartition = {
    SimpleFlussInputPartition(Seq(split))
  }

  def apply(splits: Seq[FlussOffsetRange]): FlussInputPartition = {
    SimpleFlussInputPartition(splits)
  }
}

case class FlussOffsetRange(tableBucketInfo: TableBucketInfo, fromOffset: Long, untilOffset: Long) {
  def tableBucket: TableBucket = tableBucketInfo.getTableBucket

  def partitionName: String = tableBucketInfo.getPartitionName
  def snapshotId: Long = tableBucketInfo.getSnapshotId

  /** ignore snapshot offset */
  def size: Long = untilOffset - fromOffset
}
