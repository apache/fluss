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

import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.PartitionOffset
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

/**
 * An [[Offset]] for the [[FlussSource]]. This one tracks all buckets of subscribed topics and their
 * offsets.
 */
case class FlussSourceOffset(bucketToOffsets: Map[TableBucketInfo, Long]) extends Offset {

  override def json(): String = {
    JsonUtils.toJson(this)
  }

}

case class FlussSourcePartitionOffset(tableBucket: TableBucketInfo, partitionOffset: Long)
  extends PartitionOffset

/** Companion object of the [[FlussSourceOffset]] */
private object FlussSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[TableBucketInfo, Long] = {
    offset match {
      case o: FlussSourceOffset => o.bucketToOffsets
      case so: SerializedOffset => FlussSourceOffset(so).bucketToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to FlussSourceOffset")
    }
  }

  /** Returns [[FlussSourceOffset]] from a JSON [[SerializedOffset]] */
  def apply(offset: Any): FlussSourceOffset =
    offset match {
      case o: FlussSourceOffset => o
      case json: String => JsonUtils.fromJson(json)
      case _ => throw new IllegalArgumentException(s"Can't parse $offset to FlussSourceOffset.")
    }

  /** Returns [[FlussSourceOffset]] from a streaming.Offset */
  def apply(offset: streaming.Offset): FlussSourceOffset = {
    offset match {
      case k: FlussSourceOffset => k
      case so: SerializedOffset => apply(so)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to FlussSourceOffset")
    }
  }
}
