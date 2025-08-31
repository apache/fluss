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

import com.alibaba.fluss.metadata.TableBucket

import org.apache.spark.sql

/**
 * Objects that represent desired offset range limits for starting, ending, and specific offsets.
 */
sealed private trait FlussOffsetRangeLimit

/** Represents the desire to bind to the earliest offsets in Fluss */
private case object EarliestOffsetRangeLimit extends FlussOffsetRangeLimit

/** Represents the desire to bind to the latest offsets in Fluss */
private case object LatestOffsetRangeLimit extends FlussOffsetRangeLimit

/**
 * Represents the desire to bind to specific offsets. A offset == -1 binds to the latest offset, and
 * offset == -2 binds to the earliest offset.
 */
private case class SpecificOffsetRangeLimit(partitionOffsets: Map[TableBucket, Long])
  extends FlussOffsetRangeLimit

/**
 * Represents the desire to bind to earliest offset which timestamp for the offset is equal or
 * greater than specific timestamp.
 */
private case class SpecificTimestampRangeLimit(
    topicTimestamps: Map[TableBucket, Long],
    strategyOnNoMatchingStartingOffset: StrategyOnNoMatchStartingOffset.Value)
  extends FlussOffsetRangeLimit

/**
 * Represents the desire to bind to earliest offset which timestamp for the offset is equal or
 * greater than specific timestamp. This applies the timestamp to the all topics/partitions.
 */
private case class GlobalTimestampRangeLimit(
    timestamp: Long,
    strategyOnNoMatchingStartingOffset: StrategyOnNoMatchStartingOffset.Value)
  extends FlussOffsetRangeLimit

private object FlussOffsetRangeLimit {

  /** Used to denote offset range limits that are resolved via fluss */
  val LATEST = -1L // indicates resolution to the latest offset
  val EARLIEST = -2L // indicates resolution to the earliest offset
}
private object StrategyOnNoMatchStartingOffset extends Enumeration {
  val ERROR, LATEST = Value
}
