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

package org.apache.fluss.spark.read.lake

import org.apache.fluss.config.Configuration
import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.record.LogRecord
import org.apache.fluss.spark.read.FlussPartitionReader
import org.apache.fluss.types.RowType
import org.apache.fluss.utils.CloseableIterator

import org.apache.spark.internal.Logging

/** Partition reader that reads data from a single lake split via lake storage (no Fluss connection). */
class FlussLakeAppendPartitionReader(
    tablePath: TablePath,
    partition: FlussLakeInputPartition,
    lakeSource: LakeSource[LakeSplit],
    projection: Array[Int],
    flussConfig: Configuration)
  extends FlussPartitionReader(tablePath, flussConfig)
  with Logging {

  private var recordIterator: CloseableIterator[LogRecord] = _

  initialize()

  private def initialize(): Unit = {
    logInfo(s"Reading lake split for table $tablePath $partition")

    recordIterator = lakeSource
      .createRecordReader(() => partition.lakeSplit)
      .read()
  }

  override lazy val projectedRowType: RowType = rowType.project(projection)

  override def next0(): Boolean = {
    if (closed || recordIterator == null) {
      return false
    }

    if (recordIterator.hasNext) {
      currentRow = convertToSparkRow(recordIterator.next().getRow)
      true
    } else {
      false
    }
  }

  override def close0(): Unit = {
    if (recordIterator != null) {
      recordIterator.close()
    }
  }
}
