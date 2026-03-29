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

import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.record.LogRecord
import org.apache.fluss.spark.row.DataConverter
import org.apache.fluss.types.RowType
import org.apache.fluss.utils.CloseableIterator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

/** Partition reader that reads data from a single lake split via lake storage (no Fluss connection). */
class FlussLakePartitionReader(
    tablePath: TablePath,
    rowType: RowType,
    partition: FlussLakeInputPartition,
    lakeSource: LakeSource[LakeSplit])
  extends PartitionReader[InternalRow]
  with Logging {

  private var currentRow: InternalRow = _
  private var closed = false
  private var recordIterator: CloseableIterator[LogRecord] = _

  initialize()

  private def initialize(): Unit = {
    logInfo(s"Reading lake split for table $tablePath bucket=${partition.tableBucket.getBucket}")

    val splitSerializer = lakeSource.getSplitSerializer
    val split = splitSerializer.deserialize(splitSerializer.getVersion, partition.lakeSplitBytes)

    recordIterator = lakeSource
      .createRecordReader(new LakeSource.ReaderContext[LakeSplit] {
        override def lakeSplit(): LakeSplit = split
      })
      .read()
  }

  override def next(): Boolean = {
    if (closed || recordIterator == null) {
      return false
    }

    if (recordIterator.hasNext) {
      val logRecord = recordIterator.next()
      currentRow = DataConverter.toSparkInternalRow(logRecord.getRow, rowType)
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    if (!closed) {
      closed = true
      if (recordIterator != null) {
        recordIterator.close()
      }
    }
  }
}
