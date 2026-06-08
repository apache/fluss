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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory}

/**
 * A trivial single-row [[Batch]] that emits one [[InternalRow]] containing only the precomputed row
 * count. Used as the fast path when Spark pushes down a `COUNT(*)` / `COUNT(1)` aggregate to a
 * Fluss primary key (non-lake) table and the precomputed table statistics are available on the
 * server side.
 */
class FlussCountBatch(rowCount: Long) extends Batch {

  override def planInputPartitions(): Array[InputPartition] =
    Array(FlussCountInputPartition(rowCount))

  override def createReaderFactory(): PartitionReaderFactory = FlussCountPartitionReaderFactory
}

case class FlussCountInputPartition(rowCount: Long) extends InputPartition

object FlussCountPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case FlussCountInputPartition(rowCount) => new FlussCountPartitionReader(rowCount)
      case other =>
        throw new IllegalArgumentException(
          s"Unexpected partition type for FlussCountBatch: ${other.getClass.getName}")
    }
}

class FlussCountPartitionReader(rowCount: Long) extends PartitionReader[InternalRow] {
  private var consumed: Boolean = false

  override def next(): Boolean = {
    if (consumed) {
      false
    } else {
      consumed = true
      true
    }
  }

  override def get(): InternalRow = InternalRow(rowCount)

  override def close(): Unit = ()
}
