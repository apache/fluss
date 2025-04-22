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

import com.alibaba.fluss.config.Configuration
import com.alibaba.fluss.metadata.TablePath
import com.alibaba.fluss.types.RowType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import javax.annotation.Nullable

case class FlussPartitionReaderFactory(
    flussConf: Configuration,
    tablePath: TablePath,
    sourceOutputType: RowType,
    @Nullable projectedFields: Array[Int])
  extends PartitionReaderFactory {

  private lazy val row: SparkInternalRow = {
    new SparkInternalRow(sourceOutputType)
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {

      case flussInputPartition: FlussInputPartition =>
        FlussPartitionReader(
          flussInputPartition,
          row,
          flussConf,
          tablePath,
          sourceOutputType,
          projectedFields)
      case _ =>
        throw new RuntimeException(s"It's not a Fluss input partition, $partition")
    }
  }

}
