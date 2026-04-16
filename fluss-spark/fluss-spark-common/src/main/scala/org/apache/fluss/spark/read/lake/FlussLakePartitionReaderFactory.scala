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
import org.apache.fluss.spark.read.{FlussAppendInputPartition, FlussAppendPartitionReader}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import java.util

/** Factory for lake-enabled table reads. Dispatches to lake or log reader per partition type. */
class FlussLakePartitionReaderFactory(
    tableProperties: util.Map[String, String],
    tablePath: TablePath,
    projection: Array[Int],
    flussConfig: Configuration)
  extends PartitionReaderFactory {

  @transient private lazy val lakeSource: LakeSource[LakeSplit] = {
    val source = FlussLakeUtils.createLakeSource(tableProperties, tablePath)
    source.withProject(FlussLakeUtils.lakeProjection(projection))
    source
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case lakeOnlySplit: FlussLakeInputPartition =>
        new FlussLakeAppendPartitionReader(
          tablePath,
          lakeOnlySplit,
          lakeSource,
          projection,
          flussConfig)
      case logSplit: FlussAppendInputPartition =>
        new FlussAppendPartitionReader(tablePath, projection, logSplit, flussConfig)
      case mixedSplit: FlussLakeUpsertInputPartition =>
        new FlussLakeUpsertPartitionReader(
          tablePath,
          lakeSource,
          projection,
          mixedSplit,
          flussConfig)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${partition.getClass}")
    }
  }
}
