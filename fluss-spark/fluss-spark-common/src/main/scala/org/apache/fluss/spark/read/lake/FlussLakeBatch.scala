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

import org.apache.fluss.client.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer}
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.spark.read._

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

abstract class FlussLakeBatch(
    tablePath: TablePath,
    tableInfo: TableInfo,
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    flussConfig: Configuration)
  extends FlussBatch(tablePath, tableInfo, readSchema, flussConfig) {

  override val stoppingOffsetsInitializer: OffsetsInitializer = {
    FlussOffsetInitializers.stoppingOffsetsInitializer(true, options, flussConfig)
  }

  lazy val (partitions, isFallback) = doPlan()

  override def planInputPartitions(): Array[InputPartition] = partitions

  /**
   * Plans input partitions for reading. The returned isFallback flag is true when no lake snapshot
   * exists and the plan falls back to reading directly from Fluss.
   */
  def doPlan(): (Array[InputPartition], Boolean)

  def getBucketOffsets(
      initializer: OffsetsInitializer,
      partitionName: String,
      buckets: Seq[Int],
      bucketOffsetsRetriever: BucketOffsetsRetrieverImpl): Map[Int, Long] = {
    initializer
      .getBucketOffsets(partitionName, buckets.map(Integer.valueOf).asJava, bucketOffsetsRetriever)
      .asScala
      .map(e => (e._1.intValue(), Long2long(e._2)))
      .toMap
  }
}
