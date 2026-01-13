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

package org.apache.fluss.spark.write

import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.TablePath

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType

/** An interface that extends from Spark [[StreamingWrite]]. */
trait FlussStreamingWrite extends StreamingWrite with Serializable {
  override def useCommitCoordinator(): Boolean = false

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

/** Fluss Append Streaming Write. */
class FlussAppendStreamingWrite(
    val tablePath: TablePath,
    val dataSchema: StructType,
    val flussConfig: Configuration)
  extends FlussStreamingWrite {

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory =
    FlussAppendStreamingWriterFactory(tablePath, dataSchema, flussConfig)
}

private case class FlussAppendStreamingWriterFactory(
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends StreamingDataWriterFactory
  with Logging {

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    FlussAppendDataWriter(tablePath, dataSchema, flussConfig)
  }
}

/** Fluss Upsert Streaming Write. */
case class FlussUpsertStreamingWrite(
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends FlussStreamingWrite {

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory =
    FlussUpsertStreamingWriterFactory(tablePath, dataSchema, flussConfig)
}

private case class FlussUpsertStreamingWriterFactory(
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends StreamingDataWriterFactory
  with Logging {

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    FlussUpsertDataWriter(tablePath, dataSchema, flussConfig)
  }
}
