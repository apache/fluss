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

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/** An interface that extends from Spark [[BatchWrite]]. */
trait FlussBatchWrite extends BatchWrite with Serializable {

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

}

/** Fluss Append Batch Write. */
class FlussAppendBatchWrite(
    val tablePath: TablePath,
    val dataSchema: StructType,
    val flussConfig: Configuration)
  extends FlussBatchWrite {

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    (_: Int, _: Long) => FlussAppendDataWriter(tablePath, dataSchema, flussConfig)
  }
}

/** Fluss Upsert Batch Write. */
case class FlussUpsertBatchWrite(
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends FlussBatchWrite {

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    (_: Int, _: Long) => FlussUpsertDataWriter(tablePath, dataSchema, flussConfig)
  }

}
