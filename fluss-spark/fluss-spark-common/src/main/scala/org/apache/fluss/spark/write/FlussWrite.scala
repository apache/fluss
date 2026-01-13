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

import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType

/** An interface that extends from Spark [[Write]]. */
trait FlussWrite extends Write

/** Fluss Append Write. */
case class FlussAppendWrite(
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends FlussWrite {

  override def toBatch: BatchWrite = new FlussAppendBatchWrite(tablePath, dataSchema, flussConfig)

  override def toStreaming: StreamingWrite =
    new FlussAppendStreamingWrite(tablePath, dataSchema, flussConfig)
}

/** Fluss Upsert Write. */
case class FlussUpsertWrite(
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends FlussWrite {

  override def toBatch: BatchWrite = FlussUpsertBatchWrite(tablePath, dataSchema, flussConfig)

  override def toStreaming: StreamingWrite =
    FlussUpsertStreamingWrite(tablePath, dataSchema, flussConfig)
}
