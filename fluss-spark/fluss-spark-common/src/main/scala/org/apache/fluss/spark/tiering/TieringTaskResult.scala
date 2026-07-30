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

package org.apache.fluss.spark.tiering

import org.apache.fluss.metadata.{TableBucket, TablePath}

/**
 * Serialized result from a tiering task executed on a Spark executor.
 *
 * The write result is serialized as a byte array using the factory's
 * [[org.apache.fluss.lake.serializer.SimpleVersionedSerializer]] to avoid Spark serialization
 * issues with lake-specific types.
 *
 * @param tablePath
 *   the table path
 * @param tableBucket
 *   the table bucket
 * @param partitionName
 *   optional partition name
 * @param serializedWriteResult
 *   serialized write result bytes, null if no data was written
 * @param writeResultVersion
 *   serializer version used to serialize the write result
 * @param logEndOffset
 *   the log end offset after processing
 * @param maxTimestamp
 *   the maximum timestamp encountered during processing
 * @param numberOfSplits
 *   total number of splits in this tiering round
 */
case class SerializedTaskResult(
    tablePath: TablePath,
    tableBucket: TableBucket,
    partitionName: Option[String],
    serializedWriteResult: Array[Byte],
    writeResultVersion: Int,
    logEndOffset: Long,
    maxTimestamp: Long,
    numberOfSplits: Int)
  extends Serializable
