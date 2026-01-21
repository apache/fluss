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

import org.apache.fluss.metadata.TableBucket

import org.apache.spark.sql.connector.read.InputPartition

trait FlussInputPartition extends InputPartition {

  override def preferredLocations(): Array[String] = {
    // Could return tablet server locations for data locality
    Array.empty[String]
  }

}

/**
 * Represents an input partition for reading data from a Fluss table bucket.
 *
 * @param tableBucket
 *   the table bucket to read from
 */
case class FlussAppendInputPartition(tableBucket: TableBucket) extends FlussInputPartition

/**
 * Represents an input partition for reading data from a primary key table bucket. This partition
 * includes snapshot information for hybrid snapshot-log reading.
 *
 * @param tableBucket
 *   the table bucket to read from
 * @param snapshotId
 *   the snapshot ID to read from, -1 if no snapshot
 * @param logStartingOffset
 *   the log offset where incremental reading should start
 * @param logStoppingOffset
 *   the log offset where incremental reading should end
 */
case class FlussUpsertInputPartition(
    tableBucket: TableBucket,
    snapshotId: Long,
    logStartingOffset: Long,
    logStoppingOffset: Long)
  extends FlussInputPartition
