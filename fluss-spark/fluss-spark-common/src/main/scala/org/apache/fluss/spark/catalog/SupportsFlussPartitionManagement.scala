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

package org.apache.fluss.spark.catalog

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.types.StructType

import java.util

trait SupportsFlussPartitionManagement extends AbstractSparkTable with SupportsPartitionManagement {

  override def partitionSchema(): StructType = _partitionSchema

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException("Creating partition is not supported")
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    throw new UnsupportedOperationException("Dropping partition is not supported")
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException("Replacing partition metadata is not supported")
  }

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] = {
    throw new UnsupportedOperationException("Loading partition is not supported")
  }

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    throw new UnsupportedOperationException("Listing partition is not supported")
  }
}
