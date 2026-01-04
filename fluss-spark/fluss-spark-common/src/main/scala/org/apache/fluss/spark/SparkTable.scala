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

package org.apache.fluss.spark

import org.apache.fluss.config.{Configuration => FlussConfiguration}
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.spark.catalog.{AbstractSparkTable, SupportsFlussPartitionManagement}
import org.apache.fluss.spark.write.{FlussAppendWriteBuilder, FlussUpsertWriteBuilder}

import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}

case class SparkTable(tablePath: TablePath, tableInfo: TableInfo, flussConfig: FlussConfiguration)
  extends AbstractSparkTable(tableInfo)
  with SupportsFlussPartitionManagement
  with SupportsWrite {

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    if (tableInfo.getPrimaryKeys.isEmpty) {
      new FlussAppendWriteBuilder(tablePath, logicalWriteInfo.schema(), flussConfig)
    } else {
      new FlussUpsertWriteBuilder(tablePath, logicalWriteInfo.schema(), flussConfig)
    }
  }
}
