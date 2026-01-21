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

import org.apache.fluss.client.admin.Admin
import org.apache.fluss.config.{Configuration => FlussConfiguration}
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.spark.catalog.{AbstractSparkTable, SupportsFlussPartitionManagement}
import org.apache.fluss.spark.read.{FlussAppendScanBuilder, FlussUpsertScanBuilder}
import org.apache.fluss.spark.write.{FlussAppendWriteBuilder, FlussUpsertWriteBuilder}

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SparkTable(
    tablePath: TablePath,
    tableInfo: TableInfo,
    flussConfig: FlussConfiguration,
    admin: Admin)
  extends AbstractSparkTable(admin, tableInfo)
  with SupportsFlussPartitionManagement
  with SupportsRead
  with SupportsWrite
  with SQLConfHelper {

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    if (tableInfo.getPrimaryKeys.isEmpty) {
      new FlussAppendWriteBuilder(tablePath, logicalWriteInfo.schema(), flussConfig)
    } else {
      new FlussUpsertWriteBuilder(tablePath, logicalWriteInfo.schema(), flussConfig)
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    if (tableInfo.getPrimaryKeys.isEmpty) {
      new FlussAppendScanBuilder(tablePath, tableInfo, options, flussConfig)
    } else {
      if (!conf.getConf(SparkFlussConf.READ_OPTIMIZED, false)) {
        throw new UnsupportedOperationException(
          "For now, only data in snapshot can be read, without merging them with changes. " +
            "If you can accept it, please set `spark.sql.fluss.readOptimized` true, and execute query again.")
      }
      new FlussUpsertScanBuilder(tablePath, tableInfo, options, flussConfig)
    }
  }
}
