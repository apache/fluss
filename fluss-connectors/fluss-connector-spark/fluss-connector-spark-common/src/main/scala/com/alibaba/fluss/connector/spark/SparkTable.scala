/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.spark

import com.alibaba.fluss.config.Configuration
import com.alibaba.fluss.connector.spark.catalog.SparkCatalog
import com.alibaba.fluss.connector.spark.utils.SparkTypeUtils
import com.alibaba.fluss.metadata.{PartitionSpec, TableInfo}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast}
import org.apache.spark.sql.connector.catalog.{SupportsPartitionManagement, SupportsRead, SupportsWrite, Table, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

case class SparkTable(catalog: SparkCatalog, flussConfig: Configuration, table: TableInfo)
  extends Table
  with SupportsRead
  with SupportsWrite
  with SupportsPartitionManagement {

  override def name(): String = {
    table.getTablePath.toString
  }

  override def schema(): StructType = {
    SparkTypeUtils.fromFlussRowType(table.getRowType)
  }

  override def capabilities(): util.Set[TableCapability] = ???

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = ???

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???

  override def partitionSchema(): StructType = {
    SparkTypeUtils.fromFlussRowType(
      SparkTypeUtils.project(table.getRowType, table.getPartitionKeys))
  }

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    catalog.createPartitions(
      table.getTablePath,
      convertToFlussPartitionSpec(ident, partitionSchema()),
      properties)
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    catalog.dropPartition(table.getTablePath, convertToFlussPartitionSpec(ident, partitionSchema()))
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException("Replace partition is not supported")

  }

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] = {
    throw new UnsupportedOperationException("Load partition is not supported")

  }

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    // TODO, list partitions by Identifiers. Trace by
    // https://github.com/alibaba/fluss/issues/514
    throw new UnsupportedOperationException("List partition is not supported")
  }

  private def convertToFlussPartitionSpec(
      ident: InternalRow,
      partitionSchema: StructType): PartitionSpec = {
    val partitionSpec: java.util.Map[String, String] = new util.HashMap()
    partitionSchema.zipWithIndex.foreach {
      case (field, index) =>
        val value = Cast(BoundReference(index, field.dataType, nullable = false), StringType)
          .eval(ident)
          .toString
        partitionSpec.put(field.name, value)
    }
    new PartitionSpec(partitionSpec)
  }
}
