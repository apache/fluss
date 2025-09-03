/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.spark

import com.alibaba.fluss.config.Configuration
import com.alibaba.fluss.metadata.{PartitionSpec, TableInfo}
import org.apache.fluss.spark.catalog.SparkCatalog
import org.apache.fluss.spark.utils.SparkTypeUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast, GenericInternalRow, Literal}
import org.apache.spark.sql.connector.catalog.{SupportsPartitionManagement, SupportsRead, SupportsWrite, Table, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}

import scala.collection.JavaConverters._

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

  override def capabilities(): ju.Set[TableCapability] = ???

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = ???

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???

  override def partitionSchema(): StructType = {
    SparkTypeUtils.fromFlussRowType(
      SparkTypeUtils.project(table.getRowType, table.getPartitionKeys))
  }

  override def createPartition(ident: InternalRow, properties: ju.Map[String, String]): Unit = {
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
      properties: ju.Map[String, String]): Unit = {
    throw new UnsupportedOperationException("Replace partition is not supported")

  }

  override def loadPartitionMetadata(ident: InternalRow): ju.Map[String, String] = {
    throw new UnsupportedOperationException("Load partition is not supported")

  }

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    val partitionSpec = convertToFlussPartitionSpec(names, ident)
    if (ident.numFields == partitionSchema().length) {
      if (catalog.partitionExists(table.getTablePath, partitionSpec)) {
        Array(ident)
      } else {
        Array.empty
      }
    } else {
      val indexes = names.map(partitionSchema().fieldIndex)
      val dataTypes = names.map(partitionSchema()(_).dataType)
      val currentRow = new GenericInternalRow(new Array[Any](names.length))
      catalog
        .listPartitions(table.getTablePath)
        .asScala
        .map(part => convertToPartIdent(part, partitionSchema()))
        .filter {
          partition =>
            for (i <- names.indices) {
              currentRow.values(i) = partition.get(indexes(i), dataTypes(i))
            }
            currentRow == ident
        }
    }.toArray
  }

  private def convertToFlussPartitionSpec(
      ident: InternalRow,
      partitionSchema: StructType): PartitionSpec = {
    val partitionSpec: ju.Map[String, String] = new ju.HashMap()
    partitionSchema.zipWithIndex.foreach {
      case (field, index) =>
        val value = Cast(BoundReference(index, field.dataType, nullable = false), StringType)
          .eval(ident)
          .toString
        partitionSpec.put(field.name, value)
    }
    new PartitionSpec(partitionSpec)
  }
  private def convertToFlussPartitionSpec(
      names: Array[String],
      ident: InternalRow): PartitionSpec = {
    val partitionSpec = names.zipWithIndex
      .map {
        case (name, index) =>
          val value = Cast(
            BoundReference(index, partitionSchema()(name).dataType, nullable = false),
            StringType).eval(ident).toString
          (name, value)
      }
      .toMap
      .asJava
    new PartitionSpec(partitionSpec)
  }

  def convertToPartIdent(
      partitionSpec: ju.Map[String, String],
      partitionSchema: StructType): InternalRow = {
    InternalRow.fromSeq(partitionSchema.map {
      field => Cast(Literal(partitionSpec.asScala(field.name)), field.dataType, None).eval()
    })
  }
}
