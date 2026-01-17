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

import org.apache.fluss.metadata.PartitionSpec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.util

import scala.collection.JavaConverters._

trait SupportsFlussPartitionManagement extends AbstractSparkTable with SupportsPartitionManagement {
  import SupportsFlussPartitionManagement._

  override def partitionSchema(): StructType = _partitionSchema

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    val partitionSpec = toPartitionSpec(ident, partitionSchema())
    admin.createPartition(tableInfo.getTablePath, partitionSpec, false).get()
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    val partitionSpec = toPartitionSpec(ident, partitionSchema())
    admin.dropPartition(tableInfo.getTablePath, partitionSpec, false).get()
    true
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
    assert(
      names.length == ident.numFields,
      s"Number of partition names (${names.length}) must be equal to " +
        s"the number of partition values (${ident.numFields})."
    )
    val schema = partitionSchema()
    assert(
      names.forall(fieldName => schema.fieldNames.contains(fieldName)),
      s"Some partition names ${names.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${schema.sql}'."
    )

    val flussPartitionRows = admin
      .listPartitionInfos(tableInfo.getTablePath)
      .get()
      .asScala
      .map(p => toInternalRow(p.getPartitionSpec, schema))

    val indexes = names.map(schema.fieldIndex)
    val dataTypes = names.map(schema(_).dataType)
    val currentRow = new GenericInternalRow(new Array[Any](names.length))
    flussPartitionRows.filter {
      partRow =>
        for (i <- names.indices) {
          currentRow.values(i) = partRow.get(indexes(i), dataTypes(i))
        }
        currentRow == ident
    }.toArray
  }
}

object SupportsFlussPartitionManagement {
  private def toInternalRow(
      partitionSpec: PartitionSpec,
      partitionSchema: StructType): InternalRow = {
    val row = new SpecificInternalRow(partitionSchema)
    for ((field, i) <- partitionSchema.fields.zipWithIndex) {
      val partValue = partitionSpec.getSpecMap.get(field.name)
      val value = field.dataType match {
        case dt =>
          // TODO Support more types when needed.
          PhysicalDataType(field.dataType) match {
            case PhysicalBooleanType => partValue.toBoolean
            case PhysicalIntegerType => partValue.toInt
            case PhysicalDoubleType => partValue.toDouble
            case PhysicalFloatType => partValue.toFloat
            case PhysicalLongType => partValue.toLong
            case PhysicalShortType => partValue.toShort
            case PhysicalStringType => UTF8String.fromString(partValue)
          }
      }
      row.update(i, value)
    }
    row
  }

  private def toPartitionSpec(row: InternalRow, partitionSchema: StructType): PartitionSpec = {
    val map = partitionSchema.fields.zipWithIndex.map {
      case (field, idx) =>
        (field.name, row.get(idx, field.dataType).toString)
    }.toMap
    new PartitionSpec(map.asJava)
  }
}
