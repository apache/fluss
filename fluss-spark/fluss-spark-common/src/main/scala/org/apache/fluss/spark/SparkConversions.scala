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

import org.apache.fluss.config.FlussConfigUtils
import org.apache.fluss.metadata.{Schema, TableDescriptor}
import org.apache.fluss.spark.SparkConnectorOptions._
import org.apache.fluss.spark.types.{FlussToSparkTypeVisitor, SparkToFlussTypeVisitor}
import org.apache.fluss.types.RowType

import org.apache.spark.sql.FlussIdentityTransform
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkConversions {

  def toFlussDataType(schema: StructType): RowType =
    SparkToFlussTypeVisitor.visit(schema).asInstanceOf[RowType]

  def toSparkDataType(rowType: RowType): StructType =
    FlussToSparkTypeVisitor.visit(rowType).asInstanceOf[StructType]

  def toFlussTable(
      sparkSchema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): TableDescriptor = {
    val caseInsensitiveProps = CaseInsensitiveMap(properties.asScala.toMap)

    val tableDescriptorBuilder = TableDescriptor.builder()
    val schemaBuilder = Schema.newBuilder().fromRowType(toFlussDataType(sparkSchema))

    val partitionKey = toPartitionKeys(partitions)
    tableDescriptorBuilder.partitionedBy(partitionKey: _*)

    val primaryKeys = if (caseInsensitiveProps.contains(PRIMARY_KEY.key)) {
      val pks = caseInsensitiveProps.get(PRIMARY_KEY.key).get.split(",")
      schemaBuilder.primaryKey(pks: _*)
      pks
    } else {
      Array.empty[String]
    }

    if (caseInsensitiveProps.contains(BUCKET_NUMBER.key)) {
      val bucketNum = caseInsensitiveProps.get(BUCKET_NUMBER.key).get.toInt
      val bucketKeys = if (caseInsensitiveProps.contains(BUCKET_KEY.key)) {
        caseInsensitiveProps.get(BUCKET_KEY.key).get.split(",")
      } else {
        primaryKeys.filterNot(partitionKey.contains)
      }
      tableDescriptorBuilder.distributedBy(bucketNum, bucketKeys: _*)
    }

    if (caseInsensitiveProps.contains(COMMENT.key)) {
      tableDescriptorBuilder.comment(caseInsensitiveProps.get(COMMENT.key).get)
    }

    val (tableProps, customProps) =
      caseInsensitiveProps.filterNot(SPARK_TABLE_OPTIONS.contains).partition {
        case (key, _) => key.startsWith(FlussConfigUtils.TABLE_PREFIX)
      }

    tableDescriptorBuilder
      .schema(schemaBuilder.build())
      .properties(tableProps.asJava)
      .customProperties(customProps.asJava)
      .build()
  }

  def toPartitionKeys(partitions: Array[Transform]): Array[String] = {
    val partitionKeys = mutable.ArrayBuffer.empty[String]
    partitions.foreach {
      case FlussIdentityTransform(parts) if parts.length == 1 =>
        partitionKeys += parts.head
      case p =>
        throw new UnsupportedOperationException("Unsupported partition transform: " + p)
    }
    partitionKeys.toArray
  }
}
