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

import org.apache.fluss.client.admin.Admin
import org.apache.fluss.config.AutoPartitionTimeUnit
import org.apache.fluss.metadata.{DateTruncPartitionTransform, PartitionExpression, TableInfo}
import org.apache.fluss.spark.SparkConversions
import org.apache.fluss.utils.PartitionUtils

import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.types.StructType

import java.util

import scala.collection.JavaConverters._

abstract class AbstractSparkTable(val admin: Admin, val tableInfo: TableInfo) extends Table {
  protected lazy val _schema: StructType =
    SparkConversions.toSparkDataType(tableInfo.getSchema.getRowType)

  protected lazy val _partitionSchema: StructType =
    SparkConversions.toSparkDataType(PartitionUtils.partitionRowType(tableInfo))

  override def name(): String = tableInfo.getTablePath.toString

  override def schema(): StructType = _schema

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.MICRO_BATCH_READ,
      TableCapability.STREAMING_WRITE
    ).asJava
  }

  override def partitioning(): Array[Transform] = {
    val expressionsByKey = tableInfo.getPartitionExpressions.asScala
      .map(expression => expression.getVirtualPartitionSpecKey.get() -> expression)
      .toMap
    tableInfo.getPartitionKeys.asScala.map {
      key =>
        expressionsByKey.get(key) match {
          case Some(expression) => toSparkTransform(expression)
          case None => Expressions.identity(key)
        }
    }.toArray
  }

  private def toSparkTransform(partitionExpression: PartitionExpression): Transform = {
    partitionExpression.getTransform match {
      case transform: DateTruncPartitionTransform =>
        val sourceColumn = transform.getSourceColumn
        transform.getTimeUnit match {
          case AutoPartitionTimeUnit.YEAR => Expressions.years(sourceColumn)
          case AutoPartitionTimeUnit.QUARTER =>
            Expressions.apply("quarters", Expressions.column(sourceColumn))
          case AutoPartitionTimeUnit.MONTH => Expressions.months(sourceColumn)
          case AutoPartitionTimeUnit.DAY => Expressions.days(sourceColumn)
          case AutoPartitionTimeUnit.HOUR => Expressions.hours(sourceColumn)
        }
      case transform =>
        throw new UnsupportedOperationException(
          s"Unsupported Fluss partition transform: $transform")
    }
  }
}
