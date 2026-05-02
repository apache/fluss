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

package org.apache.fluss.spark.utils

import org.apache.fluss.metadata.{PartitionInfo, TableInfo}
import org.apache.fluss.predicate.{CompoundPredicate, LeafPredicate, PartitionPredicateVisitor, Predicate => FlussPredicate, PredicateBuilder, PredicateVisitor}
import org.apache.fluss.row.{BinaryString, GenericRow}
import org.apache.fluss.types.{DataTypes, RowType}
import org.apache.fluss.utils.PartitionUtils

import org.apache.spark.sql.connector.expressions.filter.Predicate

import scala.jdk.CollectionConverters._

/** Extracts a partition-key predicate and prunes the partition list at planning time. */
object SparkPartitionPredicate {

  def extract(tableInfo: TableInfo, predicates: Seq[Predicate]): Option[FlussPredicate] = {
    val partitionKeys = tableInfo.getPartitionKeys
    if (partitionKeys.isEmpty) return None

    val rowType = partitionRowType(tableInfo)
    val onlyPartitionKeys = new PartitionPredicateVisitor(partitionKeys)

    val converted = predicates.flatMap {
      sparkPredicate =>
        SparkPredicateConverter
          .convert(rowType, sparkPredicate)
          .filter(_.visit(onlyPartitionKeys))
          .map(stringifyLiterals)
    }

    converted match {
      case Seq() => None
      case Seq(single) => Some(single)
      case many => Some(PredicateBuilder.and(many.asJava))
    }
  }

  def filterPartitions(
      partitionInfos: Seq[PartitionInfo],
      partitionPredicate: Option[FlussPredicate]): Seq[PartitionInfo] =
    partitionPredicate match {
      case None => partitionInfos
      case Some(predicate) => partitionInfos.filter(p => predicate.test(toPartitionRow(p)))
    }

  private def partitionRowType(tableInfo: TableInfo): RowType = {
    val schemaRowType = tableInfo.getRowType
    val fieldNames = schemaRowType.getFieldNames
    val partitionFieldIndexes = tableInfo.getPartitionKeys.asScala.map(fieldNames.indexOf).toArray
    schemaRowType.project(partitionFieldIndexes)
  }

  private def toPartitionRow(partitionInfo: PartitionInfo): GenericRow = {
    val values = partitionInfo.getResolvedPartitionSpec.getPartitionValues
    val row = new GenericRow(values.size)
    var i = 0
    while (i < values.size) {
      row.setField(i, BinaryString.fromString(values.get(i)))
      i += 1
    }
    row
  }

  // Partition values are stored as strings; literals must be coerced before evaluation.
  private val stringifier: PredicateVisitor[FlussPredicate] = new PredicateVisitor[FlussPredicate] {
    override def visit(leaf: LeafPredicate): FlussPredicate = {
      val converted: Seq[Object] = leaf.literals.asScala.toSeq.map {
        case null => null
        case literal =>
          BinaryString.fromString(
            PartitionUtils.convertValueOfType(literal, leaf.`type`.getTypeRoot))
      }
      new LeafPredicate(
        leaf.function,
        DataTypes.STRING,
        leaf.index,
        leaf.fieldName,
        converted.asJava)
    }

    override def visit(compound: CompoundPredicate): FlussPredicate = {
      val children = compound.children.asScala.map(_.visit(this)).asJava
      new CompoundPredicate(compound.function, children)
    }
  }

  private def stringifyLiterals(predicate: FlussPredicate): FlussPredicate =
    predicate.visit(stringifier)
}
