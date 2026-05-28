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
import org.apache.fluss.predicate.{PartitionPredicateVisitor, Predicate => FlussPredicate, PredicateBuilder}
import org.apache.fluss.utils.PartitionUtils

import org.apache.spark.sql.connector.expressions.filter.Predicate

import scala.jdk.CollectionConverters._

/** Extracts a partition-key predicate and prunes the partition list at planning time. */
object SparkPartitionPredicate {

  def extract(
      tableInfo: TableInfo,
      predicates: Seq[Predicate]): (Seq[Predicate], Option[FlussPredicate]) = {
    val partitionKeys = tableInfo.getPartitionKeys
    if (partitionKeys.isEmpty) {
      return (predicates, None)
    }

    val rowType = PartitionUtils.partitionRowType(tableInfo)
    val onlyPartitionKeys = new PartitionPredicateVisitor(partitionKeys)

    // Separate predicates: those that can be converted and only touch partition keys
    val (partitionPredicates, nonPartitionPredicates) = predicates.partition {
      sparkPredicate =>
        SparkPredicateConverter
          .convert(rowType, sparkPredicate)
          .exists(_.visit(onlyPartitionKeys))
    }

    // Convert partition predicates to FlussPredicate
    val converted = partitionPredicates.flatMap {
      sparkPredicate =>
        SparkPredicateConverter
          .convert(rowType, sparkPredicate)
          .filter(_.visit(onlyPartitionKeys))
    }

    val partitionPredicate = converted match {
      case Seq() => None
      case Seq(single) => Some(single)
      case many => Some(PredicateBuilder.and(many.asJava))
    }

    (nonPartitionPredicates, partitionPredicate)
  }

  def filterPartitions(
      tableInfo: TableInfo,
      partitionInfos: Seq[PartitionInfo],
      partitionPredicate: Option[FlussPredicate]): Seq[PartitionInfo] =
    partitionPredicate match {
      case None => partitionInfos
      case Some(predicate) =>
        val rowType = PartitionUtils.partitionRowType(tableInfo)
        partitionInfos.filter {
          p =>
            predicate.test(
              PartitionUtils.toPartitionRow(p.getResolvedPartitionSpec.getPartitionValues, rowType))
        }
    }
}
