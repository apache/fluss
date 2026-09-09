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

    val flussPredicates = predicates.map {
      sparkPredicate => (sparkPredicate, SparkPredicateConverter.convert(rowType, sparkPredicate))
    }

    val (partitionPairs, nonPartitionPairs) = flussPredicates.partition {
      case (_, predicateOpt) =>
        predicateOpt.exists(_.visit(onlyPartitionKeys))
    }

    val nonPartitionPredicates = nonPartitionPairs.map(_._1)
    val partitionPredicate = partitionPairs.flatMap(_._2) match {
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

  /**
   * Tests whether a partition (described by its ordered partition values) matches the given
   * predicate. Returns true when no predicate is provided.
   *
   * Callers pass the partition values reported by a lake split, and
   * [[org.apache.fluss.lake.source.LakeSplit#partition]] requires one value per partition column
   * for a partitioned table — a null/empty list is only legal for a non-partitioned table. An arity
   * mismatch therefore means the lake plugin broke that contract, and it is rejected rather than
   * silently admitted: the scan builder drops the partition predicate from the post-scan filters it
   * hands back to Spark, so a split admitted here is never re-filtered and would leak rows from
   * non-matching partitions into the result.
   */
  def matchesPartition(
      tableInfo: TableInfo,
      partitionValues: Seq[String],
      partitionPredicate: Option[FlussPredicate]): Boolean =
    partitionPredicate match {
      case None => true
      case Some(predicate) =>
        val rowType = PartitionUtils.partitionRowType(tableInfo)
        if (partitionValues.size != rowType.getFieldCount) {
          throw new IllegalArgumentException(
            s"Cannot evaluate partition filter for table ${tableInfo.getTablePath}: " +
              s"expected ${rowType.getFieldCount} partition value(s) for partition key(s) " +
              s"${rowType.getFieldNames.asScala.mkString("[", ", ", "]")}, but the lake split " +
              s"reported ${partitionValues.size}: ${partitionValues.mkString("[", ", ", "]")}.")
        }
        predicate.test(PartitionUtils.toPartitionRow(partitionValues.asJava, rowType))
    }
}
