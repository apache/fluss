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

import org.apache.fluss.spark.read.FlussScan

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.assertj.core.api.Assertions.assertThat

import scala.collection.JavaConverters._

/** Verifies join-driven partition pruning and fixed read boundaries against a Fluss cluster. */
class SparkRuntimeFilteringTest extends FlussSparkTestBase {

  for (
    primaryKey <- Seq(false, true); adaptive <- Seq(false, true); incremental <- Seq(false, true)
  ) {
    test(
      s"partition join applies DPP: primaryKey=$primaryKey, AQE=$adaptive, incremental=$incremental") {
      withTable("runtime_fact") {
        val properties =
          if (primaryKey) "'primary.key'='id,dt', 'bucket.num'='1'"
          else "'bucket.num'='1'"
        sql(s"""CREATE TABLE runtime_fact (id INT, dt STRING) PARTITIONED BY (dt)
               |TBLPROPERTIES($properties)""".stripMargin)
        val startMs = System.currentTimeMillis()
        sql("INSERT INTO runtime_fact VALUES (10, '0'), (11, '1'), (12, '2')")
        spark
          .range(3)
          .selectExpr("cast(id as string) as dt", "id % 2 as flag")
          .createOrReplaceTempView("runtime_keys")
        try {
          withSQLConf(
            "spark.sql.adaptive.enabled" -> adaptive.toString,
            "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly" -> "false",
            "spark.sql.optimizer.dynamicPartitionPruning.useStats" -> "false",
            "spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio" -> "1.0"
          ) {
            val endMs = System.currentTimeMillis() + 1L
            val fact = if (incremental) {
              s"fluss_incremental_between_timestamp('$DEFAULT_DATABASE.runtime_fact', '$startMs', '$endMs')"
            } else "runtime_fact"
            val query = s"""SELECT /*+ BROADCAST(d) */ f.id, f.dt FROM $fact f
                           |JOIN runtime_keys d ON f.dt = d.dt WHERE d.flag = 0""".stripMargin
            var baseline = Seq.empty[Row]
            withSQLConf("spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "false") {
              baseline = sql(query).collect().toSeq
            }
            withSQLConf("spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true") {
              val df = sql(query)
              val rows = df.collect().toSeq
              assertThat(rows.toArray).containsExactlyInAnyOrderElementsOf(baseline.asJava)
              assertThat(rows.toArray).containsExactlyInAnyOrder(Row(10, "0"), Row(12, "2"))
              val plan = df.queryExecution.executedPlan match {
                case a: AdaptiveSparkPlanExec => a.executedPlan
                case p => p
              }
              val scans = plan.collect { case b: BatchScanExec => b }
              assertThat(scans.size).isEqualTo(1)
              val batchScan = scans.head
              assertThat(batchScan.runtimeFilters.nonEmpty).isTrue()
              val scan = batchScan.scan.asInstanceOf[FlussScan]
              assertThat(scan.description()).contains("RuntimePredicates")
              assertThat(scan.timeRange.isDefined).isEqualTo(incremental)
              val initial = batchScan.inputPartitions
              val filtered = scan.toBatch.planInputPartitions()
              assertThat(initial.size).isEqualTo(3)
              assertThat(filtered.length).isEqualTo(2)
              assertThat(filtered).isSubsetOf(initial.asJava)
            }
          }
        } finally {
          spark.catalog.dropTempView("runtime_keys")
        }
      }
    }
  }

  for (primaryKey <- Seq(false, true)) {
    test(
      s"data written after planning retains runtime-filtered boundaries: primaryKey=$primaryKey") {
      withTable("runtime_boundary") {
        val properties =
          if (primaryKey) "'primary.key'='id,dt', 'bucket.num'='1'"
          else "'bucket.num'='1'"
        sql(s"""CREATE TABLE runtime_boundary (id INT, dt STRING) PARTITIONED BY (dt)
               |TBLPROPERTIES($properties)""".stripMargin)
        sql("INSERT INTO runtime_boundary VALUES (1, 'a'), (2, 'b')")
        val df = sql("SELECT * FROM runtime_boundary WHERE dt = 'a'")
        val scan = df.queryExecution.optimizedPlan.collect {
          case DataSourceV2ScanRelation(_, s: FlussScan, _, _, _) => s
        }.head
        val initial = scan.toBatch.planInputPartitions()
        sql("INSERT INTO runtime_boundary VALUES (3, 'a'), (4, 'c')")
        scan.filter(
          Array(
            new Predicate(
              "IN",
              Array[Expression](Expressions.column("dt"), Expressions.literal("a")))))
        assertThat(scan.toBatch.planInputPartitions())
          .containsExactlyElementsOf(initial.toSeq.asJava)
        assertThat(df.collect()).containsExactly(Row(1, "a"))
      }
    }
  }
}
