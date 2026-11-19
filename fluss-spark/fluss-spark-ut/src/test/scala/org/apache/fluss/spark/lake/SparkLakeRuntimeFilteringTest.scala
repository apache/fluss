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

package org.apache.fluss.spark.lake

import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.metadata.DataLakeFormat
import org.apache.fluss.spark.read.FlussScan
import org.apache.fluss.spark.read.lake.{FlussLakeInputPartition, FlussLakeUpsertInputPartition}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.assertj.core.api.Assertions.assertThat

import java.nio.file.Files

import scala.collection.JavaConverters._

/** Verifies runtime partition pruning across lake snapshots and Fluss log tails. */
@SparkLakeTest
class SparkLakeRuntimeFilteringTest extends SparkLakeTableReadTestBase {

  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.PAIMON

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.PAIMON.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    warehousePath = Files.createTempDirectory("fluss-runtime-lake").resolve("warehouse").toString
    conf.setString("datalake.paimon.warehouse", warehousePath)
    conf
  }

  override protected def lakeCatalogConf: Configuration = {
    val conf = new Configuration()
    conf.setString("metastore", "filesystem")
    conf.setString("warehouse", warehousePath)
    conf
  }

  for (primaryKey <- Seq(false, true)) {
    test(s"runtime filtering prunes lake-only and lake-union reads: primaryKey=$primaryKey") {
      val tableName = if (primaryKey) "runtime_lake_pk" else "runtime_lake_log"
      withTable(tableName) {
        val primaryKeyProperty = if (primaryKey) "'primary.key'='id,dt'," else ""
        sql(s"""CREATE TABLE $tableName (id INT, dt STRING) PARTITIONED BY (dt)
               |TBLPROPERTIES($primaryKeyProperty 'bucket.num'='1',
               |'${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}'='true',
               |'${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}'='1s')""".stripMargin)
        sql(s"INSERT INTO $tableName VALUES (10, '0'), (11, '1'), (12, '2')")
        tierToLake(tableName)
        spark
          .range(5)
          .selectExpr("cast(id as string) as dt", "id % 2 as flag")
          .createOrReplaceTempView("runtime_lake_keys")
        try {
          withSQLConf(
            "spark.sql.adaptive.enabled" -> "false",
            "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly" -> "false",
            "spark.sql.optimizer.dynamicPartitionPruning.useStats" -> "false",
            "spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio" -> "1.0"
          ) {
            for (union <- Seq(false, true)) {
              if (union) {
                // Tail on an existing lake partition plus a partition found only in Fluss.
                sql(s"INSERT INTO $tableName VALUES (20, '0'), (14, '4')")
              }
              val query = s"""SELECT /*+ BROADCAST(d) */ f.id, f.dt FROM $tableName f
                             |JOIN runtime_lake_keys d ON f.dt = d.dt WHERE d.flag = 0""".stripMargin
              var baseline = Seq.empty[Row]
              withSQLConf("spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "false") {
                baseline = sql(query).collect().toSeq
              }
              withSQLConf("spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true") {
                val df = sql(query)
                val scan = df.queryExecution.optimizedPlan.collect {
                  case DataSourceV2ScanRelation(_, s: FlussScan, _, _, _) => s
                }.head
                val initial = scan.toBatch.planInputPartitions()
                assertThat(initial.exists {
                  case _: FlussLakeInputPartition | _: FlussLakeUpsertInputPartition => true
                  case _ => false
                }).isTrue()
                val rows = df.collect().toSeq
                assertThat(rows.toArray).containsExactlyInAnyOrderElementsOf(baseline.asJava)
                val expected = Seq(Row(10, "0"), Row(12, "2")) ++
                  (if (union) Seq(Row(20, "0"), Row(14, "4")) else Seq.empty)
                assertThat(rows.toArray).containsExactlyInAnyOrderElementsOf(expected.asJava)
                val filtered = scan.toBatch.planInputPartitions()
                assertThat(filtered.length).isLessThan(initial.length)
                assertThat(filtered).isSubsetOf(initial.toSeq.asJava)
                assertThat(scan.description()).contains("RuntimePredicates")
              }
            }
          }
        } finally {
          spark.catalog.dropTempView("runtime_lake_keys")
        }
      }
    }
  }
}
