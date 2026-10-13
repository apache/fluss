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
import org.apache.fluss.metadata.{DataLakeFormat, TableBucketSnapshot}
import org.apache.fluss.spark.SparkConnectorOptions.{BUCKET_NUMBER, PRIMARY_KEY}
import org.apache.fluss.spark.read.{FlussAppendInputPartition, FlussUpsertInputPartition}

import org.apache.spark.sql.Row

import java.nio.file.Files

/**
 * Verifies that an incremental (time-range) batch read on a lake-enabled table is forced to the
 * log-only branch: even when a readable lake snapshot exists, the plan never unions lake splits and
 * never reads the kv/lake snapshot, so only the data still retained in Fluss is returned. The
 * result is the `[t1, t2)` window folded per the underlying table type.
 */
abstract class SparkLakeTimeRangeReadTest extends SparkLakeTableReadTestBase {

  private val TVF = "fluss_incremental_between_timestamp"

  test("Spark Lake Read: log table time-range forces log-only (skips lake snapshot)") {
    withTable("t_lake_tr_log") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_lake_tr_log (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      // group 1 (before the window) -> tiered to lake, but also still retained in Fluss
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t_lake_tr_log VALUES (1, "hello"), (2, "world")""")
      tierToLake("t_lake_tr_log")

      val t1 = System.currentTimeMillis()
      Thread.sleep(50)
      // group 2 (inside the window)
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t_lake_tr_log VALUES (3, "fluss"), (4, "spark")""")
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)
      // group 3 (after the window)
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t_lake_tr_log VALUES (5, "lake")""")
      Thread.sleep(200)

      val df =
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t_lake_tr_log', '$t1', '$t2') ORDER BY id")
      val partitions = lakeInputPartitions(df)
      assert(partitions.nonEmpty, "expected at least one Fluss log partition")
      assert(
        partitions.forall(_.isInstanceOf[FlussAppendInputPartition]),
        s"time-range read must be log-only (no lake splits), got: ${partitions.mkString(", ")}"
      )
      checkAnswer(df, Row(3, "fluss") :: Row(4, "spark") :: Nil)
    }
  }

  test("Spark Lake Read: pk table time-range forces log-only (skips lake + kv snapshot)") {
    withTable("t_lake_tr_pk") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_lake_tr_pk (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      // group 1 (before the window) -> tiered to lake, still retained in Fluss changelog
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_tr_pk VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)
      tierToLake("t_lake_tr_pk")

      val t1 = System.currentTimeMillis()
      Thread.sleep(50)
      // group 2 (inside the window): update id=2, insert id=4
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_tr_pk VALUES
             |(2, "bob_updated", 100), (4, "david", 88)
             |""".stripMargin)
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)
      // group 3 (after the window): update id=1, insert id=5
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_tr_pk VALUES
             |(1, "alice_updated", 91), (5, "eve", 92)
             |""".stripMargin)
      Thread.sleep(200)

      val df =
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t_lake_tr_pk', '$t1', '$t2') ORDER BY id")
      val partitions = lakeInputPartitions(df)
      assert(partitions.nonEmpty, "expected at least one Fluss changelog partition")
      assert(
        partitions.forall {
          case p: FlussUpsertInputPartition =>
            p.snapshotId == TableBucketSnapshot.NO_SNAPSHOT_ID
          case _ => false
        },
        s"time-range read must be log-only with no kv/lake snapshot " +
          s"(snapshotId == NO_SNAPSHOT_ID), got: ${partitions.mkString(", ")}"
      )
      // Only keys inserted/updated within [t1, t2): id=2 (updated), id=4 (inserted).
      checkAnswer(df, Row(2, "bob_updated", 100) :: Row(4, "david", 88) :: Nil)
    }
  }
}

@SparkLakeTest
class SparkLakePaimonTimeRangeReadTest extends SparkLakeTimeRangeReadTest {

  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.PAIMON

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.PAIMON.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    warehousePath =
      Files.createTempDirectory("fluss-testing-paimon-timerange-lake").resolve("warehouse").toString
    conf.setString("datalake.paimon.warehouse", warehousePath)
    conf
  }

  override protected def lakeCatalogConf: Configuration = {
    val conf = new Configuration()
    conf.setString("metastore", "filesystem")
    conf.setString("warehouse", warehousePath)
    conf
  }
}
