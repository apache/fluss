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

package org.apache.fluss.spark.tiering

import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.metadata.DataLakeFormat
import org.apache.fluss.spark.SparkConnectorOptions.BUCKET_NUMBER

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row

import java.nio.file.Files

/**
 * Integration test for the Spark tiering pipeline on log tables.
 *
 * Uses the Spark tiering components directly (TieringSplitGenerator, TieringTask, TieringCommitter)
 * instead of the Flink-based LakeTieringJobBuilder, to validate that the Spark tiering path
 * produces correct lake data that can be read back via Spark SQL.
 */
abstract class SparkTieringLogTableTest extends SparkTieringTestBase with Logging {

  test("Spark Tiering: log table tier and read back") {
    withTable("t_spark_tier") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_spark_tier (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_spark_tier VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLakeViaSpark("t_spark_tier")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_spark_tier ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t_spark_tier ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("gamma") :: Nil
      )
    }
  }

  test("Spark Tiering: log table union read (lake + log tail)") {
    withTable("t_spark_union") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_spark_union (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_spark_union VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLakeViaSpark("t_spark_union")

      // Insert more data after tiering (this should appear in the log tail)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_spark_union VALUES
             |(4, "delta"), (5, "epsilon")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_spark_union ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") ::
          Row(4, "delta") :: Row(5, "epsilon") :: Nil
      )
    }
  }

  test("Spark Tiering: log table incremental tiering") {
    withTable("t_spark_incr") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_spark_incr (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      // First batch
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_spark_incr VALUES
             |(1, "alpha"), (2, "beta")
             |""".stripMargin)

      tierToLakeViaSpark("t_spark_incr")

      // Second batch
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_spark_incr VALUES
             |(3, "gamma"), (4, "delta")
             |""".stripMargin)

      tierToLakeViaSpark("t_spark_incr")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_spark_incr ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Row(4, "delta") :: Nil
      )
    }
  }
}

class SparkTieringPaimonLogTableTest extends SparkTieringLogTableTest {

  override def dataLakeFormat: DataLakeFormat = DataLakeFormat.PAIMON

  override def lakeConfig: Configuration = {
    val conf = new Configuration()
    conf.setString("metastore", "filesystem")
    conf.setString("warehouse", warehousePath)
    conf
  }

  override def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", dataLakeFormat.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    warehousePath =
      Files.createTempDirectory("fluss-testing-spark-tiering").resolve("warehouse").toString
    conf.setString("datalake.paimon.warehouse", warehousePath)
    conf
  }
}
