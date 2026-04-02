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

import org.apache.fluss.config.ConfigOptions
import org.apache.fluss.spark.FlussSparkTestBase
import org.apache.fluss.spark.SparkConnectorOptions.BUCKET_NUMBER

import org.apache.spark.sql.Row

/**
 * Base class for lake-enabled log table read tests. Subclasses provide the lake format config and
 * implement tiering via [[tierToLake]].
 */
abstract class SparkLakeLogTableReadTestBase extends FlussSparkTestBase {

  protected var warehousePath: String = _

  /** Tier all pending data for the given table to the lake. */
  protected def tierToLake(tableName: String): Unit

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.foreach(t => sql(s"DROP TABLE IF EXISTS $DEFAULT_DATABASE.$t"))
    }
  }

  test("Spark Lake Read: log table falls back when no lake snapshot") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, "hello"), (2, "world"), (3, "fluss")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY id"),
        Row(1, "hello") :: Row(2, "world") :: Row(3, "fluss") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t ORDER BY name"),
        Row("fluss") :: Row("hello") :: Row("world") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table lake-only (all data in lake, no log tail)") {
    withTable("t_lake_only") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_lake_only (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_only VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake("t_lake_only")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_lake_only ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t_lake_only ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("gamma") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table union read (lake + log tail)") {
    withTable("t_union") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_union (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake("t_union")

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union VALUES
             |(4, "delta"), (5, "epsilon")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_union ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") ::
          Row(4, "delta") :: Row(5, "epsilon") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t_union ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("delta") ::
          Row("epsilon") :: Row("gamma") :: Nil
      )
    }
  }

  test("Spark Lake Read: non-FULL startup mode skips lake path") {
    withTable("t_earliest") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_earliest (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_earliest VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake("t_earliest")

      try {
        spark.conf.set("spark.sql.fluss.scan.startup.mode", "earliest")

        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.t_earliest ORDER BY id"),
          Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
        )
      } finally {
        spark.conf.set("spark.sql.fluss.scan.startup.mode", "full")
      }
    }
  }
}
