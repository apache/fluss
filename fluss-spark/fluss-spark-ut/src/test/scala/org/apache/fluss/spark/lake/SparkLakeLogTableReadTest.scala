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
import org.apache.fluss.spark.SparkConnectorOptions.BUCKET_NUMBER
import org.apache.fluss.spark.read.FlussMetrics

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import java.nio.file.Files

abstract class SparkLakeLogTableReadTest extends SparkLakeTableReadTestBase {

  test("Spark Lake Read: log table falls back when no lake snapshot") {
    val tTableName = scopedTableName("t")
    val partitionedTableName = scopedTableName("t_partitioned")
    // Test non-partitioned table
    withTable(tTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tTableName (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tTableName VALUES
             |(1, "hello"), (2, "world"), (3, "fluss")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$tTableName ORDER BY id"),
        Row(1, "hello") :: Row(2, "world") :: Row(3, "fluss") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.$tTableName ORDER BY name"),
        Row("fluss") :: Row("hello") :: Row("world") :: Nil
      )
    }

    // Test partitioned table
    withTable(partitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$partitionedTableName (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$partitionedTableName VALUES
             |(1, "hello", "2026-01-01"),
             |(2, "world", "2026-01-01"),
             |(3, "fluss", "2026-01-02")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$partitionedTableName ORDER BY id"),
        Row(1, "hello", "2026-01-01") ::
          Row(2, "world", "2026-01-01") ::
          Row(3, "fluss", "2026-01-02") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name, dt FROM $DEFAULT_DATABASE.$partitionedTableName ORDER BY name"),
        Row("fluss", "2026-01-02") ::
          Row("hello", "2026-01-01") ::
          Row("world", "2026-01-01") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table lake-only (all data in lake, no log tail)") {
    val lakeOnlyTableName = scopedTableName("t_lake_only")
    val lakeOnlyPartitionedTableName = scopedTableName("t_lake_only_partitioned")
    // Test non-partitioned table
    withTable(lakeOnlyTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$lakeOnlyTableName (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$lakeOnlyTableName VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake(lakeOnlyTableName)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$lakeOnlyTableName ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.$lakeOnlyTableName ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("gamma") :: Nil
      )

      // COUNT(*) / COUNT(1) over a lake-only read: empty projection must fall back.
      checkAnswer(sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.$lakeOnlyTableName"), Row(3L) :: Nil)
      checkAnswer(sql(s"SELECT COUNT(1) FROM $DEFAULT_DATABASE.$lakeOnlyTableName"), Row(3L) :: Nil)
    }

    // Test partitioned table
    withTable(lakeOnlyPartitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName VALUES
             |(1, "alpha", "2026-01-01"),
             |(2, "beta", "2026-01-01"),
             |(3, "gamma", "2026-01-02")
             |""".stripMargin)

      tierToLake(lakeOnlyPartitionedTableName)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName ORDER BY id"),
        Row(1, "alpha", "2026-01-01") ::
          Row(2, "beta", "2026-01-01") ::
          Row(3, "gamma", "2026-01-02") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name, dt FROM $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName ORDER BY name"),
        Row("alpha", "2026-01-01") ::
          Row("beta", "2026-01-01") ::
          Row("gamma", "2026-01-02") :: Nil
      )

      // COUNT(*) over a partitioned lake-only read.
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName"),
        Row(3L) :: Nil
      )

      // COUNT(*) with a partition filter.
      checkAnswer(
        sql(
          s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName WHERE dt = '2026-01-01'"),
        Row(2L) :: Nil
      )
    }
  }

  test("Spark Lake Read: log table lake-only projection on timestamp column") {
    val lakeTimestampTableName = scopedTableName("t_lake_timestamp")
    val lakeTimestampPartitionedTableName = scopedTableName("t_lake_timestamp_partitioned")
    // Test non-partitioned table
    withTable(lakeTimestampTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$lakeTimestampTableName (
             |  id INT,
             |  ts TIMESTAMP,
             |  name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$lakeTimestampTableName VALUES
             |(1, TIMESTAMP "2026-01-01 12:00:00", "alpha"),
             |(2, TIMESTAMP "2026-01-02 12:00:00", "beta"),
             |(3, TIMESTAMP "2026-01-03 12:00:00", "gamma")
             |""".stripMargin)

      tierToLake(lakeTimestampTableName)

      checkAnswer(
        sql(s"SELECT ts FROM $DEFAULT_DATABASE.$lakeTimestampTableName ORDER BY ts"),
        Row(java.sql.Timestamp.valueOf("2026-01-01 12:00:00")) ::
          Row(java.sql.Timestamp.valueOf("2026-01-02 12:00:00")) ::
          Row(java.sql.Timestamp.valueOf("2026-01-03 12:00:00")) :: Nil
      )
    }

    // Test partitioned table
    withTable(lakeTimestampPartitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$lakeTimestampPartitionedTableName (
             |  id INT,
             |  ts TIMESTAMP,
             |  name STRING,
             |  dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$lakeTimestampPartitionedTableName VALUES
             |(1, TIMESTAMP "2026-01-01 12:00:00", "alpha", "2026-01-01"),
             |(2, TIMESTAMP "2026-01-02 12:00:00", "beta", "2026-01-02"),
             |(3, TIMESTAMP "2026-01-03 12:00:00", "gamma", "2026-01-03")
             |""".stripMargin)

      tierToLake(lakeTimestampPartitionedTableName)

      checkAnswer(
        sql(s"SELECT ts, dt FROM $DEFAULT_DATABASE.$lakeTimestampPartitionedTableName ORDER BY ts"),
        Row(java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), "2026-01-01") ::
          Row(java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), "2026-01-02") ::
          Row(java.sql.Timestamp.valueOf("2026-01-03 12:00:00"), "2026-01-03") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table union read (lake + log tail)") {
    val unionTableName = scopedTableName("t_union")
    val unionPartitionedTableName = scopedTableName("t_union_partitioned")
    // Test non-partitioned table
    withTable(unionTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$unionTableName (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$unionTableName VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake(unionTableName)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$unionTableName VALUES
             |(4, "delta"), (5, "epsilon")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$unionTableName ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") ::
          Row(4, "delta") :: Row(5, "epsilon") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.$unionTableName ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("delta") ::
          Row("epsilon") :: Row("gamma") :: Nil
      )
    }

    // Test partitioned table
    withTable(unionPartitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$unionPartitionedTableName (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$unionPartitionedTableName VALUES
             |(1, "alpha", "2026-01-01"),
             |(2, "beta", "2026-01-01"),
             |(3, "gamma", "2026-01-02")
             |""".stripMargin)

      tierToLake(unionPartitionedTableName)

      // Insert more data after tiering (this will be in log tail)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$unionPartitionedTableName VALUES
             |(4, "delta", "2026-01-01"),
             |(5, "epsilon", "2026-01-03")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$unionPartitionedTableName ORDER BY id"),
        Row(1, "alpha", "2026-01-01") ::
          Row(2, "beta", "2026-01-01") ::
          Row(3, "gamma", "2026-01-02") ::
          Row(4, "delta", "2026-01-01") ::
          Row(5, "epsilon", "2026-01-03") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name, dt FROM $DEFAULT_DATABASE.$unionPartitionedTableName ORDER BY name"),
        Row("alpha", "2026-01-01") ::
          Row("beta", "2026-01-01") ::
          Row("delta", "2026-01-01") ::
          Row("epsilon", "2026-01-03") ::
          Row("gamma", "2026-01-02") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table projection with type-dependent columns") {
    val typeDependentTableName = scopedTableName("t_type_dependent")
    val typeDependentPartitionedTableName = scopedTableName("t_type_dependent_partitioned")
    // Test non-partitioned table
    withTable(typeDependentTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$typeDependentTableName (
             |id INT,
             |ts TIMESTAMP,
             |name STRING,
             |arr ARRAY<INT>,
             |struct_col STRUCT<col1: INT, col2: STRING>,
             |ts_ltz TIMESTAMP_LTZ
             |)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$typeDependentTableName VALUES
             |(1, TIMESTAMP "2026-01-01 12:00:00", "a", ARRAY(1, 2), STRUCT(10, 'x'),
             | TIMESTAMP "2026-01-01 12:00:00"),
             |(2, TIMESTAMP "2026-01-02 12:00:00", "b", ARRAY(3, 4), STRUCT(20, 'y'),
             | TIMESTAMP "2026-01-02 12:00:00")
             |""".stripMargin)

      tierToLake(typeDependentTableName)

      // Projection reorders type-dependent columns (array, timestamp, struct)
      checkAnswer(
        sql(
          s"SELECT arr, ts, struct_col FROM $DEFAULT_DATABASE.$typeDependentTableName ORDER BY ts"),
        Row(Seq(1, 2), java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), Row(10, "x")) ::
          Row(Seq(3, 4), java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), Row(20, "y")) :: Nil
      )

      // Projection with timestamp_ltz at shifted ordinal
      checkAnswer(
        sql(s"SELECT ts_ltz, name FROM $DEFAULT_DATABASE.$typeDependentTableName ORDER BY name"),
        Row(java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), "a") ::
          Row(java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), "b") :: Nil
      )
    }

    // Test partitioned table
    withTable(typeDependentPartitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$typeDependentPartitionedTableName (
             |id INT,
             |ts TIMESTAMP,
             |name STRING,
             |arr ARRAY<INT>,
             |struct_col STRUCT<col1: INT, col2: STRING>,
             |ts_ltz TIMESTAMP_LTZ,
             |dt STRING
             |)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$typeDependentPartitionedTableName VALUES
             |(1, TIMESTAMP "2026-01-01 12:00:00", "a", ARRAY(1, 2), STRUCT(10, 'x'),
             | TIMESTAMP "2026-01-01 12:00:00", "2026-01-01"),
             |(2, TIMESTAMP "2026-01-02 12:00:00", "b", ARRAY(3, 4), STRUCT(20, 'y'),
             | TIMESTAMP "2026-01-02 12:00:00", "2026-01-02")
             |""".stripMargin)

      tierToLake(typeDependentPartitionedTableName)

      // Projection reorders type-dependent columns (array, timestamp, struct)
      checkAnswer(
        sql(
          s"SELECT arr, ts, struct_col, dt FROM $DEFAULT_DATABASE.$typeDependentPartitionedTableName ORDER BY ts"),
        Row(
          Seq(1, 2),
          java.sql.Timestamp.valueOf("2026-01-01 12:00:00"),
          Row(10, "x"),
          "2026-01-01") ::
          Row(
            Seq(3, 4),
            java.sql.Timestamp.valueOf("2026-01-02 12:00:00"),
            Row(20, "y"),
            "2026-01-02") :: Nil
      )

      // Projection with timestamp_ltz at shifted ordinal
      checkAnswer(
        sql(
          s"SELECT ts_ltz, name, dt FROM $DEFAULT_DATABASE.$typeDependentPartitionedTableName ORDER BY name"),
        Row(java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), "a", "2026-01-01") ::
          Row(java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), "b", "2026-01-02") :: Nil
      )
    }
  }

  test("Spark Lake Read: filter pushdown — lake-only (all data in lake)") {
    val tableName = scopedTableName("t_pd_lake")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, amount INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 100, 'alpha'), (2, 200, 'beta'), (3, 300, 'gamma')
             |""".stripMargin)
      tierToLake(tableName)

      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName WHERE amount > 150 ORDER BY id")
      checkAnswer(query, Row(2, 200, "beta") :: Row(3, 300, "gamma") :: Nil)
      assertPushedNames(query, Set(">"))
    }
  }

  test("Spark Lake Read: filter pushdown — union (lake + log tail)") {
    val tableName = scopedTableName("t_pd_union")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, amount INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 100, 'alpha'), (2, 200, 'beta'), (3, 300, 'gamma')
             |""".stripMargin)
      tierToLake(tableName)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(4, 400, 'delta'), (5, 500, 'epsilon')
             |""".stripMargin)

      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName WHERE amount >= 200 ORDER BY id")
      checkAnswer(
        query,
        Row(2, 200, "beta") ::
          Row(3, 300, "gamma") ::
          Row(4, 400, "delta") ::
          Row(5, 500, "epsilon") :: Nil
      )
      assertPushedNames(query, Set(">="))
    }
  }

  test("Spark Lake Read: filter pushdown — fallback (no lake snapshot)") {
    val tableName = scopedTableName("t_pd_fallback")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, amount INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 100, 'alpha'), (2, 200, 'beta'), (3, 300, 'gamma')
             |""".stripMargin)

      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName WHERE amount = 200")
      checkAnswer(query, Row(2, 200, "beta") :: Nil)
      assertPushedNames(query, Set("="))
    }
  }

  test("Spark Lake Read: log table partition filter pushdown prunes partitions") {
    val tableName = scopedTableName("t_pd_part_lake")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName
             |  (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      // Lake-only partitions: tier all data, then filter by partition key.
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 'alpha', '2026-01-01'),
             |(2, 'beta', '2026-01-01'),
             |(3, 'gamma', '2026-01-02'),
             |(4, 'delta', '2026-01-03')
             |""".stripMargin)
      tierToLake(tableName)

      val lakeOnlyDf = sql(s"""
                              |SELECT * FROM $DEFAULT_DATABASE.$tableName
                              |WHERE dt = '2026-01-02' ORDER BY id""".stripMargin)
      checkAnswer(lakeOnlyDf, Row(3, "gamma", "2026-01-02") :: Nil)
      assert(
        lakeInputPartitions(lakeOnlyDf).length == 1,
        s"Expected 1 input partition after partition pruning"
      )

      // Append more data after tiering so the planner mixes lake splits and Fluss log tail.
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(5, 'epsilon', '2026-01-01'),
             |(6, 'zeta', '2026-01-04')
             |""".stripMargin)

      val unionDf = sql(s"""
                           |SELECT * FROM $DEFAULT_DATABASE.$tableName
                           |WHERE dt = '2026-01-01' ORDER BY id""".stripMargin)
      checkAnswer(
        unionDf,
        Row(1, "alpha", "2026-01-01") ::
          Row(2, "beta", "2026-01-01") ::
          Row(5, "epsilon", "2026-01-01") :: Nil
      )
      // Only one partition should be planned: lake split + log tail for dt='2026-01-01'.
      val unionParts = lakeInputPartitions(unionDf)
      assert(
        unionParts.length == 2,
        s"Expected 2 input partitions (one lake split + one log tail) after pruning, " +
          s"got ${unionParts.length}")

      // Check the description carries the partition filter for visibility in EXPLAIN output.
      assert(
        unionDf.queryExecution.executedPlan.toString.contains("PartitionFilter"),
        s"Plan should contain PartitionFilter:\n${unionDf.queryExecution.executedPlan}"
      )
    }
  }

  test("Spark Lake Read: log table partition filter pushdown in fallback (no lake snapshot)") {
    val tableName = scopedTableName("t_pd_part_fb")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName
             |  (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 'alpha', '2026-01-01'),
             |(2, 'beta', '2026-01-02'),
             |(3, 'gamma', '2026-01-03')
             |""".stripMargin)

      // No tiering performed -> falls back to reading directly from Fluss.
      val df = sql(s"""
                      |SELECT * FROM $DEFAULT_DATABASE.$tableName
                      |WHERE dt = '2026-01-02' ORDER BY id""".stripMargin)
      checkAnswer(df, Row(2, "beta", "2026-01-02") :: Nil)
      assert(
        lakeInputPartitions(df).length == 1,
        s"Expected fallback to plan 1 input partition after pruning"
      )
    }
  }

  test("Spark Lake Read: filter pushdown — partitioned lake table") {
    val tableName = scopedTableName("t_pd_partitioned")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName
             |  (id INT, amount INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 100, 'alpha', '2026-01-01'),
             |(2, 200, 'beta', '2026-01-01'),
             |(3, 300, 'gamma', '2026-01-02')
             |""".stripMargin)
      tierToLake(tableName)

      val query = sql(s"""
                         |SELECT id, amount, dt FROM $DEFAULT_DATABASE.$tableName
                         |WHERE amount >= 200 ORDER BY id""".stripMargin)
      checkAnswer(query, Row(2, 200, "2026-01-01") :: Row(3, 300, "2026-01-02") :: Nil)
      assertPushedNames(query, Set(">="))
    }
  }

  test("Spark Lake Read: filter pushdown — non-pushable predicate falls back to Spark") {
    val tableName = scopedTableName("t_pd_nonpushable")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, amount INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 100, 'alpha'), (2, 200, 'beta'), (3, 300, 'gamma'), (4, 400, 'delta')
             |""".stripMargin)
      tierToLake(tableName)

      val query =
        sql(s"SELECT id FROM $DEFAULT_DATABASE.$tableName WHERE amount % 200 = 0 ORDER BY id")
      checkAnswer(query, Row(2) :: Row(4) :: Nil)
      // The modulo expression is not convertible; only the implicit IS_NOT_NULL Spark adds is pushed.
      assertResult(Set("IS_NOT_NULL"))(pushedPredicates(query).map(_.name()).toSet)
    }
  }

  test("Spark Lake Read: log table union read with limit pushdown") {
    val tableName = scopedTableName("t_union_limit")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake(tableName)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(4, "delta"), (5, "epsilon")
             |""".stripMargin)

      val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName LIMIT 2")
      assert(flussScan(df).flatMap(_.limit).distinct == Seq(2))

      // Verify limit pushdown actually reduces rows read via metrics
      df.collect()
      val batchScanExec = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val numRowsRead = batchScanExec.metrics(FlussMetrics.NUM_ROWS_READ).value
      assert(numRowsRead == 2L, s"Expected 2 rows read with limit pushdown, got $numRowsRead")
    }
  }

  test("Spark Lake Read: non-FULL startup mode skips lake path") {
    val tableName = scopedTableName("t_earliest")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake(tableName)

      try {
        spark.conf.set("spark.sql.fluss.scan.startup.mode", "earliest")

        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName ORDER BY id"),
          Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
        )
      } finally {
        spark.conf.set("spark.sql.fluss.scan.startup.mode", "full")
      }
    }
  }

}

@SparkLakeTest
class SparkLakePaimonLogTableReadTest extends SparkLakeLogTableReadTest {
  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.PAIMON

  test("Spark Lake Read: custom lake path falls back when no lake snapshot") {
    val tableName = scopedTableName("t_custom_lake_path_fallback")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_DATABASE_NAME.key()}' = 'custom_lake_db',
             |  '${ConfigOptions.TABLE_DATALAKE_TABLE_NAME.key()}' = 'custom_lake_table_fallback',
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, "hello"), (2, "fluss")
             |""".stripMargin)

      checkAnswer(
        sql(s"""
               |SELECT * FROM $DEFAULT_DATABASE.$tableName
               |WHERE id >= 1
               |ORDER BY id
               |""".stripMargin),
        Row(1, "hello") :: Row(2, "fluss") :: Nil
      )
    }
  }

  test("Spark Lake Read: custom lake path rejects an actual lake read") {
    val tableName = scopedTableName("t_custom_lake_path_snapshot")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_DATABASE_NAME.key()}' = 'custom_lake_db',
             |  '${ConfigOptions.TABLE_DATALAKE_TABLE_NAME.key()}' = 'custom_lake_table_snapshot',
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, "hello"), (2, "fluss")
             |""".stripMargin)
      tierToLake(tableName)

      val error = intercept[UnsupportedOperationException] {
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName").collect()
      }
      assert(
        error.getMessage == "Custom lake table path is not supported for Spark lake reads yet.")
    }
  }

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.PAIMON.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    warehousePath =
      Files.createTempDirectory("fluss-testing-lake-read").resolve("warehouse").toString
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

@SparkLakeTest
@SparkLakeIcebergTest
class SparkLakeIcebergLogTableReadTest extends SparkLakeLogTableReadTest {
  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.ICEBERG

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.ICEBERG.toString)
    conf.setString("datalake.iceberg.type", "hadoop")
    warehousePath =
      Files.createTempDirectory("fluss-testing-iceberg-lake-read").resolve("warehouse").toString
    conf.setString("datalake.iceberg.warehouse", warehousePath)
    conf
  }

  override protected def lakeCatalogConf: Configuration = {
    val conf = new Configuration()
    conf.setString("type", "hadoop")
    conf.setString("warehouse", warehousePath)
    conf
  }
}
