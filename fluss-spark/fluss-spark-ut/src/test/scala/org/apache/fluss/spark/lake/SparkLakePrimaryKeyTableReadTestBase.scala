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
import org.apache.fluss.spark.SparkConnectorOptions.{BUCKET_NUMBER, PRIMARY_KEY}

import org.apache.spark.sql.Row

import java.nio.file.Files

/**
 * Base class for lake-enabled primary key table read tests. Subclasses provide the lake format
 * config and lake catalog configuration.
 */
abstract class SparkLakePrimaryKeyTableReadTestBase extends SparkLakeTableReadTestBase {

  test("Spark Lake Read: pk table falls back when no lake snapshot") {
    // Test non-partitioned table
    withTable("t_non_partitioned") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_non_partitioned (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_non_partitioned VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_non_partitioned ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob", 85) :: Row(3, "charlie", 95) :: Nil
      )
    }

    // Test partitioned table
    withTable("t_partitioned") {
      sql(
        s"""
           |CREATE TABLE $DEFAULT_DATABASE.t_partitioned (id INT, name STRING, score INT, dt STRING)
           | PARTITIONED BY (dt)
           | TBLPROPERTIES (
           |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
           |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
           |  '${PRIMARY_KEY.key()}' = 'id,dt',
           |  '${BUCKET_NUMBER.key()}' = 1)
           |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_partitioned VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_partitioned ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") :: Nil
      )

      // Test column projection with different order
      checkAnswer(
        sql(s"SELECT dt, name, id FROM $DEFAULT_DATABASE.t_partitioned ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob", 2) ::
          Row("2026-01-02", "charlie", 3) :: Nil
      )
    }
  }

  test("Spark Lake Read: pk table lake-only (all data in lake, no kv tail)") {
    // Test non-partitioned table
    withTable("t_lake_only") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_lake_only (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_only VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLake("t_lake_only")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_lake_only ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob", 85) :: Row(3, "charlie", 95) :: Nil
      )
    }

    // Test partitioned table
    withTable("t_lake_only_partitioned") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_lake_only_partitioned (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_only_partitioned VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLake("t_lake_only_partitioned")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_lake_only_partitioned ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") :: Nil
      )

      // Test column projection with different order
      checkAnswer(
        sql(s"SELECT dt, name, id FROM $DEFAULT_DATABASE.t_lake_only_partitioned ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob", 2) ::
          Row("2026-01-02", "charlie", 3) :: Nil
      )
    }
  }

  test("Spark Lake Read: pk table union read (lake + kv tail)") {
    // Test non-partitioned table
    withTable("t_union") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_union (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLake("t_union")

      // Insert more data after tiering (this will be in kv tail)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union VALUES
             |(4, "david", 88), (5, "eve", 92)
             |""".stripMargin)

      // Union read: should see both lake data and kv tail data
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_union ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob", 85) :: Row(3, "charlie", 95) ::
          Row(4, "david", 88) :: Row(5, "eve", 92) :: Nil
      )
    }

    // Test partitioned table
    withTable("t_union_partitioned") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_union_partitioned (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union_partitioned VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLake("t_union_partitioned")

      // Insert more data after tiering
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union_partitioned VALUES
             |(4, "david", 88, "2026-01-01"),
             |(5, "eve", 92, "2026-01-03")
             |""".stripMargin)

      // Union read with partition filter
      checkAnswer(
        sql(s"""
               |SELECT * FROM $DEFAULT_DATABASE.t_union_partitioned
               |WHERE dt = '2026-01-01' ORDER BY id""".stripMargin),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(4, "david", 88, "2026-01-01") :: Nil
      )

      // Union read all partitions
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_union_partitioned ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") ::
          Row(4, "david", 88, "2026-01-01") ::
          Row(5, "eve", 92, "2026-01-03") :: Nil
      )

      // Test column projection with different order
      checkAnswer(
        sql(s"SELECT dt, name, id FROM $DEFAULT_DATABASE.t_union_partitioned ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob", 2) ::
          Row("2026-01-02", "charlie", 3) ::
          Row("2026-01-01", "david", 4) ::
          Row("2026-01-03", "eve", 5) :: Nil
      )
    }
  }

  test("Spark Lake Read: pk table union read with updates") {
    // Test non-partitioned table
    withTable("t_update") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_update (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_update VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLake("t_update")

      // Update existing record and insert new record
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_update VALUES
             |(2, "bob_updated", 100), (4, "david", 88)
             |""".stripMargin)

      // Union read: should see updated value for id=2 from kv tail
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_update ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob_updated", 100) ::
          Row(3, "charlie", 95) :: Row(4, "david", 88) :: Nil
      )
    }

    // Test partitioned table
    withTable("t_update_partitioned") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_update_partitioned (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_update_partitioned VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLake("t_update_partitioned")

      // Update existing record and insert new record
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_update_partitioned VALUES
             |(2, "bob_updated", 100, "2026-01-01"),
             |(4, "david", 88, "2026-01-02")
             |""".stripMargin)

      // Union read: should see updated value for id=2 from kv tail
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_update_partitioned ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob_updated", 100, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") ::
          Row(4, "david", 88, "2026-01-02") :: Nil
      )

      // Test column projection with different order
      checkAnswer(
        sql(s"SELECT dt, name, id FROM $DEFAULT_DATABASE.t_update_partitioned ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob_updated", 2) ::
          Row("2026-01-02", "charlie", 3) ::
          Row("2026-01-02", "david", 4) :: Nil
      )
    }
  }

  test("Spark Lake Read: primary key table projection with type-dependent columns") {
    withTable("t") {
      val tablePath = createTablePath("t")
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (
             |pk INT,
             |ts TIMESTAMP,
             |name STRING,
             |arr ARRAY<INT>,
             |struct_col STRUCT<col1: INT, col2: STRING>,
             |ts_ltz TIMESTAMP_LTZ
             |)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'pk',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, TIMESTAMP "2026-01-01 12:00:00", "a", ARRAY(1, 2), STRUCT(10, 'x'),
             | TIMESTAMP "2026-01-01 12:00:00"),
             |(2, TIMESTAMP "2026-01-02 12:00:00", "b", ARRAY(3, 4), STRUCT(20, 'y'),
             | TIMESTAMP "2026-01-02 12:00:00")
             |""".stripMargin)

      // Log-only: projection reorders type-dependent columns (PK not in projection)
      checkAnswer(
        sql(s"SELECT arr, ts, struct_col FROM $DEFAULT_DATABASE.t ORDER BY ts"),
        Row(Seq(1, 2), java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), Row(10, "x")) ::
          Row(Seq(3, 4), java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), Row(20, "y")) :: Nil
      )

      // Trigger snapshot, then test with snapshot + log merge
      flussServer.triggerAndWaitSnapshot(tablePath)

      tierToLake("t")

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, TIMESTAMP "2026-03-01 12:00:00", "a_updated", ARRAY(10, 20), STRUCT(100, 'xx'),
             | TIMESTAMP "2026-03-01 12:00:00")
             |""".stripMargin)

      // Snapshot + log: projection with type-dependent columns at shifted ordinals
      checkAnswer(
        sql(s"SELECT ts_ltz, arr, name FROM $DEFAULT_DATABASE.t ORDER BY name"),
        Row(java.sql.Timestamp.valueOf("2026-03-01 12:00:00"), Seq(10, 20), "a_updated") ::
          Row(java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), Seq(3, 4), "b") :: Nil
      )
    }
  }
}

class SparkLakePaimonPrimaryKeyTableReadTest extends SparkLakePrimaryKeyTableReadTestBase {

  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.PAIMON

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.PAIMON.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    warehousePath =
      Files.createTempDirectory("fluss-testing-paimon-pk-lake-read").resolve("warehouse").toString
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
