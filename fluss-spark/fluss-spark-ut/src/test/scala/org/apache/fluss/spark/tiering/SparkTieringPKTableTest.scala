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
import org.apache.fluss.spark.SparkConnectorOptions.{BUCKET_NUMBER, PRIMARY_KEY}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row

import java.nio.file.Files

/**
 * Integration test for the Spark tiering pipeline on primary key (PK) tables.
 *
 * Validates that the Spark tiering path correctly handles PK table semantics including lake-only
 * reads, union reads (lake + kv tail), and update merge for both non-partitioned and partitioned
 * tables.
 */
abstract class SparkTieringPKTableTest extends SparkTieringTestBase with Logging {

  test("Spark Tiering: pk table tier and read back") {
    withTable("t_pk_tier") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_pk_tier (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_tier VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLakeViaSpark("t_pk_tier")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_pk_tier ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob", 85) :: Row(3, "charlie", 95) :: Nil
      )

      // Column projection
      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t_pk_tier ORDER BY name"),
        Row("alice") :: Row("bob") :: Row("charlie") :: Nil
      )
    }
  }

  test("Spark Tiering: pk table union read (lake + kv tail)") {
    withTable("t_pk_union") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_pk_union (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_union VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLakeViaSpark("t_pk_union")

      // Insert new rows after tiering (these live in the kv tail)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_union VALUES
             |(4, "david", 88), (5, "eve", 92)
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_pk_union ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob", 85) :: Row(3, "charlie", 95) ::
          Row(4, "david", 88) :: Row(5, "eve", 92) :: Nil
      )
    }
  }

  test("Spark Tiering: pk table union read with updates") {
    withTable("t_pk_update") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_pk_update (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_update VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLakeViaSpark("t_pk_update")

      // Update id=2 and insert a new record id=4
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_update VALUES
             |(2, "bob_updated", 100), (4, "david", 88)
             |""".stripMargin)

      // Union read: updated value for id=2 should come from kv tail
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_pk_update ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob_updated", 100) ::
          Row(3, "charlie", 95) :: Row(4, "david", 88) :: Nil
      )
    }
  }

  test("Spark Tiering: pk table incremental tiering") {
    withTable("t_pk_incr") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_pk_incr (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      // First batch
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_incr VALUES
             |(1, "alice", 90), (2, "bob", 85)
             |""".stripMargin)

      tierToLakeViaSpark("t_pk_incr")

      // Second batch: new records + update existing
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_incr VALUES
             |(2, "bob_v2", 92), (3, "charlie", 95)
             |""".stripMargin)

      tierToLakeViaSpark("t_pk_incr")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_pk_incr ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob_v2", 92) :: Row(3, "charlie", 95) :: Nil
      )
    }
  }

  test("Spark Tiering: partitioned pk table tier and read back") {
    withTable("t_pk_tier_pt") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_pk_tier_pt
             |  (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_tier_pt VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLakeViaSpark("t_pk_tier_pt")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_pk_tier_pt ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") :: Nil
      )

      // Column projection with different order
      checkAnswer(
        sql(s"SELECT dt, name, id FROM $DEFAULT_DATABASE.t_pk_tier_pt ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob", 2) ::
          Row("2026-01-02", "charlie", 3) :: Nil
      )
    }
  }

  test("Spark Tiering: partitioned pk table union read (lake + kv tail)") {
    withTable("t_pk_union_pt") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_pk_union_pt
             |  (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_union_pt VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLakeViaSpark("t_pk_union_pt")

      // Insert more data after tiering: existing partition + new partition
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_union_pt VALUES
             |(4, "david", 88, "2026-01-01"),
             |(5, "eve", 92, "2026-01-03")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_pk_union_pt ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") ::
          Row(4, "david", 88, "2026-01-01") ::
          Row(5, "eve", 92, "2026-01-03") :: Nil
      )
    }
  }

  test("Spark Tiering: partitioned pk table union read with updates") {
    withTable("t_pk_update_pt") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_pk_update_pt
             |  (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_update_pt VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLakeViaSpark("t_pk_update_pt")

      // Update id=2 (same partition) and insert new id=4 (same partition as charlie)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_pk_update_pt VALUES
             |(2, "bob_updated", 100, "2026-01-01"),
             |(4, "david", 88, "2026-01-02")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_pk_update_pt ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob_updated", 100, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") ::
          Row(4, "david", 88, "2026-01-02") :: Nil
      )
    }
  }
}

class SparkTieringPaimonPKTableTest extends SparkTieringPKTableTest {

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
      Files.createTempDirectory("fluss-testing-spark-tiering-pk").resolve("warehouse").toString
    conf.setString("datalake.paimon.warehouse", warehousePath)
    conf
  }
}
