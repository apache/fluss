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
import org.apache.fluss.spark.read.FlussMetrics
import org.apache.fluss.spark.read.FlussUpsertInputPartition

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import java.nio.file.Files

/**
 * Base class for lake-enabled primary key table read tests. Subclasses provide the lake format
 * config and lake catalog configuration.
 */
abstract class SparkLakePrimaryKeyTableReadTestBase extends SparkLakeTableReadTestBase {

  test("Spark Lake Read: pk table falls back when no lake snapshot") {
    val nonPartitionedTableName = scopedTableName("t_non_partitioned")
    val partitionedTableName = scopedTableName("t_partitioned")
    // Test non-partitioned table
    withTable(nonPartitionedTableName) {
      sql(
        s"""
           |CREATE TABLE $DEFAULT_DATABASE.$nonPartitionedTableName (id INT, name STRING, score INT)
           | TBLPROPERTIES (
           |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
           |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
           |  '${PRIMARY_KEY.key()}' = 'id',
           |  '${BUCKET_NUMBER.key()}' = 1)
           |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$nonPartitionedTableName VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$nonPartitionedTableName ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob", 85) :: Row(3, "charlie", 95) :: Nil
      )
    }

    // Test partitioned table
    withTable(partitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$partitionedTableName (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$partitionedTableName VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$partitionedTableName ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") :: Nil
      )

      // Test column projection with different order
      checkAnswer(
        sql(s"SELECT dt, name, id FROM $DEFAULT_DATABASE.$partitionedTableName ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob", 2) ::
          Row("2026-01-02", "charlie", 3) :: Nil
      )
    }
  }

  test("Spark Lake Read: pk table fallback uses Fluss kv snapshot for log tail merge") {
    val fbHybridTableName = scopedTableName("t_fb_hybrid")
    val fbHybridPartitionedTableName = scopedTableName("t_fb_hybrid_partitioned")
    // Non-partitioned table
    withTable(fbHybridTableName) {
      val tablePath = createTablePath(fbHybridTableName)
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$fbHybridTableName (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$fbHybridTableName VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      flussServer.triggerAndWaitSnapshot(tablePath)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$fbHybridTableName VALUES
             |(2, "bob_updated", 100), (4, "david", 88)
             |""".stripMargin)

      val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.$fbHybridTableName ORDER BY id")
      val partitions = lakeInputPartitions(df).map(_.asInstanceOf[FlussUpsertInputPartition])
      assert(
        partitions.exists(_.snapshotId != TableBucketSnapshot.NO_SNAPSHOT_ID),
        s"Expected at least one hybrid partition with snapshotId >= 0, got: ${partitions.mkString(", ")}"
      )
      checkAnswer(
        df,
        Row(1, "alice", 90) :: Row(2, "bob_updated", 100) ::
          Row(3, "charlie", 95) :: Row(4, "david", 88) :: Nil
      )
    }

    // Partitioned table
    withTable(fbHybridPartitionedTableName) {
      val tablePath = createTablePath(fbHybridPartitionedTableName)
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$fbHybridPartitionedTableName (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$fbHybridPartitionedTableName VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      flussServer.triggerAndWaitSnapshot(tablePath)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$fbHybridPartitionedTableName VALUES
             |(2, "bob_updated", 100, "2026-01-01"),
             |(4, "david", 88, "2026-01-02")
             |""".stripMargin)

      val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.$fbHybridPartitionedTableName ORDER BY id")
      val partitions = lakeInputPartitions(df).map(_.asInstanceOf[FlussUpsertInputPartition])
      assert(
        partitions.exists(_.snapshotId != TableBucketSnapshot.NO_SNAPSHOT_ID),
        s"Expected at least one hybrid partition with snapshotId >= 0, got: ${partitions.mkString(", ")}"
      )
      checkAnswer(
        df,
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob_updated", 100, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") ::
          Row(4, "david", 88, "2026-01-02") :: Nil
      )
    }
  }

  test("Spark Lake Read: pk table lake-only (all data in lake, no kv tail)") {
    val lakeOnlyTableName = scopedTableName("t_lake_only")
    val lakeOnlyPartitionedTableName = scopedTableName("t_lake_only_partitioned")
    // Test non-partitioned table
    withTable(lakeOnlyTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$lakeOnlyTableName (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$lakeOnlyTableName VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLake(lakeOnlyTableName)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$lakeOnlyTableName ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob", 85) :: Row(3, "charlie", 95) :: Nil
      )
    }

    // Test partitioned table
    withTable(lakeOnlyPartitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLake(lakeOnlyPartitionedTableName)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") :: Nil
      )

      // Test column projection with different order
      checkAnswer(
        sql(
          s"SELECT dt, name, id FROM $DEFAULT_DATABASE.$lakeOnlyPartitionedTableName ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob", 2) ::
          Row("2026-01-02", "charlie", 3) :: Nil
      )
    }
  }

  test("Spark Lake Read: pk table union read (lake + kv tail)") {
    val unionTableName = scopedTableName("t_union")
    val unionPartitionedTableName = scopedTableName("t_union_partitioned")
    // Test non-partitioned table
    withTable(unionTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$unionTableName (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$unionTableName VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLake(unionTableName)

      // Insert more data after tiering (this will be in kv tail)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$unionTableName VALUES
             |(4, "david", 88), (5, "eve", 92)
             |""".stripMargin)

      // Union read: should see both lake data and kv tail data
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$unionTableName ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob", 85) :: Row(3, "charlie", 95) ::
          Row(4, "david", 88) :: Row(5, "eve", 92) :: Nil
      )
    }

    // Test partitioned table
    withTable(unionPartitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$unionPartitionedTableName (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$unionPartitionedTableName VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLake(unionPartitionedTableName)

      // Insert more data after tiering
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$unionPartitionedTableName VALUES
             |(4, "david", 88, "2026-01-01"),
             |(5, "eve", 92, "2026-01-03")
             |""".stripMargin)

      // Union read with partition filter
      checkAnswer(
        sql(s"""
               |SELECT * FROM $DEFAULT_DATABASE.$unionPartitionedTableName
               |WHERE dt = '2026-01-01' ORDER BY id""".stripMargin),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(4, "david", 88, "2026-01-01") :: Nil
      )

      // Union read all partitions
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$unionPartitionedTableName ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob", 85, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") ::
          Row(4, "david", 88, "2026-01-01") ::
          Row(5, "eve", 92, "2026-01-03") :: Nil
      )

      // Test column projection with different order
      checkAnswer(
        sql(s"SELECT dt, name, id FROM $DEFAULT_DATABASE.$unionPartitionedTableName ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob", 2) ::
          Row("2026-01-02", "charlie", 3) ::
          Row("2026-01-01", "david", 4) ::
          Row("2026-01-03", "eve", 5) :: Nil
      )
    }
  }

  test("Spark Lake Read: pk table union read with updates") {
    val updateTableName = scopedTableName("t_update")
    val updatePartitionedTableName = scopedTableName("t_update_partitioned")
    // Test non-partitioned table
    withTable(updateTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$updateTableName (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$updateTableName VALUES
             |(1, "alice", 90), (2, "bob", 85), (3, "charlie", 95)
             |""".stripMargin)

      tierToLake(updateTableName)

      // Update existing record and insert new record
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$updateTableName VALUES
             |(2, "bob_updated", 100), (4, "david", 88)
             |""".stripMargin)

      // Union read: should see updated value for id=2 from kv tail
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$updateTableName ORDER BY id"),
        Row(1, "alice", 90) :: Row(2, "bob_updated", 100) ::
          Row(3, "charlie", 95) :: Row(4, "david", 88) :: Nil
      )
    }

    // Test partitioned table
    withTable(updatePartitionedTableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$updatePartitionedTableName (id INT, name STRING, score INT, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$updatePartitionedTableName VALUES
             |(1, "alice", 90, "2026-01-01"),
             |(2, "bob", 85, "2026-01-01"),
             |(3, "charlie", 95, "2026-01-02")
             |""".stripMargin)

      tierToLake(updatePartitionedTableName)

      // Update existing record and insert new record
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$updatePartitionedTableName VALUES
             |(2, "bob_updated", 100, "2026-01-01"),
             |(4, "david", 88, "2026-01-02")
             |""".stripMargin)

      // Union read: should see updated value for id=2 from kv tail
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$updatePartitionedTableName ORDER BY id"),
        Row(1, "alice", 90, "2026-01-01") ::
          Row(2, "bob_updated", 100, "2026-01-01") ::
          Row(3, "charlie", 95, "2026-01-02") ::
          Row(4, "david", 88, "2026-01-02") :: Nil
      )

      // Test column projection with different order
      checkAnswer(
        sql(s"SELECT dt, name, id FROM $DEFAULT_DATABASE.$updatePartitionedTableName ORDER BY id"),
        Row("2026-01-01", "alice", 1) ::
          Row("2026-01-01", "bob_updated", 2) ::
          Row("2026-01-02", "charlie", 3) ::
          Row("2026-01-02", "david", 4) :: Nil
      )
    }
  }

  test("Spark Lake Read: pk filter pushdown — lake-only on non-pk column") {
    val tableName = scopedTableName("t_pd_pk_lake")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 'alice', 90), (2, 'bob', 85), (3, 'charlie', 95), (4, 'dave', 70)
             |""".stripMargin)
      tierToLake(tableName)

      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName WHERE score >= 90 ORDER BY id")
      checkAnswer(query, Row(1, "alice", 90) :: Row(3, "charlie", 95) :: Nil)
      assertPushedNames(query, Set(">="))
    }
  }

  test("Spark Lake Read: pk filter pushdown — union (lake + kv tail)") {
    val tableName = scopedTableName("t_pd_pk_union")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 'alice', 90), (2, 'bob', 85), (3, 'charlie', 95)
             |""".stripMargin)
      tierToLake(tableName)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(4, 'dave', 88), (5, 'eve', 92)
             |""".stripMargin)

      val query =
        sql(s"SELECT id, score FROM $DEFAULT_DATABASE.$tableName WHERE score >= 90 ORDER BY id")
      checkAnswer(query, Row(1, 90) :: Row(3, 95) :: Row(5, 92) :: Nil)
      assertPushedNames(query, Set(">="))
    }
  }

  test("Spark Lake Read: union with limit pushdown") {
    val tableName = scopedTableName("t_pk_union_limit")
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id INT, name STRING, score INT)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, 'alice', 90), (2, 'bob', 85), (3, 'charlie', 95)
             |""".stripMargin)
      tierToLake(tableName)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(4, 'dave', 88), (5, 'eve', 92)
             |""".stripMargin)

      val query =
        sql(s"SELECT id, score FROM $DEFAULT_DATABASE.$tableName LIMIT 2")
      assert(flussScan(query).flatMap(_.limit).distinct == Seq(2))

      // Verify limit pushdown actually reduces rows read via metrics
      query.collect()
      val batchScanExec = query.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val numRowsRead = batchScanExec.metrics(FlussMetrics.NUM_ROWS_READ).value
      assert(numRowsRead == 2L, s"Expected 2 rows read with limit pushdown, got $numRowsRead")
    }
  }

  test("Spark Lake Read: primary key table projection with type-dependent columns") {
    val tableName = scopedTableName("t")
    withTable(tableName) {
      val tablePath = createTablePath(tableName)
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (
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
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, TIMESTAMP "2026-01-01 12:00:00", "a", ARRAY(1, 2), STRUCT(10, 'x'),
             | TIMESTAMP "2026-01-01 12:00:00"),
             |(2, TIMESTAMP "2026-01-02 12:00:00", "b", ARRAY(3, 4), STRUCT(20, 'y'),
             | TIMESTAMP "2026-01-02 12:00:00")
             |""".stripMargin)

      // Log-only: projection reorders type-dependent columns (PK not in projection)
      checkAnswer(
        sql(s"SELECT arr, ts, struct_col FROM $DEFAULT_DATABASE.$tableName ORDER BY ts"),
        Row(Seq(1, 2), java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), Row(10, "x")) ::
          Row(Seq(3, 4), java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), Row(20, "y")) :: Nil
      )

      // Trigger snapshot, then test with snapshot + log merge
      flussServer.triggerAndWaitSnapshot(tablePath)

      tierToLake(tableName)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.$tableName VALUES
             |(1, TIMESTAMP "2026-03-01 12:00:00", "a_updated", ARRAY(10, 20), STRUCT(100, 'xx'),
             | TIMESTAMP "2026-03-01 12:00:00")
             |""".stripMargin)

      // Snapshot + log: projection with type-dependent columns at shifted ordinals
      checkAnswer(
        sql(s"SELECT ts_ltz, arr, name FROM $DEFAULT_DATABASE.$tableName ORDER BY name"),
        Row(java.sql.Timestamp.valueOf("2026-03-01 12:00:00"), Seq(10, 20), "a_updated") ::
          Row(java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), Seq(3, 4), "b") :: Nil
      )
    }
  }

}

@SparkLakeTest
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
