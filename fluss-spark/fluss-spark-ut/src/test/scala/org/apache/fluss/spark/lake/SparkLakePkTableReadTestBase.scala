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
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder
import org.apache.fluss.flink.tiering.source.TieringSourceOptions
import org.apache.fluss.metadata.{DataLakeFormat, TableBucket, TablePath}
import org.apache.fluss.spark.FlussSparkTestBase
import org.apache.fluss.spark.SparkConnectorOptions.{BUCKET_NUMBER, PRIMARY_KEY}

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.spark.sql.Row

import java.time.Duration

import scala.jdk.CollectionConverters._

/**
 * Base class for lake-enabled primary key table read tests. Subclasses provide the lake format
 * config and lake catalog configuration.
 */
abstract class SparkLakePkTableReadTestBase extends FlussSparkTestBase {

  protected var warehousePath: String = _

  /** The lake format used by this test. */
  protected def dataLakeFormat: DataLakeFormat

  /** Lake catalog configuration specific to the format. */
  protected def lakeCatalogConf: Configuration

  private val TIERING_PARALLELISM = 2
  private val CHECKPOINT_INTERVAL_MS = 1000L
  private val POLL_INTERVAL: Duration = Duration.ofMillis(500L)
  private val SYNC_TIMEOUT: Duration = Duration.ofMinutes(2)
  private val SYNC_POLL_INTERVAL_MS = 500L

  /** Tier all pending data for the given table to the lake. */
  protected def tierToLake(tableName: String): Unit = {
    val tablePath = createTablePath(tableName)
    val tableInfo = loadFlussTable(tablePath).getTableInfo
    val tableId = tableInfo.getTableId
    val numBuckets = tableInfo.getNumBuckets

    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    execEnv.setParallelism(TIERING_PARALLELISM)
    execEnv.enableCheckpointing(CHECKPOINT_INTERVAL_MS)

    val flussConfig = new Configuration(flussServer.getClientConfig)
    flussConfig.set(TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL, POLL_INTERVAL)

    val jobClient = LakeTieringJobBuilder
      .newBuilder(
        execEnv,
        flussConfig,
        lakeCatalogConf,
        new Configuration(),
        dataLakeFormat.toString)
      .build()

    try {
      // Collect all buckets to wait for sync
      val tableBuckets = if (tableInfo.isPartitioned) {
        // For partitioned table, get all partitions and their buckets
        val partitionInfos = admin.listPartitionInfos(tablePath).get()
        partitionInfos.asScala.flatMap {
          partitionInfo =>
            (0 until numBuckets).map {
              bucket => new TableBucket(tableId, partitionInfo.getPartitionId, bucket)
            }
        }.toSet
      } else {
        // For non-partitioned table, just use bucket 0
        Set(new TableBucket(tableId, 0))
      }

      val deadline = System.currentTimeMillis() + SYNC_TIMEOUT.toMillis
      val syncedBuckets = scala.collection.mutable.Set[TableBucket]()

      while (syncedBuckets.size < tableBuckets.size && System.currentTimeMillis() < deadline) {
        tableBuckets.foreach {
          tableBucket =>
            if (!syncedBuckets.contains(tableBucket)) {
              try {
                val replica = flussServer.waitAndGetLeaderReplica(tableBucket)
                // For pk table, we also check the LogTablet's lake snapshot id
                // which is updated when data is tiered to lake
                if (replica.getLogTablet.getLakeTableSnapshotId >= 0) {
                  syncedBuckets.add(tableBucket)
                }
              } catch {
                case _: Exception =>
              }
            }
        }
        if (syncedBuckets.size < tableBuckets.size) {
          Thread.sleep(SYNC_POLL_INTERVAL_MS)
        }
      }

      assert(
        syncedBuckets.size == tableBuckets.size,
        s"Not all buckets synced to lake within $SYNC_TIMEOUT. " +
          s"Synced: ${syncedBuckets.size}, Total: ${tableBuckets.size}"
      )
    } finally {
      jobClient.cancel().get()
    }
  }

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.foreach(t => sql(s"DROP TABLE IF EXISTS $DEFAULT_DATABASE.$t"))
    }
  }

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
}
