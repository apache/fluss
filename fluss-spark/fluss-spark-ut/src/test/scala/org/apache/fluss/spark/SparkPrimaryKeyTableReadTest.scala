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

import org.apache.fluss.client.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer}
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.metadata.{TableBucket, TablePath}
import org.apache.fluss.spark.read.{FlussMetrics, FlussScan, FlussUpsertInputPartition}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.assertj.core.api.Assertions.assertThat

import scala.collection.JavaConverters._

/** This test case is used to verify the correctness of primary key table read. */
class SparkPrimaryKeyTableReadTest extends FlussSparkTestBase {

  /**
   * Do not set [[ConfigOptions.KV_SNAPSHOT_INTERVAL]] here, we want to control the snapshot trigger
   * by manually.
   */
  override def flussConf: Configuration = {
    new Configuration()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql(
      s"set ${SparkFlussConf.SPARK_FLUSS_CONF_PREFIX}${SparkFlussConf.SCAN_START_UP_MODE.key()}=full")
    sql(
      s"set ${SparkFlussConf.SPARK_FLUSS_CONF_PREFIX}${SparkFlussConf.READ_OPTIMIZED_OPTION.key()}=false")
  }

  test("Spark Read: primary key table") {
    withTable("t") {
      val tablePath = createTablePath("t")
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (orderId BIGINT, itemId BIGINT, amount INT, address STRING)
             |TBLPROPERTIES("primary.key" = "orderId", "bucket.num" = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(600L, 21L, 601, "addr1"), (700L, 22L, 602, "addr2"),
             |(800L, 23L, 603, "addr3"), (900L, 24L, 604, "addr4"),
             |(1000L, 25L, 605, "addr5")
             |""".stripMargin)

      var inputPartitions = genInputPartition(tablePath, null)
      // Data is only stored in log.
      assertThat(inputPartitions.exists(hasSnapshotData)).isEqualTo(false)
      assertThat(inputPartitions.forall(hasLogChanges)).isEqualTo(true)
      // Read data from log scanner.
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1") ::
          Row(700L, 22L, 602, "addr2") ::
          Row(800L, 23L, 603, "addr3") ::
          Row(900L, 24L, 604, "addr4") ::
          Row(1000L, 25L, 605, "addr5") :: Nil
      )

      // Trigger snapshot.
      flussServer.triggerAndWaitSnapshot(tablePath)
      inputPartitions = genInputPartition(tablePath, null)
      assertThat(inputPartitions.forall(hasSnapshotData)).isEqualTo(true)
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1") ::
          Row(700L, 22L, 602, "addr2") ::
          Row(800L, 23L, 603, "addr3") ::
          Row(900L, 24L, 604, "addr4") ::
          Row(1000L, 25L, 605, "addr5") :: Nil
      )

      // Upsert.
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(700L, 220L, 602, "addr2"),
             |(900L, 240L, 604, "addr4"),
             |(1100L, 260L, 606, "addr6")
             |""".stripMargin)

      inputPartitions = genInputPartition(tablePath, null)
      // Data is stored in both snapshot and log.
      assertThat(inputPartitions.exists(hasSnapshotData)).isEqualTo(true)
      assertThat(inputPartitions.exists(hasLogChanges)).isEqualTo(true)
      checkAnswer(
        sql(s"""
               |SELECT orderId, itemId, address FROM $DEFAULT_DATABASE.t
               |WHERE amount <= 603 ORDER BY orderId""".stripMargin),
        Row(600L, 21L, "addr1") ::
          Row(700L, 220L, "addr2") ::
          Row(800L, 23L, "addr3") ::
          Nil
      )
      withSQLConf(
        s"${SparkFlussConf.SPARK_FLUSS_CONF_PREFIX}${SparkFlussConf.READ_OPTIMIZED_OPTION.key()}" -> "true") {
        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
          Row(600L, 21L, 601, "addr1") ::
            Row(700L, 22L, 602, "addr2") ::
            Row(800L, 23L, 603, "addr3") ::
            Row(900L, 24L, 604, "addr4") ::
            Row(1000L, 25L, 605, "addr5") :: Nil
        )
      }

      // Trigger snapshot.
      flussServer.triggerAndWaitSnapshot(tablePath)
      checkAnswer(
        sql(s"""
               |SELECT orderId, itemId, address FROM $DEFAULT_DATABASE.t
               |WHERE amount <= 603 ORDER BY orderId""".stripMargin),
        Row(600L, 21L, "addr1") ::
          Row(700L, 220L, "addr2") ::
          Row(800L, 23L, "addr3") ::
          Nil
      )

      // Only support FULL startup mode.
      withSQLConf(
        s"${SparkFlussConf.SPARK_FLUSS_CONF_PREFIX}${SparkFlussConf.SCAN_START_UP_MODE.key()}" -> "latest") {
        intercept[UnsupportedOperationException](
          sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId").show())
      }
    }
  }

  test("Spark Read: partitioned primary key table") {
    withTable("t") {
      val tablePath = createTablePath("t")
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING)
             |PARTITIONED BY (dt)
             |TBLPROPERTIES("primary.key" = "orderId,dt", "bucket.num" = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(600L, 21L, 601, "addr1", "2026-01-01"), (700L, 22L, 602, "addr2", "2026-01-01"),
             |(800L, 23L, 603, "addr3", "2026-01-02"), (900L, 24L, 604, "addr4", "2026-01-02"),
             |(1000L, 25L, 605, "addr5", "2026-01-03")
             |""".stripMargin)

      var inputPartitions = admin.listPartitionInfos(tablePath).get().asScala.flatMap {
        p => genInputPartition(tablePath, p.getPartitionName)
      }
      // Data is only in log.
      assertThat(inputPartitions.exists(hasSnapshotData)).isEqualTo(false)
      assertThat(inputPartitions.forall(hasLogChanges)).isEqualTo(true)
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 22L, 602, "addr2", "2026-01-01") ::
          Row(800L, 23L, 603, "addr3", "2026-01-02") ::
          Row(900L, 24L, 604, "addr4", "2026-01-02") ::
          Row(1000L, 25L, 605, "addr5", "2026-01-03") ::
          Nil
      )

      // Trigger snapshot.
      flussServer.triggerAndWaitSnapshot(tablePath)
      var inputPartition0 = genInputPartition(tablePath, "2026-01-01").head
      assertThat(hasSnapshotData(inputPartition0)).isEqualTo(true)
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 22L, 602, "addr2", "2026-01-01") ::
          Row(800L, 23L, 603, "addr3", "2026-01-02") ::
          Row(900L, 24L, 604, "addr4", "2026-01-02") ::
          Row(1000L, 25L, 605, "addr5", "2026-01-03") ::
          Nil
      )

      // Upsert.
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(700L, 220L, 602, "addr2_updated", "2026-01-01"),
             |(900L, 240L, 604, "addr4_updated", "2026-01-02"),
             |(1100L, 260L, 606, "addr6", "2026-01-03")
             |""".stripMargin)

      inputPartition0 = genInputPartition(tablePath, "2026-01-01").head
      // Data(2026-01-01, bucketId=0) is stored in both snapshot and log.
      assertThat(hasSnapshotData(inputPartition0)).isEqualTo(true)
      assertThat(hasLogChanges(inputPartition0)).isEqualTo(true)
      // Read with partition filter
      checkAnswer(
        sql(s"""
               |SELECT * FROM $DEFAULT_DATABASE.t
               |WHERE dt = '2026-01-01'
               |ORDER BY orderId""".stripMargin),
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 220L, 602, "addr2_updated", "2026-01-01") ::
          Nil
      )

      // Trigger a bucket snapshot.
      flussServer.triggerAndWaitSnapshot(inputPartition0.tableBucket)
      checkAnswer(
        sql(s"""
               |SELECT * FROM $DEFAULT_DATABASE.t
               |WHERE dt = '2026-01-01'
               |ORDER BY orderId""".stripMargin),
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 220L, 602, "addr2_updated", "2026-01-01") ::
          Nil
      )

      // Trigger snapshot.
      flussServer.triggerAndWaitSnapshot(tablePath)
      inputPartitions = admin.listPartitionInfos(tablePath).get().asScala.flatMap {
        p => genInputPartition(tablePath, p.getPartitionName)
      }
      assertThat(inputPartitions.forall(hasSnapshotData)).isEqualTo(true)
      // Read with multiple partition filters
      checkAnswer(
        sql(
          s"SELECT * FROM $DEFAULT_DATABASE.t WHERE dt IN ('2026-01-01', '2026-01-02') ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 220L, 602, "addr2_updated", "2026-01-01") ::
          Row(800L, 23L, 603, "addr3", "2026-01-02") ::
          Row(900L, 240L, 604, "addr4_updated", "2026-01-02") ::
          Nil
      )
    }
  }

  test("Spark Read: primary key table with random project") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id int, name string, pk int, pk2 string) TBLPROPERTIES('primary.key'='pk,pk2')")
      checkAnswer(sql("SELECT * FROM t"), Nil)
      sql("INSERT INTO t VALUES (1, 'a', 10, 'x'), (2, 'b', 20, 'y')")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Row(1, "a", 10, "x") :: Row(2, "b", 20, "y") :: Nil)
      checkAnswer(sql("SELECT pk, id FROM t ORDER BY id"), Row(10, 1) :: Row(20, 2) :: Nil)
    }
  }

  test("Spark Read: primary key table projection with type-dependent columns") {
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
             |) TBLPROPERTIES("primary.key" = "pk", "bucket.num" = 1)
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

  private def genInputPartition(
      tablePath: TablePath,
      partitionName: String): Array[FlussUpsertInputPartition] = {
    val kvSnapshots = if (partitionName == null) {
      admin.getLatestKvSnapshots(tablePath).get()
    } else {
      admin.getLatestKvSnapshots(tablePath, partitionName).get()
    }
    val bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)
    val latestOffsetsInitializer = OffsetsInitializer.latest()
    val tableId = kvSnapshots.getTableId
    val partitionId = kvSnapshots.getPartitionId
    val bucketIds = kvSnapshots.getBucketIds
    val bucketIdToLogOffset =
      latestOffsetsInitializer.getBucketOffsets(partitionName, bucketIds, bucketOffsetsRetriever)
    bucketIds.asScala.map {
      bucketId =>
        val tableBucket = new TableBucket(tableId, partitionId, bucketId)
        val snapshotId = kvSnapshots.getSnapshotId(bucketId).orElse(-1L)
        val logStartingOffset = kvSnapshots.getLogOffset(bucketId).orElse(-2L)
        val logEndingOffset = bucketIdToLogOffset.get(bucketId)

        FlussUpsertInputPartition(tableBucket, snapshotId, logStartingOffset, logEndingOffset)
    }.toArray
  }

  private def hasLogChanges(inputPartition: FlussUpsertInputPartition): Boolean = {
    inputPartition.logStoppingOffset > 0 && inputPartition.logStartingOffset < inputPartition.logStoppingOffset
  }

  private def hasSnapshotData(inputPartition: FlussUpsertInputPartition): Boolean = {
    inputPartition.snapshotId >= 0
  }

  test("Spark Read: primary key table scan metrics") {
    withTable("t") {
      val tablePath = createTablePath("t")
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (id INT, name STRING)
             |TBLPROPERTIES("primary.key" = "id", "bucket.num" = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES (1, 'a'), (2, 'b'), (3, 'c')
             |""".stripMargin)

      flussServer.triggerAndWaitSnapshot(tablePath)

      val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.t")

      // Verify scan description and supportedCustomMetrics before execution
      val scan = df.queryExecution.optimizedPlan
        .collectFirst { case r: DataSourceV2ScanRelation => r }
        .get
        .scan
        .asInstanceOf[FlussScan]

      assert(scan.description().contains("FlussScan"))
      assert(scan.description().contains("Upsert"))
      assert(scan.supportedCustomMetrics().exists(_.name() == FlussMetrics.NUM_ROWS_READ))

      // Execute the query to trigger metric accumulation
      df.collect()

      // Verify numRowsRead is accumulated in BatchScanExec after execution
      val batchScanExec = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get

      val numRowsRead = batchScanExec.metrics(FlussMetrics.NUM_ROWS_READ).value
      assert(numRowsRead == 3L, s"Expected 3 rows read, got $numRowsRead")
    }
  }
}
