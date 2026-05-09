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
import org.apache.fluss.spark.read.{FlussCountScan, FlussMetrics, FlussScan, FlussUpsertInputPartition, FlussUpsertScan}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
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

      // Insert one row with a NULL nullable column to make the COUNT(nullable_col)
      // assertion below actually discriminate between row count and non-null count.
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1200L, 270L, 607, NULL)
             |""".stripMargin)

      // COUNT(*) — empty projection case (regression test for #2724). PK table has 7 rows
      // after upserts (600/700/800/900/1000/1100/1200).
      checkAnswer(sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.t"), Row(7L) :: Nil)
      checkAnswer(sql(s"SELECT COUNT(1) FROM $DEFAULT_DATABASE.t"), Row(7L) :: Nil)

      // COUNT(*) with filter
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.t WHERE amount <= 603"),
        Row(3L) :: Nil
      )

      // COUNT on a nullable column — address is nullable STRING and one row has NULL,
      // so COUNT(address) must be strictly less than COUNT(*).
      checkAnswer(
        sql(s"SELECT COUNT(address) FROM $DEFAULT_DATABASE.t"),
        Row(6L) :: Nil
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

      // COUNT(*) on partitioned PK table
      checkAnswer(sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.t"), Row(6L) :: Nil)

      // COUNT(*) with partition filter
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.t WHERE dt = '2026-01-01'"),
        Row(2L) :: Nil
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

  test("Spark Read: primary key table COUNT aggregate pushdown") {
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
             |(1000L, 25L, 605, "addr5"), (1100L, 26L, 606, "addr6")
             |""".stripMargin)

      // Extra NULL row so COUNT(nullable_col) strictly differs from COUNT(*).
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1200L, 27L, 607, NULL)
             |""".stripMargin)

      // COUNT(*) — pushable.
      val countStarDf = spark.sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.t")
      assertThat(countStarDf.collect().head.getLong(0)).isEqualTo(7L)
      assert(planContainsScan[FlussCountScan](countStarDf))

      // COUNT(1) — Spark rewrites COUNT(<literal>) to CountStar, so it is pushable.
      val countOneDf = spark.sql(s"SELECT COUNT(1) FROM $DEFAULT_DATABASE.t")
      assertThat(countOneDf.collect().head.getLong(0)).isEqualTo(7L)
      assert(planContainsScan[FlussCountScan](countOneDf))

      // COUNT(non-null PK column) — pushable; PK columns are forced NOT NULL, so equals COUNT(*).
      val countPkDf = spark.sql(s"SELECT COUNT(orderId) FROM $DEFAULT_DATABASE.t")
      assertThat(countPkDf.collect().head.getLong(0)).isEqualTo(7L)
      assert(planContainsScan[FlussCountScan](countPkDf))

      // COUNT(nullable column) — NOT pushable; value (6) strictly smaller than COUNT(*) (7).
      val countNullableDf = spark.sql(s"SELECT COUNT(address) FROM $DEFAULT_DATABASE.t")
      assertThat(countNullableDf.collect().head.getLong(0)).isEqualTo(6L)
      assert(!planContainsScan[FlussCountScan](countNullableDf))

      // COUNT(DISTINCT ...) — NOT pushable.
      val countDistinctDf = spark.sql(s"SELECT COUNT(DISTINCT orderId) FROM $DEFAULT_DATABASE.t")
      assertThat(countDistinctDf.collect().head.getLong(0)).isEqualTo(7L)
      assert(!planContainsScan[FlussCountScan](countDistinctDf))

      // COUNT(*) with GROUP BY — NOT pushable.
      val groupedDf =
        spark.sql(s"SELECT itemId, COUNT(*) FROM $DEFAULT_DATABASE.t GROUP BY itemId")
      assertThat(groupedDf.count()).isEqualTo(7L)
      assert(!planContainsScan[FlussCountScan](groupedDf))
    }
  }

  private def planContainsScan[T <: FlussScan: scala.reflect.ClassTag](
                                                                        df: org.apache.spark.sql.DataFrame): Boolean = {
    val cls = implicitly[scala.reflect.ClassTag[T]].runtimeClass
    df.queryExecution.executedPlan.collectFirst {
      case b: BatchScanExec if cls.isInstance(b.scan) => b
    }.isDefined
  }

  test("Spark Read: partition pushdown — equality on partition key (PK table)") {
    withPkPartitionedTable {
      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE dt = '2026-01-01' ORDER BY orderId")
      checkAnswer(
        query,
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 22L, 602, "addr2", "2026-01-01") :: Nil)
      assert(partitionPredicate(query).isDefined)
    }
  }

  test("Spark Read: partition pushdown — IN on partition key (PK table)") {
    withPkPartitionedTable {
      val query = sql(s"""
                         |SELECT orderId, dt FROM $DEFAULT_DATABASE.t
                         |WHERE dt IN ('2026-01-01', '2026-01-03') ORDER BY orderId""".stripMargin)
      checkAnswer(
        query,
        Row(600L, "2026-01-01") ::
          Row(700L, "2026-01-01") ::
          Row(1000L, "2026-01-03") :: Nil)
      assert(partitionPredicate(query).isDefined)
    }
  }

  test("Spark Read: partition pushdown — non-matching partition prunes to empty (PK table)") {
    withPkPartitionedTable {
      val query = sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE dt = '2099-01-01'")
      checkAnswer(query, Nil)
      assert(partitionPredicate(query).isDefined)
    }
  }

  test("Spark Read: partition pushdown — predicate on non-partition column not extracted") {
    withPkPartitionedTable {
      val query = sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE amount = 603 ORDER BY orderId")
      checkAnswer(query, Row(800L, 23L, 603, "addr3", "2026-01-02") :: Nil)
      // amount is not a partition key — no partition predicate extracted.
      assert(partitionPredicate(query).isEmpty)
    }
  }

  test("Spark Read: scan description surfaces partition filter when pushed (PK table)") {
    withPkPartitionedTable {
      val withPart = sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE dt = '2026-01-01'")
      val noPart = sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE amount = 603")
      val withDesc = flussUpsertScan(withPart).get.description()
      val noDesc = flussUpsertScan(noPart).get.description()
      assert(withDesc.contains("[PartitionFilter:"))
      assert(withDesc.contains("dt"))
      assert(!noDesc.contains("PartitionFilter"))
    }
  }

  test("Spark Read: mixed partition and non-partition filter (PK table)") {
    withPkPartitionedTable {
      val query = sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE dt = '2026-01-01' AND amount > 601")
      checkAnswer(query, Row(700L, 22L, 602, "addr2", "2026-01-01") :: Nil)
      // Partition predicate extracted for partition pruning
      assert(partitionPredicate(query).isDefined)
      // Non-partition predicate (amount > 601) remains as a Filter node in the plan
      val executedPlan = query.queryExecution.executedPlan match {
        case aqe: AdaptiveSparkPlanExec => aqe.executedPlan
        case e: SparkPlan => e
      }
      assert(
        executedPlan.exists(_.isInstanceOf[FilterExec]),
        s"Expected Filter node in plan for non-partition predicate, got: $executedPlan")

      val numRowsRead = executedPlan
        .collectFirst { case b: BatchScanExec => b.metrics(FlussMetrics.NUM_ROWS_READ).value }
        .getOrElse(0L)
      assert(numRowsRead == 2L, s"Expected 2 rows read for single partition, got $numRowsRead")
    }
  }

  test("Spark Read: partition-only filter should not leave FilterExec in plan (PK table)") {
    withPkPartitionedTable {
      val query = sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE dt = '2026-01-01'")
      checkAnswer(
        query,
        Row(700L, 22L, 602, "addr2", "2026-01-01") :: Row(
          600L,
          21L,
          601,
          "addr1",
          "2026-01-01") :: Nil)
      // Partition predicate extracted for partition pruning
      assert(partitionPredicate(query).isDefined)
      // No FilterExec should remain since all predicates are partition predicates
      val executedPlan = query.queryExecution.executedPlan match {
        case aqe: AdaptiveSparkPlanExec => aqe.executedPlan
        case e: SparkPlan => e
      }
      assert(
        !executedPlan.exists(_.isInstanceOf[FilterExec]),
        s"Expected no Filter node in plan for partition-only predicate, got: $executedPlan")

      val numRowsRead = executedPlan
        .collectFirst { case b: BatchScanExec => b.metrics(FlussMetrics.NUM_ROWS_READ).value }
        .getOrElse(0L)
      assert(numRowsRead == 2L, s"Expected 2 rows read for single partition, got $numRowsRead")
    }
  }

  private def withPkPartitionedTable(body: => Unit): Unit = withTable("t") {
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.t (
           |  orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING
           |)
           |PARTITIONED BY (dt)
           |TBLPROPERTIES("primary.key" = "orderId,dt", "bucket.num" = 1)""".stripMargin)
    sql(s"""
           |INSERT INTO $DEFAULT_DATABASE.t VALUES
           |(600L, 21L, 601, "addr1", "2026-01-01"), (700L, 22L, 602, "addr2", "2026-01-01"),
           |(800L, 23L, 603, "addr3", "2026-01-02"), (900L, 24L, 604, "addr4", "2026-01-02"),
           |(1000L, 25L, 605, "addr5", "2026-01-03")
           |""".stripMargin)
    body
  }

  test("Spark Read: primary key table limit pushdown") {
    withPkPartitionedTable {
      val dfNoLimit = sql(s"SELECT * FROM $DEFAULT_DATABASE.t")
      assert(flussUpsertScan(dfNoLimit).flatMap(_.limit).isEmpty)

      val dfLimit = sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE dt = '2026-01-01' LIMIT 1")
      assert(flussUpsertScan(dfLimit).flatMap(_.limit).contains(1))

      // Verify limit pushdown actually reduces rows read via metrics
      dfLimit.collect()
      val batchScanExec = dfLimit.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val numRowsRead = batchScanExec.metrics(FlussMetrics.NUM_ROWS_READ).value
      assert(numRowsRead == 1L, s"Expected 1 rows read with limit pushdown, got $numRowsRead")
    }
  }

  private def partitionPredicate(df: DataFrame): Option[org.apache.fluss.predicate.Predicate] = {
    flussUpsertScan(df).flatMap(_.partitionPredicate)
  }

  private def flussUpsertScan(df: DataFrame): Option[FlussUpsertScan] = {
    // AQE hides the scan under an adaptive wrapper in executedPlan, so check optimizedPlan too.
    val scans =
      df.queryExecution.executedPlan.collect {
        case b: BatchScanExec => b.scan
      } ++ df.queryExecution.optimizedPlan.collect {
        case DataSourceV2ScanRelation(_, scan, _, _, _) => scan
      }
    scans.collectFirst { case f: FlussUpsertScan => f }
  }
}
