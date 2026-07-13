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
import org.apache.fluss.metadata.{DataLakeFormat, PartitionSpec, TableChange}
import org.apache.fluss.spark.SparkConnectorOptions.{BUCKET_KEY, BUCKET_NUMBER, PRIMARY_KEY}

import org.apache.spark.sql.Row

import java.nio.file.Files
import java.util.Collections

import scala.jdk.CollectionConverters._

/**
 * Tests union read on partitioned lake tables whose partitions carry different bucket counts after
 * an ALTER TABLE ... SET ('bucket.num' = N): the partition created before the ALTER keeps its
 * original bucket count while the partition created afterwards uses the new one, so the read path
 * must enumerate buckets per partition instead of using the table-level bucket count.
 */
abstract class SparkLakeRescaleBucketReadTest extends SparkLakeTableReadTestBase {

  private val OLD_BUCKET_NUM = 2
  private val NEW_BUCKET_NUM = 4

  private def alterBucketNum(tableName: String, newBucketNum: Int): Unit = {
    admin
      .alterTable(
        createTablePath(tableName),
        Collections.singletonList(TableChange.set("bucket.num", newBucketNum.toString)),
        false)
      .get()
  }

  private def assertPartitionBucketCounts(tableName: String, expected: Map[String, Int]): Unit = {
    val partitionInfos = admin.listPartitionInfos(createTablePath(tableName)).get().asScala
    val actual = partitionInfos.map(p => p.getPartitionName -> p.getBucketCount.intValue()).toMap
    assert(actual == expected, s"Expected partition bucket counts $expected, got $actual")
  }

  test("Spark Lake Read: log table union read across partitions with different bucket counts") {
    withTable("t_rescale_log") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_rescale_log (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = $OLD_BUCKET_NUM)
             |""".stripMargin)

      // partition "old" is created before the ALTER and keeps OLD_BUCKET_NUM buckets
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_log VALUES
             |(1, 'alpha', 'old'), (2, 'beta', 'old'), (3, 'gamma', 'old'), (4, 'delta', 'old')
             |""".stripMargin)

      alterBucketNum("t_rescale_log", NEW_BUCKET_NUM)

      // partition "new" is created after the ALTER and uses NEW_BUCKET_NUM buckets
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_log VALUES
             |(5, 'epsilon', 'new'), (6, 'zeta', 'new'), (7, 'eta', 'new'), (8, 'theta', 'new')
             |""".stripMargin)

      assertPartitionBucketCounts(
        "t_rescale_log",
        Map("old" -> OLD_BUCKET_NUM, "new" -> NEW_BUCKET_NUM))

      tierToLake("t_rescale_log")

      // lake-only read enumerates each partition by its own bucket count
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_log ORDER BY id"),
        Row(1, "alpha", "old") :: Row(2, "beta", "old") ::
          Row(3, "gamma", "old") :: Row(4, "delta", "old") ::
          Row(5, "epsilon", "new") :: Row(6, "zeta", "new") ::
          Row(7, "eta", "new") :: Row(8, "theta", "new") :: Nil
      )

      // more rows after tiering, so the planner mixes lake splits and fluss log tail
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_log VALUES
             |(9, 'iota', 'old'), (10, 'kappa', 'new')
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_log ORDER BY id"),
        Row(1, "alpha", "old") :: Row(2, "beta", "old") ::
          Row(3, "gamma", "old") :: Row(4, "delta", "old") ::
          Row(5, "epsilon", "new") :: Row(6, "zeta", "new") ::
          Row(7, "eta", "new") :: Row(8, "theta", "new") ::
          Row(9, "iota", "old") :: Row(10, "kappa", "new") :: Nil
      )

      // partition filter on the partition keeping the original bucket count
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_log WHERE dt = 'old' ORDER BY id"),
        Row(1, "alpha", "old") :: Row(2, "beta", "old") ::
          Row(3, "gamma", "old") :: Row(4, "delta", "old") ::
          Row(9, "iota", "old") :: Nil
      )
    }
  }

  test("Spark Lake Read: pk table union read across partitions with different bucket counts") {
    withTable("t_rescale_pk") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_rescale_pk (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${PRIMARY_KEY.key()}' = 'id,dt',
             |  '${BUCKET_NUMBER.key()}' = $OLD_BUCKET_NUM)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_pk VALUES
             |(1, 'alice', 'old'), (2, 'bob', 'old'), (3, 'charlie', 'old')
             |""".stripMargin)

      alterBucketNum("t_rescale_pk", NEW_BUCKET_NUM)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_pk VALUES
             |(4, 'david', 'new'), (5, 'erin', 'new'), (6, 'frank', 'new')
             |""".stripMargin)

      assertPartitionBucketCounts(
        "t_rescale_pk",
        Map("old" -> OLD_BUCKET_NUM, "new" -> NEW_BUCKET_NUM))

      tierToLake("t_rescale_pk")

      // lake-only read enumerates each partition by its own bucket count
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_pk ORDER BY id"),
        Row(1, "alice", "old") :: Row(2, "bob", "old") :: Row(3, "charlie", "old") ::
          Row(4, "david", "new") :: Row(5, "erin", "new") :: Row(6, "frank", "new") :: Nil
      )

      // updates and new keys after tiering, so the read merges lake snapshot and log tail
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_pk VALUES
             |(2, 'bob_updated', 'old'), (7, 'grace', 'new')
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_pk ORDER BY id"),
        Row(1, "alice", "old") :: Row(2, "bob_updated", "old") :: Row(3, "charlie", "old") ::
          Row(4, "david", "new") :: Row(5, "erin", "new") :: Row(6, "frank", "new") ::
          Row(7, "grace", "new") :: Nil
      )

      // partition filter on the partition keeping the original bucket count
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_pk WHERE dt = 'old' ORDER BY id"),
        Row(1, "alice", "old") :: Row(2, "bob_updated", "old") :: Row(3, "charlie", "old") :: Nil
      )
    }
  }

  test("Spark Lake Read: unaware-bucket log table rescale does not propagate to Paimon") {
    withTable("t_rescale_unaware") {
      // no BUCKET_KEY -> Unaware Bucket table (Paimon BUCKET = -1). ALTER bucket.num rescales the
      // Fluss side per partition but must be skipped for the Paimon schema.
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_rescale_unaware (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = $OLD_BUCKET_NUM)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_unaware VALUES
             |(1, 'alpha', 'old'), (2, 'beta', 'old'), (3, 'gamma', 'old'), (4, 'delta', 'old')
             |""".stripMargin)

      alterBucketNum("t_rescale_unaware", NEW_BUCKET_NUM)

      // Server-side "propagation is skipped for unaware bucket tables" is covered by
      // AlterBucketNumTest#testAlterBucketNumSkipsLakePropagationForUnawareBucketTable; this
      // test focuses on the Spark read semantics across the rescale.
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_unaware VALUES
             |(5, 'epsilon', 'new'), (6, 'zeta', 'new'), (7, 'eta', 'new'), (8, 'theta', 'new')
             |""".stripMargin)

      // Fluss still rescales per partition even for unaware bucket tables
      assertPartitionBucketCounts(
        "t_rescale_unaware",
        Map("old" -> OLD_BUCKET_NUM, "new" -> NEW_BUCKET_NUM))

      tierToLake("t_rescale_unaware")

      // union read returns all data regardless of the Fluss-side rescale
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_unaware ORDER BY id"),
        Row(1, "alpha", "old") :: Row(2, "beta", "old") ::
          Row(3, "gamma", "old") :: Row(4, "delta", "old") ::
          Row(5, "epsilon", "new") :: Row(6, "zeta", "new") ::
          Row(7, "eta", "new") :: Row(8, "theta", "new") :: Nil
      )

      // more rows after tiering to force a lake + fluss log merge
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_unaware VALUES
             |(9, 'iota', 'old'), (10, 'kappa', 'new')
             |""".stripMargin)
      checkAnswer(
        sql(s"SELECT id FROM $DEFAULT_DATABASE.t_rescale_unaware ORDER BY id"),
        (1 to 10).map(Row(_)).toList)
    }
  }

  test("Spark Lake Read: lake-only partition dropped from Fluss after rescale") {
    withTable("t_rescale_lake_only") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_rescale_lake_only (id INT, name STRING, dt STRING)
             | PARTITIONED BY (dt)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = $OLD_BUCKET_NUM)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_lake_only VALUES
             |(1, 'alpha', 'old'), (2, 'beta', 'old'), (3, 'gamma', 'old'), (4, 'delta', 'old')
             |""".stripMargin)

      alterBucketNum("t_rescale_lake_only", NEW_BUCKET_NUM)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_rescale_lake_only VALUES
             |(5, 'epsilon', 'new'), (6, 'zeta', 'new'), (7, 'eta', 'new'), (8, 'theta', 'new')
             |""".stripMargin)

      assertPartitionBucketCounts(
        "t_rescale_lake_only",
        Map("old" -> OLD_BUCKET_NUM, "new" -> NEW_BUCKET_NUM))

      tierToLake("t_rescale_lake_only")

      // Drop the rescaled "old" partition from Fluss: its data (stamped with OLD_BUCKET_NUM)
      // now survives only in the lake, so the planner must serve it from the lake-only branch
      // that enumerates the ACTUAL lake bucket ids instead of any Fluss-side bucket count.
      val tablePath = createTablePath("t_rescale_lake_only")
      admin
        .dropPartition(tablePath, new PartitionSpec(Collections.singletonMap("dt", "old")), false)
        .get()
      val deadline = System.currentTimeMillis() + 60000
      while (
        admin.listPartitionInfos(tablePath).get().size() > 1
        && System.currentTimeMillis() < deadline
      ) {
        Thread.sleep(500)
      }
      assert(
        admin.listPartitionInfos(tablePath).get().size() == 1,
        "old partition was not dropped from Fluss in time")

      // union read: the dropped partition comes entirely from the lake, the surviving
      // partition from lake + fluss
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_lake_only ORDER BY id"),
        Row(1, "alpha", "old") :: Row(2, "beta", "old") ::
          Row(3, "gamma", "old") :: Row(4, "delta", "old") ::
          Row(5, "epsilon", "new") :: Row(6, "zeta", "new") ::
          Row(7, "eta", "new") :: Row(8, "theta", "new") :: Nil
      )

      // partition filter on the lake-only partition still returns its data
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_rescale_lake_only WHERE dt = 'old' ORDER BY id"),
        Row(1, "alpha", "old") :: Row(2, "beta", "old") ::
          Row(3, "gamma", "old") :: Row(4, "delta", "old") :: Nil
      )
    }
  }
}

class SparkLakePaimonRescaleBucketReadTest extends SparkLakeRescaleBucketReadTest {
  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.PAIMON

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.PAIMON.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    warehousePath =
      Files.createTempDirectory("fluss-testing-rescale-lake-read").resolve("warehouse").toString
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
