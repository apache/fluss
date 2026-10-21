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

import org.apache.fluss.client.admin.OffsetSpec
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.metadata.{DataLakeFormat, TableBucket}
import org.apache.fluss.record.ChangeType
import org.apache.fluss.row.GenericRow
import org.apache.fluss.spark.SparkConnectorOptions.{BUCKET_NUMBER, PRIMARY_KEY}
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.spark.read.lake.FlussLakeUpsertInputPartition

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.assertj.core.api.Assertions.assertThat

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.time.Duration

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Exercises the SQL entry point against real Paimon bases and Fluss commit timestamps. */
@SparkLakeTest
class SparkSnapshotAtTimestampTvfTest extends SparkLakeTableReadTestBase {
  private val TVF = "fluss_snapshot_at_timestamp"
  private val key = SparkFlussConf.SCAN_AS_OF_TIMESTAMP.key()

  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.PAIMON

  override protected def flussConf: Configuration = {
    val config = super.flussConf
    config.setString("datalake.format", "paimon")
    config.setString("datalake.paimon.metastore", "filesystem")
    config.setString("datalake.paimon.cache-enabled", "false")
    warehousePath = Files.createTempDirectory("snapshot-tvf-paimon").resolve("warehouse").toString
    config.setString("datalake.paimon.warehouse", warehousePath)
    config
  }

  override protected def lakeCatalogConf: Configuration = {
    val config = new Configuration()
    config.setString("metastore", "filesystem")
    config.setString("warehouse", warehousePath)
    config
  }

  private def create(name: String, primaryKey: Boolean): Unit = {
    val pk = if (primaryKey) s", '${PRIMARY_KEY.key()}'='id'" else ""
    sql(s"""CREATE TABLE $DEFAULT_DATABASE.$name (id INT, v INT)
           | TBLPROPERTIES ('${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}'='true',
           | '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}'='1s',
           | '${BUCKET_NUMBER.key()}'='1' $pk)""".stripMargin)
  }

  private def latest(name: String): Map[TableBucket, Long] = {
    val path = createTablePath(name)
    val id = admin.getTableInfo(path).get().getTableId
    admin
      .listOffsets(path, Seq(Int.box(0)).asJava, new OffsetSpec.LatestSpec())
      .all()
      .get()
      .asScala
      .map { case (bucket, offset) => new TableBucket(id, bucket) -> offset.longValue() }
      .toMap
  }

  private def logs(name: String): Seq[(Long, Long, ChangeType, Int, Int)] = {
    val table = loadFlussTable(createTablePath(name))
    val scanner = table.newScan().createLogScanner()
    try {
      val stop = latest(name).values.head
      scanner.subscribe(0, 0L)
      val result = mutable.ArrayBuffer.empty[(Long, Long, ChangeType, Int, Int)]
      var consumed = 0L
      val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos
      while (consumed < stop && System.nanoTime() < deadline) {
        val batch = scanner.poll(Duration.ofSeconds(1))
        batch.iterator().asScala.foreach {
          r =>
            result += ((
              r.logOffset(),
              r.timestamp(),
              r.getChangeType,
              r.getRow.getInt(0),
              r.getRow.getInt(1)))
            consumed = r.logOffset() + 1
        }
        Option(batch.consumedUpToOffset(new TableBucket(table.getTableInfo.getTableId, 0)))
          .foreach(offset => consumed = math.max(consumed, offset.longValue()))
      }
      assertThat(Long.box(consumed)).isGreaterThanOrEqualTo(Long.box(stop))
      result.toSeq
    } finally {
      scanner.close()
      table.close()
    }
  }

  test("SQL snapshot restores an older base plus exact bounded upserts and deletes") {
    withTable("snapshot_pk") {
      val name = "snapshot_pk"
      create(name, primaryKey = true)
      val table = loadFlussTable(createTablePath(name))
      val writer = table.newUpsert().createWriter()
      try {
        Seq((1, 10), (2, 10), (3, 10), (9, 99)).foreach {
          case (id, value) => writer.upsert(GenericRow.of(Int.box(id), Int.box(value))).get()
        }
        writer.flush()
        tierToLake(name, latest(name))
        val base = admin.getReadableLakeSnapshot(createTablePath(name)).get()
        val baseOffsetsPath = Paths.get(
          flussServer.getZooKeeperClient
            .getLakeTable(table.getTableInfo.getTableId)
            .get()
            .getLatestLakeSnapshotMetadata
            .getTieredOffsetsFilePath
            .toUri)
        val retainedOffsetBytes = Files.readAllBytes(baseOffsetsPath)
        writer.upsert(GenericRow.of(Int.box(1), Int.box(20))).get()
        writer.delete(GenericRow.of(Int.box(2), null)).get()
        writer.upsert(GenericRow.of(Int.box(4), Int.box(20))).get()
        writer.flush()
        val beforeMarker = logs(name).map(_._2).max
        while (System.currentTimeMillis() <= beforeMarker) {
          Thread.`yield`()
        }
        writer.upsert(GenericRow.of(Int.box(1), Int.box(30))).get()
        writer.flush()
        val timestamp = logs(name)
          .filter(
            r =>
              r._4 == 1 && r._5 == 30 &&
                r._3 != ChangeType.UPDATE_BEFORE)
          .map(_._2)
          .max
        writer.upsert(GenericRow.of(Int.box(4), Int.box(40))).get()
        writer.upsert(GenericRow.of(Int.box(2), Int.box(50))).get()
        writer.flush()
        tierToLake(name, latest(name))
        assertThat(
          Long.box(admin.getReadableLakeSnapshot(createTablePath(name)).get().getSnapshotId))
          .isGreaterThan(Long.box(base.getSnapshotId))

        // Ordinary non-DV tiering currently discards older offsets even while Paimon retains
        // the snapshots. Exercise that real failure before explicitly modeling retained metadata.
        assertThat(Files.exists(baseOffsetsPath)).isFalse()
        val missing = intercept[IOException] {
          sql(s"SELECT * FROM $TVF('$name', $timestamp)").collect()
        }
        assertThat(missing.getMessage)
          .contains(
            "Missing fluss-offsets file",
            s"snapshotId=${base.getSnapshotId}",
            baseOffsetsPath.toString)
        // Fixture only: restore the exact original offset file, representing an independently
        // retained complete base. Production code never recreates or guesses deleted offsets.
        Files.write(baseOffsetsPath, retainedOffsetBytes)

        // Independent reference state: apply actual records strictly before the boundary.
        val reference = mutable.Map.empty[Int, Int]
        logs(name).filter(_._2 < timestamp).sortBy(_._1).foreach {
          case (_, _, change, id, value) =>
            if (change == ChangeType.DELETE || change == ChangeType.UPDATE_BEFORE)
              reference.remove(id)
            else reference(id) = value
        }
        assertThat(Int.box(reference(1))).isEqualTo(Int.box(20))
        assertThat(reference.contains(2)).isFalse()
        assertThat(Int.box(reference(4))).isEqualTo(Int.box(20))
        assertThat(Int.box(reference(9)))
          .isEqualTo(Int.box(99)) // unchanged key exists only in the base
        val from = s"$TVF('$DEFAULT_DATABASE.$name', $timestamp)"
        val query = sql(s"SELECT * FROM $from")
        val partitions = lakeInputPartitions(query).collect {
          case p: FlussLakeUpsertInputPartition => p
        }.distinct
        assertThat(Int.box(partitions.length)).isEqualTo(Int.box(1))
        assertThat(Long.box(partitions.head.logStartingOffset))
          .isEqualTo(base.getTableBucketsOffset.values().iterator().next())
        checkAnswer(query, reference.toSeq.map { case (id, value) => Row(id, value) })
        checkAnswer(
          sql(s"SELECT v FROM $from WHERE v >= 20 ORDER BY v LIMIT 2"),
          reference.values.filter(_ >= 20).toSeq.sorted.take(2).map(Row(_)))
        checkAnswer(
          sql(s"SELECT id FROM $from WHERE v = 10"),
          reference.toSeq.filter(_._2 == 10).map(r => Row(r._1)))
        checkAnswer(sql(s"SELECT count(*) FROM $from"), Seq(Row(reference.size.toLong)))
        assertThat(pushedPredicates(query)).isEmpty()
        checkAnswer(
          sql(s"SELECT * FROM $TVF('$name', now())"),
          Seq(Row(1, 30), Row(2, 50), Row(3, 10), Row(4, 40), Row(9, 99)))

        // Repeated planning keeps the same S/L/B even if a newer base becomes available.
        val again = lakeInputPartitions(query).collect {
          case p: FlussLakeUpsertInputPartition => p
        }.distinct
        assertThat(Long.box(again.head.logStoppingOffset))
          .isEqualTo(Long.box(partitions.head.logStoppingOffset))
      } finally {
        table.close()
      }
    }
  }

  test("SQL snapshot of append data preserves duplicates and excludes equal-T records") {
    withTable("snapshot_log") {
      val name = "snapshot_log"
      create(name, primaryKey = false)
      sql(s"INSERT INTO $DEFAULT_DATABASE.$name VALUES (1, 10), (1, 10), (2, 20)")
      // A filtered first fetch can carry only consumed-offset progress.
      checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.$name WHERE id = 999"), Nil)
      tierToLake(name, latest(name))
      sql(s"INSERT INTO $DEFAULT_DATABASE.$name VALUES (3, 30), (3, 30)")
      // The lake contributes duplicates while the filtered tail contributes progress only.
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.$name WHERE id = 1"),
        Seq(Row(1, 10), Row(1, 10)))
      val previous = logs(name).map(_._2).max
      while (System.currentTimeMillis() <= previous) {
        Thread.`yield`()
      }
      sql(s"INSERT INTO $DEFAULT_DATABASE.$name VALUES (4, 40)")
      val timestamp = logs(name).filter(_._4 == 4).map(_._2).max
      val reference = logs(name).filter(_._2 < timestamp).map(r => Row(r._4, r._5))
      assertThat(Int.box(reference.size)).isEqualTo(Int.box(5))
      checkAnswer(sql(s"SELECT * FROM $TVF('$name', $timestamp)"), reference)
    }
  }

  test("WAL partial updates replay complete after-images including untouched value columns") {
    withTable("snapshot_wal") {
      val name = "snapshot_wal"
      sql(s"""CREATE TABLE $DEFAULT_DATABASE.$name (id INT, v INT, other INT)
             | TBLPROPERTIES ('${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}'='true',
             | '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}'='1s',
             | '${ConfigOptions.TABLE_CHANGELOG_IMAGE.key()}'='WAL',
             | '${BUCKET_NUMBER.key()}'='1', '${PRIMARY_KEY.key()}'='id')""".stripMargin)
      sql(s"INSERT INTO $DEFAULT_DATABASE.$name VALUES (1, 10, 100), (2, 20, 200)")
      tierToLake(name, latest(name))
      val table = loadFlussTable(createTablePath(name))
      try {
        val writer = table.newUpsert().partialUpdate("id", "v").createWriter()
        writer.upsert(GenericRow.of(Int.box(1), Int.box(11), null)).get()
        writer.flush()
        val previous = logs(name).map(_._2).max
        while (System.currentTimeMillis() <= previous) {
          Thread.`yield`()
        }
        writer.upsert(GenericRow.of(Int.box(1), Int.box(12), null)).get()
        writer.flush()
        val boundary = logs(name).filter(_._5 == 12).map(_._2).max
        checkAnswer(
          sql(s"SELECT * FROM $TVF('$name', $boundary)"),
          Seq(Row(1, 11, 100), Row(2, 20, 200)))
      } finally {
        table.close()
      }
    }
  }

  test("SQL arguments normalize once and now uses the same clock as future validation") {
    withTable("snapshot_args") {
      create("snapshot_args", primaryKey = true)
      def option(expr: String): String =
        sql(s"SELECT * FROM $TVF('snapshot_args', $expr)").queryExecution.analyzed.collectFirst {
          case r: DataSourceV2Relation => r.options.get(key)
        }.get
      for (expr <- Seq("now()", "current_timestamp()", "now() - INTERVAL 1 HOUR")) {
        val before = System.currentTimeMillis()
        val value = option(expr).toLong
        assertThat(Long.box(value)).isLessThanOrEqualTo(Long.box(System.currentTimeMillis()))
        if (!expr.contains("HOUR"))
          assertThat(Long.box(value)).isGreaterThanOrEqualTo(Long.box(before))
      }
      assertThat(option("TIMESTAMP '2026-01-01 00:00:00'"))
        .isEqualTo(option("DATE '2026-01-01'"))
      assertThat(option("TIMESTAMP_NTZ '2026-01-01 00:00:00'"))
        .isEqualTo(option("'2026-01-01 00:00:00'"))
      for (invalid <- Seq("NULL", "true", "1.2", "''", "'bad'", "now() + INTERVAL 1 MILLISECOND")) {
        intercept[Exception](option(invalid))
      }
      Seq("", "'snapshot_args'", "'snapshot_args', 0, 1").foreach {
        args => intercept[Exception](sql(s"SELECT * FROM $TVF($args)").queryExecution.analyzed)
      }
      val pinned = sql(s"SELECT * FROM $TVF('snapshot_args', '2026-01-01 00:00:00')")
      val original = pinned.queryExecution.analyzed.collectFirst {
        case r: DataSourceV2Relation => r.options.get(key)
      }.get
      withSQLConf("spark.sql.session.timeZone" -> "America/Los_Angeles") {
        assertThat(pinned.queryExecution.analyzed.collectFirst {
          case r: DataSourceV2Relation => r.options.get(key)
        }.get).isEqualTo(original)
      }
      intercept[Exception] {
        sql(s"SELECT * FROM $TVF('snapshot_args', 0)").collect()
      } // no log-only fallback in the absence of a retained base
    }
  }
}
