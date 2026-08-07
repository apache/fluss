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

import org.apache.fluss.client.table.Table
import org.apache.fluss.row.{BinaryString, GenericRow}

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter

/**
 * Verifies the `fluss_incremental_between_timestamp` table-valued function. The window is
 * left-closed, right-open `[start, end)` on the record commit timestamp, and the function's options
 * are scoped to the single query.
 */
class SparkTimeRangeTvfTest extends FlussSparkTestBase {

  private val TVF = "fluss_incremental_between_timestamp"

  private def createLogTable(name: String): Unit =
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.$name
           |(orderId BIGINT, itemId BIGINT, amount INT, address STRING)
           |""".stripMargin)

  /** Truncates to a whole second so a millis value, a datetime string and a TIMESTAMP agree. */
  private def secondAligned(ms: Long): Long = (ms / 1000L) * 1000L

  private def waitPast(ms: Long): Unit = {
    while (System.currentTimeMillis() <= ms) {
      Thread.sleep(20)
    }
  }

  private def formatTs(ms: Long): String =
    Instant
      .ofEpochMilli(ms)
      .atZone(ZoneId.of(spark.sessionState.conf.sessionLocalTimeZone))
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

  private def fullMessage(t: Throwable): String = {
    val sw = new java.io.StringWriter()
    t.printStackTrace(new java.io.PrintWriter(sw))
    sw.toString
  }

  test("TVF: log table window [t1, t2)") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1L, 11L, 101, "a1"), (2L, 12L, 102, "a2")""".stripMargin)
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(3L, 13L, 103, "a3"), (4L, 14L, 104, "a4")""".stripMargin)
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (5L, 15L, 105, "a5")""")
      Thread.sleep(200)

      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2') ORDER BY orderId"),
        Row(3L, 13L, 103, "a3") :: Row(4L, 14L, 104, "a4") :: Nil)

      // projection and filter still work on top of the TVF relation
      checkAnswer(
        sql(s"""SELECT address FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2')
               |WHERE amount = 104""".stripMargin),
        Row("a4") :: Nil)
    }
  }

  test("TVF: two-argument form reads up to the latest data") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (1L, 11L, 101, "a1")""")
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (2L, 12L, 102, "a2")""")
      Thread.sleep(300)
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (3L, 13L, 103, "a3")""")
      Thread.sleep(200)

      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1') ORDER BY orderId"),
        Row(2L, 12L, 102, "a2") :: Row(3L, 13L, 103, "a3") :: Nil)
    }
  }

  private def createPkTable(name: String): Unit =
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.$name
           |(orderId BIGINT, itemId BIGINT, amount INT, address STRING)
           |TBLPROPERTIES("primary.key" = "orderId", "bucket.num" = 1)
           |""".stripMargin)

  private def createPartitionedPkTable(name: String): Unit =
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.$name
           |(orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING)
           |PARTITIONED BY (dt)
           |TBLPROPERTIES("primary.key" = "orderId,dt", "bucket.num" = 1)
           |""".stripMargin)

  test("TVF: primary key table folds to +I/+U and excludes deletes") {
    withTable("t") {
      val tablePath = createTablePath("t")
      createPkTable("t")

      val writer = loadFlussTable(tablePath).newUpsert().createWriter()
      writer.upsert(row(1L, 11L, 101, "a1")).get()
      writer.upsert(row(2L, 12L, 102, "a2")).get()
      writer.upsert(row(3L, 13L, 103, "a3")).get()
      writer.flush()
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      // in-window: update key 2, insert key 4, delete key 1
      writer.upsert(row(2L, 120L, 1002, "a2_upd")).get()
      writer.upsert(row(4L, 14L, 104, "a4")).get()
      writer.delete(deleteKey(1L)).get()
      writer.flush()
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)

      // after the window
      writer.upsert(row(5L, 15L, 105, "a5")).get()
      writer.flush()
      Thread.sleep(200)

      val table = loadFlussTable(tablePath)
      // evidence: the window changelog really contains -U/+U (key 2), +I (key 4), -D (key 1)
      val changes = changelogInWindow(table, t1, t2)
      assertThat(changes.filter(_._1 == "-U").map(_._2)).isEqualTo(Seq(2L))
      assertThat(changes.filter(_._1 == "+U").map(_._2)).isEqualTo(Seq(2L))
      assertThat(changes.filter(_._1 == "+I").map(_._2)).isEqualTo(Seq(4L))
      assertThat(changes.filter(_._1 == "-D").map(_._2)).isEqualTo(Seq(1L))

      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2') ORDER BY orderId"),
        Row(2L, 120L, 1002, "a2_upd") :: Row(4L, 14L, 104, "a4") :: Nil)
    }
  }

  test("TVF: primary key table collapses repeated -U/+U updates into the latest value") {
    withTable("t") {
      val tablePath = createTablePath("t")
      createPkTable("t")

      val writer = loadFlussTable(tablePath).newUpsert().createWriter()
      // before the window: keys 1-3 inserted
      writer.upsert(row(1L, 11L, 101, "a1")).get()
      writer.upsert(row(2L, 12L, 102, "a2")).get()
      writer.upsert(row(3L, 13L, 103, "a3")).get()
      writer.flush()
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      // in-window: -U/+U twice on key 1, -U/+U once on key 2, key 3 untouched
      writer.upsert(row(1L, 110L, 1001, "a1_v2")).get()
      writer.upsert(row(1L, 111L, 1002, "a1_v3")).get()
      writer.upsert(row(2L, 120L, 2001, "a2_v2")).get()
      writer.flush()
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)

      // after the window
      writer.upsert(row(1L, 112L, 1003, "a1_v4")).get()
      writer.flush()
      Thread.sleep(200)

      // evidence: three genuine -U/+U pairs exist in the window changelog (two for key 1, one
      // for key 2), so the folding assertions below operate on real -U/+U records
      val changes = changelogInWindow(loadFlussTable(tablePath), t1, t2)
      assertThat(changes.filter(_._1 == "-U").map(_._2)).isEqualTo(Seq(1L, 1L, 2L))
      assertThat(changes.filter(_._1 == "+U").map(_._2)).isEqualTo(Seq(1L, 1L, 2L))

      // each updated key appears exactly once, with its last in-window value
      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2') ORDER BY orderId"),
        Row(1L, 111L, 1002, "a1_v3") :: Row(2L, 120L, 2001, "a2_v2") :: Nil)

      // the two-argument form reads through to the latest state
      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1') ORDER BY orderId"),
        Row(1L, 112L, 1003, "a1_v4") :: Row(2L, 120L, 2001, "a2_v2") :: Nil)
    }
  }

  test("TVF: primary key table cancels out +I followed by -D in the window") {
    withTable("t") {
      val tablePath = createTablePath("t")
      createPkTable("t")

      val writer = loadFlussTable(tablePath).newUpsert().createWriter()
      // before the window
      writer.upsert(row(1L, 11L, 101, "a1")).get()
      writer.flush()
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      // in-window: insert key 2 then delete it again (cancels out), insert key 3 (survives)
      writer.upsert(row(2L, 12L, 102, "a2")).get()
      writer.delete(deleteKey(2L)).get()
      writer.upsert(row(3L, 13L, 103, "a3")).get()
      writer.flush()
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)

      // evidence: the window changelog holds +I then -D for key 2, and +I for key 3
      val changes = changelogInWindow(loadFlussTable(tablePath), t1, t2)
      assertThat(changes.filter(r => r._1 == "+I" || r._1 == "-D"))
        .isEqualTo(Seq(("+I", 2L), ("-D", 2L), ("+I", 3L)))

      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2') ORDER BY orderId"),
        Row(3L, 13L, 103, "a3") :: Nil)
    }
  }

  test("TVF: primary key table keeps a key deleted then re-inserted in the window") {
    withTable("t") {
      val tablePath = createTablePath("t")
      createPkTable("t")

      val writer = loadFlussTable(tablePath).newUpsert().createWriter()
      // before the window
      writer.upsert(row(1L, 11L, 101, "a1")).get()
      writer.upsert(row(2L, 12L, 102, "a2")).get()
      writer.flush()
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      // in-window: delete key 1 then re-insert it with new values (-D then +I survives),
      // delete key 2 permanently (-D only, excluded)
      writer.delete(deleteKey(1L)).get()
      writer.upsert(row(1L, 110L, 1001, "a1_new")).get()
      writer.delete(deleteKey(2L)).get()
      writer.flush()
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)

      // evidence: the window changelog holds -D then +I for key 1, and -D for key 2
      assertThat(changelogInWindow(loadFlussTable(tablePath), t1, t2))
        .isEqualTo(Seq(("-D", 1L), ("+I", 1L), ("-D", 2L)))

      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2') ORDER BY orderId"),
        Row(1L, 110L, 1001, "a1_new") :: Nil)
    }
  }

  test("TVF: primary key table window containing only -D returns nothing") {
    withTable("t") {
      val tablePath = createTablePath("t")
      createPkTable("t")

      val writer = loadFlussTable(tablePath).newUpsert().createWriter()
      writer.upsert(row(1L, 11L, 101, "a1")).get()
      writer.upsert(row(2L, 12L, 102, "a2")).get()
      writer.flush()
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      writer.delete(deleteKey(1L)).get()
      writer.delete(deleteKey(2L)).get()
      writer.flush()
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)

      // evidence: the window changelog holds exactly two -D records, so the empty result below
      // reflects genuine delete folding rather than an empty window
      val changes = changelogInWindow(loadFlussTable(tablePath), t1, t2)
      assertThat(changes.map(_._1)).isEqualTo(Seq("-D", "-D"))
      assertThat(changes.map(_._2)).isEqualTo(Seq(1L, 2L))

      checkAnswer(sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2')"), Nil)
    }
  }

  test("TVF: partitioned primary key table folds changes across partitions") {
    withTable("t_pk_part") {
      val tablePath = createTablePath("t_pk_part")
      createPartitionedPkTable("t_pk_part")

      val writer = loadFlussTable(tablePath).newUpsert().createWriter()
      // before the window: one row per partition
      writer.upsert(pkRow(1L, 11L, 101, "a1", "2026-01-01")).get()
      writer.upsert(pkRow(2L, 12L, 102, "a2", "2026-01-02")).get()
      writer.flush()
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      // in-window: update key 1 (partition 1), permanent delete of key 2 (partition 2),
      // insert key 3 then delete it again in partition 1 (cancels out)
      writer.upsert(pkRow(1L, 110L, 1001, "a1_upd", "2026-01-01")).get()
      writer.delete(deleteKey(2L, "2026-01-02")).get()
      writer.upsert(pkRow(3L, 13L, 103, "a3", "2026-01-01")).get()
      writer.delete(deleteKey(3L, "2026-01-01")).get()
      writer.flush()
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)

      // after the window
      writer.upsert(pkRow(4L, 14L, 104, "a4", "2026-01-01")).get()
      writer.flush()
      Thread.sleep(200)

      // evidence: the window changelog holds -U/+U (key 1), -D (key 2), +I then -D (key 3);
      // the -D comparison sorts first because poll order across partitions is not deterministic
      val changes = changelogInWindow(loadFlussTable(tablePath), t1, t2)
      assertThat(changes.filter(_._1 == "-U").map(_._2)).isEqualTo(Seq(1L))
      assertThat(changes.filter(_._1 == "+U").map(_._2)).isEqualTo(Seq(1L))
      assertThat(changes.filter(_._1 == "-D").map(_._2).sorted).isEqualTo(Seq(2L, 3L))
      assertThat(changes.filter(_._1 == "+I").map(_._2)).isEqualTo(Seq(3L))

      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t_pk_part', '$t1', '$t2') ORDER BY orderId"),
        Row(1L, 110L, 1001, "a1_upd", "2026-01-01") :: Nil)

      // partition filter on top of the TVF relation
      checkAnswer(
        sql(s"""SELECT orderId FROM $TVF('$DEFAULT_DATABASE.t_pk_part', '$t1')
               |WHERE dt = '2026-01-01' ORDER BY orderId""".stripMargin),
        Row(1L) :: Row(4L) :: Nil
      )
    }
  }

  test("TVF: session-level scan.incremental.* options are ignored") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (1L, 11L, 101, "a1")""")
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (2L, 12L, 102, "a2")""")
      Thread.sleep(200)

      // A stale window in session configuration must not leak into reads: the scan.incremental.*
      // options are only honored as per-query scan options (TVF arguments / DataFrameReader).
      withSQLConf(
        s"spark.sql.fluss.${SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()}" ->
          "2000-01-01 00:00:00",
        s"spark.sql.fluss.${SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()}" ->
          "2000-01-02 00:00:00"
      ) {
        // A plain batch read still returns the full table.
        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
          Row(1L, 11L, 101, "a1") :: Row(2L, 12L, 102, "a2") :: Nil)

        // The TVF window is unaffected by the session values.
        checkAnswer(
          sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1') ORDER BY orderId"),
          Row(2L, 12L, 102, "a2") :: Nil)
      }
    }
  }

  test("TVF: empty window returns no rows") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (1L, 11L, 101, "a1")""")
      Thread.sleep(400)
      val ta = System.currentTimeMillis()
      Thread.sleep(300)
      val tb = System.currentTimeMillis()
      Thread.sleep(300)
      // written after the [ta, tb) gap, so the window contains no data
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (2L, 12L, 102, "a2")""")
      Thread.sleep(200)

      checkAnswer(sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$ta', '$tb')"), Nil)
    }
  }

  test("TVF: epoch millis, datetime string and TIMESTAMP literal yield the same window") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (1L, 11L, 101, "a1")""")
      Thread.sleep(1500)
      val t1 = secondAligned(System.currentTimeMillis())
      waitPast(t1)

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (2L, 12L, 102, "a2")""")
      Thread.sleep(1500)
      val t2 = secondAligned(System.currentTimeMillis())
      waitPast(t2)

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (3L, 13L, 103, "a3")""")
      Thread.sleep(200)

      val expected = Row(2L, 12L, 102, "a2") :: Nil

      // epoch milliseconds as a string
      checkAnswer(sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2')"), expected)
      // epoch milliseconds as an integral literal
      checkAnswer(sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', ${t1}L, ${t2}L)"), expected)
      // 'yyyy-MM-dd HH:mm:ss' in the session time zone
      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '${formatTs(t1)}', '${formatTs(t2)}')"),
        expected)
      // TIMESTAMP literals
      checkAnswer(
        sql(s"""SELECT * FROM $TVF('$DEFAULT_DATABASE.t',
               |TIMESTAMP '${formatTs(t1)}', TIMESTAMP '${formatTs(t2)}')""".stripMargin),
        expected
      )
    }
  }

  test("TVF: Spark expressions as timestamp arguments") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1L, 11L, 101, "a1"), (2L, 12L, 102, "a2"), (3L, 13L, 103, "a3")""".stripMargin)
      // unix_timestamp() has second granularity, so make every row strictly older than the
      // truncated "now" to keep the window boundaries deterministic.
      Thread.sleep(1300)

      // [now - 1h, now) covers every row written above, as epoch milliseconds
      // (unix_timestamp() returns seconds)
      checkAnswer(
        sql(s"""SELECT * FROM $TVF(
               |  '$DEFAULT_DATABASE.t',
               |  CAST((unix_timestamp() - 3600) * 1000 AS STRING),
               |  CAST(unix_timestamp() * 1000 AS STRING)) ORDER BY orderId""".stripMargin),
        Row(1L, 11L, 101, "a1") :: Row(2L, 12L, 102, "a2") :: Row(3L, 13L, 103, "a3") :: Nil
      )

      // the same window as datetime strings
      checkAnswer(
        sql(s"""SELECT * FROM $TVF(
               |  '$DEFAULT_DATABASE.t',
               |  date_format(now() - INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm:ss'),
               |  date_format(now(), 'yyyy-MM-dd HH:mm:ss')) ORDER BY orderId""".stripMargin),
        Row(1L, 11L, 101, "a1") :: Row(2L, 12L, 102, "a2") :: Row(3L, 13L, 103, "a3") :: Nil
      )

      // [now, latest) excludes them, proving the expression is really evaluated and applied
      checkAnswer(
        sql(s"""SELECT * FROM $TVF(
               |  '$DEFAULT_DATABASE.t',
               |  CAST(unix_timestamp() * 1000 AS STRING))""".stripMargin),
        Nil
      )
    }
  }

  test("TVF: partitioned log table window read") {
    withTable("t_part") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_part
             |(orderId BIGINT, itemId BIGINT, amount INT, dt STRING)
             |PARTITIONED BY (dt)
             |""".stripMargin)

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t_part VALUES
             |(1L, 11L, 101, "2026-01-01"), (2L, 12L, 102, "2026-01-02")""".stripMargin)
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t_part VALUES
             |(3L, 13L, 103, "2026-01-01"), (4L, 14L, 104, "2026-01-02")""".stripMargin)
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t_part VALUES (5L, 15L, 105, "2026-01-01")""")
      Thread.sleep(200)

      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t_part', '$t1', '$t2') ORDER BY orderId"),
        Row(3L, 13L, 103, "2026-01-01") :: Row(4L, 14L, 104, "2026-01-02") :: Nil
      )

      // partition filter on top of the TVF relation
      checkAnswer(
        sql(s"""SELECT orderId FROM $TVF('$DEFAULT_DATABASE.t_part', '$t1', '$t2')
               |WHERE dt = '2026-01-01'""".stripMargin),
        Row(3L) :: Nil)
    }
  }

  test("TVF: wrong argument count fails with a usage hint") {
    withTable("t") {
      createLogTable("t")
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (1L, 11L, 101, "a1")""")

      // only the table identifier
      val tooFew = intercept[Exception] {
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t')").collect()
      }
      assertThat(fullMessage(tooFew)).contains("endTimestamp")

      // one argument too many
      val tooMany = intercept[Exception] {
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '1', '2', '3')").collect()
      }
      assertThat(fullMessage(tooMany)).contains("endTimestamp")
    }
  }

  test("TVF: unknown table fails") {
    val ex = intercept[Exception] {
      sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.not_exist_tvf_table', '1', '2')").collect()
    }
    assertThat(fullMessage(ex)).contains("not_exist_tvf_table")
  }

  private def row(orderId: Long, itemId: Long, amount: Int, address: String): GenericRow =
    GenericRow.of(
      Long.box(orderId),
      Long.box(itemId),
      Int.box(amount),
      BinaryString.fromString(address))

  /**
   * Raw changelog records of `table` whose commit timestamp falls inside [start, end), as
   * (changeType, orderId) pairs in log order. Used to prove the claimed change types (-U/+U/-D/+I)
   * really exist in the window, so the folded-output assertions below cannot pass vacuously.
   */
  private def changelogInWindow(table: Table, start: Long, end: Long): Seq[(String, Long)] = {
    val scanner = table.newScan().createLogScanner()
    try {
      if (table.getTableInfo.isPartitioned) {
        admin.listPartitionInfos(table.getTableInfo.getTablePath).get().forEach {
          pi => scanner.subscribeFromBeginning(pi.getPartitionId, 0)
        }
      } else {
        scanner.subscribeFromBeginning(0)
      }
      val records = scala.collection.mutable.ArrayBuffer[(String, Long)]()
      // Poll until records arrive and a poll comes back empty (all caught up), or the deadline.
      // Mirrors FlussSparkTestBase.getRowsWithChangeType: the high watermark may advance in
      // stages, so a single early empty poll must not end the scan.
      val deadline = System.currentTimeMillis() + 10000
      var hasReceivedAny = false
      var done = false
      while (!done && System.currentTimeMillis() < deadline) {
        val polled = scanner.poll(Duration.ofSeconds(1))
        if (!polled.isEmpty) {
          hasReceivedAny = true
          polled.forEach {
            r =>
              if (r.timestamp() >= start && r.timestamp() < end) {
                records += ((r.getChangeType.shortString(), r.getRow.getLong(0)))
              }
          }
        } else if (hasReceivedAny) {
          done = true
        }
      }
      records.toSeq
    } finally {
      scanner.close()
    }
  }

  private def pkRow(
      orderId: Long,
      itemId: Long,
      amount: Int,
      address: String,
      dt: String): GenericRow =
    GenericRow.of(
      Long.box(orderId),
      Long.box(itemId),
      Int.box(amount),
      BinaryString.fromString(address),
      BinaryString.fromString(dt))

  private def deleteKey(orderId: Long): GenericRow =
    GenericRow.of(Long.box(orderId), null, null, null)

  private def deleteKey(orderId: Long, dt: String): GenericRow =
    GenericRow.of(Long.box(orderId), null, null, null, BinaryString.fromString(dt))
}
