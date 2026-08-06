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

import org.apache.fluss.row.{BinaryString, GenericRow}

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

import java.time.{Instant, ZoneId}
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

  test("TVF: primary key table folds to +I/+U and excludes deletes") {
    withTable("t") {
      val tablePath = createTablePath("t")
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t
             |(orderId BIGINT, itemId BIGINT, amount INT, address STRING)
             |TBLPROPERTIES("primary.key" = "orderId", "bucket.num" = 1)
             |""".stripMargin)

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

      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2') ORDER BY orderId"),
        Row(2L, 120L, 1002, "a2_upd") :: Row(4L, 14L, 104, "a4") :: Nil)
    }
  }

  test("TVF: options are scoped to the query and do not leak into later reads") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (1L, 11L, 101, "a1")""")
      Thread.sleep(500)
      val t1 = System.currentTimeMillis()
      Thread.sleep(50)
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (2L, 12L, 102, "a2")""")
      Thread.sleep(500)
      val t2 = System.currentTimeMillis()
      Thread.sleep(50)
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (3L, 13L, 103, "a3")""")
      Thread.sleep(200)

      // No SET is required for the TVF to work.
      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$t1', '$t2') ORDER BY orderId"),
        Row(2L, 12L, 102, "a2") :: Nil)

      // The following plain read must still see the whole table.
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row(1L, 11L, 101, "a1") :: Row(2L, 12L, 102, "a2") :: Row(3L, 13L, 103, "a3") :: Nil)
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

  test("TVF: future end timestamp is rejected") {
    withTable("t") {
      createLogTable("t")
      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES (1L, 11L, 101, "a1")""")
      Thread.sleep(200)

      val start = System.currentTimeMillis() - 60000
      val future = System.currentTimeMillis() + 3600000
      val ex = intercept[Exception] {
        sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t', '$start', '$future')").collect()
      }
      assertThat(fullMessage(ex)).contains("current timestamp")
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

  test("TVF: Spark expressions as epoch-millis arguments") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1L, 11L, 101, "a1"), (2L, 12L, 102, "a2"), (3L, 13L, 103, "a3")""".stripMargin)
      // unix_timestamp() has second granularity, so make every row strictly older than the
      // truncated "now" to keep the window boundaries deterministic.
      Thread.sleep(1300)

      // [now - 1h, now) covers every row written above
      checkAnswer(
        sql(s"""SELECT * FROM $TVF(
               |  '$DEFAULT_DATABASE.t',
               |  CAST((unix_timestamp() - 3600) * 1000 AS STRING),
               |  CAST(unix_timestamp() * 1000 AS STRING)) ORDER BY orderId""".stripMargin),
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

  test("TVF: Spark expressions as datetime-string arguments") {
    withTable("t") {
      createLogTable("t")

      sql(s"""INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1L, 11L, 101, "a1"), (2L, 12L, 102, "a2")""".stripMargin)
      Thread.sleep(1300)

      checkAnswer(
        sql(s"""SELECT * FROM $TVF(
               |  '$DEFAULT_DATABASE.t',
               |  date_format(now() - INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm:ss'),
               |  date_format(now(), 'yyyy-MM-dd HH:mm:ss')) ORDER BY orderId""".stripMargin),
        Row(1L, 11L, 101, "a1") :: Row(2L, 12L, 102, "a2") :: Nil
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

  private def deleteKey(orderId: Long): GenericRow =
    GenericRow.of(Long.box(orderId), null, null, null)
}
