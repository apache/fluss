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
import org.apache.fluss.spark.read.{FlussOffsetInitializers, FlussTimeRange}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.assertj.core.api.Assertions.assertThat

/**
 * Verifies the `fluss_incremental_between_timestamp` table-valued function. The window is
 * left-closed, right-open `[start, end)` on the record commit timestamp, and the function's options
 * are scoped to the single query. Tables are partitioned unless a case says otherwise.
 */
class SparkTimeRangeTvfTest extends FlussSparkTestBase {

  private val TVF = "fluss_incremental_between_timestamp"

  private val P1 = "2026-01-01"
  private val P2 = "2026-01-02"

  test("TVF: log table window") {
    withTable("t_log") {
      createLogTable("t_log")

      insert("t_log", s"""(1L, 11L, 101, "a1", "$P1"), (2L, 12L, 102, "a2", "$P2")""")
      val t1 = boundary()
      insert("t_log", s"""(3L, 13L, 103, "a3", "$P1"), (4L, 14L, 104, "a4", "$P2")""")
      val t2 = boundary()
      insert("t_log", s"""(5L, 15L, 105, "a5", "$P1")""")
      val t3 = boundary()
      val t4 = boundary()

      // the window spans every partition
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_log", t1, t2)} ORDER BY orderId"),
        Row(3L, 13L, 103, "a3", P1) :: Row(4L, 14L, 104, "a4", P2) :: Nil)

      // projection, filter and partition pruning still work on top of the TVF relation
      checkAnswer(
        sql(s"SELECT address FROM ${tvf("t_log", t1, t2)} WHERE amount = 104"),
        Row("a4") :: Nil)
      checkAnswer(
        sql(s"SELECT orderId FROM ${tvf("t_log", t1, t2)} WHERE dt = '$P1'"),
        Row(3L) :: Nil)

      // without an end timestamp the window runs up to the latest data
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_log", t2)} ORDER BY orderId"),
        Row(5L, 15L, 105, "a5", P1) :: Nil)

      // a window without writes yields nothing
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t3, t4)}"), Nil)

      // the table argument may be unqualified or fully qualified
      checkAnswer(sql(s"SELECT * FROM $TVF('t_log', '$t2')"), Row(5L, 15L, 105, "a5", P1) :: Nil)
      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_CATALOG.$DEFAULT_DATABASE.t_log', '$t2')"),
        Row(5L, 15L, 105, "a5", P1) :: Nil)

      // the omitted end bound is pinned when the statement is analyzed, so a row committed before
      // the scan is planned stays outside the window
      val pinned = sql(s"SELECT * FROM ${tvf("t_log", t2)} ORDER BY orderId")
      insert("t_log", s"""(6L, 16L, 106, "a6", "$P1")""")
      Thread.sleep(200)
      checkAnswer(pinned, Row(5L, 15L, 105, "a5", P1) :: Nil)
    }
  }

  test("TVF: primary key table folds the window changelog") {
    withTable("t_fold") {
      createPkTable("t_fold")

      val writer = loadFlussTable(createTablePath("t_fold")).newUpsert().createWriter()
      writer.upsert(row(1L, 11L, 101, "a1", P1)).get()
      writer.upsert(row(2L, 12L, 102, "a2", P2)).get()
      writer.upsert(row(3L, 13L, 103, "a3", P1)).get()
      writer.upsert(row(4L, 14L, 104, "a4", P2)).get()
      writer.flush()
      val t1 = boundary()

      // key 1 updated twice, key 2 updated once, key 3 deleted then re-inserted, key 4 deleted,
      // key 5 inserted then deleted again
      writer.upsert(row(1L, 110L, 1001, "a1_v2", P1)).get()
      writer.upsert(row(1L, 111L, 1002, "a1_v3", P1)).get()
      writer.upsert(row(2L, 120L, 1002, "a2_upd", P2)).get()
      writer.delete(deleteKey(3L, P1)).get()
      writer.upsert(row(3L, 130L, 1003, "a3_new", P1)).get()
      writer.delete(deleteKey(4L, P2)).get()
      writer.upsert(row(5L, 15L, 105, "a5", P2)).get()
      writer.delete(deleteKey(5L, P2)).get()
      writer.flush()
      val t2 = boundary()

      writer.upsert(row(1L, 112L, 1004, "a1_v4", P1)).get()
      writer.flush()
      val t3 = boundary()
      val t4 = boundary()

      // each changed key appears once with its last in-window value; deleted keys and an insert
      // cancelled by a delete are excluded
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_fold", t1, t2)} ORDER BY orderId"),
        Row(1L, 111L, 1002, "a1_v3", P1) ::
          Row(2L, 120L, 1002, "a2_upd", P2) ::
          Row(3L, 130L, 1003, "a3_new", P1) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_fold", t1)} ORDER BY orderId"),
        Row(1L, 112L, 1004, "a1_v4", P1) ::
          Row(2L, 120L, 1002, "a2_upd", P2) ::
          Row(3L, 130L, 1003, "a3_new", P1) :: Nil
      )

      checkAnswer(
        sql(s"SELECT orderId FROM ${tvf("t_fold", t1, t2)} WHERE dt = '$P1' ORDER BY orderId"),
        Row(1L) :: Row(3L) :: Nil)

      checkAnswer(sql(s"SELECT * FROM ${tvf("t_fold", t3, t4)}"), Nil)
    }
  }

  test("TVF: non-partitioned tables") {
    withTable("t_np", "t_np_pk") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_np
             |(orderId BIGINT, itemId BIGINT, amount INT, address STRING)
             |""".stripMargin)
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_np_pk
             |(orderId BIGINT, itemId BIGINT, amount INT, address STRING)
             |TBLPROPERTIES("primary.key" = "orderId", "bucket.num" = 1)
             |""".stripMargin)

      val writer = loadFlussTable(createTablePath("t_np_pk")).newUpsert().createWriter()
      insert("t_np", """(1L, 11L, 101, "a1")""")
      writer.upsert(unpartitionedRow(1L, 11L, 101, "a1")).get()
      writer.flush()
      val t1 = boundary()

      insert("t_np", """(2L, 12L, 102, "a2")""")
      writer.upsert(unpartitionedRow(1L, 110L, 1001, "a1_upd")).get()
      writer.upsert(unpartitionedRow(2L, 12L, 102, "a2")).get()
      writer.flush()
      Thread.sleep(200)

      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np", t1)}"), Row(2L, 12L, 102, "a2") :: Nil)
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_np_pk", t1)} ORDER BY orderId"),
        Row(1L, 110L, 1001, "a1_upd") :: Row(2L, 12L, 102, "a2") :: Nil)
    }
  }

  test("TVF: timestamp argument forms resolve to the same window") {
    withTable("t_ts") {
      createLogTable("t_ts")

      // every accepted form of the same instant must resolve to the same window; asserted on the
      // options the reader consumes, so no data has to be written
      def assertAllFormsAgree(): Unit = {
        val startTs = "2026-01-01 00:00:00"
        val endTs = "2026-01-02 00:00:00"
        val expected = FlussTimeRange(parseTs(startTs), parseTs(endTs))
        Seq(
          s"'${expected.startMs}', '${expected.endMs}'",
          s"${expected.startMs}L, ${expected.endMs}L",
          s"'$startTs', '$endTs'",
          s"TIMESTAMP '$startTs', TIMESTAMP '$endTs'",
          s"TIMESTAMP_NTZ '$startTs', TIMESTAMP_NTZ '$endTs'",
          "DATE '2026-01-01', DATE '2026-01-02'"
        ).foreach(args => assertThat(resolvedWindow("t_ts", args)).isEqualTo(expected))
      }

      assertAllFormsAgree()
      // the session time zone applies to the datetime, TIMESTAMP_NTZ and DATE forms alike
      withSQLConf("spark.sql.session.timeZone" -> "Asia/Shanghai")(assertAllFormsAgree())

      // an omitted end bound is pinned to the analysis time
      val before = System.currentTimeMillis()
      val openEnded = resolvedWindow("t_ts", "'2026-01-01 00:00:00'")
      assertThat(openEnded.endMs).isBetween(before, System.currentTimeMillis())

      // constant expressions are evaluated during analysis
      val lastHour = resolvedWindow(
        "t_ts",
        "date_format(now() - INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm:ss'), " +
          "CAST(unix_timestamp() * 1000 AS STRING)")
      assertThat(lastHour.endMs - lastHour.startMs).isBetween(3590000L, 3610000L)

      val aroundToday =
        resolvedWindow("t_ts", "current_date() - INTERVAL 1 DAY, current_date() + INTERVAL 1 DAY")
      assertThat(aroundToday.startMs).isLessThan(before)
      assertThat(aroundToday.endMs).isGreaterThan(before)
    }
  }

  test("TVF: invalid usage fails fast") {
    withTable("t_bad", "t_bad_pk") {
      createLogTable("t_bad")
      createPkTable("t_bad_pk")

      val writer = loadFlussTable(createTablePath("t_bad_pk")).newUpsert().createWriter()
      insert("t_bad", s"""(1L, 11L, 101, "a1", "$P1")""")
      writer.upsert(row(1L, 11L, 101, "a1", P1)).get()
      writer.flush()
      Thread.sleep(300)
      val t1 = System.currentTimeMillis()

      // a blank start must not silently read the full table
      assertThat(failureOf(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t_bad', ' ')"))
        .contains(TVF)
        .contains("must not be blank")

      assertThat(failureOf(s"SELECT * FROM ${tvf("t_bad", t1 + 1000, t1)}"))
        .contains("strictly before")

      assertThat(failureOf(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t_bad')"))
        .contains("endTimestamp")
      assertThat(failureOf(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.t_bad', '1', '2', '3')"))
        .contains("endTimestamp")

      assertThat(failureOf(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.not_exist', '1', '2')"))
        .contains("not_exist")

      // an incremental read reconciles the changelog and cannot serve a read-optimized scan
      withSQLConf(sessionKey(SparkFlussConf.READ_OPTIMIZED_OPTION.key()) -> "true") {
        assertThat(failureOf(s"SELECT * FROM ${tvf("t_bad_pk", t1)}"))
          .contains(SparkFlussConf.READ_OPTIMIZED_OPTION.key())
      }
    }
  }

  test("TVF: window bounds are never read from session configuration") {
    withTable("t_conf", "t_conf_pk") {
      createLogTable("t_conf")
      createPkTable("t_conf_pk")

      val writer = loadFlussTable(createTablePath("t_conf_pk")).newUpsert().createWriter()
      insert("t_conf", s"""(1L, 11L, 101, "a1", "$P1")""")
      writer.upsert(row(1L, 11L, 101, "a1", P1)).get()
      writer.flush()
      val t1 = boundary()

      insert("t_conf", s"""(2L, 12L, 102, "a2", "$P2")""")
      writer.upsert(row(2L, 12L, 102, "a2", P2)).get()
      writer.flush()
      Thread.sleep(200)

      val window = Row(2L, 12L, 102, "a2", P2) :: Nil

      withSQLConf(
        sessionKey(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()) -> "2000-01-01 00:00:00",
        sessionKey(SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()) -> "2000-01-02 00:00:00"
      ) {
        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.t_conf ORDER BY orderId"),
          Row(1L, 11L, 101, "a1", P1) :: Row(2L, 12L, 102, "a2", P2) :: Nil)
        checkAnswer(sql(s"SELECT * FROM ${tvf("t_conf", t1)}"), window)
        checkAnswer(sql(s"SELECT * FROM ${tvf("t_conf_pk", t1)}"), window)
      }
    }
  }

  private def createLogTable(name: String): Unit =
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.$name
           |(orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING)
           |PARTITIONED BY (dt)
           |""".stripMargin)

  private def createPkTable(name: String): Unit =
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.$name
           |(orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING)
           |PARTITIONED BY (dt)
           |TBLPROPERTIES("primary.key" = "orderId,dt", "bucket.num" = 1)
           |""".stripMargin)

  private def insert(table: String, values: String): Unit =
    sql(s"INSERT INTO $DEFAULT_DATABASE.$table VALUES $values")

  private def tvf(table: String, timestamps: Long*): String =
    s"$TVF('$DEFAULT_DATABASE.$table'${timestamps.map(ts => s", '$ts'").mkString})"

  private def sessionKey(option: String): String =
    s"${SparkFlussConf.SPARK_FLUSS_CONF_PREFIX}$option"

  /** A timestamp after everything written so far and before anything written next. */
  private def boundary(): Long = {
    Thread.sleep(500)
    val ms = System.currentTimeMillis()
    Thread.sleep(50)
    ms
  }

  /**
   * The window the reader would apply for a TVF call, taken from the scan options of the analyzed
   * relation. Only the table metadata is touched; no data is read.
   */
  private def resolvedWindow(table: String, args: String): FlussTimeRange = {
    val plan =
      sql(s"SELECT * FROM $TVF('$DEFAULT_DATABASE.$table', $args)").queryExecution.analyzed
    val options = plan
      .collectFirst { case relation: DataSourceV2Relation => relation.options }
      .getOrElse(fail(s"no Fluss relation resolved for $TVF($args)"))
    FlussOffsetInitializers.incrementalTimeRange(options).get
  }

  /** Parses a `yyyy-MM-dd HH:mm:ss` string the way the scan options do. */
  private def parseTs(datetime: String): Long =
    FlussOffsetInitializers.parseTimestamp(
      datetime,
      SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key())

  /** The full stack trace of the failure raised by `query`, which must fail. */
  private def failureOf(query: String): String = {
    val ex = intercept[Exception](sql(query).collect())
    val sw = new java.io.StringWriter()
    ex.printStackTrace(new java.io.PrintWriter(sw))
    sw.toString
  }

  private def row(
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

  private def deleteKey(orderId: Long, dt: String): GenericRow =
    GenericRow.of(Long.box(orderId), null, null, null, BinaryString.fromString(dt))

  private def unpartitionedRow(
      orderId: Long,
      itemId: Long,
      amount: Int,
      address: String): GenericRow =
    GenericRow.of(
      Long.box(orderId),
      Long.box(itemId),
      Int.box(amount),
      BinaryString.fromString(address))
}
