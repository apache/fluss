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

import org.apache.fluss.config.Configuration
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

  /** Snapshots are triggered explicitly, so no window can straddle an automatic one. */
  override protected def flussConf: Configuration = new Configuration()

  private val TVF = "fluss_incremental_between_timestamp"

  private val P1 = "2026-01-01"
  private val P2 = "2026-01-02"

  test("TVF: log table window") {
    withTable("t_log") {
      createLogTable("t_log")

      val t0 = boundary()
      insert("t_log", s"""(1L, 11L, 101, "a1", "$P1"), (2L, 12L, 102, "a2", "$P2")""")
      val t1 = boundary()
      insert("t_log", s"""(3L, 13L, 103, "a3", "$P1"), (4L, 14L, 104, "a4", "$P2")""")
      val t2 = boundary()
      insert("t_log", s"""(5L, 15L, 105, "a5", "$P1")""")
      val t3 = boundary()
      // nothing is committed in [t3, t4), but rows follow it
      val t4 = boundary()
      insert("t_log", s"""(6L, 16L, 106, "a6", "$P2"), (7L, 17L, 107, "a7", "$P1")""")
      val t5 = boundary()
      val t6 = boundary()

      val batch1 = Row(1L, 11L, 101, "a1", P1) :: Row(2L, 12L, 102, "a2", P2) :: Nil
      val batch2 = Row(3L, 13L, 103, "a3", P1) :: Row(4L, 14L, 104, "a4", P2) :: Nil
      val batch3 = Row(5L, 15L, 105, "a5", P1) :: Nil
      val batch4 = Row(6L, 16L, 106, "a6", P2) :: Row(7L, 17L, 107, "a7", P1) :: Nil

      // consecutive windows partition the table exactly, each one spanning every partition
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t0, t1)} ORDER BY orderId"), batch1)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t1, t2)} ORDER BY orderId"), batch2)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t2, t3)} ORDER BY orderId"), batch3)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t4, t5)} ORDER BY orderId"), batch4)

      // a window may span several writes, and one covering everything matches a plain batch read
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t1, t4)} ORDER BY orderId"), batch2 ::: batch3)
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_log", t0, t6)} ORDER BY orderId"),
        batch1 ::: batch2 ::: batch3 ::: batch4)

      // projection, filter and partition pruning still work on top of the TVF relation
      checkAnswer(
        sql(s"SELECT address FROM ${tvf("t_log", t1, t2)} WHERE amount = 104"),
        Row("a4") :: Nil)
      checkAnswer(
        sql(s"SELECT orderId FROM ${tvf("t_log", t1, t2)} WHERE dt = '$P1'"),
        Row(3L) :: Nil)

      // without an end timestamp the window runs up to the latest data
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t2)} ORDER BY orderId"), batch3 ::: batch4)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t4)} ORDER BY orderId"), batch4)

      // a window without writes yields nothing, whether it precedes, splits or trails the data
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t3, t4)}"), Nil)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_log", t5, t6)}"), Nil)

      // the table argument may be unqualified or fully qualified
      checkAnswer(sql(s"SELECT * FROM $TVF('t_log', '$t4')"), batch4)
      checkAnswer(
        sql(s"SELECT * FROM $TVF('$DEFAULT_CATALOG.$DEFAULT_DATABASE.t_log', '$t4')"),
        batch4)

      // the omitted end bound is pinned when the statement is analyzed, so a row committed before
      // the scan is planned stays outside the window
      val pinned = sql(s"SELECT * FROM ${tvf("t_log", t4)} ORDER BY orderId")
      insert("t_log", s"""(8L, 18L, 108, "a8", "$P1")""")
      Thread.sleep(200)
      checkAnswer(pinned, batch4)
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
      // nothing is committed in [t3, t4), but changes follow it: the start offset then resolves to
      // a record past the end bound instead of to the end of the log
      val t4 = boundary()

      writer.delete(deleteKey(2L, P2)).get()
      writer.flush()
      val t5 = boundary()

      writer.upsert(row(6L, 16L, 106, "a6", P1)).get()
      writer.flush()
      val t6 = boundary()
      val t7 = boundary()

      // each changed key appears once with its last in-window value; deleted keys and an insert
      // cancelled by a delete are excluded
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_fold", t1, t2)} ORDER BY orderId"),
        Row(1L, 111L, 1002, "a1_v3", P1) ::
          Row(2L, 120L, 1002, "a2_upd", P2) ::
          Row(3L, 130L, 1003, "a3_new", P1) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_fold", t2, t3)} ORDER BY orderId"),
        Row(1L, 112L, 1004, "a1_v4", P1) :: Nil)

      // a window whose only change is a delete folds to nothing, even though it does read records
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_fold", t4, t5)}"), Nil)

      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_fold", t5, t6)} ORDER BY orderId"),
        Row(6L, 16L, 106, "a6", P1) :: Nil)

      // spanning several batches: key 1 keeps its last value, key 2 is dropped by the later delete
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_fold", t2, t6)} ORDER BY orderId"),
        Row(1L, 112L, 1004, "a1_v4", P1) :: Row(6L, 16L, 106, "a6", P1) :: Nil)

      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_fold", t1)} ORDER BY orderId"),
        Row(1L, 112L, 1004, "a1_v4", P1) ::
          Row(3L, 130L, 1003, "a3_new", P1) ::
          Row(6L, 16L, 106, "a6", P1) :: Nil
      )

      checkAnswer(
        sql(s"SELECT orderId FROM ${tvf("t_fold", t1, t2)} WHERE dt = '$P1' ORDER BY orderId"),
        Row(1L) :: Row(3L) :: Nil)

      // windows without any change yield nothing, whether they split or trail the changelog
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_fold", t3, t4)}"), Nil)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_fold", t6, t7)}"), Nil)
    }
  }

  test("TVF: primary key table with several buckets") {
    withTable("t_multi") {
      createPkTable("t_multi", buckets = 3)

      val writer = loadFlussTable(createTablePath("t_multi")).newUpsert().createWriter()
      (1 to 6).foreach {
        k =>
          val dt = if (k % 2 == 0) P2 else P1
          writer.upsert(row(k.toLong, (10 + k).toLong, 100 + k, s"a$k", dt)).get()
      }
      writer.flush()
      val t1 = boundary()

      // the window touches only some of the six bucket/partition pairs, so the rest are planned
      // and read down to nothing
      writer.upsert(row(2L, 120L, 1002, "a2_upd", P2)).get()
      writer.delete(deleteKey(5L, P1)).get()
      writer.upsert(row(7L, 17L, 107, "a7", P1)).get()
      writer.flush()
      val t2 = boundary()

      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_multi", t1, t2)} ORDER BY orderId"),
        Row(2L, 120L, 1002, "a2_upd", P2) :: Row(7L, 17L, 107, "a7", P1) :: Nil)
    }
  }

  test("TVF: primary key table never serves the kv snapshot") {
    withTable("t_snap") {
      createPkTable("t_snap")
      val tablePath = createTablePath("t_snap")
      val writer = loadFlussTable(tablePath).newUpsert().createWriter()

      val t0 = boundary()
      writer.upsert(row(1L, 11L, 101, "a1", P1)).get()
      writer.upsert(row(2L, 12L, 102, "a2", P2)).get()
      writer.flush()
      // key 1 and key 2 now live in a kv snapshot, so any read that consults it sees them
      flussServer.triggerAndWaitSnapshot(tablePath)
      val t1 = boundary()

      writer.upsert(row(1L, 110L, 1001, "a1_upd", P1)).get()
      writer.upsert(row(3L, 13L, 103, "a3", P1)).get()
      writer.flush()
      val t2 = boundary()
      // the second batch is snapshotted as well, so the window's own rows are in the snapshot too
      flussServer.triggerAndWaitSnapshot(tablePath)
      val t3 = boundary()

      // a read-optimized full read serves the snapshot alone, which is how we know it is there
      withSQLConf(sessionKey(SparkFlussConf.READ_OPTIMIZED_OPTION.key()) -> "true") {
        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.t_snap ORDER BY orderId"),
          Row(1L, 110L, 1001, "a1_upd", P1) ::
            Row(2L, 12L, 102, "a2", P2) ::
            Row(3L, 13L, 103, "a3", P1) :: Nil
        )
      }

      // key 2 changed before the window and only survives in the snapshot, so it must not appear
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_snap", t1, t2)} ORDER BY orderId"),
        Row(1L, 110L, 1001, "a1_upd", P1) :: Row(3L, 13L, 103, "a3", P1) :: Nil)

      // an earlier window still folds from the changelog, at the values it held back then
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_snap", t0, t1)} ORDER BY orderId"),
        Row(1L, 11L, 101, "a1", P1) :: Row(2L, 12L, 102, "a2", P2) :: Nil)

      // a window with no changes stays empty instead of falling back to the whole snapshot
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_snap", t2, t3)}"), Nil)
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
      val t0 = boundary()
      insert("t_np", """(1L, 11L, 101, "a1")""")
      writer.upsert(unpartitionedRow(1L, 11L, 101, "a1")).get()
      writer.flush()
      val t1 = boundary()

      insert("t_np", """(2L, 12L, 102, "a2")""")
      writer.upsert(unpartitionedRow(1L, 110L, 1001, "a1_upd")).get()
      writer.upsert(unpartitionedRow(2L, 12L, 102, "a2")).get()
      writer.flush()
      val t2 = boundary()

      // nothing is committed in [t2, t3), but writes follow it
      val t3 = boundary()

      insert("t_np", """(3L, 13L, 103, "a3")""")
      writer.delete(unpartitionedDeleteKey(2L)).get()
      writer.flush()
      val t4 = boundary()

      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np", t0, t1)}"), Row(1L, 11L, 101, "a1") :: Nil)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np_pk", t0, t1)}"), Row(1L, 11L, 101, "a1") :: Nil)

      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np", t1, t2)}"), Row(2L, 12L, 102, "a2") :: Nil)
      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_np_pk", t1, t2)} ORDER BY orderId"),
        Row(1L, 110L, 1001, "a1_upd") :: Row(2L, 12L, 102, "a2") :: Nil)

      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np", t2, t3)}"), Nil)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np_pk", t2, t3)}"), Nil)

      // the pk window only deletes, so it folds to nothing while the log table keeps its append
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np", t3, t4)}"), Row(3L, 13L, 103, "a3") :: Nil)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np_pk", t3, t4)}"), Nil)

      checkAnswer(
        sql(s"SELECT * FROM ${tvf("t_np", t1)} ORDER BY orderId"),
        Row(2L, 12L, 102, "a2") :: Row(3L, 13L, 103, "a3") :: Nil)
      checkAnswer(sql(s"SELECT * FROM ${tvf("t_np_pk", t1)}"), Row(1L, 110L, 1001, "a1_upd") :: Nil)
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
      assertThat(failureOf(s"SELECT * FROM ${tvf("t_bad", t1, t1)}"))
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

  private def createPkTable(name: String, buckets: Int = 1): Unit =
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.$name
           |(orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING)
           |PARTITIONED BY (dt)
           |TBLPROPERTIES("primary.key" = "orderId,dt", "bucket.num" = $buckets)
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

  private def unpartitionedDeleteKey(orderId: Long): GenericRow =
    GenericRow.of(Long.box(orderId), null, null, null)
}
