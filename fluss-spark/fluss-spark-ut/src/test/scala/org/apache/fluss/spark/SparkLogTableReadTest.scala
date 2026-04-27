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

import org.apache.fluss.spark.read.{FlussMetrics, FlussScan}
import org.apache.fluss.spark.read.FlussAppendScan

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.assertj.core.api.Assertions.assertThat

class SparkLogTableReadTest extends FlussSparkTestBase {

  test("Spark Read: log table") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (orderId BIGINT, itemId BIGINT, amount INT, address STRING)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t
             |VALUES
             |(600L, 21L, 601, "addr1"), (700L, 22L, 602, "addr2"),
             |(800L, 23L, 603, "addr3"), (900L, 24L, 604, "addr4"),
             |(1000L, 25L, 605, "addr5")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1") ::
          Row(700L, 22L, 602, "addr2") ::
          Row(800L, 23L, 603, "addr3") ::
          Row(900L, 24L, 604, "addr4") ::
          Row(1000L, 25L, 605, "addr5") :: Nil
      )

      // projection
      checkAnswer(
        sql(s"SELECT address, itemId FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row("addr1", 21L) ::
          Row("addr2", 22L) ::
          Row("addr3", 23L) ::
          Row("addr4", 24L) ::
          Row("addr5", 25L) :: Nil
      )

      // filter
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE amount % 2 = 0 ORDER BY orderId"),
        Row(700L, 22L, 602, "addr2") ::
          Row(900L, 24L, 604, "addr4") :: Nil
      )

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(700L, 220L, 602, "addr2"),
             |(900L, 240L, 604, "addr4"),
             |(1100L, 260L, 606, "addr6")
             |""".stripMargin)
      // projection + filter
      checkAnswer(
        sql(s"""
               |SELECT orderId, itemId FROM $DEFAULT_DATABASE.t
               |WHERE orderId >= 900 ORDER BY orderId, itemId""".stripMargin),
        Row(900L, 24L) ::
          Row(900L, 240L) ::
          Row(1000L, 25L) ::
          Row(1100L, 260L) :: Nil
      )
    }
  }

  test("Spark Read: partitioned log table") {
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE $DEFAULT_DATABASE.t (orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING)
           |PARTITIONED BY (dt)
           |""".stripMargin
      )

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(600L, 21L, 601, "addr1", "2026-01-01"), (700L, 22L, 602, "addr2", "2026-01-01"),
             |(800L, 23L, 603, "addr3", "2026-01-02"), (900L, 24L, 604, "addr4", "2026-01-02"),
             |(1000L, 25L, 605, "addr5", "2026-01-03")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 22L, 602, "addr2", "2026-01-01") ::
          Row(800L, 23L, 603, "addr3", "2026-01-02") ::
          Row(900L, 24L, 604, "addr4", "2026-01-02") ::
          Row(1000L, 25L, 605, "addr5", "2026-01-03") :: Nil
      )

      // Read with partition filter
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE dt = '2026-01-01' ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 22L, 602, "addr2", "2026-01-01") :: Nil
      )

      // Read with multiple partitions filter
      checkAnswer(
        sql(s"""
               |SELECT orderId, address, dt FROM $DEFAULT_DATABASE.t
               |WHERE dt IN ('2026-01-01', '2026-01-02')
               |ORDER BY orderId""".stripMargin),
        Row(600L, "addr1", "2026-01-01") ::
          Row(700L, "addr2", "2026-01-01") ::
          Row(800L, "addr3", "2026-01-02") ::
          Row(900L, "addr4", "2026-01-02") :: Nil
      )
    }
  }

  test("Spark: all data types") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (
             |id INT,
             |flag BOOLEAN,
             |small SHORT,
             |value INT,
             |big BIGINT,
             |real FLOAT,
             |amount DOUBLE,
             |name STRING,
             |decimal_val DECIMAL(10, 2),
             |date_val DATE,
             |timestamp_ntz_val TIMESTAMP,
             |timestamp_ltz_val TIMESTAMP_LTZ
             |)""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, true, 100s, 1000, 10000L, 12.34f, 56.78, "string_val",
             | 123.45, DATE "2026-01-01", TIMESTAMP "2026-01-01 12:00:00", TIMESTAMP "2026-01-01 12:00:00"),
             |(2, false, 200s, 2000, 20000L, 23.45f, 67.89, "another_str",
             | 223.45, DATE "2026-01-02", TIMESTAMP "2026-01-02 12:00:00", TIMESTAMP "2026-01-02 12:00:00")
             |""".stripMargin)

      // Read all data types
      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t"),
        Row(
          1,
          true,
          100.toShort,
          1000,
          10000L,
          12.34f,
          56.78,
          "string_val",
          java.math.BigDecimal.valueOf(123.45),
          java.sql.Date.valueOf("2026-01-01"),
          java.sql.Timestamp.valueOf("2026-01-01 12:00:00"),
          java.sql.Timestamp.valueOf("2026-01-01 12:00:00")
        ) :: Row(
          2,
          false,
          200.toShort,
          2000,
          20000L,
          23.45f,
          67.89,
          "another_str",
          java.math.BigDecimal.valueOf(223.45),
          java.sql.Date.valueOf("2026-01-02"),
          java.sql.Timestamp.valueOf("2026-01-02 12:00:00"),
          java.sql.Timestamp.valueOf("2026-01-02 12:00:00")
        ) :: Nil
      )

      // Test projection on selected columns
      checkAnswer(
        sql(s"SELECT id, name, amount FROM $DEFAULT_DATABASE.t ORDER BY id"),
        Row(1, "string_val", java.math.BigDecimal.valueOf(56.78)) ::
          Row(2, "another_str", java.math.BigDecimal.valueOf(67.89)) :: Nil
      )

      // Filter by boolean field
      checkAnswer(
        sql(s"SELECT id, flag, name FROM $DEFAULT_DATABASE.t WHERE flag = true ORDER BY id"),
        Row(1, true, "string_val") :: Nil
      )

      // Filter by numeric field
      checkAnswer(
        sql(s"SELECT id, value, name FROM $DEFAULT_DATABASE.t WHERE value > 1500 ORDER BY id"),
        Row(2, 2000, "another_str") :: Nil
      )

      // Filter by string field
      checkAnswer(
        sql(s"SELECT id, name FROM $DEFAULT_DATABASE.t WHERE name LIKE '%another%' ORDER BY id"),
        Row(2, "another_str") :: Nil
      )
    }
  }

  test("Spark Read: log table projection with type-dependent columns") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (
             |id INT,
             |ts TIMESTAMP,
             |name STRING,
             |arr ARRAY<INT>,
             |struct_col STRUCT<col1: INT, col2: STRING>,
             |ts_ltz TIMESTAMP_LTZ
             |)""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, TIMESTAMP "2026-01-01 12:00:00", "a", ARRAY(1, 2), STRUCT(10, 'x'),
             | TIMESTAMP "2026-01-01 12:00:00"),
             |(2, TIMESTAMP "2026-01-02 12:00:00", "b", ARRAY(3, 4), STRUCT(20, 'y'),
             | TIMESTAMP "2026-01-02 12:00:00")
             |""".stripMargin)

      // Projection reorders type-dependent columns (array, timestamp, struct)
      checkAnswer(
        sql(s"SELECT arr, ts, struct_col FROM $DEFAULT_DATABASE.t ORDER BY ts"),
        Row(Seq(1, 2), java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), Row(10, "x")) ::
          Row(Seq(3, 4), java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), Row(20, "y")) :: Nil
      )

      // Projection with timestamp_ltz at shifted ordinal
      checkAnswer(
        sql(s"SELECT ts_ltz, name FROM $DEFAULT_DATABASE.t ORDER BY name"),
        Row(java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), "a") ::
          Row(java.sql.Timestamp.valueOf("2026-01-02 12:00:00"), "b") :: Nil
      )
    }
  }

  test("Spark Read: nested data types table") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (
             |id INT,
             |arr ARRAY<INT>,
             |map MAP<STRING, INT>,
             |struct_col STRUCT<col1: INT, col2: STRING>
             |)""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, ARRAY(1, 2, 3), MAP("k1", 111, "k2", 222), STRUCT(100, 'nested_value')),
             |(2, ARRAY(7, 8, 9), MAP("k1", 333, "k2", 444), STRUCT(200, 'nested_value2'))
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY id"),
        Row(
          1,
          Seq(1, 2, 3),
          Map("k1" -> 111, "k2" -> 222),
          Row(100, "nested_value")
        ) :: Row(
          2,
          Seq(7, 8, 9),
          Map("k1" -> 333, "k2" -> 444),
          Row(200, "nested_value2")
        ) :: Nil
      )
    }
  }

  test("Spark Read: filter pushdown — equality on integer column") {
    withSampleTable {
      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE amount = 603 ORDER BY orderId")
      checkAnswer(query, Row(800L, 23L, 603, "addr3") :: Nil)
      assertPushedNames(query, Set("="))
    }
  }

  test("Spark Read: filter pushdown — range predicate on bigint column") {
    withSampleTable {
      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE orderId >= 900 ORDER BY orderId")
      checkAnswer(
        query,
        Row(900L, 24L, 604, "addr4") ::
          Row(1000L, 25L, 605, "addr5") :: Nil)
      assertPushedNames(query, Set(">="))
    }
  }

  test("Spark Read: filter pushdown — AND of two pushable predicates") {
    withSampleTable {
      val query = sql(s"""
                         |SELECT * FROM $DEFAULT_DATABASE.t
                         |WHERE orderId >= 700 AND amount < 605
                         |ORDER BY orderId""".stripMargin)
      checkAnswer(
        query,
        Row(700L, 22L, 602, "addr2") ::
          Row(800L, 23L, 603, "addr3") ::
          Row(900L, 24L, 604, "addr4") :: Nil)
      val pushed = pushedPredicates(query).map(_.name())
      assert(pushed.contains(">=") && pushed.contains("<"))
    }
  }

  test("Spark Read: filter pushdown — IS NULL / IS NOT NULL on string column") {
    withSampleTable {
      val query =
        sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.t WHERE address IS NOT NULL")
      checkAnswer(query, Row(5L) :: Nil)
      assertPushedNames(query, Set("IS_NOT_NULL"))
    }
  }

  test("Spark Read: filter pushdown — LIKE 'prefix%' is pushed as STARTS_WITH") {
    withSampleTable {
      val query =
        sql(s"SELECT orderId FROM $DEFAULT_DATABASE.t WHERE address LIKE 'addr%' ORDER BY orderId")
      checkAnswer(query, Row(600L) :: Row(700L) :: Row(800L) :: Row(900L) :: Row(1000L) :: Nil)
      assertPushedNames(query, Set("STARTS_WITH"))
    }
  }

  test("Spark Read: filter pushdown — IN on string column") {
    withSampleTable {
      val query = sql(s"""
                         |SELECT orderId FROM $DEFAULT_DATABASE.t
                         |WHERE address IN ('addr1', 'addr3')
                         |ORDER BY orderId""".stripMargin)
      checkAnswer(query, Row(600L) :: Row(800L) :: Nil)
      val pushed = pushedPredicates(query).map(_.name())
      // Spark may rewrite small INs as =/OR.
      assertThat(pushed.exists(name => name == "IN" || name == "=" || name == "OR")).isTrue
    }
  }

  test("Spark Read: filter pushdown — non-pushable predicate falls back to Spark") {
    withSampleTable {
      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE amount % 2 = 0 ORDER BY orderId")
      checkAnswer(
        query,
        Row(700L, 22L, 602, "addr2") ::
          Row(900L, 24L, 604, "addr4") :: Nil)
      val pushed = pushedPredicates(query).map(_.name())
      assertThat(pushed.exists(_ == "=")).isFalse
    }
  }

  test("Spark Read: filter pushdown — mixed pushable + non-pushable") {
    withSampleTable {
      val query = sql(s"""
                         |SELECT * FROM $DEFAULT_DATABASE.t
                         |WHERE orderId >= 700 AND amount % 2 = 0
                         |ORDER BY orderId""".stripMargin)
      checkAnswer(
        query,
        Row(700L, 22L, 602, "addr2") ::
          Row(900L, 24L, 604, "addr4") :: Nil)
      val pushed = pushedPredicates(query).map(_.name())
      assert(pushed.contains(">="))
    }
  }

  test("Spark Read: filter pushdown is skipped for non-ARROW log format") {
    withTable("indexed") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.indexed (
             |  orderId BIGINT,
             |  amount  INT
             |) TBLPROPERTIES('table.log.format' = 'indexed')""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.indexed VALUES
             |(600L, 601), (700L, 602), (800L, 603)""".stripMargin)

      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.indexed WHERE amount = 602 ORDER BY orderId")
      checkAnswer(query, Row(700L, 602) :: Nil)
      assert(pushedPredicates(query).isEmpty)
    }
  }

  test("Spark Read: filter pushdown — DATE and TIMESTAMP_NTZ literals") {
    withTable("typed") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.typed (
             |  id INT,
             |  dt DATE,
             |  ts TIMESTAMP_NTZ
             |)""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.typed VALUES
             |(1, DATE '2026-01-01', TIMESTAMP_NTZ '2026-01-01 12:00:00'),
             |(2, DATE '2026-01-02', TIMESTAMP_NTZ '2026-01-02 12:00:00'),
             |(3, DATE '2026-01-03', TIMESTAMP_NTZ '2026-01-03 12:00:00')
             |""".stripMargin)

      val byDate =
        sql(s"SELECT id FROM $DEFAULT_DATABASE.typed WHERE dt = DATE '2026-01-02'")
      checkAnswer(byDate, Row(2) :: Nil)
      assertPushedNames(byDate, Set("="))

      val byTs = sql(s"""SELECT id FROM $DEFAULT_DATABASE.typed
                        |WHERE ts >= TIMESTAMP_NTZ '2026-01-02 00:00:00'""".stripMargin)
      checkAnswer(byTs, Row(2) :: Row(3) :: Nil)
      assertPushedNames(byTs, Set(">="))
    }
  }

  test("Spark Read: filter pushdown on partitioned log table — data column") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (
             |  orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING
             |)
             |PARTITIONED BY (dt)""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(600L, 21L, 601, "addr1", "2026-01-01"), (700L, 22L, 602, "addr2", "2026-01-01"),
             |(800L, 23L, 603, "addr3", "2026-01-02"), (900L, 24L, 604, "addr4", "2026-01-02"),
             |(1000L, 25L, 605, "addr5", "2026-01-03")
             |""".stripMargin)

      val query =
        sql(s"SELECT orderId, amount, dt FROM $DEFAULT_DATABASE.t WHERE amount = 603")
      checkAnswer(query, Row(800L, 603, "2026-01-02") :: Nil)
      assertPushedNames(query, Set("="))
    }
  }

  private def withSampleTable(body: => Unit): Unit = withTable("t") {
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.t (
           |  orderId BIGINT,
           |  itemId  BIGINT,
           |  amount  INT,
           |  address STRING
           |)""".stripMargin)
    sql(s"""
           |INSERT INTO $DEFAULT_DATABASE.t VALUES
           |(600L, 21L, 601, 'addr1'), (700L, 22L, 602, 'addr2'),
           |(800L, 23L, 603, 'addr3'), (900L, 24L, 604, 'addr4'),
           |(1000L, 25L, 605, 'addr5')
           |""".stripMargin)
    body
  }

  private def pushedPredicates(df: DataFrame): Array[Predicate] = {
    // AQE hides the scan under an adaptive wrapper in executedPlan, so check optimizedPlan too.
    val scans =
      df.queryExecution.executedPlan.collect {
        case b: BatchScanExec => b.scan
      } ++ df.queryExecution.optimizedPlan.collect {
        case DataSourceV2ScanRelation(_, scan, _, _, _) => scan
      }
    scans
      .collect { case f: FlussAppendScan => f.pushedSparkPredicates }
      .flatten
      .toArray
  }

  private def assertPushedNames(df: DataFrame, expected: Set[String]): Unit = {
    val pushed = pushedPredicates(df).map(_.name()).toSet
    assert(
      expected.exists(pushed.contains),
      s"Expected any of $expected in pushed predicates, got $pushed")
  }

  test("Spark Read: log table scan metrics") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (id INT, name STRING)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')
             |""".stripMargin)

      val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.t")

      // Verify scan description and supportedCustomMetrics before execution
      val scan = df.queryExecution.optimizedPlan
        .collectFirst { case r: DataSourceV2ScanRelation => r }
        .get
        .scan
        .asInstanceOf[FlussScan]

      assert(scan.description().contains("FlussScan"))
      assert(scan.description().contains("Append"))
      assert(scan.supportedCustomMetrics().exists(_.name() == FlussMetrics.NUM_ROWS_READ))

      // Execute the query to trigger metric accumulation
      df.collect()

      // Verify numRowsRead is accumulated in BatchScanExec after execution
      val batchScanExec = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get

      val numRowsRead = batchScanExec.metrics(FlussMetrics.NUM_ROWS_READ).value
      assert(numRowsRead == 5L, s"Expected 5 rows read, got $numRowsRead")
    }
  }
}
