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

import org.apache.spark.sql.Row

class SparkReadTest extends FlussSparkTestBase {

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

  test("Spark Read: primary key table") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (orderId BIGINT, itemId BIGINT, amount INT, address STRING)
             |TBLPROPERTIES("primary.key" = "orderId")
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
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

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(700L, 220L, 602, "addr2"),
             |(900L, 240L, 604, "addr4"),
             |(1100L, 260L, 606, "addr6")
             |""".stripMargin)

      checkAnswer(
        sql(s"""
               |SELECT orderId, itemId, address FROM $DEFAULT_DATABASE.t
               |WHERE amount <= 603 ORDER BY orderId""".stripMargin),
        Row(600L, 21L, "addr1") ::
          Row(700L, 220L, "addr2") ::
          Row(800L, 23L, "addr3") ::
          Nil
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

  test("Spark Read: partitioned primary key table") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (orderId BIGINT, itemId BIGINT, amount INT, address STRING, dt STRING)
             |PARTITIONED BY (dt)
             |TBLPROPERTIES("primary.key" = "orderId,dt")
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(600L, 21L, 601, "addr1", "2026-01-01"), (700L, 22L, 602, "addr2", "2026-01-01"),
             |(800L, 23L, 603, "addr3", "2026-01-02"), (900L, 24L, 604, "addr4", "2026-01-02"),
             |(1000L, 25L, 605, "addr5", "2026-01-03")
             |""".stripMargin)
      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(700L, 220L, 602, "addr2_updated", "2026-01-01"),
             |(900L, 240L, 604, "addr4_updated", "2026-01-02"),
             |(1100L, 260L, 606, "addr6", "2026-01-03")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY orderId"),
        Row(600L, 21L, 601, "addr1", "2026-01-01") ::
          Row(700L, 220L, 602, "addr2_updated", "2026-01-01") ::
          Row(800L, 23L, 603, "addr3", "2026-01-02") ::
          Row(900L, 240L, 604, "addr4_updated", "2026-01-02") ::
          Row(1000L, 25L, 605, "addr5", "2026-01-03") ::
          Row(1100L, 260L, 606, "addr6", "2026-01-03") ::
          Nil
      )

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

  test("Spark Read: nested data types table") {
    withTable("t") {
      // TODO: support map type
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (
             |id INT,
             |arr ARRAY<INT>,
             |struct_col STRUCT<col1: INT, col2: STRING>
             |)""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, ARRAY(1, 2, 3), STRUCT(100, 'nested_value')),
             |(2, ARRAY(7, 8, 9), STRUCT(200, 'nested_value2'))
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY id"),
        Row(
          1,
          Seq(1, 2, 3),
          Row(100, "nested_value")
        ) :: Row(
          2,
          Seq(7, 8, 9),
          Row(200, "nested_value2")
        ) :: Nil
      )
    }
  }
}
