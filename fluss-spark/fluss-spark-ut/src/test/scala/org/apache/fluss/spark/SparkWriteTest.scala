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

import org.apache.fluss.metadata.{Schema, TableDescriptor}
import org.apache.fluss.row.{BinaryString, GenericRow, InternalRow}
import org.apache.fluss.spark.util.TestUtils.{createGenericRow, FLUSS_ROWTYPE}
import org.apache.fluss.types.DataTypes

import org.assertj.core.api.Assertions.assertThat

import java.sql.Timestamp
import java.time.Duration

import scala.collection.JavaConverters._

class SparkWriteTest extends FlussSparkTestBase {

  import SparkWriteTest._

  test("Fluss Write: all data types") {
    val tablePath = createTablePath("test_all_data_types")
    val tableDescriptor: TableDescriptor = TableDescriptor.builder
      .schema(Schema.newBuilder().fromRowType(FLUSS_ROWTYPE).build)
      .build()
    createFlussTable(tablePath, tableDescriptor)

    spark.sql(s"""
                 |INSERT INTO $DEFAULT_DATABASE.test_all_data_types
                 |VALUES (
                 |  true, 1, 10, 100, 1000L, 12.3F, 45.6D,
                 |  1234567.89, 12345678900987654321.12,
                 |  "test",
                 |  TO_TIMESTAMP('2025-12-31 10:00:00', 'yyyy-MM-dd kk:mm:ss'),
                 |  array(11.11F, 22.22F), struct(123L, "apache fluss")
                 |)
                 |""".stripMargin)

    val table = loadFlussTable(tablePath)
    val rows = getRowsWithChangeType(table).map(_._2)
    assertThat(rows.length).isEqualTo(1)

    val row = rows.head
    assertThat(row.getFieldCount).isEqualTo(13)
    assertThat(row.getBoolean(0)).isEqualTo(true)
    assertThat(row.getByte(1)).isEqualTo(1.toByte)
    assertThat(row.getShort(2)).isEqualTo(10.toShort)
    assertThat(row.getInt(3)).isEqualTo(100)
    assertThat(row.getLong(4)).isEqualTo(1000L)
    assertThat(row.getFloat(5)).isEqualTo(12.3f)
    assertThat(row.getDouble(6)).isEqualTo(45.6)
    assertThat(row.getDecimal(7, 10, 2).toBigDecimal).isEqualTo(BigDecimal("1234567.89").bigDecimal)
    assertThat(row.getDecimal(8, 38, 2).toBigDecimal)
      .isEqualTo(BigDecimal("12345678900987654321.12").bigDecimal)
    assertThat(row.getString(9).toString).isEqualTo("test")
    assertThat(row.getTimestampLtz(10, 6).toInstant)
      .isEqualTo(Timestamp.valueOf("2025-12-31 10:00:00.0").toInstant)
    assertThat(row.getArray(11).toFloatArray).containsExactly(Array(11.11f, 22.22f): _*)
    val nestedRow = row.getRow(12, 2)
    assertThat(nestedRow.getFieldCount).isEqualTo(2)
    assertThat(nestedRow.getLong(0)).isEqualTo(123L)
    assertThat(nestedRow.getString(1).toString).isEqualTo("apache fluss")
  }

  test("Fluss Write: log table") {
    val tablePath = createTablePath(logTableName)
    createFlussTable(tablePath, logTableDescriptor)

    sql(s"""
           |INSERT INTO $DEFAULT_DATABASE.$logTableName VALUES
           |(600L, 21L, 601, "addr1"), (700L, 22L, 602, "addr2"),
           |(800L, 23L, 603, "addr3"), (900L, 24L, 604, "addr4"),
           |(1000L, 25L, 605, "addr5")
           |""".stripMargin)

    val table = loadFlussTable(tablePath)
    val flussRows = getRowsWithChangeType(table).map(_._2)

    val expectRows = Array(
      createGenericRow(600L, 21L, 601, BinaryString.fromString("addr1")),
      createGenericRow(700L, 22L, 602, BinaryString.fromString("addr2")),
      createGenericRow(800L, 23L, 603, BinaryString.fromString("addr3")),
      createGenericRow(900L, 24L, 604, BinaryString.fromString("addr4")),
      createGenericRow(1000L, 25L, 605, BinaryString.fromString("addr5"))
    )
    assertThat(flussRows.length).isEqualTo(5)
    assertThat(flussRows).containsAll(expectRows.toIterable.asJava)
  }

  test("Fluss Write: upsert table") {
    val tablePath = createTablePath(pkTableName)
    createFlussTable(tablePath, pkTableDescriptor)
    val table = loadFlussTable(tablePath)
    val logScanner = table.newScan.createLogScanner
    (0 until table.getTableInfo.getNumBuckets).foreach(i => logScanner.subscribeFromBeginning(i))

    // insert data
    sql(s"""
           |INSERT INTO $DEFAULT_DATABASE.$pkTableName VALUES
           |(600L, 21L, 601, "addr1"), (700L, 22L, 602, "addr2"),
           |(800L, 23L, 603, "addr3"), (900L, 24L, 604, "addr4"),
           |(1000L, 25L, 605, "addr5")
           |""".stripMargin)

    val flussRows1 = getRowsWithChangeType(table, Some(logScanner))
    val expectRows1 = Array(
      ("+I", createGenericRow(600L, 21L, 601, BinaryString.fromString("addr1"))),
      ("+I", createGenericRow(700L, 22L, 602, BinaryString.fromString("addr2"))),
      ("+I", createGenericRow(800L, 23L, 603, BinaryString.fromString("addr3"))),
      ("+I", createGenericRow(900L, 24L, 604, BinaryString.fromString("addr4"))),
      ("+I", createGenericRow(1000L, 25L, 605, BinaryString.fromString("addr5")))
    )
    assertThat(flussRows1.length).isEqualTo(5)
    assertThat(flussRows1).containsAll(expectRows1.toIterable.asJava)

    // update data
    sql(s"""
           |INSERT INTO $DEFAULT_DATABASE.$pkTableName
           |VALUES (800L, 230L, 603, "addr3"), (900L, 240L, 604, "addr4")
           |""".stripMargin)

    val flussRows2 = getRowsWithChangeType(table, Some(logScanner))
    val expectRows2 = Array(
      ("-U", createGenericRow(800L, 23L, 603, BinaryString.fromString("addr3"))),
      ("+U", createGenericRow(800L, 230L, 603, BinaryString.fromString("addr3"))),
      ("-U", createGenericRow(900L, 24L, 604, BinaryString.fromString("addr4"))),
      ("+U", createGenericRow(900L, 240L, 604, BinaryString.fromString("addr4")))
    )
    assertThat(flussRows2.length).isEqualTo(4)
    assertThat(flussRows2).containsAll(expectRows2.toIterable.asJava)
  }
}

object SparkWriteTest {

  val pkTableName: String = "orders_test_pk"
  val pkSchema: Schema = Schema.newBuilder
    .column("orderId", DataTypes.BIGINT)
    .column("itemId", DataTypes.BIGINT)
    .column("amount", DataTypes.INT)
    .column("address", DataTypes.STRING)
    .primaryKey("orderId")
    .build
  val pkTableDescriptor: TableDescriptor =
    TableDescriptor.builder.schema(pkSchema).distributedBy(1, "orderId").build

  val logTableName: String = "orders_test_log"
  val logSchema: Schema = Schema.newBuilder
    .column("orderId", DataTypes.BIGINT)
    .column("itemId", DataTypes.BIGINT)
    .column("amount", DataTypes.INT)
    .column("address", DataTypes.STRING)
    .build
  val logTableDescriptor: TableDescriptor = TableDescriptor.builder.schema(logSchema).build

  case class GenericRowBuilder(fieldCount: Int) {
    val row = new GenericRow(fieldCount)

    def setField(pos: Int, value: Any): GenericRowBuilder = {
      row.setField(pos, value)
      this
    }

    def builder(): InternalRow = {
      row
    }
  }
}
