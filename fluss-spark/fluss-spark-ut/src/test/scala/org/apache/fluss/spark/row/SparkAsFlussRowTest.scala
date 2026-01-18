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

package org.apache.fluss.spark.row

import org.apache.fluss.spark.FlussSparkTestBase
import org.apache.fluss.spark.util.TestUtils.SCHEMA

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.assertThat

import java.sql.Timestamp

class SparkAsFlussRowTest extends FlussSparkTestBase {

  private var row: SparkAsFlussRow = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data = InternalRow.fromSeq(
      Seq(
        true,
        1.toByte,
        10.toShort,
        100,
        1000L,
        12.3f,
        45.6d,
        Decimal(BigDecimal("1234567.89")),
        Decimal(BigDecimal("12345678900987654321.12")),
        UTF8String.fromString("test"),
        Timestamp.valueOf("2025-12-31 10:00:00").getTime * 1000,
        new GenericArrayData(Array(11.11f, 22.22f)),
        ArrayBasedMapData.apply(
          Array(UTF8String.fromString("k1"), UTF8String.fromString("k2")).asInstanceOf[Array[Any]],
          Array(111, 222)),
        InternalRow.apply(123L, UTF8String.fromString("apache fluss"))
      ))
    row = new SparkAsFlussRow(SCHEMA).replace(data)
  }

  test("Fluss SparkAsFlussRow") {
    assertThat(row.fieldCount).isEqualTo(14)

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

    // test array type
    assertThat(row.getArray(11).toFloatArray).containsExactly(Array(11.11f, 22.22f): _*)

    // test map type
    assertThat(row.getMap(12).size()).isEqualTo(2)
    assertThat(row.getMap(12).keyArray().getString(0).toString).isEqualTo("k1")
    assertThat(row.getMap(12).keyArray().getString(1).toString).isEqualTo("k2")
    assertThat(row.getMap(12).valueArray().toIntArray).containsExactly(Array(111, 222): _*)

    // test row type
    val nestedRow = row.getRow(13, 2)
    assertThat(nestedRow.getFieldCount).isEqualTo(2)
    assertThat(nestedRow.getLong(0)).isEqualTo(123L)
    assertThat(nestedRow.getString(1).toString).isEqualTo("apache fluss")
  }
}
