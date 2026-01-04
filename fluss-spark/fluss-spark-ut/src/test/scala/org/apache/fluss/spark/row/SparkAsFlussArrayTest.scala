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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{BooleanType, ByteType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.assertThat

class SparkAsFlussArrayTest extends FlussSparkTestBase {

  test("Fluss SparkAsFlussArray: Boolean Type") {

    val data = Array(true, false, false, true)
    val sparkArrayData = new GenericArrayData(data)
    val flussArray = new SparkAsFlussArray(sparkArrayData, BooleanType)

    assertThat(flussArray.size()).isEqualTo(4)
    assertThat(flussArray.toBooleanArray).containsExactly(data: _*)
    (1 until data.length).foreach(i => assertThat(flussArray.getBoolean(i)).isEqualTo(data(i)))
  }

  test("Fluss SparkAsFlussArray: Byte Type") {
    val data = Array(12.toByte, 34.toByte, 56.toByte, 78.toByte)
    val sparkArrayData = new GenericArrayData(data)
    val flussArray = new SparkAsFlussArray(sparkArrayData, ByteType)

    assertThat(flussArray.size()).isEqualTo(4)
    assertThat(flussArray.toByteArray).containsExactly(data: _*)
    (1 until data.length).foreach(i => assertThat(flussArray.getByte(i)).isEqualTo(data(i)))
  }

  test("Fluss SparkAsFlussArray: Short Type") {
    val data = Array(12.toShort, 34.toShort, 56.toShort, 78.toShort)
    val sparkArrayData = new GenericArrayData(data)
    val flussArray = new SparkAsFlussArray(sparkArrayData, ShortType)

    assertThat(flussArray.size()).isEqualTo(4)
    assertThat(flussArray.toShortArray).containsExactly(data: _*)
    (1 until data.length).foreach(i => assertThat(flussArray.getShort(i)).isEqualTo(data(i)))
  }

  test("Fluss SparkAsFlussArray: Int Type") {
    val data = Array(1234, 3456, 5678, 7890)
    val sparkArrayData = new GenericArrayData(data)
    val flussArray = new SparkAsFlussArray(sparkArrayData, IntegerType)

    assertThat(flussArray.size()).isEqualTo(4)
    assertThat(flussArray.toIntArray).containsExactly(data: _*)
    (1 until data.length).foreach(i => assertThat(flussArray.getInt(i)).isEqualTo(data(i)))
  }

  test("Fluss SparkAsFlussArray: Long Type") {
    val data = Array(1234567890L, 3456789012L, 5678901234L, 7890123456L)
    val sparkArrayData = new GenericArrayData(data)
    val flussArray = new SparkAsFlussArray(sparkArrayData, LongType)

    assertThat(flussArray.size()).isEqualTo(4)
    assertThat(flussArray.toLongArray).containsExactly(data: _*)
    (1 until data.length).foreach(i => assertThat(flussArray.getLong(i)).isEqualTo(data(i)))
  }

  test("Fluss SparkAsFlussArray: Float Type") {
    val data = Array(123456.78f, 345678.90f, 567890.12f, 789012.34f)
    val sparkArrayData = new GenericArrayData(data)
    val flussArray = new SparkAsFlussArray(sparkArrayData, FloatType)

    assertThat(flussArray.size()).isEqualTo(4)
    assertThat(flussArray.toFloatArray).containsExactly(data: _*)
    (1 until data.length).foreach(i => assertThat(flussArray.getFloat(i)).isEqualTo(data(i)))
  }

  test("Fluss SparkAsFlussArray: Double Type") {
    val data = Array(1234567890.12, 3456789012.34, 5678901234.56, 7890123456.78)
    val sparkArrayData = new GenericArrayData(data)
    val flussArray = new SparkAsFlussArray(sparkArrayData, DoubleType)

    assertThat(flussArray.size()).isEqualTo(4)
    assertThat(flussArray.toDoubleArray).containsExactly(data: _*)
    (1 until data.length).foreach(i => assertThat(flussArray.getDouble(i)).isEqualTo(data(i)))
  }

  test("Fluss SparkAsFlussArray: Struct Type") {
    val schema = StructType(
      Array(
        StructField("id", LongType),
        StructField("salary", new DecimalType(10, 2)),
        StructField("pt", StringType)
      ))
    val data = Array(
      InternalRow.apply(1L, Decimal(BigDecimal("123.4")), UTF8String.fromString("apache")),
      null,
      InternalRow.apply(2L, Decimal(BigDecimal("567.8")), UTF8String.fromString("fluss"))
    )
    val flussRow = data.map(new SparkAsFlussRow(schema).replace)

    val sparkArrayData = new GenericArrayData(data)
    val flussArray = new SparkAsFlussArray(sparkArrayData, schema)

    assertThat(flussArray.size()).isEqualTo(3)
    (1 until data.length).foreach {
      i =>
        val isNull = if (i == 1) true else false
        assertThat(flussArray.isNullAt(i)).isEqualTo(isNull)
        if (!isNull) {
          assertThat(flussArray.getRow(i, 3).getLong(0)).isEqualTo(flussRow(i).getLong(0))
          assertThat(flussArray.getRow(i, 3).getDecimal(1, 10, 2).toBigDecimal)
            .isEqualTo(flussRow(i).getDecimal(1, 10, 2).toBigDecimal)
          assertThat(flussArray.getRow(i, 3).getString(2).toString)
            .isEqualTo(flussRow(i).getString(2).toString)
        }
    }
  }
}
