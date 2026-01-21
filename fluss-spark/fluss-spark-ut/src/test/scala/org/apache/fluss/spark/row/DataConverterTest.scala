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

import org.apache.fluss.row.{BinaryString, Decimal => FlussDecimal, GenericArray, GenericRow, TimestampLtz, TimestampNtz}
import org.apache.fluss.types.{ArrayType, CharType, DataTypes, DecimalType, LocalZonedTimestampType, RowType, TimestampType}

import org.apache.spark.sql.types.{Decimal => SparkDecimal}
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

class DataConverterTest extends AnyFunSuite {

  test("toSparkObject: null value") {
    val result = DataConverter.toSparkObject(null, DataTypes.INT)
    assertThat(result).isNull()
  }

  test("toSparkObject: primitive types") {
    // Integer
    val intValue = Integer.valueOf(42)
    val intResult = DataConverter.toSparkObject(intValue, DataTypes.INT)
    assertThat(intResult).isEqualTo(42)

    // Long
    val longValue = java.lang.Long.valueOf(12345L)
    val longResult = DataConverter.toSparkObject(longValue, DataTypes.BIGINT)
    assertThat(longResult).isEqualTo(12345L)

    // Boolean
    val boolValue = java.lang.Boolean.valueOf(true)
    val boolResult = DataConverter.toSparkObject(boolValue, DataTypes.BOOLEAN)
    assertThat(boolResult).isEqualTo(true)

    // Float
    val floatValue = java.lang.Float.valueOf(3.14f)
    val floatResult = DataConverter.toSparkObject(floatValue, DataTypes.FLOAT)
    assertThat(floatResult).isEqualTo(3.14f)

    // Double
    val doubleValue = java.lang.Double.valueOf(2.718)
    val doubleResult = DataConverter.toSparkObject(doubleValue, DataTypes.DOUBLE)
    assertThat(doubleResult).isEqualTo(2.718)
  }

  test("toSparkTimestamp: TimestampNtz") {
    val timestamp = TimestampNtz.fromMillis(1234567890123L)
    val result = DataConverter.toSparkTimestamp(timestamp)
    // Fluss stores in milliseconds, Spark uses microseconds
    assertThat(result).isEqualTo(1234567890123000L)
  }

  test("toSparkTimestamp: TimestampLtz") {
    val timestamp = TimestampLtz.fromEpochMillis(1234567890123L)
    val result = DataConverter.toSparkTimestamp(timestamp)
    assertThat(result).isEqualTo(1234567890123000L)
  }

  test("toSparkDecimal: positive decimal") {
    val flussDecimal = FlussDecimal.fromBigDecimal(new java.math.BigDecimal("123.45"), 5, 2)
    val sparkDecimal = DataConverter.toSparkDecimal(flussDecimal)

    assertThat(sparkDecimal).isInstanceOf(classOf[SparkDecimal])
    assertThat(sparkDecimal.toBigDecimal.doubleValue()).isEqualTo(123.45)
  }

  test("toSparkDecimal: negative decimal") {
    val flussDecimal = FlussDecimal.fromBigDecimal(new java.math.BigDecimal("-999.99"), 5, 2)
    val sparkDecimal = DataConverter.toSparkDecimal(flussDecimal)

    assertThat(sparkDecimal.toBigDecimal.doubleValue()).isEqualTo(-999.99)
  }

  test("toSparkDecimal: zero") {
    val flussDecimal = FlussDecimal.fromBigDecimal(java.math.BigDecimal.ZERO, 5, 2)
    val sparkDecimal = DataConverter.toSparkDecimal(flussDecimal)

    assertThat(sparkDecimal.toBigDecimal.doubleValue()).isEqualTo(0.0)
  }

  test("toSparkUTF8String: ASCII string") {
    val binaryString = BinaryString.fromString("Hello World")
    val utf8String = DataConverter.toSparkUTF8String(binaryString)

    assertThat(utf8String).isInstanceOf(classOf[UTF8String])
    assertThat(utf8String.toString).isEqualTo("Hello World")
  }

  test("toSparkUTF8String: UTF-8 Chinese characters") {
    val binaryString = BinaryString.fromString("你好世界")
    val utf8String = DataConverter.toSparkUTF8String(binaryString)

    assertThat(utf8String.toString).isEqualTo("你好世界")
  }

  test("toSparkUTF8String: empty string") {
    val binaryString = BinaryString.fromString("")
    val utf8String = DataConverter.toSparkUTF8String(binaryString)

    assertThat(utf8String.toString).isEqualTo("")
  }

  test("toSparkUTF8String: special characters") {
    val binaryString = BinaryString.fromString("Test\n\t\"Special\"")
    val utf8String = DataConverter.toSparkUTF8String(binaryString)

    assertThat(utf8String.toString).isEqualTo("Test\n\t\"Special\"")
  }

  test("toSparkArray: integer array") {
    val flussArray = new GenericArray(
      Array[Object](
        Integer.valueOf(1),
        Integer.valueOf(2),
        Integer.valueOf(3),
        Integer.valueOf(4),
        Integer.valueOf(5)))
    val arrayType = new ArrayType(DataTypes.INT)
    val sparkArray = DataConverter.toSparkArray(flussArray, arrayType)

    assertThat(sparkArray.numElements()).isEqualTo(5)
    assertThat(sparkArray.getInt(0)).isEqualTo(1)
    assertThat(sparkArray.getInt(4)).isEqualTo(5)
  }

  test("toSparkArray: string array") {
    val strings = Array[Object](
      BinaryString.fromString("a"),
      BinaryString.fromString("b"),
      BinaryString.fromString("c"))
    val flussArray = new GenericArray(strings)
    val arrayType = new ArrayType(DataTypes.STRING)
    val sparkArray = DataConverter.toSparkArray(flussArray, arrayType)

    assertThat(sparkArray.numElements()).isEqualTo(3)
    assertThat(sparkArray.getUTF8String(0).toString).isEqualTo("a")
    assertThat(sparkArray.getUTF8String(2).toString).isEqualTo("c")
  }

  test("toSparkArray: empty array") {
    val flussArray = new GenericArray(Array.empty[Object])
    val arrayType = new ArrayType(DataTypes.INT)
    val sparkArray = DataConverter.toSparkArray(flussArray, arrayType)

    assertThat(sparkArray.numElements()).isEqualTo(0)
  }

  test("toSparkArray: array with null elements") {
    val flussArray = new GenericArray(Array[Object](Integer.valueOf(1), null, Integer.valueOf(3)))
    val arrayType = new ArrayType(DataTypes.INT)
    val sparkArray = DataConverter.toSparkArray(flussArray, arrayType)

    assertThat(sparkArray.numElements()).isEqualTo(3)
    assertThat(sparkArray.getInt(0)).isEqualTo(1)
    assertThat(sparkArray.isNullAt(1)).isTrue()
    assertThat(sparkArray.getInt(2)).isEqualTo(3)
  }

  test("toSparkInternalRow: simple row") {
    val rowType = RowType
      .builder()
      .field("id", DataTypes.INT)
      .field("name", DataTypes.STRING)
      .field("age", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(3)
    flussRow.setField(0, Integer.valueOf(1))
    flussRow.setField(1, BinaryString.fromString("Alice"))
    flussRow.setField(2, Integer.valueOf(25))

    val sparkRow = DataConverter.toSparkInternalRow(flussRow, rowType)

    assertThat(sparkRow.numFields).isEqualTo(3)
    assertThat(sparkRow.getInt(0)).isEqualTo(1)
    assertThat(sparkRow.getUTF8String(1).toString).isEqualTo("Alice")
    assertThat(sparkRow.getInt(2)).isEqualTo(25)
  }

  test("toSparkInternalRow: row with null fields") {
    val rowType = RowType
      .builder()
      .field("id", DataTypes.INT)
      .field("name", DataTypes.STRING)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, Integer.valueOf(1))
    flussRow.setField(1, null)

    val sparkRow = DataConverter.toSparkInternalRow(flussRow, rowType)

    assertThat(sparkRow.getInt(0)).isEqualTo(1)
    assertThat(sparkRow.isNullAt(1)).isTrue()
  }

  test("toSparkInternalRow: nested row") {
    val innerRowType = RowType
      .builder()
      .field("city", DataTypes.STRING)
      .field("code", DataTypes.INT)
      .build()

    val outerRowType = RowType
      .builder()
      .field("id", DataTypes.INT)
      .field("address", innerRowType)
      .build()

    val innerRow = new GenericRow(2)
    innerRow.setField(0, BinaryString.fromString("Beijing"))
    innerRow.setField(1, Integer.valueOf(100))

    val outerRow = new GenericRow(2)
    outerRow.setField(0, Integer.valueOf(1))
    outerRow.setField(1, innerRow)

    val sparkRow = DataConverter.toSparkInternalRow(outerRow, outerRowType)

    assertThat(sparkRow.getInt(0)).isEqualTo(1)
    val sparkInnerRow = sparkRow.getStruct(1, 2)
    assertThat(sparkInnerRow.getUTF8String(0).toString).isEqualTo("Beijing")
    assertThat(sparkInnerRow.getInt(1)).isEqualTo(100)
  }

  test("toSparkObject: CHAR type") {
    val binaryString = BinaryString.fromString("test")
    val charType = new CharType(10)
    val result = DataConverter.toSparkObject(binaryString, charType)

    assertThat(result).isInstanceOf(classOf[UTF8String])
    assertThat(result.asInstanceOf[UTF8String].toString).isEqualTo("test")
  }

  test("toSparkObject: DECIMAL type") {
    val flussDecimal = FlussDecimal.fromBigDecimal(new java.math.BigDecimal("12.34"), 4, 2)
    val decimalType = new DecimalType(4, 2)
    val result = DataConverter.toSparkObject(flussDecimal, decimalType)

    assertThat(result).isInstanceOf(classOf[SparkDecimal])
  }

  test("toSparkObject: TIMESTAMP_WITHOUT_TIME_ZONE type") {
    val timestamp = TimestampNtz.fromMillis(1000000L)
    val timestampType = new TimestampType(3)
    val result = DataConverter.toSparkObject(timestamp, timestampType)

    assertThat(result).isInstanceOf(classOf[java.lang.Long])
    assertThat(result.asInstanceOf[Long]).isEqualTo(1000000000L) // microseconds
  }

  test("toSparkObject: TIMESTAMP_WITH_LOCAL_TIME_ZONE type") {
    val timestamp = TimestampLtz.fromEpochMillis(2000000L)
    val timestampType = new LocalZonedTimestampType(3)
    val result = DataConverter.toSparkObject(timestamp, timestampType)

    assertThat(result).isInstanceOf(classOf[java.lang.Long])
    assertThat(result.asInstanceOf[Long]).isEqualTo(2000000000L) // microseconds
  }

  test("toSparkObject: ROW type") {
    val rowType = RowType
      .builder()
      .field("x", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(1)
    flussRow.setField(0, Integer.valueOf(42))

    val result = DataConverter.toSparkObject(flussRow, rowType)

    assertThat(result).isNotNull()
    assertThat(result.asInstanceOf[FlussAsSparkRow].getInt(0)).isEqualTo(42)
  }

  test("toSparkMap: unsupported") {
    assertThrows[UnsupportedOperationException] {
      DataConverter.toSparkMap(null, null)
    }
  }
}
