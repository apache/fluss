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
import org.apache.fluss.types.{ArrayType, BinaryType, DataTypes, LocalZonedTimestampType, RowType, TimestampType}

import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

class FlussAsSparkRowTest extends AnyFunSuite {

  test("basic row operations: numFields and fieldCount") {
    val rowType = RowType
      .builder()
      .field("id", DataTypes.INT)
      .field("name", DataTypes.STRING)
      .field("age", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(3)
    flussRow.setField(0, Integer.valueOf(1))
    flussRow.setField(1, BinaryString.fromString("Alice"))
    flussRow.setField(2, Integer.valueOf(30))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.numFields).isEqualTo(3)
    assertThat(sparkRow.fieldCount).isEqualTo(3)
  }

  test("isNullAt: check null fields") {
    val rowType = RowType
      .builder()
      .field("col1", DataTypes.INT)
      .field("col2", DataTypes.STRING)
      .field("col3", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(3)
    flussRow.setField(0, Integer.valueOf(1))
    flussRow.setField(1, null)
    flussRow.setField(2, Integer.valueOf(3))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.isNullAt(0)).isFalse()
    assertThat(sparkRow.isNullAt(1)).isTrue()
    assertThat(sparkRow.isNullAt(2)).isFalse()
  }

  test("getBoolean: read boolean values") {
    val rowType = RowType
      .builder()
      .field("flag1", DataTypes.BOOLEAN)
      .field("flag2", DataTypes.BOOLEAN)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, java.lang.Boolean.TRUE)
    flussRow.setField(1, java.lang.Boolean.FALSE)

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getBoolean(0)).isTrue()
    assertThat(sparkRow.getBoolean(1)).isFalse()
  }

  test("getByte: read byte values") {
    val rowType = RowType
      .builder()
      .field("b1", DataTypes.TINYINT)
      .field("b2", DataTypes.TINYINT)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, java.lang.Byte.valueOf(10.toByte))
    flussRow.setField(1, java.lang.Byte.valueOf((-5).toByte))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getByte(0)).isEqualTo(10.toByte)
    assertThat(sparkRow.getByte(1)).isEqualTo((-5).toByte)
  }

  test("getShort: read short values") {
    val rowType = RowType
      .builder()
      .field("s1", DataTypes.SMALLINT)
      .field("s2", DataTypes.SMALLINT)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, java.lang.Short.valueOf(100.toShort))
    flussRow.setField(1, java.lang.Short.valueOf((-200).toShort))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getShort(0)).isEqualTo(100.toShort)
    assertThat(sparkRow.getShort(1)).isEqualTo((-200).toShort)
  }

  test("getInt: read integer values") {
    val rowType = RowType
      .builder()
      .field("num1", DataTypes.INT)
      .field("num2", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, Integer.valueOf(42))
    flussRow.setField(1, Integer.valueOf(-999))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getInt(0)).isEqualTo(42)
    assertThat(sparkRow.getInt(1)).isEqualTo(-999)
  }

  test("getLong: read BIGINT values") {
    val rowType = RowType
      .builder()
      .field("big1", DataTypes.BIGINT)
      .field("big2", DataTypes.BIGINT)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, java.lang.Long.valueOf(123456789L))
    flussRow.setField(1, java.lang.Long.valueOf(-987654321L))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getLong(0)).isEqualTo(123456789L)
    assertThat(sparkRow.getLong(1)).isEqualTo(-987654321L)
  }

  test("getLong: read TIMESTAMP_WITHOUT_TIME_ZONE") {
    val timestampType = new TimestampType(3)
    val rowType = RowType
      .builder()
      .field("ts", timestampType)
      .build()

    val timestamp = TimestampNtz.fromMillis(1234567890L)
    val flussRow = new GenericRow(1)
    flussRow.setField(0, timestamp)

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    // Spark expects microseconds
    assertThat(sparkRow.getLong(0)).isEqualTo(1234567890000L)
  }

  test("getLong: read TIMESTAMP_WITH_LOCAL_TIME_ZONE") {
    val timestampType = new LocalZonedTimestampType(3)
    val rowType = RowType
      .builder()
      .field("ts_ltz", timestampType)
      .build()

    val timestamp = TimestampLtz.fromEpochMillis(9876543210L)
    val flussRow = new GenericRow(1)
    flussRow.setField(0, timestamp)

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    // Spark expects microseconds
    assertThat(sparkRow.getLong(0)).isEqualTo(9876543210000L)
  }

  test("getFloat: read float values") {
    val rowType = RowType
      .builder()
      .field("f1", DataTypes.FLOAT)
      .field("f2", DataTypes.FLOAT)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, java.lang.Float.valueOf(3.14f))
    flussRow.setField(1, java.lang.Float.valueOf(-2.5f))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getFloat(0)).isEqualTo(3.14f)
    assertThat(sparkRow.getFloat(1)).isEqualTo(-2.5f)
  }

  test("getDouble: read double values") {
    val rowType = RowType
      .builder()
      .field("d1", DataTypes.DOUBLE)
      .field("d2", DataTypes.DOUBLE)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, java.lang.Double.valueOf(2.718281828))
    flussRow.setField(1, java.lang.Double.valueOf(-1.414))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getDouble(0)).isEqualTo(2.718281828)
    assertThat(sparkRow.getDouble(1)).isEqualTo(-1.414)
  }

  test("getDecimal: read decimal values") {
    val rowType = RowType
      .builder()
      .field("price", DataTypes.DECIMAL(10, 2))
      .build()

    val decimal = FlussDecimal.fromBigDecimal(new java.math.BigDecimal("123.45"), 10, 2)
    val flussRow = new GenericRow(1)
    flussRow.setField(0, decimal)

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    val sparkDecimal = sparkRow.getDecimal(0, 10, 2)
    assertThat(sparkDecimal.toBigDecimal.doubleValue()).isEqualTo(123.45)
  }

  test("getUTF8String: read string values") {
    val rowType = RowType
      .builder()
      .field("name", DataTypes.STRING)
      .field("desc", DataTypes.STRING)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, BinaryString.fromString("Hello"))
    flussRow.setField(1, BinaryString.fromString("World"))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getUTF8String(0).toString).isEqualTo("Hello")
    assertThat(sparkRow.getUTF8String(1).toString).isEqualTo("World")
  }

  test("getUTF8String: read UTF-8 characters") {
    val rowType = RowType
      .builder()
      .field("chinese", DataTypes.STRING)
      .field("emoji", DataTypes.STRING)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, BinaryString.fromString("你好世界"))
    flussRow.setField(1, BinaryString.fromString("😊🎉"))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getUTF8String(0).toString).isEqualTo("你好世界")
    assertThat(sparkRow.getUTF8String(1).toString).isEqualTo("😊🎉")
  }

  test("getBinary: read binary values") {
    val binaryType = new BinaryType(5)
    val rowType = RowType
      .builder()
      .field("data", binaryType)
      .build()

    val binaryData = Array[Byte](1, 2, 3, 4, 5)
    val flussRow = new GenericRow(1)
    flussRow.setField(0, binaryData)

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    val result = sparkRow.getBinary(0)
    assertThat(result).isEqualTo(binaryData)
  }

  test("getStruct: read nested row") {
    val innerRowType = RowType
      .builder()
      .field("city", DataTypes.STRING)
      .field("zipcode", DataTypes.INT)
      .build()

    val outerRowType = RowType
      .builder()
      .field("id", DataTypes.INT)
      .field("address", innerRowType)
      .build()

    val innerRow = new GenericRow(2)
    innerRow.setField(0, BinaryString.fromString("Beijing"))
    innerRow.setField(1, Integer.valueOf(100000))

    val outerRow = new GenericRow(2)
    outerRow.setField(0, Integer.valueOf(1))
    outerRow.setField(1, innerRow)

    val sparkRow = new FlussAsSparkRow(outerRowType).replace(outerRow)

    assertThat(sparkRow.getInt(0)).isEqualTo(1)

    val innerSparkRow = sparkRow.getStruct(1, 2)
    assertThat(innerSparkRow.getUTF8String(0).toString).isEqualTo("Beijing")
    assertThat(innerSparkRow.getInt(1)).isEqualTo(100000)
  }

  test("getArray: read array field") {
    val arrayType = new ArrayType(DataTypes.INT)
    val rowType = RowType
      .builder()
      .field("numbers", arrayType)
      .build()

    val array = new GenericArray(
      Array[Object](
        Integer.valueOf(1),
        Integer.valueOf(2),
        Integer.valueOf(3),
        Integer.valueOf(4),
        Integer.valueOf(5)))
    val flussRow = new GenericRow(1)
    flussRow.setField(0, array)

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    val sparkArray = sparkRow.getArray(0)
    assertThat(sparkArray.numElements()).isEqualTo(5)
    assertThat(sparkArray.getInt(0)).isEqualTo(1)
    assertThat(sparkArray.getInt(4)).isEqualTo(5)
  }

  test("getMap: unsupported operation") {
    val rowType = RowType
      .builder()
      .field("dummy", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(1)
    flussRow.setField(0, Integer.valueOf(1))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThrows[UnsupportedOperationException] {
      sparkRow.getMap(0)
    }
  }

  test("getInterval: unsupported operation") {
    val rowType = RowType
      .builder()
      .field("dummy", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(1)
    flussRow.setField(0, Integer.valueOf(1))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThrows[UnsupportedOperationException] {
      sparkRow.getInterval(0)
    }
  }

  test("setNullAt: unsupported operation") {
    val rowType = RowType
      .builder()
      .field("col", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(1)
    flussRow.setField(0, Integer.valueOf(1))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThrows[UnsupportedOperationException] {
      sparkRow.setNullAt(0)
    }
  }

  test("update: unsupported operation") {
    val rowType = RowType
      .builder()
      .field("col", DataTypes.INT)
      .build()

    val flussRow = new GenericRow(1)
    flussRow.setField(0, Integer.valueOf(1))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThrows[UnsupportedOperationException] {
      sparkRow.update(0, Integer.valueOf(2))
    }
  }

  test("copy: creates deep copy") {
    val rowType = RowType
      .builder()
      .field("id", DataTypes.INT)
      .field("name", DataTypes.STRING)
      .build()

    val flussRow = new GenericRow(2)
    flussRow.setField(0, Integer.valueOf(1))
    flussRow.setField(1, BinaryString.fromString("Original"))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)
    val copiedRow = sparkRow.copy()

    assertThat(copiedRow.getInt(0)).isEqualTo(1)
    assertThat(copiedRow.getUTF8String(1).toString).isEqualTo("Original")

    // Verify it's a different instance
    assertThat(copiedRow).isNotSameAs(sparkRow)
  }

  test("replace: reuses wrapper instance") {
    val rowType = RowType
      .builder()
      .field("val", DataTypes.INT)
      .build()

    val row1 = new GenericRow(1)
    row1.setField(0, Integer.valueOf(1))

    val row2 = new GenericRow(1)
    row2.setField(0, Integer.valueOf(2))

    val sparkRow = new FlussAsSparkRow(rowType)
    sparkRow.replace(row1)
    assertThat(sparkRow.getInt(0)).isEqualTo(1)

    sparkRow.replace(row2)
    assertThat(sparkRow.getInt(0)).isEqualTo(2)
  }

  test("complex row: all data types") {
    val rowType = RowType
      .builder()
      .field("bool_col", DataTypes.BOOLEAN)
      .field("byte_col", DataTypes.TINYINT)
      .field("short_col", DataTypes.SMALLINT)
      .field("int_col", DataTypes.INT)
      .field("long_col", DataTypes.BIGINT)
      .field("float_col", DataTypes.FLOAT)
      .field("double_col", DataTypes.DOUBLE)
      .field("string_col", DataTypes.STRING)
      .field("decimal_col", DataTypes.DECIMAL(10, 2))
      .build()

    val flussRow = new GenericRow(9)
    flussRow.setField(0, java.lang.Boolean.TRUE)
    flussRow.setField(1, java.lang.Byte.valueOf(10.toByte))
    flussRow.setField(2, java.lang.Short.valueOf(100.toShort))
    flussRow.setField(3, Integer.valueOf(1000))
    flussRow.setField(4, java.lang.Long.valueOf(10000L))
    flussRow.setField(5, java.lang.Float.valueOf(3.14f))
    flussRow.setField(6, java.lang.Double.valueOf(2.718))
    flussRow.setField(7, BinaryString.fromString("test"))
    flussRow.setField(8, FlussDecimal.fromBigDecimal(new java.math.BigDecimal("99.99"), 10, 2))

    val sparkRow = new FlussAsSparkRow(rowType).replace(flussRow)

    assertThat(sparkRow.getBoolean(0)).isTrue()
    assertThat(sparkRow.getByte(1)).isEqualTo(10.toByte)
    assertThat(sparkRow.getShort(2)).isEqualTo(100.toShort)
    assertThat(sparkRow.getInt(3)).isEqualTo(1000)
    assertThat(sparkRow.getLong(4)).isEqualTo(10000L)
    assertThat(sparkRow.getFloat(5)).isEqualTo(3.14f)
    assertThat(sparkRow.getDouble(6)).isEqualTo(2.718)
    assertThat(sparkRow.getUTF8String(7).toString).isEqualTo("test")
    assertThat(sparkRow.getDecimal(8, 10, 2).toBigDecimal.doubleValue()).isEqualTo(99.99)
  }
}
