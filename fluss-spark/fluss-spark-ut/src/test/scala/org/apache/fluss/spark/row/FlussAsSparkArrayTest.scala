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

import org.apache.fluss.row.{BinaryString, Decimal => FlussDecimal, GenericArray, GenericMap, GenericRow, TimestampLtz, TimestampNtz}
import org.apache.fluss.types.{ArrayType, BinaryType, DataTypes, IntType, LocalZonedTimestampType, MapType, RowType, StringType, TimestampType}

import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class FlussAsSparkArrayTest extends AnyFunSuite {

  test("numElements: empty array") {
    val elementType = DataTypes.INT
    val flussArray = new GenericArray(Array.empty[Object])
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.numElements()).isEqualTo(0)
  }

  test("numElements: non-empty array") {
    val elementType = DataTypes.INT
    val flussArray = new GenericArray(
      Array[Object](
        Integer.valueOf(1),
        Integer.valueOf(2),
        Integer.valueOf(3),
        Integer.valueOf(4),
        Integer.valueOf(5)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.numElements()).isEqualTo(5)
  }

  test("isNullAt: check null elements") {
    val elementType = DataTypes.INT
    val flussArray = new GenericArray(Array[Object](Integer.valueOf(1), null, Integer.valueOf(3)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.isNullAt(0)).isFalse()
    assertThat(sparkArray.isNullAt(1)).isTrue()
    assertThat(sparkArray.isNullAt(2)).isFalse()
  }

  test("getBoolean: read boolean array") {
    val elementType = DataTypes.BOOLEAN
    val flussArray = new GenericArray(
      Array[Object](
        java.lang.Boolean.TRUE,
        java.lang.Boolean.FALSE,
        java.lang.Boolean.TRUE
      ))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getBoolean(0)).isTrue()
    assertThat(sparkArray.getBoolean(1)).isFalse()
    assertThat(sparkArray.getBoolean(2)).isTrue()
  }

  test("getByte: read byte array") {
    val elementType = DataTypes.TINYINT
    val flussArray = new GenericArray(
      Array[Object](
        java.lang.Byte.valueOf(1.toByte),
        java.lang.Byte.valueOf(10.toByte),
        java.lang.Byte.valueOf((-5).toByte)
      ))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getByte(0)).isEqualTo(1.toByte)
    assertThat(sparkArray.getByte(1)).isEqualTo(10.toByte)
    assertThat(sparkArray.getByte(2)).isEqualTo((-5).toByte)
  }

  test("getShort: read short array") {
    val elementType = DataTypes.SMALLINT
    val flussArray = new GenericArray(
      Array[Object](
        java.lang.Short.valueOf(100.toShort),
        java.lang.Short.valueOf(200.toShort),
        java.lang.Short.valueOf((-50).toShort)
      ))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getShort(0)).isEqualTo(100.toShort)
    assertThat(sparkArray.getShort(1)).isEqualTo(200.toShort)
    assertThat(sparkArray.getShort(2)).isEqualTo((-50).toShort)
  }

  test("getInt: read integer array") {
    val elementType = DataTypes.INT
    val flussArray = new GenericArray(
      Array[Object](
        Integer.valueOf(10),
        Integer.valueOf(20),
        Integer.valueOf(30),
        Integer.valueOf(40),
        Integer.valueOf(50)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getInt(0)).isEqualTo(10)
    assertThat(sparkArray.getInt(2)).isEqualTo(30)
    assertThat(sparkArray.getInt(4)).isEqualTo(50)
  }

  test("getLong: read BIGINT array") {
    val elementType = DataTypes.BIGINT
    val flussArray = new GenericArray(
      Array[Object](
        java.lang.Long.valueOf(100L),
        java.lang.Long.valueOf(200L),
        java.lang.Long.valueOf(300L)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getLong(0)).isEqualTo(100L)
    assertThat(sparkArray.getLong(1)).isEqualTo(200L)
    assertThat(sparkArray.getLong(2)).isEqualTo(300L)
  }

  test("getLong: read TIMESTAMP_WITHOUT_TIME_ZONE array") {
    val timestampType = new TimestampType(3)
    val timestamps = Array[Object](
      TimestampNtz.fromMillis(1000L),
      TimestampNtz.fromMillis(2000L),
      TimestampNtz.fromMillis(3000L)
    )
    val flussArray = new GenericArray(timestamps)
    val sparkArray = new FlussAsSparkArray(timestampType).replace(flussArray)

    // Spark expects microseconds
    assertThat(sparkArray.getLong(0)).isEqualTo(1000000L)
    assertThat(sparkArray.getLong(1)).isEqualTo(2000000L)
    assertThat(sparkArray.getLong(2)).isEqualTo(3000000L)
  }

  test("getLong: read TIMESTAMP_WITH_LOCAL_TIME_ZONE array") {
    val timestampType = new LocalZonedTimestampType(3)
    val timestamps = Array[Object](
      TimestampLtz.fromEpochMillis(5000L),
      TimestampLtz.fromEpochMillis(6000L)
    )
    val flussArray = new GenericArray(timestamps)
    val sparkArray = new FlussAsSparkArray(timestampType).replace(flussArray)

    // Spark expects microseconds
    assertThat(sparkArray.getLong(0)).isEqualTo(5000000L)
    assertThat(sparkArray.getLong(1)).isEqualTo(6000000L)
  }

  test("getLong: unsupported type throws exception") {
    val elementType = DataTypes.STRING
    val flussArray = new GenericArray(Array[Object](BinaryString.fromString("test")))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThrows[UnsupportedOperationException] {
      sparkArray.getLong(0)
    }
  }

  test("getFloat: read float array") {
    val elementType = DataTypes.FLOAT
    val flussArray = new GenericArray(
      Array[Object](
        java.lang.Float.valueOf(1.1f),
        java.lang.Float.valueOf(2.2f),
        java.lang.Float.valueOf(3.3f)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getFloat(0)).isEqualTo(1.1f)
    assertThat(sparkArray.getFloat(1)).isEqualTo(2.2f)
    assertThat(sparkArray.getFloat(2)).isEqualTo(3.3f)
  }

  test("getDouble: read double array") {
    val elementType = DataTypes.DOUBLE
    val flussArray = new GenericArray(
      Array[Object](
        java.lang.Double.valueOf(1.11),
        java.lang.Double.valueOf(2.22),
        java.lang.Double.valueOf(3.33)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getDouble(0)).isEqualTo(1.11)
    assertThat(sparkArray.getDouble(1)).isEqualTo(2.22)
    assertThat(sparkArray.getDouble(2)).isEqualTo(3.33)
  }

  test("getDecimal: read decimal array") {
    val elementType = DataTypes.DECIMAL(10, 2)
    val decimals = Array[Object](
      FlussDecimal.fromBigDecimal(new java.math.BigDecimal("10.50"), 10, 2),
      FlussDecimal.fromBigDecimal(new java.math.BigDecimal("20.75"), 10, 2),
      FlussDecimal.fromBigDecimal(new java.math.BigDecimal("30.99"), 10, 2)
    )
    val flussArray = new GenericArray(decimals)
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getDecimal(0, 10, 2).toBigDecimal.doubleValue()).isEqualTo(10.50)
    assertThat(sparkArray.getDecimal(1, 10, 2).toBigDecimal.doubleValue()).isEqualTo(20.75)
    assertThat(sparkArray.getDecimal(2, 10, 2).toBigDecimal.doubleValue()).isEqualTo(30.99)
  }

  test("getUTF8String: read string array") {
    val elementType = DataTypes.STRING
    val strings = Array[Object](
      BinaryString.fromString("apple"),
      BinaryString.fromString("banana"),
      BinaryString.fromString("cherry"))
    val flussArray = new GenericArray(strings)
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getUTF8String(0).toString).isEqualTo("apple")
    assertThat(sparkArray.getUTF8String(1).toString).isEqualTo("banana")
    assertThat(sparkArray.getUTF8String(2).toString).isEqualTo("cherry")
  }

  test("getUTF8String: read UTF-8 characters array") {
    val elementType = DataTypes.STRING
    val strings = Array[Object](
      BinaryString.fromString("你好"),
      BinaryString.fromString("世界"),
      BinaryString.fromString("😊"))
    val flussArray = new GenericArray(strings)
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.getUTF8String(0).toString).isEqualTo("你好")
    assertThat(sparkArray.getUTF8String(1).toString).isEqualTo("世界")
    assertThat(sparkArray.getUTF8String(2).toString).isEqualTo("😊")
  }

  test("getBinary: read binary array") {
    val binaryType = new BinaryType(3)
    val binaries = Array[Object](
      Array[Byte](1, 2, 3),
      Array[Byte](4, 5, 6),
      Array[Byte](7, 8, 9)
    )
    val flussArray = new GenericArray(binaries)
    val sparkArray = new FlussAsSparkArray(binaryType).replace(flussArray)

    assertThat(sparkArray.getBinary(0)).isEqualTo(Array[Byte](1, 2, 3))
    assertThat(sparkArray.getBinary(1)).isEqualTo(Array[Byte](4, 5, 6))
    assertThat(sparkArray.getBinary(2)).isEqualTo(Array[Byte](7, 8, 9))
  }

  test("getStruct: read row array") {
    val rowType = RowType
      .builder()
      .field("id", DataTypes.INT)
      .field("name", DataTypes.STRING)
      .build()

    val row1 = new GenericRow(2)
    row1.setField(0, Integer.valueOf(1))
    row1.setField(1, BinaryString.fromString("Alice"))

    val row2 = new GenericRow(2)
    row2.setField(0, Integer.valueOf(2))
    row2.setField(1, BinaryString.fromString("Bob"))

    val flussArray = new GenericArray(Array[Object](row1, row2))
    val sparkArray = new FlussAsSparkArray(rowType).replace(flussArray)

    val sparkRow1 = sparkArray.getStruct(0, 2)
    assertThat(sparkRow1.getInt(0)).isEqualTo(1)
    assertThat(sparkRow1.getUTF8String(1).toString).isEqualTo("Alice")

    val sparkRow2 = sparkArray.getStruct(1, 2)
    assertThat(sparkRow2.getInt(0)).isEqualTo(2)
    assertThat(sparkRow2.getUTF8String(1).toString).isEqualTo("Bob")
  }

  test("getArray: read nested array") {
    val innerArrayType = new ArrayType(DataTypes.INT)
    val innerArray1 =
      new GenericArray(Array[Object](Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3)))
    val innerArray2 =
      new GenericArray(Array[Object](Integer.valueOf(4), Integer.valueOf(5), Integer.valueOf(6)))

    val outerArray = new GenericArray(Array[Object](innerArray1, innerArray2))
    val sparkOuterArray = new FlussAsSparkArray(innerArrayType).replace(outerArray)

    val sparkInnerArray1 = sparkOuterArray.getArray(0)
    assertThat(sparkInnerArray1.numElements()).isEqualTo(3)
    assertThat(sparkInnerArray1.getInt(0)).isEqualTo(1)
    assertThat(sparkInnerArray1.getInt(2)).isEqualTo(3)

    val sparkInnerArray2 = sparkOuterArray.getArray(1)
    assertThat(sparkInnerArray2.numElements()).isEqualTo(3)
    assertThat(sparkInnerArray2.getInt(0)).isEqualTo(4)
    assertThat(sparkInnerArray2.getInt(2)).isEqualTo(6)
  }

  test("getMap: unsupported operation") {
    val mapType = DataTypes.MAP(DataTypes.INT, DataTypes.STRING)
    val flussArray = GenericArray.of(new GenericMap(Map(1 -> "map").asJava))
    val sparkArray = new FlussAsSparkArray(mapType).replace(flussArray)

    assertThrows[UnsupportedOperationException] {
      sparkArray.getMap(0)
    }
  }

  test("getInterval: unsupported operation") {
    val elementType = DataTypes.INT
    val flussArray = new GenericArray(Array[Object](Integer.valueOf(1)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThrows[UnsupportedOperationException] {
      sparkArray.getInterval(0)
    }
  }

  test("setNullAt: unsupported operation") {
    val elementType = DataTypes.INT
    val flussArray = new GenericArray(Array[Object](Integer.valueOf(1)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThrows[UnsupportedOperationException] {
      sparkArray.setNullAt(0)
    }
  }

  test("update: unsupported operation") {
    val elementType = DataTypes.INT
    val flussArray = new GenericArray(Array[Object](Integer.valueOf(1)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThrows[UnsupportedOperationException] {
      sparkArray.update(0, Integer.valueOf(2))
    }
  }

  test("copy: creates deep copy") {
    val elementType = DataTypes.INT
    val flussArray =
      new GenericArray(Array[Object](Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3)))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    val copiedArray = sparkArray.copy()

    assertThat(copiedArray.numElements()).isEqualTo(3)
    assertThat(copiedArray.getInt(0)).isEqualTo(1)
    assertThat(copiedArray.getInt(1)).isEqualTo(2)
    assertThat(copiedArray.getInt(2)).isEqualTo(3)

    // Verify it's a different instance
    assertThat(copiedArray).isNotSameAs(sparkArray)
  }

  test("replace: reuses wrapper instance") {
    val elementType = DataTypes.INT

    val array1 = new GenericArray(Array[Object](Integer.valueOf(1), Integer.valueOf(2)))
    val array2 =
      new GenericArray(Array[Object](Integer.valueOf(10), Integer.valueOf(20), Integer.valueOf(30)))

    val sparkArray = new FlussAsSparkArray(elementType)
    sparkArray.replace(array1)
    assertThat(sparkArray.numElements()).isEqualTo(2)
    assertThat(sparkArray.getInt(0)).isEqualTo(1)

    sparkArray.replace(array2)
    assertThat(sparkArray.numElements()).isEqualTo(3)
    assertThat(sparkArray.getInt(0)).isEqualTo(10)
    assertThat(sparkArray.getInt(2)).isEqualTo(30)
  }

  test("array with all nulls") {
    val elementType = DataTypes.INT
    val flussArray = new GenericArray(Array[Object](null, null, null))
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.numElements()).isEqualTo(3)
    assertThat(sparkArray.isNullAt(0)).isTrue()
    assertThat(sparkArray.isNullAt(1)).isTrue()
    assertThat(sparkArray.isNullAt(2)).isTrue()
  }

  test("large array: 1000 elements") {
    val elementType = DataTypes.INT
    val elements = (0 until 1000).map(i => Integer.valueOf(i)).toArray[Object]
    val flussArray = new GenericArray(elements)
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.numElements()).isEqualTo(1000)
    assertThat(sparkArray.getInt(0)).isEqualTo(0)
    assertThat(sparkArray.getInt(500)).isEqualTo(500)
    assertThat(sparkArray.getInt(999)).isEqualTo(999)
  }

  test("mixed nulls and values in array") {
    val elementType = DataTypes.STRING
    val elements = Array[Object](
      BinaryString.fromString("first"),
      null,
      BinaryString.fromString("third"),
      null,
      BinaryString.fromString("fifth")
    )
    val flussArray = new GenericArray(elements)
    val sparkArray = new FlussAsSparkArray(elementType).replace(flussArray)

    assertThat(sparkArray.numElements()).isEqualTo(5)
    assertThat(sparkArray.getUTF8String(0).toString).isEqualTo("first")
    assertThat(sparkArray.isNullAt(1)).isTrue()
    assertThat(sparkArray.getUTF8String(2).toString).isEqualTo("third")
    assertThat(sparkArray.isNullAt(3)).isTrue()
    assertThat(sparkArray.getUTF8String(4).toString).isEqualTo("fifth")
  }
}
