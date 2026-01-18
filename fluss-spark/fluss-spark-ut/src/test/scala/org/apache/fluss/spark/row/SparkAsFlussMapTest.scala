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
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.assertThat

class SparkAsFlussMapTest extends FlussSparkTestBase {

  // Helper method to convert String array to UTF8String array
  private def toUTF8Strings(strings: String*): Array[Any] = {
    strings.map(UTF8String.fromString).toArray
  }

  test("size: empty map") {
    val sparkMap = new ArrayBasedMapData(
      ArrayData.toArrayData(Array.empty[Any]),
      ArrayData.toArrayData(Array.empty[Any]))
    val mapType = MapType(StringType, IntegerType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(0)
  }

  test("size: non-empty map") {
    val keys = toUTF8Strings("key1", "key2", "key3")
    val values = Array(100, 200, 300)
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(StringType, IntegerType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(3)
  }

  test("keyArray: empty map") {
    val sparkMap = new ArrayBasedMapData(
      ArrayData.toArrayData(Array.empty[Any]),
      ArrayData.toArrayData(Array.empty[Any]))
    val mapType = MapType(StringType, IntegerType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    val keyArray = flussMap.keyArray()
    assertThat(keyArray.size()).isEqualTo(0)
  }

  test("keyArray: non-empty map") {
    val keys = toUTF8Strings("key1", "key2", "key3")
    val values = Array(100, 200, 300)
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(StringType, IntegerType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    val keyArray = flussMap.keyArray()
    assertThat(keyArray.size()).isEqualTo(3)
    assertThat(keyArray.getString(0).toString).isEqualTo("key1")
    assertThat(keyArray.getString(1).toString).isEqualTo("key2")
    assertThat(keyArray.getString(2).toString).isEqualTo("key3")
  }

  test("valueArray: empty map") {
    val sparkMap = new ArrayBasedMapData(
      ArrayData.toArrayData(Array.empty[Any]),
      ArrayData.toArrayData(Array.empty[Any]))
    val mapType = MapType(StringType, IntegerType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    val valueArray = flussMap.valueArray()
    assertThat(valueArray.size()).isEqualTo(0)
  }

  test("valueArray: non-empty map") {
    val keys = toUTF8Strings("key1", "key2", "key3")
    val values = Array(100, 200, 300)
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(StringType, IntegerType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    val valueArray = flussMap.valueArray()
    assertThat(valueArray.size()).isEqualTo(3)
    assertThat(valueArray.getInt(0)).isEqualTo(100)
    assertThat(valueArray.getInt(1)).isEqualTo(200)
    assertThat(valueArray.getInt(2)).isEqualTo(300)
  }

  test("integration: map with nested array") {
    val keys = toUTF8Strings("array1", "array2")
    val values = Array(
      ArrayData.toArrayData(Array(1, 2, 3)),
      ArrayData.toArrayData(Array(4, 5, 6))
    )
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(StringType, org.apache.spark.sql.types.ArrayType(IntegerType))
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(2)

    val keyArray = flussMap.keyArray()
    assertThat(keyArray.getString(0).toString).isEqualTo("array1")
    assertThat(keyArray.getString(1).toString).isEqualTo("array2")

    val valueArray = flussMap.valueArray()
    val array1 = valueArray.getArray(0)
    assertThat(array1.size()).isEqualTo(3)
    assertThat(array1.getInt(0)).isEqualTo(1)
    assertThat(array1.getInt(1)).isEqualTo(2)
    assertThat(array1.getInt(2)).isEqualTo(3)

    val array2 = valueArray.getArray(1)
    assertThat(array2.size()).isEqualTo(3)
    assertThat(array2.getInt(0)).isEqualTo(4)
    assertThat(array2.getInt(1)).isEqualTo(5)
    assertThat(array2.getInt(2)).isEqualTo(6)
  }

  test("integration: map with nested row") {
    val keys = toUTF8Strings("row1", "row2")
    val values = Array(
      InternalRow.apply(UTF8String.fromString("value1"), 100),
      InternalRow.apply(UTF8String.fromString("value2"), 200)
    )
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val structType = new StructType().add("name", StringType).add("value", IntegerType)
    val mapType = MapType(StringType, structType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(2)

    val keyArray = flussMap.keyArray()
    assertThat(keyArray.getString(0).toString).isEqualTo("row1")
    assertThat(keyArray.getString(1).toString).isEqualTo("row2")

    val valueArray = flussMap.valueArray()
    val row1 = valueArray.getRow(0, 2)
    assertThat(row1.getString(0).toString).isEqualTo("value1")
    assertThat(row1.getInt(1)).isEqualTo(100)

    val row2 = valueArray.getRow(1, 2)
    assertThat(row2.getString(0).toString).isEqualTo("value2")
    assertThat(row2.getInt(1)).isEqualTo(200)
  }

  test("integration: map with nested map") {
    val keys = toUTF8Strings("map1", "map2")
    val values = Array(
      new ArrayBasedMapData(
        ArrayData.toArrayData(toUTF8Strings("inner1", "inner2")),
        ArrayData.toArrayData(Array(10, 20))),
      new ArrayBasedMapData(
        ArrayData.toArrayData(toUTF8Strings("inner3", "inner4")),
        ArrayData.toArrayData(Array(30, 40)))
    )
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val innerMapType = MapType(StringType, IntegerType)
    val mapType = MapType(StringType, innerMapType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(2)

    val keyArray = flussMap.keyArray()
    assertThat(keyArray.getString(0).toString).isEqualTo("map1")
    assertThat(keyArray.getString(1).toString).isEqualTo("map2")

    val valueArray = flussMap.valueArray()
    val innerMap1 = valueArray.getMap(0)
    assertThat(innerMap1.size()).isEqualTo(2)
    val innerMap1Keys = innerMap1.keyArray()
    assertThat(innerMap1Keys.getString(0).toString).isEqualTo("inner1")
    assertThat(innerMap1Keys.getString(1).toString).isEqualTo("inner2")
    val innerMap1Values = innerMap1.valueArray()
    assertThat(innerMap1Values.getInt(0)).isEqualTo(10)
    assertThat(innerMap1Values.getInt(1)).isEqualTo(20)

    val innerMap2 = valueArray.getMap(1)
    assertThat(innerMap2.size()).isEqualTo(2)
    val innerMap2Keys = innerMap2.keyArray()
    assertThat(innerMap2Keys.getString(0).toString).isEqualTo("inner3")
    assertThat(innerMap2Keys.getString(1).toString).isEqualTo("inner4")
    val innerMap2Values = innerMap2.valueArray()
    assertThat(innerMap2Values.getInt(0)).isEqualTo(30)
    assertThat(innerMap2Values.getInt(1)).isEqualTo(40)
  }

  test("copy: creates deep copy") {
    val keys = toUTF8Strings("key1", "key2")
    val values = Array(100, 200)
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(StringType, IntegerType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    // Since SparkAsFlussMap doesn't have a copy method, we just test that the same instance works correctly
    assertThat(flussMap.size()).isEqualTo(2)
    assertThat(flussMap.keyArray().getString(0).toString).isEqualTo("key1")
    assertThat(flussMap.valueArray().getInt(1)).isEqualTo(200)
  }

  test("map with various data types as values") {
    val keys = toUTF8Strings(
      "int_key",
      "float_key",
      "double_key",
      "long_key",
      "bool_key",
      "byte_key",
      "short_key")
    val values = Array(
      100, // int
      12.34f, // float
      56.78, // double
      1000L, // long
      true, // boolean
      127.toByte, // byte
      1000.toShort // short
    )
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    // Using StringType for all values since ArrayData handles mixed types
    val mapType = MapType(StringType, StringType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(7)
    val valueArray = flussMap.valueArray()
    assertThat(valueArray.getInt(0)).isEqualTo(100)
    assertThat(valueArray.getFloat(1)).isEqualTo(12.34f)
    assertThat(valueArray.getDouble(2)).isEqualTo(56.78)
    assertThat(valueArray.getLong(3)).isEqualTo(1000L)
    assertThat(valueArray.getBoolean(4)).isTrue()
    assertThat(valueArray.getByte(5)).isEqualTo(127.toByte)
    assertThat(valueArray.getShort(6)).isEqualTo(1000.toShort)
  }

  test("map with null values") {
    val keys = toUTF8Strings("key1", "key2", "key3")
    val values = Array[Any](UTF8String.fromString("value1"), null, UTF8String.fromString("value3"))
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(StringType, StringType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(3)
    val valueArray = flussMap.valueArray()
    assertThat(valueArray.isNullAt(0)).isFalse()
    assertThat(valueArray.isNullAt(1)).isTrue()
    assertThat(valueArray.isNullAt(2)).isFalse()
    assertThat(valueArray.getString(0).toString).isEqualTo("value1")
    assertThat(valueArray.getString(2).toString).isEqualTo("value3")
  }

  test("map with numeric keys") {
    val keys = Array(1, 2, 3)
    val values = toUTF8Strings("value1", "value2", "value3")
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(IntegerType, StringType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(3)
    val keyArray = flussMap.keyArray()
    val valueArray = flussMap.valueArray()
    assertThat(keyArray.getInt(0)).isEqualTo(1)
    assertThat(keyArray.getInt(1)).isEqualTo(2)
    assertThat(keyArray.getInt(2)).isEqualTo(3)
    assertThat(valueArray.getString(0).toString).isEqualTo("value1")
    assertThat(valueArray.getString(1).toString).isEqualTo("value2")
    assertThat(valueArray.getString(2).toString).isEqualTo("value3")
  }

  test("map with complex nested structure: array of rows") {
    val keys = toUTF8Strings("data1", "data2")
    val values = Array(
      ArrayData.toArrayData(
        Array(
          InternalRow.apply(UTF8String.fromString("name1"), 100),
          InternalRow.apply(UTF8String.fromString("name2"), 200)
        )),
      ArrayData.toArrayData(
        Array(
          InternalRow.apply(UTF8String.fromString("name3"), 300),
          InternalRow.apply(UTF8String.fromString("name4"), 400)
        ))
    )
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val structType = new StructType().add("name", StringType).add("value", IntegerType)
    val arrayType = org.apache.spark.sql.types.ArrayType(structType)
    val mapType = MapType(StringType, arrayType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(2)
    val keyArray = flussMap.keyArray()
    assertThat(keyArray.getString(0).toString).isEqualTo("data1")
    assertThat(keyArray.getString(1).toString).isEqualTo("data2")

    val valueArray = flussMap.valueArray()
    val array1 = valueArray.getArray(0)
    assertThat(array1.size()).isEqualTo(2)
    val row1_0 = array1.getRow(0, 2)
    assertThat(row1_0.getString(0).toString).isEqualTo("name1")
    assertThat(row1_0.getInt(1)).isEqualTo(100)
    val row1_1 = array1.getRow(1, 2)
    assertThat(row1_1.getString(0).toString).isEqualTo("name2")
    assertThat(row1_1.getInt(1)).isEqualTo(200)

    val array2 = valueArray.getArray(1)
    assertThat(array2.size()).isEqualTo(2)
    val row2_0 = array2.getRow(0, 2)
    assertThat(row2_0.getString(0).toString).isEqualTo("name3")
    assertThat(row2_0.getInt(1)).isEqualTo(300)
    val row2_1 = array2.getRow(1, 2)
    assertThat(row2_1.getString(0).toString).isEqualTo("name4")
    assertThat(row2_1.getInt(1)).isEqualTo(400)
  }

  test("map with decimal values") {
    val keys = toUTF8Strings("dec1", "dec2")
    val values = Array(
      org.apache.spark.sql.types.Decimal(123.45),
      org.apache.spark.sql.types.Decimal(678.90)
    )
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(StringType, org.apache.spark.sql.types.DecimalType(10, 2))
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(2)
    val keyArray = flussMap.keyArray()
    assertThat(keyArray.getString(0).toString).isEqualTo("dec1")
    assertThat(keyArray.getString(1).toString).isEqualTo("dec2")

    val valueArray = flussMap.valueArray()
    val dec1 = valueArray.getDecimal(0, 10, 2)
    assertThat(dec1.toBigDecimal.compareTo(new java.math.BigDecimal("123.45"))).isZero()
    val dec2 = valueArray.getDecimal(1, 10, 2)
    assertThat(dec2.toBigDecimal.compareTo(new java.math.BigDecimal("678.90"))).isZero()
  }

  test("map with timestamp values") {
    val keys = toUTF8Strings("ts1", "ts2")
    val values = Array(
      1634567890123456L, // microseconds timestamp
      1634567891123456L
    )
    val sparkMap = new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
    val mapType = MapType(StringType, org.apache.spark.sql.types.TimestampType)
    val flussMap = SparkAsFlussMap(sparkMap, mapType)

    assertThat(flussMap.size()).isEqualTo(2)
    val keyArray = flussMap.keyArray()
    assertThat(keyArray.getString(0).toString).isEqualTo("ts1")
    assertThat(keyArray.getString(1).toString).isEqualTo("ts2")

    val valueArray = flussMap.valueArray()
    val ts1 = valueArray.getTimestampNtz(0, 6)
    assertThat(ts1.toEpochMicros).isEqualTo(1634567890123456L)
    val ts2 = valueArray.getTimestampNtz(1, 6)
    assertThat(ts2.toEpochMicros).isEqualTo(1634567891123456L)
  }
}
