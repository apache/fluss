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

import org.apache.fluss.row.{BinaryString, GenericArray, GenericMap, GenericRow}
import org.apache.fluss.spark.FlussSparkTestBase
import org.apache.fluss.types.{ArrayType, BigIntType, CharType, IntType, MapType, RowType, StringType}

import org.assertj.core.api.Assertions.assertThat

import scala.collection.JavaConverters._

class FlussAsSparkMapTest extends FlussSparkTestBase {

  test("numElements: empty map") {
    val flussMap = new GenericMap(Map.empty[Object, Object].asJava)
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), new IntType()))
      .replace(flussMap)

    assertThat(sparkMap.numElements()).isEqualTo(0)
  }

  test("numElements: non-empty map") {
    val flussMap = createSimpleMap()
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), new IntType()))
      .replace(flussMap)

    assertThat(sparkMap.numElements()).isEqualTo(3)
  }

  test("keyArray: empty map") {
    val flussMap = new GenericMap(Map.empty[Object, Object].asJava)
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), new IntType()))
      .replace(flussMap)

    val sparkKeyArray = sparkMap.keyArray()
    assertThat(sparkKeyArray.numElements()).isEqualTo(0)
  }

  test("keyArray: non-empty map") {
    val flussMap = createSimpleMap()
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), new IntType()))
      .replace(flussMap)

    val sparkKeyArray = sparkMap.keyArray()
    assertThat(sparkKeyArray.numElements()).isEqualTo(3)
    // Keys are in insertion order for GenericMap
    val key0 = sparkKeyArray.getUTF8String(0).toString
    val key1 = sparkKeyArray.getUTF8String(1).toString
    val key2 = sparkKeyArray.getUTF8String(2).toString
    assertThat(Set(key0, key1, key2)).isEqualTo(Set("key1", "key2", "key3"))
  }

  test("valueArray: empty map") {
    val flussMap = new GenericMap(Map.empty[Object, Object].asJava)
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), new IntType()))
      .replace(flussMap)

    val sparkValueArray = sparkMap.valueArray()
    assertThat(sparkValueArray.numElements()).isEqualTo(0)
  }

  test("valueArray: non-empty map") {
    val flussMap = createSimpleMap()
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), new IntType()))
      .replace(flussMap)

    val sparkValueArray = sparkMap.valueArray()
    assertThat(sparkValueArray.numElements()).isEqualTo(3)
    assertThat(sparkValueArray.getInt(0)).isEqualTo(100)
    assertThat(sparkValueArray.getInt(1)).isEqualTo(200)
    assertThat(sparkValueArray.getInt(2)).isEqualTo(300)
  }

  test("copy: creates deep copy") {
    val flussMap = createSimpleMap()
    val originalSparkMap = new FlussAsSparkMap(new MapType(new CharType(10), new IntType()))
      .replace(flussMap)
    val copiedSparkMap = originalSparkMap.copy()

    assertThat(copiedSparkMap.numElements()).isEqualTo(3)
  }

  test("integration: map with nested array") {
    val flussMap = createMapWithNestedArrays()
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), new ArrayType(new IntType())))
      .replace(flussMap)

    assertThat(sparkMap.numElements()).isEqualTo(2)

    val sparkKeyArray = sparkMap.keyArray()
    assertThat(sparkKeyArray.getUTF8String(0).toString).isEqualTo("arr1")
    assertThat(sparkKeyArray.getUTF8String(1).toString).isEqualTo("arr2")

    val sparkValueArray = sparkMap.valueArray()
    // Check that we have 2 arrays
    assertThat(sparkValueArray.numElements()).isEqualTo(2)
  }

  test("integration: map with nested row") {
    val flussMap = createMapWithNestedRows()
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), createSimpleRowType()))
      .replace(flussMap)

    assertThat(sparkMap.numElements()).isEqualTo(2)

    val sparkKeyArray = sparkMap.keyArray()
    val key0 = sparkKeyArray.getUTF8String(0).toString
    val key1 = sparkKeyArray.getUTF8String(1).toString
    assertThat(Set(key0, key1)).isEqualTo(Set("row1", "row2"))
  }

  test("integration: map with nested map") {
    val flussMap = createMapWithNestedMaps()
    val sparkMap = new FlussAsSparkMap(new MapType(new CharType(10), createSimpleMapType()))
      .replace(flussMap)

    assertThat(sparkMap.numElements()).isEqualTo(2)

    val sparkKeyArray = sparkMap.keyArray()
    val key0 = sparkKeyArray.getUTF8String(0).toString
    val key1 = sparkKeyArray.getUTF8String(1).toString
    assertThat(Set(key0, key1)).isEqualTo(Set("map1", "map2"))

    val sparkValueArray = sparkMap.valueArray()
    assertThat(sparkValueArray.numElements()).isEqualTo(2)
  }

  private def createSimpleMap(): GenericMap = {
    new GenericMap(
      Map(
        BinaryString.fromString("key1") -> Integer.valueOf(100),
        BinaryString.fromString("key2") -> Integer.valueOf(200),
        BinaryString.fromString("key3") -> Integer.valueOf(300)
      ).asJava
    )
  }

  private def createMapWithNestedArrays(): GenericMap = {
    new GenericMap(
      Map(
        BinaryString.fromString("arr1") -> new GenericArray(
          Array[Object](Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3))),
        BinaryString.fromString("arr2") -> new GenericArray(
          Array[Object](Integer.valueOf(4), Integer.valueOf(5), Integer.valueOf(6)))
      ).asJava
    )
  }

  private def createMapWithNestedRows(): GenericMap = {
    val row1 = new GenericRow(2)
    row1.setField(0, java.lang.Long.valueOf(100L))
    row1.setField(1, BinaryString.fromString("value1"))

    val row2 = new GenericRow(2)
    row2.setField(0, java.lang.Long.valueOf(200L))
    row2.setField(1, BinaryString.fromString("value2"))

    new GenericMap(
      Map(
        BinaryString.fromString("row1") -> row1,
        BinaryString.fromString("row2") -> row2
      ).asJava
    )
  }

  private def createMapWithNestedMaps(): GenericMap = {
    val innerMap1 = new GenericMap(
      Map(
        BinaryString.fromString("inner1") -> Integer.valueOf(10),
        BinaryString.fromString("inner2") -> Integer.valueOf(20)
      ).asJava
    )

    val innerMap2 = new GenericMap(
      Map(
        BinaryString.fromString("inner3") -> Integer.valueOf(30),
        BinaryString.fromString("inner4") -> Integer.valueOf(40)
      ).asJava
    )

    new GenericMap(
      Map(
        BinaryString.fromString("map1") -> innerMap1,
        BinaryString.fromString("map2") -> innerMap2
      ).asJava
    )
  }

  private def createSimpleRowType(): RowType = {
    val fields = java.util.Arrays.asList(
      new org.apache.fluss.types.DataField("field1", new BigIntType()),
      new org.apache.fluss.types.DataField("field2", new StringType())
    )
    new RowType(fields)
  }

  private def createSimpleMapType(): MapType = {
    new MapType(new CharType(10), new IntType())
  }
}
