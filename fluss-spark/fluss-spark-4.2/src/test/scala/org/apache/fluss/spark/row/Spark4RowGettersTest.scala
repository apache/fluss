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

import org.apache.fluss.row.{GenericArray, GenericRow}
import org.apache.fluss.spark.SparkConversions
import org.apache.fluss.types.{DataTypes, RowType}

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.scalatest.funsuite.AnyFunSuite

class Spark4RowGettersTest extends AnyFunSuite {

  test("Spark unsafe projection reads binary views from Fluss rows and arrays") {
    val bytes = Array[Byte](1, 2, 3)
    val rowType = RowType
      .builder()
      .field("binary", DataTypes.BINARY(3))
      .field("binaries", DataTypes.ARRAY(DataTypes.BINARY(3)))
      .build()
    val row = GenericRow.of(bytes, new GenericArray(Array[Object](bytes, null)))
    val sparkRow = new FlussAsSparkRow(rowType).replace(row)
    val projected = UnsafeProjection.create(SparkConversions.toSparkDataType(rowType))(sparkRow)

    assertThat(projected.getBinary(0)).containsExactly(bytes: _*)
    val array = projected.getArray(1)
    assertThat(array.numElements()).isEqualTo(2)
    assertThat(array.getBinary(0)).containsExactly(bytes: _*)
    assertThat(array.isNullAt(1)).isTrue()
  }

  test("Spark binary views preserve variable-length bytes, empty values and null array elements") {
    val rowType = RowType.builder().field("values", DataTypes.ARRAY(DataTypes.BYTES)).build()
    val values = new GenericArray(Array[Object](Array.empty[Byte], Array[Byte](1, 2, 3), null))
    val row = new FlussAsSparkRow(rowType).replace(GenericRow.of(values))
    val array = row.getArray(0)
    assertThat(array.getBinaryView(0).numBytes()).isZero()
    assertThat(array.getBinaryView(1).getBytes).containsExactly(1.toByte, 2.toByte, 3.toByte)
    val projected =
      UnsafeProjection.create(SparkConversions.toSparkDataType(rowType))(row).getArray(0)
    assertThat(projected.getBinary(0)).isEmpty()
    assertThat(projected.getBinary(1)).containsExactly(1.toByte, 2.toByte, 3.toByte)
    assertThat(projected.isNullAt(2)).isTrue()
  }

  test("Fluss rows and arrays reject Spark VARIANT getters explicitly") {
    val rowType = RowType.builder().field("id", DataTypes.INT).build()
    val row = new FlussAsSparkRow(rowType).replace(GenericRow.of(Integer.valueOf(1)))
    val array = new FlussAsSparkArray(DataTypes.INT)
      .replace(new GenericArray(Array[Object](Integer.valueOf(1))))

    Seq[() => Any](() => row.getVariant(0), () => array.getVariant(0)).foreach {
      getter =>
        assertThatThrownBy(() => getter())
          .isInstanceOf(classOf[UnsupportedOperationException])
          .hasMessageContaining("VARIANT")
    }
  }
}
