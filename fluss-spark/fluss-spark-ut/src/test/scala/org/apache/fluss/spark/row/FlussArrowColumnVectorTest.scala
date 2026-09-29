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

import org.apache.fluss.row.{BinaryString, Decimal, GenericArray, GenericMap, GenericRow, TimestampLtz, TimestampNtz}
import org.apache.fluss.spark.SparkConversions
import org.apache.fluss.types.{DataTypes, RowType}

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

/** Checks the columnar representation against the existing Spark row conversion. */
class FlussArrowColumnVectorTest extends AnyFunSuite {

  test("primitive vectors, decimals, binary values, nulls and reordered projection") {
    val types = Array(
      DataTypes.BOOLEAN(),
      DataTypes.TINYINT(),
      DataTypes.SMALLINT(),
      DataTypes.INT(),
      DataTypes.BIGINT(),
      DataTypes.FLOAT(),
      DataTypes.DOUBLE(),
      DataTypes.STRING(),
      DataTypes.BINARY(3),
      DataTypes.BYTES(),
      DataTypes.DECIMAL(38, 8),
      DataTypes.DATE()
    )
    val rowType = RowType.of(types: _*)
    val row = GenericRow.of(
      Boolean.box(true),
      Byte.box(-7),
      Short.box(300),
      Int.box(-123),
      Long.box(123456789L),
      Float.box(1.25f),
      Double.box(-2.75d),
      BinaryString.fromString("你好"),
      Array[Byte](1, 0, -1),
      Array.empty[Byte],
      Decimal.fromBigDecimal(new java.math.BigDecimal("12345678901234567890.12345678"), 38, 8),
      Int.box(-1)
    )
    val data = new ArrowBatchTestUtils(rowType)
    try {
      val projection = types.indices.reverse.toArray
      val projectedType = rowType.project(projection)
      val batch = FlussArrowColumnVector.toBatch(
        data.batch(0, 0, row, new GenericRow(types.length), row),
        rowType,
        projection,
        1,
        2)
      try {
        assertThat(batch.numRows()).isEqualTo(2)
        for (i <- types.indices) {
          assertThat(batch.column(i).hasNull).isTrue
          assertThat(batch.column(i).numNulls()).isEqualTo(1)
          assertThat(batch.column(i).isNullAt(0)).isTrue
        }
        val expected = GenericRow.of(projection.map(row.getField): _*)
        val unsafe = UnsafeProjection.create(SparkConversions.toSparkDataType(projectedType))
        assertThat(unsafe(batch.getRow(1)).copy())
          .isEqualTo(unsafe(DataConverter.toSparkInternalRow(expected, projectedType)).copy())
        assertThat(batch.column(4).getUTF8String(0)).isNull()
        assertThat(batch.column(3).getBinary(0)).isNull()
        assertThat(batch.column(1).getDecimal(0, 38, 8)).isNull()
      } finally batch.close()
    } finally data.close()
  }

  test("boolean and validity bitmaps cross byte boundaries in sliced batches") {
    val rowType = RowType.of(DataTypes.BOOLEAN())
    val data = new ArrowBatchTestUtils(rowType)
    try {
      val rows =
        (0 until 20).map(i => GenericRow.of(if (i % 3 == 0) null else Boolean.box(i % 2 == 0)))
      val arrow = data.batch(0, 0, rows: _*)
      val memory = arrow.getVectorSchemaRoot.getVector(0).getAllocator
      val batch = FlussArrowColumnVector.toBatch(arrow, rowType, Array(0), 5, 12)
      try {
        assertThat(batch.column(0).numNulls()).isEqualTo(4)
        for (i <- 0 until 12) {
          assertThat(batch.column(0).isNullAt(i)).isEqualTo((i + 5) % 3 == 0)
          if ((i + 5) % 3 != 0) {
            assertThat(batch.column(0).getBoolean(i)).isEqualTo((i + 5) % 2 == 0)
          }
        }
        batch.closeIfFreeable()
        assertThat(memory.getAllocatedMemory).isZero
      } finally batch.close()
    } finally data.close()
  }

  test("all timestamp and time precisions preserve Spark units") {
    for (precision <- 0 to 9) {
      val rowType = RowType.of(
        DataTypes.TIMESTAMP(precision),
        DataTypes.TIMESTAMP_LTZ(precision),
        DataTypes.TIME(precision))
      val data = new ArrowBatchTestUtils(rowType)
      try {
        // Choose values exactly representable at each precision, including before the epoch.
        val micros = if (precision == 0) -2000000L else if (precision <= 3) -1234000L else -1234567L
        val millis = Math.floorDiv(micros, 1000L)
        val nanos = Math.floorMod(micros, 1000L).toInt * 1000
        val ntz = TimestampNtz.fromMillis(millis, nanos)
        val ltz = TimestampLtz.fromEpochMillis(millis, nanos)
        val time = if (precision == 0) 12000 else 12345
        val row = GenericRow.of(ntz, ltz, Int.box(time))
        val batch =
          FlussArrowColumnVector.toBatch(data.batch(0, 0, row), rowType, Array(0, 1, 2), 0, 1)
        try {
          assertThat(batch.column(0).getLong(0)).isEqualTo(micros)
          assertThat(batch.column(1).getLong(0)).isEqualTo(micros)
          assertThat(batch.column(2).getInt(0)).isEqualTo(time)
        } finally batch.close()
      } finally data.close()
    }
  }

  test("nanosecond timestamps truncate to microseconds on both sides of the epoch") {
    val rowType = RowType.of(DataTypes.TIMESTAMP(9), DataTypes.TIMESTAMP_LTZ(9))
    val data = new ArrowBatchTestUtils(rowType)
    try {
      for (millis <- Seq(-1L, 0L, 1L)) {
        val ntz = TimestampNtz.fromMillis(millis, 999999)
        val ltz = TimestampLtz.fromEpochMillis(millis, 999999)
        val batch = FlussArrowColumnVector.toBatch(
          data.batch(0, 0, GenericRow.of(ntz, ltz)),
          rowType,
          Array(0, 1),
          0,
          1)
        try {
          assertThat(batch.column(0).getLong(0)).isEqualTo(ntz.toEpochMicros)
          assertThat(batch.column(1).getLong(0)).isEqualTo(ltz.toEpochMicros)
        } finally batch.close()
      }
    } finally data.close()
  }

  test("nested arrays, maps and structs retain offsets, nulls and empty values") {
    val elementType = RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.STRING()))
    val rowType = RowType.of(
      DataTypes.ARRAY(elementType),
      DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())),
      elementType)
    val nested =
      GenericRow.of(Int.box(7), new GenericArray(Array[AnyRef](BinaryString.fromString("x"), null)))
    val entries = new java.util.LinkedHashMap[AnyRef, AnyRef]()
    entries.put(BinaryString.fromString("one"), new GenericArray(Array(1, 2)))
    entries.put(BinaryString.fromString("empty"), new GenericArray(Array.empty[Int]))
    entries.put(BinaryString.fromString("null"), null)
    val row =
      GenericRow.of(new GenericArray(Array[AnyRef](nested, null)), new GenericMap(entries), nested)
    val empty = GenericRow.of(
      new GenericArray(Array.empty[AnyRef]),
      new GenericMap(new java.util.HashMap[AnyRef, AnyRef]()),
      null)
    val data = new ArrowBatchTestUtils(rowType)
    try {
      val batch = FlussArrowColumnVector.toBatch(
        data.batch(0, 0, row, empty, new GenericRow(3), row),
        rowType,
        Array(0, 1, 2),
        1,
        3)
      try {
        assertThat(batch.column(0).getArray(1)).isNull()
        assertThat(batch.column(1).getMap(1)).isNull()
        val unsafe = UnsafeProjection.create(SparkConversions.toSparkDataType(rowType))
        Seq(empty, new GenericRow(3), row).zipWithIndex.foreach {
          case (expected, index) =>
            assertThat(unsafe(batch.getRow(index)).copy())
              .isEqualTo(unsafe(DataConverter.toSparkInternalRow(expected, rowType)).copy())
        }
      } finally batch.close()
    } finally data.close()
  }

  test("nested character values use Spark's string execution representation") {
    val nestedType = RowType.of(DataTypes.CHAR(3))
    val rowType = RowType.of(DataTypes.ARRAY(nestedType))
    val data = new ArrowBatchTestUtils(rowType)
    try {
      val row = GenericRow.of(
        new GenericArray(Array[AnyRef](GenericRow.of(BinaryString.fromString("abc")))))
      val batch = FlussArrowColumnVector.toBatch(data.batch(0, 0, row), rowType, Array(0), 0, 1)
      try {
        val copied = batch.column(0).getArray(0).copy()
        assertThat(copied.getStruct(0, 1).getUTF8String(0).toString).isEqualTo("abc")
      } finally batch.close()
    } finally data.close()
  }

  test("character vectors and empty projection retain values and row count") {
    val rowType = RowType.of(DataTypes.CHAR(3))
    val data = new ArrowBatchTestUtils(rowType)
    try {
      val batch = FlussArrowColumnVector.toBatch(
        data.batch(0, 0, GenericRow.of(BinaryString.fromString("abc"))),
        rowType,
        Array(0),
        0,
        1)
      try assertThat(batch.column(0).getUTF8String(0).toString).isEqualTo("abc")
      finally batch.close()
      val empty = FlussArrowColumnVector.toBatch(
        data.batch(0, 0, GenericRow.of(BinaryString.fromString("abc"))),
        rowType,
        Array.empty[Int],
        0,
        1)
      try {
        assertThat(empty.numCols()).isZero
        assertThat(empty.numRows()).isEqualTo(1)
        assertThat(empty.rowIterator().hasNext).isTrue
      } finally empty.close()
    } finally data.close()
  }
}
