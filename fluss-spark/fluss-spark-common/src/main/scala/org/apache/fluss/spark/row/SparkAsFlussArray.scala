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

import org.apache.fluss.row.{BinaryString, Decimal, InternalArray => FlussInternalArray, InternalMap, InternalRow => FlussInternalRow, TimestampLtz, TimestampNtz}

import org.apache.spark.sql.catalyst.util.{ArrayData => SparkArrayData}
import org.apache.spark.sql.types.{ArrayType => SparkArrayType, DataType => SparkDataType, StructType}

/** Wraps a Spark [[SparkArrayData]] as a Fluss [[FlussInternalArray]]. */
class SparkAsFlussArray(arrayData: SparkArrayData, elementType: SparkDataType)
  extends FlussInternalArray
  with Serializable {

  /** Returns the number of elements in this array. */
  override def size(): Int = arrayData.numElements()

  override def toBooleanArray: Array[Boolean] = arrayData.toBooleanArray()

  override def toByteArray: Array[Byte] = arrayData.toByteArray()

  override def toShortArray: Array[Short] = arrayData.toShortArray()

  override def toIntArray: Array[Int] = arrayData.toIntArray()

  override def toLongArray: Array[Long] = arrayData.toLongArray()

  override def toFloatArray: Array[Float] = arrayData.toFloatArray()

  override def toDoubleArray: Array[Double] = arrayData.toDoubleArray()

  /** Returns true if the element is null at the given position. */
  override def isNullAt(pos: Int): Boolean = arrayData.isNullAt(pos)

  /** Returns the boolean value at the given position. */
  override def getBoolean(pos: Int): Boolean = arrayData.getBoolean(pos)

  /** Returns the byte value at the given position. */
  override def getByte(pos: Int): Byte = arrayData.getByte(pos)

  /** Returns the short value at the given position. */
  override def getShort(pos: Int): Short = arrayData.getShort(pos)

  /** Returns the integer value at the given position. */
  override def getInt(pos: Int): Int = arrayData.getInt(pos)

  /** Returns the long value at the given position. */
  override def getLong(pos: Int): Long = arrayData.getLong(pos)

  /** Returns the float value at the given position. */
  override def getFloat(pos: Int): Float = arrayData.getFloat(pos)

  /** Returns the double value at the given position. */
  override def getDouble(pos: Int): Double = arrayData.getDouble(pos)

  /** Returns the string value at the given position with fixed length. */
  override def getChar(pos: Int, length: Int): BinaryString =
    BinaryString.fromBytes(arrayData.getUTF8String(pos).getBytes)

  /** Returns the string value at the given position. */
  override def getString(pos: Int): BinaryString =
    BinaryString.fromBytes(arrayData.getUTF8String(pos).getBytes)

  /**
   * Returns the decimal value at the given position.
   *
   * <p>The precision and scale are required to determine whether the decimal value was stored in a
   * compact representation (see [[Decimal]]).
   */
  override def getDecimal(pos: Int, precision: Int, scale: Int): Decimal = {
    val sparkDecimal = arrayData.getDecimal(pos, precision, scale)
    if (sparkDecimal.precision <= org.apache.spark.sql.types.Decimal.MAX_LONG_DIGITS)
      Decimal.fromUnscaledLong(
        sparkDecimal.toUnscaledLong,
        sparkDecimal.precision,
        sparkDecimal.scale)
    else
      Decimal.fromBigDecimal(
        sparkDecimal.toJavaBigDecimal,
        sparkDecimal.precision,
        sparkDecimal.scale)
  }

  /**
   * Returns the timestamp value at the given position.
   *
   * <p>The precision is required to determine whether the timestamp value was stored in a compact
   * representation (see [[TimestampNtz]]).
   */
  override def getTimestampNtz(pos: Int, precision: Int): TimestampNtz =
    TimestampNtz.fromMicros(arrayData.getLong(pos))

  /**
   * Returns the timestamp value at the given position.
   *
   * <p>The precision is required to determine whether the timestamp value was stored in a compact
   * representation (see [[TimestampLtz]]).
   */
  override def getTimestampLtz(pos: Int, precision: Int): TimestampLtz =
    TimestampLtz.fromEpochMicros(arrayData.getLong(pos))

  /** Returns the binary value at the given position with fixed length. */
  override def getBinary(pos: Int, length: Int): Array[Byte] = arrayData.getBinary(pos)

  /** Returns the binary value at the given position. */
  override def getBytes(pos: Int): Array[Byte] = arrayData.getBinary(pos)

  /** Returns the array value at the given position. */
  override def getArray(pos: Int) = new SparkAsFlussArray(
    arrayData.getArray(pos),
    elementType.asInstanceOf[SparkArrayType].elementType)

  /** Returns the row value at the given position. */
  override def getRow(pos: Int, numFields: Int): FlussInternalRow =
    new SparkAsFlussRow(elementType.asInstanceOf[StructType])
      .replace(arrayData.getStruct(pos, numFields))

  /** Returns the map value at the given position. */
  override def getMap(pos: Int): InternalMap = {
    throw new UnsupportedOperationException()
  }
}
