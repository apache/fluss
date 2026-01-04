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

import org.apache.fluss.row.{BinaryString, Decimal, InternalRow => FlussInternalRow, TimestampLtz, TimestampNtz}

import org.apache.spark.sql.catalyst.{InternalRow => SparkInternalRow}
import org.apache.spark.sql.types.StructType

/** Wraps a Spark [[SparkInternalRow]] as a Fluss [[FlussInternalRow]]. */
class SparkAsFlussRow(schema: StructType) extends FlussInternalRow with Serializable {

  val fieldCount: Int = schema.length

  var row: SparkInternalRow = _

  def replace(row: SparkInternalRow): SparkAsFlussRow = {
    this.row = row
    this
  }

  /**
   * Returns the number of fields in this row.
   *
   * <p>The number does not include [[org.apache.fluss.record.ChangeType]]. It is kept separately.
   */
  override def getFieldCount: Int = fieldCount

  /** Returns true if the element is null at the given position. */
  override def isNullAt(pos: Int): Boolean = row.isNullAt(pos)

  /** Returns the boolean value at the given position. */
  override def getBoolean(pos: Int): Boolean = row.getBoolean(pos)

  /** Returns the byte value at the given position. */
  override def getByte(pos: Int): Byte = row.getByte(pos)

  /** Returns the short value at the given position. */
  override def getShort(pos: Int): Short = row.getShort(pos)

  /** Returns the integer value at the given position. */
  override def getInt(pos: Int): Int = row.getInt(pos)

  /** Returns the long value at the given position. */
  override def getLong(pos: Int): Long = row.getLong(pos)

  /** Returns the float value at the given position. */
  override def getFloat(pos: Int): Float = row.getFloat(pos)

  /** Returns the double value at the given position. */
  override def getDouble(pos: Int): Double = row.getDouble(pos)

  /** Returns the string value at the given position with fixed length. */
  override def getChar(pos: Int, length: Int): BinaryString =
    BinaryString.fromString(row.getUTF8String(pos).toString)

  /** Returns the string value at the given position. */
  override def getString(pos: Int): BinaryString = BinaryString.fromString(row.getString(pos))

  /**
   * Returns the decimal value at the given position.
   *
   * <p>The precision and scale are required to determine whether the decimal value was stored in a
   * compact representation (see [[Decimal]]).
   */
  override def getDecimal(pos: Int, precision: Int, scale: Int): Decimal = {
    val sparkDecimal = row.getDecimal(pos, precision, scale)
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
    TimestampNtz.fromMicros(row.getLong(pos))

  /**
   * Returns the timestamp value at the given position.
   *
   * <p>The precision is required to determine whether the timestamp value was stored in a compact
   * representation (see [[TimestampLtz]]).
   */
  override def getTimestampLtz(pos: Int, precision: Int): TimestampLtz =
    TimestampLtz.fromEpochMicros(row.getLong(pos))

  /** Returns the binary value at the given position with fixed length. */
  override def getBinary(pos: Int, length: Int): Array[Byte] = row.getBinary(pos)

  /** Returns the binary value at the given position. */
  override def getBytes(pos: Int): Array[Byte] = row.getBinary(pos)

  /** Returns the array value at the given position. */
  override def getArray(pos: Int) =
    new SparkAsFlussArray(row.getArray(pos), schema.fields(pos).dataType)

  /** Returns the row value at the given position. */
  override def getRow(pos: Int, numFields: Int): FlussInternalRow =
    new SparkAsFlussRow(schema.fields(pos).dataType.asInstanceOf[StructType])
      .replace(row.getStruct(pos, numFields))

}
