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

import org.apache.fluss.row.{InternalRow => FlussInternalRow}
import org.apache.fluss.types.{ArrayType => FlussArrayType, BinaryType => FlussBinaryType, LocalZonedTimestampType, RowType, TimestampType}
import org.apache.fluss.utils.InternalRowUtils

import org.apache.spark.sql.catalyst.{InternalRow => SparkInteralRow}
import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader
import org.apache.spark.sql.catalyst.util.{ArrayData => SparkArrayData, MapData => SparkMapData}
import org.apache.spark.sql.types.{DataType => SparkDataType, Decimal => SparkDecimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class FlussAsSparkRow(rowType: RowType) extends SparkInteralRow {

  val fieldCount: Int = rowType.getFieldCount

  var row: FlussInternalRow = _

  def replace(row: FlussInternalRow): FlussAsSparkRow = {
    this.row = row
    this
  }

  override def numFields: Int = fieldCount

  override def setNullAt(ordinal: Int): Unit = throw new UnsupportedOperationException()

  override def update(ordinal: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def copy(): SparkInteralRow = {
    new FlussAsSparkRow(rowType).replace(InternalRowUtils.copyRow(row, rowType))
  }

  override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)

  override def getByte(ordinal: Int): Byte = row.getByte(ordinal)

  override def getShort(ordinal: Int): Short = row.getShort(ordinal)

  override def getInt(ordinal: Int): Int = row.getInt(ordinal)

  override def getLong(ordinal: Int): Long = {
    val flussType = rowType.getTypeAt(ordinal)
    flussType match {
      case ltz: LocalZonedTimestampType =>
        row.getTimestampLtz(ordinal, ltz.getPrecision).toEpochMicros
      case ntz: TimestampType =>
        row.getTimestampNtz(ordinal, ntz.getPrecision).toEpochMicros
      case _ =>
        row.getLong(ordinal)
    }
  }

  override def getFloat(ordinal: Int): Float = row.getFloat(ordinal)

  override def getDouble(ordinal: Int): Double = row.getDouble(ordinal)

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): SparkDecimal = {
    val flussDecimal = row.getDecimal(ordinal, precision, scale)
    DataConverter.toSparkDecimal(flussDecimal)
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    DataConverter.toSparkUTF8String(row.getString(ordinal))
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    val binaryType = rowType.getTypeAt(ordinal).asInstanceOf[FlussBinaryType]
    row.getBinary(ordinal, binaryType.getLength)
  }

  override def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException()

  override def getStruct(ordinal: Int, numFields: Int): SparkInteralRow = {
    val subRowType = rowType.getTypeAt(ordinal).asInstanceOf[RowType]
    val flussSubRow = row.getRow(ordinal, numFields)
    DataConverter.toSparkInternalRow(flussSubRow, subRowType)
  }

  override def getArray(ordinal: Int): SparkArrayData = {
    val arrayType = rowType.getTypeAt(ordinal).asInstanceOf[FlussArrayType]
    val flussArray = row.getArray(ordinal)
    DataConverter.toSparkArray(flussArray, arrayType)
  }

  override def getMap(ordinal: Int): SparkMapData = {
    // TODO: support map type in fluss-spark
    throw new UnsupportedOperationException()
  }

  override def get(ordinal: Int, dataType: SparkDataType): AnyRef = {
    SpecializedGettersReader.read(this, ordinal, dataType, true, true)
  }
}
