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

import org.apache.fluss.row.{InternalArray => FlussInternalArray}
import org.apache.fluss.types.{ArrayType => FlussArrayType, BigIntType => FlussBigIntType, BinaryType => FlussBinaryType, DataType => FlussDataType, LocalZonedTimestampType => FlussTimestampLTZType, MapType => FlussMapType, RowType, TimestampType => FlussTimestampNTZType}
import org.apache.fluss.utils.InternalRowUtils

import org.apache.spark.sql.catalyst.{InternalRow => SparkInternalRow}
import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader
import org.apache.spark.sql.catalyst.util.{ArrayData => SparkArrayData, MapData => SparkMapData}
import org.apache.spark.sql.types.{DataType => SparkDataType, Decimal => SparkDecimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class FlussAsSparkArray(elementType: FlussDataType) extends SparkArrayData {

  var flussArray: FlussInternalArray = _

  def replace(array: FlussInternalArray): SparkArrayData = {
    this.flussArray = array
    this
  }

  override def numElements(): Int = flussArray.size()

  override def copy(): SparkArrayData = {
    new FlussAsSparkArray(elementType).replace(InternalRowUtils.copyArray(flussArray, elementType))
  }

  override def array: Array[Any] = {
    val elementGetter = FlussInternalArray.createElementGetter(elementType);
    Array.range(0, numElements()).map {
      ordinal =>
        DataConverter.toSparkObject(
          elementGetter.getElementOrNull(flussArray, ordinal),
          elementType)
    }
  }

  override def setNullAt(ordinal: Int): Unit = throw new UnsupportedOperationException()

  override def update(ordinal: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def isNullAt(ordinal: Int): Boolean = flussArray.isNullAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean = flussArray.getBoolean(ordinal)

  override def getByte(ordinal: Int): Byte = flussArray.getByte(ordinal)

  override def getShort(ordinal: Int): Short = flussArray.getShort(ordinal)

  override def getInt(ordinal: Int): Int = flussArray.getInt(ordinal)

  override def getLong(ordinal: Int): Long = {
    elementType match {
      case _: FlussBigIntType =>
        flussArray.getLong(ordinal)
      case ntz: FlussTimestampNTZType =>
        flussArray.getTimestampNtz(ordinal, ntz.getPrecision).toEpochMicros
      case ltz: FlussTimestampLTZType =>
        flussArray.getTimestampLtz(ordinal, ltz.getPrecision).toEpochMicros
      case _ =>
        throw new UnsupportedOperationException("Unsupported type: " + elementType)
    }
  }

  override def getFloat(ordinal: Int): Float = flussArray.getFloat(ordinal)

  override def getDouble(ordinal: Int): Double = flussArray.getDouble(ordinal)

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): SparkDecimal = {
    DataConverter.toSparkDecimal(flussArray.getDecimal(ordinal, precision, scale))
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    DataConverter.toSparkUTF8String(flussArray.getString(ordinal))
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    val binaryType = elementType.asInstanceOf[FlussBinaryType]
    flussArray.getBinary(ordinal, binaryType.getLength)
  }

  override def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException()

  override def getStruct(ordinal: Int, numFields: Int): SparkInternalRow = {
    DataConverter.toSparkInternalRow(
      flussArray.getRow(ordinal, numFields),
      elementType.asInstanceOf[RowType])
  }

  override def getArray(ordinal: Int): SparkArrayData = {
    DataConverter.toSparkArray(
      flussArray.getArray(ordinal),
      elementType.asInstanceOf[FlussArrayType])
  }

  override def getMap(ordinal: Int): SparkMapData = {
    DataConverter.toSparkMap(flussArray.getMap(ordinal), elementType.asInstanceOf[FlussMapType])
  }

  override def get(ordinal: Int, dataType: SparkDataType): AnyRef = {
    SpecializedGettersReader.read(this, ordinal, dataType, true, true)
  }
}
