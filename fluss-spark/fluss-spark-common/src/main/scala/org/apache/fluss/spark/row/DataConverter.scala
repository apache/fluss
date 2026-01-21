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

import org.apache.fluss.row.{BinaryString => FlussBinaryString, Decimal => FlussDecimal, InternalArray => FlussInternalArray, InternalMap => FlussInternalMap, InternalRow => FlussInternalRow, TimestampLtz => FlussTimestampLtz, TimestampNtz => FlussTimestampNtz}
import org.apache.fluss.types.{ArrayType => FlussArrayType, DataType => FlussDataType, MapType => FlussMapType, RowType}
import org.apache.fluss.types.DataTypeRoot._
import org.apache.fluss.utils.InternalRowUtils

import org.apache.spark.sql.catalyst.{InternalRow => SparkInteralRow}
import org.apache.spark.sql.catalyst.util.{ArrayData => SparkArrayData, MapData => SparkMapData}
import org.apache.spark.sql.types.{Decimal => SparkDecimal}
import org.apache.spark.unsafe.types.UTF8String

object DataConverter {

  def toSparkObject(o: Object, dataType: FlussDataType): Any = {
    if (o == null) {
      return null
    }

    dataType.getTypeRoot match {
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        toSparkTimestamp(o.asInstanceOf[FlussTimestampNtz])
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        toSparkTimestamp(o.asInstanceOf[FlussTimestampLtz])
      case CHAR =>
        toSparkUTF8String(o.asInstanceOf[FlussBinaryString])
      case DECIMAL =>
        toSparkDecimal(o.asInstanceOf[FlussDecimal])
      case ARRAY =>
        toSparkArray(o.asInstanceOf[FlussInternalArray], dataType.asInstanceOf[FlussArrayType])
      case MAP =>
        toSparkMap(o.asInstanceOf[FlussInternalMap], dataType.asInstanceOf[FlussMapType])
      case ROW =>
        toSparkInternalRow(o.asInstanceOf[FlussInternalRow], dataType.asInstanceOf[RowType])
      case _ => o
    }
  }

  def toSparkTimestamp(timestamp: FlussTimestampNtz): Long = {
    timestamp.toEpochMicros
  }

  def toSparkTimestamp(timestamp: FlussTimestampLtz): Long = {
    timestamp.toEpochMicros
  }

  def toSparkDecimal(flussDecimal: FlussDecimal): SparkDecimal = {
    SparkDecimal(flussDecimal.toBigDecimal)
  }

  def toSparkUTF8String(flussBinaryString: FlussBinaryString): UTF8String = {
    UTF8String.fromBytes(flussBinaryString.toBytes)
  }

  def toSparkArray(flussArray: FlussInternalArray, arrayType: FlussArrayType): SparkArrayData = {
    val elementType = arrayType.getElementType
    new FlussAsSparkArray(elementType)
      .replace(InternalRowUtils.copyArray(flussArray, elementType))
  }

  def toSparkMap(flussMap: FlussInternalMap, mapType: FlussMapType): SparkMapData = {
    // TODO: support map type in fluss-spark
    throw new UnsupportedOperationException()
  }

  def toSparkInternalRow(flussRow: FlussInternalRow, rowType: RowType): SparkInteralRow = {
    new FlussAsSparkRow(rowType).replace(flussRow)
  }
}
