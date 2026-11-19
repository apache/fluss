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

import org.apache.fluss.record.ArrowBatchData
import org.apache.fluss.spark.types.FlussToSparkTypeVisitor
import org.apache.fluss.types.{ArrayType, BinaryType, DataType, LocalZonedTimestampType, MapType, RowType, TimestampType, TimeType}

import org.apache.spark.sql.types.{ArrayType => SparkArrayType, CharType, DataType => SparkDataType, Decimal, MapType => SparkMapType, StringType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A borrowed view of an Arrow scan vector, using Fluss logical types for Spark's representation. In
 * particular, Arrow timestamps use several units and LTZ vectors have no Arrow timezone. Children
 * borrow the same root; only the containing batch releases memory.
 */
class FlussArrowColumnVector private (
    batch: ArrowBatchData,
    path: Seq[Int],
    flussType: DataType,
    rowOffset: Int,
    rowCount: Int)
  extends ColumnVector(
    FlussArrowColumnVector.sparkType(flussType.accept(FlussToSparkTypeVisitor))) {

  // Infer the unshaded vector type from the existing ArrowBatchData interoperability API.
  private val vector = path.tail.foldLeft(batch.getVectorSchemaRoot.getVector(path.head)) {
    (parent, child) => parent.getChildrenFromFields.get(child)
  }

  private val children: Array[ColumnVector] = flussType match {
    case array: ArrayType =>
      Array(
        child(Seq(0), array.getElementType, 0, vector.getChildrenFromFields.get(0).getValueCount))
    case map: MapType =>
      val entries = vector.getChildrenFromFields.get(0)
      Array(
        child(Seq(0, 0), map.getKeyType, 0, entries.getValueCount),
        child(Seq(0, 1), map.getValueType, 0, entries.getValueCount))
    case row: RowType =>
      (0 until row.getFieldCount)
        .map(i => child(Seq(i), row.getTypeAt(i), rowOffset, rowCount))
        .toArray
    case _ => Array.empty[ColumnVector]
  }

  private def child(indices: Seq[Int], dataType: DataType, offset: Int, count: Int): ColumnVector =
    new FlussArrowColumnVector(batch, path ++ indices, dataType, offset, count)

  override def close(): Unit = ()

  override def hasNull: Boolean = numNulls() > 0

  override def numNulls(): Int = {
    if (rowOffset == 0 && rowCount == vector.getValueCount) {
      vector.getNullCount
    } else {
      (0 until rowCount).count(isNullAt)
    }
  }

  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId + rowOffset)

  override def getBoolean(rowId: Int): Boolean = {
    val index = rowId + rowOffset
    (vector.getDataBuffer.getByte(index / 8) & (1 << (index % 8))) != 0
  }

  override def getByte(rowId: Int): Byte = vector.getDataBuffer.getByte(rowId + rowOffset)

  override def getShort(rowId: Int): Short = vector.getDataBuffer.getShort((rowId + rowOffset) * 2L)

  override def getInt(rowId: Int): Int = flussType match {
    case time: TimeType =>
      val precision = time.getPrecision
      if (precision == 0) {
        vector.getDataBuffer.getInt((rowId + rowOffset) * 4L) * 1000
      } else if (precision <= 3) {
        vector.getDataBuffer.getInt((rowId + rowOffset) * 4L)
      } else {
        val value = vector.getDataBuffer.getLong((rowId + rowOffset) * 8L)
        (value / (if (precision <= 6) 1000L else 1000000L)).toInt
      }
    case _ => vector.getDataBuffer.getInt((rowId + rowOffset) * 4L)
  }

  override def getLong(rowId: Int): Long = {
    val value = vector.getDataBuffer.getLong((rowId + rowOffset) * 8L)
    flussType match {
      case timestamp: TimestampType => timestampMicros(value, timestamp.getPrecision)
      case timestamp: LocalZonedTimestampType => timestampMicros(value, timestamp.getPrecision)
      case _ => value
    }
  }

  private def timestampMicros(value: Long, precision: Int): Long = {
    if (precision == 0) value * 1000000L
    else if (precision <= 3) value * 1000L
    else if (precision <= 6) value
    else Math.floorDiv(value, 1000L)
  }

  override def getFloat(rowId: Int): Float = vector.getDataBuffer.getFloat((rowId + rowOffset) * 4L)

  override def getDouble(rowId: Int): Double =
    vector.getDataBuffer.getDouble((rowId + rowOffset) * 8L)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
    if (isNullAt(rowId)) null
    else
      Decimal(
        vector.getObject(rowId + rowOffset).asInstanceOf[java.math.BigDecimal],
        precision,
        scale)
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    if (isNullAt(rowId)) null else UTF8String.fromBytes(getBinary(rowId))
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    if (isNullAt(rowId)) {
      return null
    }
    val index = rowId + rowOffset
    val (start, length) = flussType match {
      case binary: BinaryType => (index.toLong * binary.getLength, binary.getLength)
      case _ =>
        val start = vector.getOffsetBuffer.getInt(index * 4L)
        (start.toLong, vector.getOffsetBuffer.getInt((index + 1L) * 4L) - start)
    }
    val bytes = new Array[Byte](length)
    vector.getDataBuffer.getBytes(start, bytes)
    bytes
  }

  override def getArray(rowId: Int): ColumnarArray = {
    if (isNullAt(rowId)) {
      return null
    }
    val index = rowId + rowOffset
    val start = vector.getOffsetBuffer.getInt(index * 4L)
    val end = vector.getOffsetBuffer.getInt((index + 1L) * 4L)
    new ColumnarArray(children(0), start, end - start)
  }

  override def getMap(rowId: Int): ColumnarMap = {
    if (isNullAt(rowId)) {
      return null
    }
    val index = rowId + rowOffset
    val start = vector.getOffsetBuffer.getInt(index * 4L)
    val end = vector.getOffsetBuffer.getInt((index + 1L) * 4L)
    new ColumnarMap(children(0), children(1), start, end - start)
  }

  override def getChild(ordinal: Int): ColumnVector = children(ordinal)
}

/** Creates projected Spark batches whose close operation owns the complete Arrow root. */
object FlussArrowColumnVector {

  // Spark's execution representation of CHAR is STRING, including inside nested values.
  private def sparkType(dataType: SparkDataType): SparkDataType = dataType match {
    case _: CharType => StringType
    case SparkArrayType(elementType, containsNull) =>
      SparkArrayType(sparkType(elementType), containsNull)
    case SparkMapType(keyType, valueType, containsNull) =>
      SparkMapType(sparkType(keyType), sparkType(valueType), containsNull)
    case StructType(fields) =>
      StructType(fields.map(field => field.copy(dataType = sparkType(field.dataType))))
    case other => other
  }

  /** Wraps a contiguous row range and transfers ownership of the Arrow batch to Spark. */
  def toBatch(
      data: ArrowBatchData,
      rowType: RowType,
      projection: Array[Int],
      rowOffset: Int,
      rowCount: Int): ColumnarBatch = {
    val columns = projection.map {
      index =>
        new FlussArrowColumnVector(data, Seq(index), rowType.getTypeAt(index), rowOffset, rowCount)
          .asInstanceOf[ColumnVector]
    }
    new ColumnarBatch(columns, rowCount) {
      override def close(): Unit = data.close()

      override def closeIfFreeable(): Unit = close()
    }
  }
}
