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

package org.apache.fluss.spark.types

import org.apache.fluss.types._

import org.apache.spark.sql.types.{DataType => SparkDataType, DataTypes => SparkDataTypes}

import scala.collection.JavaConverters._

object FlussToSparkTypeVisitor extends DataTypeVisitor[SparkDataType] {

  override def visit(charType: CharType): SparkDataType = {
    org.apache.spark.sql.types.CharType(charType.getLength)
  }

  override def visit(stringType: StringType): SparkDataType = {
    SparkDataTypes.StringType
  }

  override def visit(booleanType: BooleanType): SparkDataType = {
    SparkDataTypes.BooleanType
  }

  override def visit(binaryType: BinaryType): SparkDataType = {
    SparkDataTypes.BinaryType
  }

  override def visit(bytesType: BytesType): SparkDataType = {
    SparkDataTypes.BinaryType
  }

  override def visit(decimalType: DecimalType): SparkDataType = {
    org.apache.spark.sql.types.DecimalType(decimalType.getPrecision, decimalType.getScale)
  }

  override def visit(tinyIntType: TinyIntType): SparkDataType = {
    SparkDataTypes.ByteType
  }

  override def visit(smallIntType: SmallIntType): SparkDataType = {
    SparkDataTypes.ShortType
  }

  override def visit(intType: IntType): SparkDataType = {
    SparkDataTypes.IntegerType
  }

  override def visit(bigIntType: BigIntType): SparkDataType = {
    SparkDataTypes.LongType
  }

  override def visit(floatType: FloatType): SparkDataType = {
    SparkDataTypes.FloatType
  }

  override def visit(doubleType: DoubleType): SparkDataType = {
    SparkDataTypes.DoubleType
  }

  override def visit(dateType: DateType): SparkDataType = {
    SparkDataTypes.DateType
  }

  override def visit(timeType: TimeType): SparkDataType = {
    SparkDataTypes.IntegerType
  }

  override def visit(timestampType: TimestampType): SparkDataType = {
    SparkDataTypes.TimestampNTZType
  }

  override def visit(localZonedTimestampType: LocalZonedTimestampType): SparkDataType = {
    SparkDataTypes.TimestampType
  }

  override def visit(arrayType: ArrayType): SparkDataType = {
    SparkDataTypes.createArrayType(
      arrayType.getElementType.accept(this),
      arrayType.getElementType.isNullable)
  }

  override def visit(mapType: MapType): SparkDataType = {
    SparkDataTypes.createMapType(
      mapType.getKeyType.accept(this),
      mapType.getValueType.accept(this),
      mapType.getValueType.isNullable
    )
  }

  override def visit(rowType: RowType): SparkDataType = {
    val sparkFields = rowType.getFields.asScala.map {
      flussField =>
        SparkDataTypes.createStructField(
          flussField.getName,
          flussField.getType.accept(this),
          flussField.getType.isNullable)
    }
    SparkDataTypes.createStructType(sparkFields.toArray)
  }
}
