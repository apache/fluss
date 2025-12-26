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

import org.apache.fluss.types.{ArrayType => FlussArrayType, DataField => FlussDataField, DataType => FlussDataType, MapType => FlussMapType, _}

import org.apache.spark.sql.types.{ArrayType => SparkArrayType, DataType => SparkDataType, MapType => SparkMapType, StructType, UserDefinedType}

import scala.collection.JavaConverters._

object SparkToFlussTypeVisitor {

  def visit(dataType: SparkDataType): FlussDataType = {
    dataType match {
      case st: StructType =>
        visitStructType(st)
      case mt: SparkMapType =>
        visitMapType(mt)
      case at: SparkArrayType =>
        visitArrayType(at)
      case _: UserDefinedType[_] =>
        throw new UnsupportedOperationException("User-defined type is not supported");
      case t =>
        visitPrimitiveType(t)
    }
  }

  private def visitStructType(st: StructType): RowType = {
    val flussDataFields = st.fields.map {
      field =>
        val flussDataType = visit(field.dataType)
        new FlussDataField(field.name, flussDataType, field.getComment().orNull)
    }
    new RowType(flussDataFields.toList.asJava)
  }

  private def visitMapType(mt: SparkMapType): FlussMapType = {
    new FlussMapType(visit(mt.keyType), visit(mt.valueType).copy(mt.valueContainsNull))
  }

  private def visitArrayType(at: SparkArrayType): FlussArrayType = {
    new FlussArrayType(visit(at.elementType).copy(at.containsNull))
  }

  private def visitPrimitiveType(t: SparkDataType): FlussDataType = {
    t match {
      case _: org.apache.spark.sql.types.BooleanType =>
        new BooleanType()
      case _: org.apache.spark.sql.types.ByteType =>
        new TinyIntType()
      case _: org.apache.spark.sql.types.ShortType =>
        new SmallIntType()
      case _: org.apache.spark.sql.types.IntegerType =>
        new IntType()
      case _: org.apache.spark.sql.types.LongType =>
        new BigIntType()
      case _: org.apache.spark.sql.types.FloatType =>
        new FloatType()
      case _: org.apache.spark.sql.types.DoubleType =>
        new DoubleType()
      case dt: org.apache.spark.sql.types.DecimalType =>
        new DecimalType(dt.precision, dt.scale)
      case x: org.apache.spark.sql.types.BinaryType =>
        new BytesType()
      case _: org.apache.spark.sql.types.VarcharType =>
        new StringType()
      case ct: org.apache.spark.sql.types.CharType =>
        new CharType(ct.length)
      case _: org.apache.spark.sql.types.StringType =>
        new StringType()
      case _: org.apache.spark.sql.types.DateType =>
        new DateType()
      case _: org.apache.spark.sql.types.TimestampType =>
        // spark only support 6 digits of precision
        new LocalZonedTimestampType(6)
      case _: org.apache.spark.sql.types.TimestampNTZType =>
        // spark only support 6 digits of precision
        new TimestampType(6)
      case _ =>
        throw new UnsupportedOperationException(s"Data type(${t.catalogString}) is not supported");
    }
  }

}
