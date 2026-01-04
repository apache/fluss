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

package org.apache.fluss.spark.util

import org.apache.fluss.row.GenericRow
import org.apache.fluss.spark.SparkConversions
import org.apache.fluss.types.RowType

import org.apache.spark.sql.types._

object TestUtils {

  val SCHEMA: StructType = StructType(
    Seq(
      StructField("c_bool", BooleanType),
      StructField("c_byte", ByteType),
      StructField("c_short", ShortType),
      StructField("c_int", IntegerType),
      StructField("c_long", LongType),
      StructField("c_float", FloatType),
      StructField("c_double", DoubleType),
      StructField("c_decimal_1", DecimalType(10, 2)),
      StructField("c_decimal_2", DecimalType(38, 2)),
      StructField("c_string", StringType),
      // StructField("date", DateType),
      StructField("c_timestamp", TimestampType),
      StructField("c_array", ArrayType(FloatType, containsNull = false)),
      StructField(
        "c_row",
        StructType(Seq(StructField("id", LongType), StructField("name", StringType))))
    )
  )

  val FLUSS_ROWTYPE: RowType = SparkConversions.toFlussDataType(SCHEMA)

  def createGenericRow(values: Any*): GenericRow = {
    GenericRow.of(values.map(_.asInstanceOf[java.lang.Object]): _*)
  }
}
