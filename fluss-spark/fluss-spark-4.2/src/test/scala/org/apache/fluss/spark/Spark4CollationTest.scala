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

package org.apache.fluss.spark

import org.apache.fluss.metadata.TablePath
import org.apache.fluss.spark.read.{FlussAppendScan, FlussUpsertScan}
import org.apache.fluss.spark.utils.SparkPredicateConverter
import org.apache.fluss.types.{DataTypes, RowType}

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, Literal}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types.{ArrayType, CharType, DataType, IntegerType, MapType, StringType, StructType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.assertThat

class Spark4CollationTest extends FlussSparkTestBase {

  test("non-binary string collations are rejected at every schema nesting level") {
    val collated = StringType("UTF8_LCASE")
    val types = Seq[DataType](
      collated,
      CharType(8, "UTF8_LCASE"),
      VarcharType(8, "UTF8_LCASE"),
      ArrayType(collated),
      MapType(collated, IntegerType),
      MapType(IntegerType, collated),
      new StructType().add("nested", collated)
    )
    types.foreach {
      dataType =>
        val error = intercept[UnsupportedOperationException] {
          SparkConversions.toFlussDataType(new StructType().add("value", dataType))
        }
        assertThat(error.getMessage).contains("UTF8_BINARY")
    }

    Seq("STRING COLLATE UTF8_LCASE", "ARRAY<STRING COLLATE UTF8_LCASE>").foreach {
      declaredType =>
        val error = intercept[UnsupportedOperationException] {
          sql(s"CREATE TABLE rejected_collation (value $declaredType)")
        }
        assertThat(error.getMessage).contains("UTF8_BINARY")
        assertThat(admin.tableExists(TablePath.of(DEFAULT_DATABASE, "rejected_collation")).get())
          .isFalse()
    }
  }

  test("collated V2 predicate literals cannot become bytewise Fluss pushdowns") {
    val rowType =
      RowType.of(Array[org.apache.fluss.types.DataType](DataTypes.STRING()), Array("value"))
    def literal(literalType: DataType): Literal[UTF8String] = new Literal[UTF8String] {
      override def value(): UTF8String = UTF8String.fromString("a")
      override def dataType(): DataType = literalType
    }
    val reference = Expressions.column("value")
    Seq("=", ">", ">=", "<", "<=", "<=>", "IN", "STARTS_WITH", "ENDS_WITH", "CONTAINS")
      .foreach {
        name =>
          val collated =
            new Predicate(name, Array[Expression](reference, literal(StringType("UTF8_LCASE"))))
          assertThat(SparkPredicateConverter.convert(rowType, collated).isEmpty).isTrue()
          val binary = new Predicate(name, Array[Expression](reference, literal(StringType)))
          assertThat(SparkPredicateConverter.convert(rowType, binary).nonEmpty).isTrue()
      }
  }

  test("explicit collated filters remain residual for log and primary-key partitioned tables") {
    Seq(false, true).foreach {
      primaryKey =>
        withTable("collation_rows") {
          val properties = if (primaryKey) "TBLPROPERTIES ('primary.key' = 'id,part')" else ""
          sql(s"""CREATE TABLE collation_rows (id INT, value STRING, part STRING)
                 |PARTITIONED BY (part) $properties""".stripMargin)
          // Separate writes make a bytewise pushdown capable of losing the uppercase batch.
          sql("INSERT INTO collation_rows VALUES (1, 'A', 'P')")
          sql("INSERT INTO collation_rows VALUES (2, 'a', 'p')")
          sql("INSERT INTO collation_rows VALUES (3, 'B', 'Q')")

          val dataFilter = sql("SELECT id FROM collation_rows WHERE value COLLATE UTF8_LCASE = 'a'")
          checkAnswer(dataFilter, Row(1) :: Row(2) :: Nil)
          val partitionFilter =
            sql("SELECT id FROM collation_rows WHERE part COLLATE UTF8_LCASE = 'p'")
          checkAnswer(partitionFilter, Row(1) :: Row(2) :: Nil)

          Seq(dataFilter, partitionFilter).foreach {
            df =>
              val scans = df.queryExecution.optimizedPlan.collect {
                case relation: DataSourceV2ScanRelation => relation.scan
              }
              assertThat(scans.nonEmpty).isTrue()
              scans.foreach {
                case scan: FlussAppendScan =>
                  assertThat(scan.pushedSparkPredicates.isEmpty).isTrue()
                  assertThat(scan.partitionPredicate.isEmpty).isTrue()
                case scan: FlussUpsertScan =>
                  assertThat(scan.pushedSparkPredicates.isEmpty).isTrue()
                  assertThat(scan.partitionPredicate.isEmpty).isTrue()
                case scan => fail(s"Unexpected scan: $scan")
              }
          }
        }
    }
  }
}
