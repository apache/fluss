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

package org.apache.fluss.spark.extensions

import org.apache.fluss.spark.FlussSparkSessionExtensions
import org.apache.fluss.spark.catalyst.plans.logical.{FlussCallArgument, FlussCallStatement, FlussNamedArgument, FlussPositionalArgument}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.types.DataTypes
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant

class CallStatementParserTest extends FunSuite with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private var parser: ParserInterface = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    val optionalSession = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    optionalSession.foreach(_.stop())
    SparkSession.clearActiveSession()

    spark = SparkSession
      .builder()
      .master("local[2]")
      .config("spark.sql.extensions", classOf[FlussSparkSessionExtensions].getName)
      .getOrCreate()

    parser = spark.sessionState.sqlParser
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
      parser = null
    }
    super.afterEach()
  }

  test("testCallWithBackticks") {
    val call =
      parser.parsePlan("CALL cat.`system`.`no_args_func`()").asInstanceOf[FlussCallStatement]
    assert(call.name.toList == List("cat", "system", "no_args_func"))
    assert(call.args.size == 0)
  }

  test("testCallWithNamedArguments") {
    val callStatement = parser
      .parsePlan("CALL catalog.system.named_args_func(arg1 => 1, arg2 => 'test', arg3 => true)")
      .asInstanceOf[FlussCallStatement]

    assert(callStatement.name.toList == List("catalog", "system", "named_args_func"))
    assert(callStatement.args.size == 3)
    assertArgument(callStatement, 0, Some("arg1"), 1, DataTypes.IntegerType)
    assertArgument(callStatement, 1, Some("arg2"), "test", DataTypes.StringType)
    assertArgument(callStatement, 2, Some("arg3"), true, DataTypes.BooleanType)
  }

  test("testCallWithPositionalArguments") {
    val callStatement = parser
      .parsePlan(
        "CALL catalog.system.positional_args_func(1, '${spark.sql.extensions}', 2L, true, 3.0D, 4.0e1, 500e-1BD, TIMESTAMP '2017-02-03T10:37:30.00Z')")
      .asInstanceOf[FlussCallStatement]

    assert(callStatement.name.toList == List("catalog", "system", "positional_args_func"))
    assert(callStatement.args.size == 8)
    assertArgument(callStatement, 0, None, 1, DataTypes.IntegerType)
    assertArgument(
      callStatement,
      1,
      None,
      classOf[FlussSparkSessionExtensions].getName,
      DataTypes.StringType)
    assertArgument(callStatement, 2, None, 2L, DataTypes.LongType)
    assertArgument(callStatement, 3, None, true, DataTypes.BooleanType)
    assertArgument(callStatement, 4, None, 3.0, DataTypes.DoubleType)
    assertArgument(callStatement, 5, None, 4.0e1, DataTypes.DoubleType)
    assertArgument(
      callStatement,
      6,
      None,
      new BigDecimal("500e-1"),
      DataTypes.createDecimalType(3, 1))
    assertArgument(
      callStatement,
      7,
      None,
      Timestamp.from(Instant.parse("2017-02-03T10:37:30.00Z")),
      DataTypes.TimestampType)
  }

  test("testCallWithMixedArguments") {
    val callStatement = parser
      .parsePlan("CALL catalog.system.mixed_args_func(arg1 => 1, 'test')")
      .asInstanceOf[FlussCallStatement]

    assert(callStatement.name.toList == List("catalog", "system", "mixed_args_func"))
    assert(callStatement.args.size == 2)
    assertArgument(callStatement, 0, Some("arg1"), 1, DataTypes.IntegerType)
    assertArgument(callStatement, 1, None, "test", DataTypes.StringType)
  }

  test("testCallSimpleProcedure") {
    val callStatement = parser
      .parsePlan("CALL system.simple_procedure(table => 'db.table')")
      .asInstanceOf[FlussCallStatement]

    assert(callStatement.name.toList == List("system", "simple_procedure"))
    assert(callStatement.args.size == 1)
    assertArgument(callStatement, 0, Some("table"), "db.table", DataTypes.StringType)
  }

  private def assertArgument(
      callStatement: FlussCallStatement,
      index: Int,
      expectedName: Option[String],
      expectedValue: Any,
      expectedType: org.apache.spark.sql.types.DataType): Unit = {

    val callArgument = callStatement.args(index)

    expectedName match {
      case None =>
        assert(callArgument.isInstanceOf[FlussPositionalArgument])
      case Some(name) =>
        val namedArgument = callArgument.asInstanceOf[FlussNamedArgument]
        assert(namedArgument.name == name)
    }

    assert(callStatement.args(index).expr == Literal.create(expectedValue, expectedType))
  }
}
