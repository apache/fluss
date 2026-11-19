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

import org.apache.fluss.config.ConfigOptions

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.{NamedParameterContext, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.Call
import org.apache.spark.sql.connector.catalog.{Identifier, ProcedureCatalog}
import org.apache.spark.sql.connector.catalog.procedures.{BoundProcedure, ProcedureParameter, UnboundProcedure}
import org.apache.spark.sql.connector.read.{LocalScan, Scan}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.Assertions.assertThat

import java.util.Collections

class Spark4SqlExtensionTest extends FlussSparkTestBase {
  private val legacyBinding = "spark.sql.legacy.parameterSubstitution.constantsOnly"

  private def inBothBindingModes(test: => Unit): Unit = {
    Seq("false", "true").foreach(mode => withSQLConf(legacyBinding -> mode)(test))
  }

  test("ordinary SQL preserves named and positional parameter binding in both modes") {
    inBothBindingModes {
      checkAnswer(
        spark.sql("SELECT :number AS n, :text AS value", Map("number" -> 7, "text" -> "a?b")),
        Row(7, "a?b") :: Nil)
      checkAnswer(
        spark.sql("SELECT ? + ? AS total, '?' AS marker", Array[Any](11, 31)),
        Row(42, "?") :: Nil)
    }
  }

  test("Fluss CALL binds named and positional SQL parameters before procedure resolution") {
    val key = ConfigOptions.KV_SNAPSHOT_INTERVAL.key()
    inBothBindingModes {
      Seq(
        spark.sql(
          s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array(:key))",
          Map("key" -> key)),
        spark.sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(array(?))", Array[Any](key))
      ).foreach(result => checkAnswer(result, Row(key, "1 s", "STATIC") :: Nil))
    }
  }

  test("other catalogs retain native CALL parsing, binding and execution") {
    spark.conf.set("spark.sql.catalog.native_call", classOf[Spark4NativeCallTestCatalog].getName)
    val parser = spark.sessionState.sqlParser
    assertThat(parser.parsePlan("CALL native_call.sys.increment(41)").isInstanceOf[Call]).isTrue()
    assertThat(parser.parsePlan("CALL unknown_catalog.sys.increment(41)").isInstanceOf[Call])
      .isTrue()
    inBothBindingModes {
      checkAnswer(spark.sql("CALL native_call.sys.increment(?)", Array[Any](41)), Row(42) :: Nil)
      checkAnswer(
        spark.sql("CALL native_call.sys.increment(input => :value)", Map("value" -> 9)),
        Row(10) :: Nil)
    }
  }

  test("routine parameter and query parsing delegate to Spark with original syntax errors") {
    val parser = spark.sessionState.sqlParser
    val schema = parser.parseRoutineParam("id INT DEFAULT 7 COMMENT 'number', value STRING")
    assertThat(schema.fieldNames).containsExactly("id", "value")
    assertThat(schema("id").dataType).isEqualTo(IntegerType)
    assertThat(schema("id").getComment().get).isEqualTo("number")
    assertThat(schema("value").dataType).isEqualTo(StringType)
    assertThat(parser.parseQuery("SELECT 1").resolved).isFalse()
    intercept[ParseException] {
      parser.parseQuery(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs()")
    }
    Seq("SELECT 1\nFROM )", s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(\nconfig_keys => )")
      .foreach {
        statement =>
          val error = intercept[ParseException](parser.parsePlan(statement))
          assertThat(error.command.get).isEqualTo(statement)
          assertThat(error.start.line.get).isEqualTo(2)
          assertThat(error.getMessage).contains("Syntax error")
      }
    val statement = "SELECT :number +\n)"
    val parameterError = intercept[ParseException] {
      spark.sql(statement, Map("number" -> 123456789))
    }
    val nativeError = intercept[ParseException] {
      new SparkSqlParser().parsePlanWithParameters(
        statement,
        NamedParameterContext(Map("number" -> Literal(123456789))))
    }
    assertThat(parameterError.command).isEqualTo(nativeError.command)
    assertThat(parameterError.getCondition).isEqualTo(nativeError.getCondition)
    assertThat(parameterError.start.line).isEqualTo(nativeError.start.line)
    assertThat(parameterError.start.startPosition).isEqualTo(nativeError.start.startPosition)
    assertThat(parameterError.start.line.get).isEqualTo(2)
    assertThat(parameterError.getMessage).contains("Syntax error")
    val context =
      parameterError.getQueryContext.map(c => (c.fragment(), c.startIndex(), c.stopIndex()))
    val nativeContext =
      nativeError.getQueryContext.map(c => (c.fragment(), c.startIndex(), c.stopIndex()))
    assert(context.toSeq == nativeContext.toSeq)
  }

  test("Fluss procedure argument validation retains unknown, duplicate and mixed errors") {
    Seq(
      ("unknown => array()", "unknown"),
      ("config_keys => array(), config_keys => array()", "duplicated"),
      ("config_keys => array(), array()", "Cannot mix"),
      ("array(), array()", "Too many")
    ).foreach {
      case (arguments, expected) =>
        val error = intercept[Exception] {
          sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs($arguments)").collect()
        }
        assertThat(error.getMessage).contains(expected)
    }
  }

  test("parameterized TVF queries preserve exact window options and row results in both modes") {
    withTable("bound_window") {
      sql("CREATE TABLE bound_window (id INT, value STRING)")
      sql("INSERT INTO bound_window VALUES (1, 'before')")
      Thread.sleep(100)
      val start = System.currentTimeMillis()
      Thread.sleep(100)
      sql("INSERT INTO bound_window VALUES (2, 'inside')")
      Thread.sleep(100)
      val end = System.currentTimeMillis()
      Thread.sleep(100)
      sql("INSERT INTO bound_window VALUES (3, 'after')")
      val table = s"$DEFAULT_CATALOG.$DEFAULT_DATABASE.bound_window"
      inBothBindingModes {
        Seq(
          spark.sql(
            "SELECT id FROM fluss_incremental_between_timestamp(:table, :start, :end)",
            Map("table" -> table, "start" -> start, "end" -> end)),
          spark.sql(
            "SELECT id FROM fluss_incremental_between_timestamp(?, ?, ?)",
            Array[Any](table, start, end))
        ).foreach {
          result =>
            checkAnswer(result, Row(2) :: Nil)
            val relations = result.queryExecution.analyzed.collect {
              case relation: DataSourceV2Relation => relation
            }
            assertThat(relations.size).isEqualTo(1)
            assertThat(
              relations.head.options.get(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()))
              .isEqualTo(start.toString)
            assertThat(
              relations.head.options.get(SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()))
              .isEqualTo(end.toString)
        }
      }
    }
  }
}

/** A native Spark procedure proves the Fluss extension leaves other catalogs executable. */
class Spark4NativeCallTestCatalog extends ProcedureCatalog {
  private var catalogName: String = _
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
  override def listProcedures(namespace: Array[String]): Array[Identifier] =
    Array(Identifier.of(namespace, "increment"))
  override def loadProcedure(identifier: Identifier): UnboundProcedure = new UnboundProcedure {
    override def name(): String = "increment"
    override def description(): String = "Increment a value using Spark's native procedure API"
    override def bind(inputType: StructType): BoundProcedure = new BoundProcedure {
      override def name(): String = "increment"
      override def description(): String = "Increment a value"
      override def isDeterministic(): Boolean = true
      override def parameters(): Array[ProcedureParameter] =
        Array(ProcedureParameter.in("input", IntegerType).build())
      override def call(input: InternalRow): java.util.Iterator[Scan] = {
        val value = input.getInt(0) + 1
        val result = new LocalScan {
          override def readSchema(): StructType = new StructType().add("value", IntegerType)
          override def rows(): Array[InternalRow] = Array(InternalRow(value))
        }
        Collections.singleton[Scan](result).iterator()
      }
    }
  }
}
