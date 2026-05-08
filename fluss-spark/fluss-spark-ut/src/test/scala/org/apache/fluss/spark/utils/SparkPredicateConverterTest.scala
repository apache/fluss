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

package org.apache.fluss.spark.utils

import org.apache.fluss.predicate.{CompoundPredicate, LeafPredicate, Predicate => FlussPredicate, PredicateBuilder}
import org.apache.fluss.row.{BinaryString, Decimal, TimestampLtz, TimestampNtz}
import org.apache.fluss.types.{DataField, DataType => FlussDataType, DataTypes, RowType}

import org.apache.spark.sql.connector.expressions.{Expression, Expressions, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And, Not, Or, Predicate}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, Decimal => SparkDecimal, DecimalType, IntegerType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import java.lang.{Boolean => JBoolean, Byte => JByte, Long => JLong}
import java.time.LocalDate
import java.util.Arrays

class SparkPredicateConverterTest extends AnyFunSuite {

  private val rowType: RowType = rowTypeOf(
    ("id", DataTypes.INT()),
    ("name", DataTypes.STRING()),
    ("score", DataTypes.DOUBLE()),
    ("active", DataTypes.BOOLEAN()),
    ("age", DataTypes.TINYINT()),
    ("balance", DataTypes.DECIMAL(10, 2)),
    ("dt", DataTypes.DATE()),
    ("ts", DataTypes.TIMESTAMP()),
    ("tsLtz", DataTypes.TIMESTAMP_LTZ())
  )

  test("EqualTo on integer column converts to equal predicate") {
    val predicate = convert(pred("=", ref("id"), lit(Integer.valueOf(42), IntegerType)))
    val expected = new PredicateBuilder(rowType).equal(0, Integer.valueOf(42))
    assertThat(predicate).isEqualTo(expected)
  }

  test("EqualTo on string column wraps UTF8String as BinaryString") {
    val predicate = convert(pred("=", ref("name"), lit(UTF8String.fromString("alice"), StringType)))
    val expected = new PredicateBuilder(rowType).equal(1, BinaryString.fromString("alice"))
    assertThat(predicate).isEqualTo(expected)
  }

  test("EqualTo on null literal returns null predicate value") {
    val predicate = convert(pred("=", ref("id"), lit(null, IntegerType)))
    val expected = new PredicateBuilder(rowType).equal(0, null.asInstanceOf[Object])
    assertThat(predicate).isEqualTo(expected)
  }

  test("EqualNullSafe with null maps to isNull") {
    val predicate = convert(pred("<=>", ref("id"), lit(null, IntegerType)))
    val expected = new PredicateBuilder(rowType).isNull(0)
    assertThat(predicate).isEqualTo(expected)
  }

  test("EqualNullSafe with value maps to equal") {
    val predicate = convert(pred("<=>", ref("id"), lit(Integer.valueOf(7), IntegerType)))
    val expected = new PredicateBuilder(rowType).equal(0, Integer.valueOf(7))
    assertThat(predicate).isEqualTo(expected)
  }

  test("GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual") {
    val builder = new PredicateBuilder(rowType)
    assertThat(convert(pred(">", ref("id"), lit(Integer.valueOf(10), IntegerType))))
      .isEqualTo(builder.greaterThan(0, Integer.valueOf(10)))
    assertThat(convert(pred(">=", ref("id"), lit(Integer.valueOf(10), IntegerType))))
      .isEqualTo(builder.greaterOrEqual(0, Integer.valueOf(10)))
    assertThat(convert(pred("<", ref("id"), lit(Integer.valueOf(10), IntegerType))))
      .isEqualTo(builder.lessThan(0, Integer.valueOf(10)))
    assertThat(convert(pred("<=", ref("id"), lit(Integer.valueOf(10), IntegerType))))
      .isEqualTo(builder.lessOrEqual(0, Integer.valueOf(10)))
  }

  test("IsNull and IsNotNull") {
    val builder = new PredicateBuilder(rowType)
    assertThat(convert(pred("IS_NULL", ref("name"))))
      .isEqualTo(builder.isNull(1))
    assertThat(convert(pred("IS_NOT_NULL", ref("name"))))
      .isEqualTo(builder.isNotNull(1))
  }

  test("In with multiple string literals") {
    val in = pred(
      "IN",
      ref("name"),
      lit(UTF8String.fromString("a"), StringType),
      lit(UTF8String.fromString("b"), StringType),
      lit(UTF8String.fromString("c"), StringType))
    val expected = new PredicateBuilder(rowType).in(
      1,
      Arrays.asList[Object](
        BinaryString.fromString("a"),
        BinaryString.fromString("b"),
        BinaryString.fromString("c")))
    assertThat(convert(in)).isEqualTo(expected)
  }

  test("And composes children") {
    val and = new And(
      pred("=", ref("id"), lit(Integer.valueOf(1), IntegerType)),
      pred("=", ref("name"), lit(UTF8String.fromString("x"), StringType)))
    val builder = new PredicateBuilder(rowType)
    val expected = PredicateBuilder.and(
      builder.equal(0, Integer.valueOf(1)),
      builder.equal(1, BinaryString.fromString("x")))
    assertThat(convert(and)).isEqualTo(expected)
  }

  test("Or composes children") {
    val or = new Or(
      pred("=", ref("id"), lit(Integer.valueOf(1), IntegerType)),
      pred("=", ref("id"), lit(Integer.valueOf(2), IntegerType)))
    val builder = new PredicateBuilder(rowType)
    val expected = PredicateBuilder.or(
      builder.equal(0, Integer.valueOf(1)),
      builder.equal(0, Integer.valueOf(2)))
    assertThat(convert(or)).isEqualTo(expected)
  }

  test("Not wrapping IsNull maps to isNotNull") {
    val not = new Not(pred("IS_NULL", ref("name")))
    assertThat(convert(not)).isEqualTo(new PredicateBuilder(rowType).isNotNull(1))
  }

  test("Not wrapping IsNotNull maps to isNull") {
    val not = new Not(pred("IS_NOT_NULL", ref("name")))
    assertThat(convert(not)).isEqualTo(new PredicateBuilder(rowType).isNull(1))
  }

  test("Not wrapping a non-null-check is not supported") {
    val not = new Not(pred("=", ref("id"), lit(Integer.valueOf(1), IntegerType)))
    assert(SparkPredicateConverter.convert(rowType, not).isEmpty)
  }

  test("Not wrapping equality on a boolean column rewrites to its complement") {
    val not = new Not(pred("=", ref("active"), lit(JBoolean.TRUE, BooleanType)))
    assertThat(convert(not))
      .isEqualTo(new PredicateBuilder(rowType).equal(3, JBoolean.FALSE))
  }

  test("StringStartsWith / EndsWith / Contains on string column") {
    val builder = new PredicateBuilder(rowType)
    assertThat(
      convert(pred("STARTS_WITH", ref("name"), lit(UTF8String.fromString("ali"), StringType))))
      .isEqualTo(builder.startsWith(1, BinaryString.fromString("ali")))
    assertThat(
      convert(pred("ENDS_WITH", ref("name"), lit(UTF8String.fromString("son"), StringType))))
      .isEqualTo(builder.endsWith(1, BinaryString.fromString("son")))
    assertThat(convert(pred("CONTAINS", ref("name"), lit(UTF8String.fromString("li"), StringType))))
      .isEqualTo(builder.contains(1, BinaryString.fromString("li")))
  }

  test("StringStartsWith rejected on non-string column") {
    val p = pred("STARTS_WITH", ref("id"), lit(UTF8String.fromString("1"), StringType))
    assert(SparkPredicateConverter.convert(rowType, p).isEmpty)
  }

  test("Boolean literal") {
    val predicate = convert(pred("=", ref("active"), lit(JBoolean.TRUE, BooleanType)))
    val expected = new PredicateBuilder(rowType).equal(3, JBoolean.TRUE)
    assertThat(predicate).isEqualTo(expected)
  }

  test("Tinyint literal from Integer auto-narrows") {
    val predicate = convert(pred("=", ref("age"), lit(Integer.valueOf(25), IntegerType)))
    val expected = new PredicateBuilder(rowType).equal(4, JByte.valueOf(25.toByte))
    assertThat(predicate).isEqualTo(expected)
  }

  test("Decimal literal from Spark Decimal") {
    val bd = new java.math.BigDecimal("123.45")
    val predicate =
      convert(pred("=", ref("balance"), lit(SparkDecimal(bd, 10, 2), DecimalType(10, 2))))
    val expected = new PredicateBuilder(rowType).equal(5, Decimal.fromBigDecimal(bd, 10, 2))
    assertThat(predicate).isEqualTo(expected)
  }

  test("Date literal from epoch days") {
    val days = Integer.valueOf(LocalDate.of(2025, 1, 15).toEpochDay.toInt)
    val predicate = convert(pred("=", ref("dt"), lit(days, DateType)))
    val expected = new PredicateBuilder(rowType).equal(6, LocalDate.of(2025, 1, 15))
    assertThat(predicate).isEqualTo(expected)
  }

  test("Timestamp NTZ literal from epoch micros") {
    val micros = 1735639200000000L
    val predicate = convert(pred("=", ref("ts"), lit(JLong.valueOf(micros), TimestampNTZType)))
    val expected = new PredicateBuilder(rowType).equal(7, TimestampNtz.fromMicros(micros))
    assertThat(predicate).isEqualTo(expected)
  }

  test("Timestamp LTZ literal from epoch micros") {
    val micros = 1735639200000000L
    val predicate = convert(pred("=", ref("tsLtz"), lit(JLong.valueOf(micros), TimestampType)))
    val expected = new PredicateBuilder(rowType).equal(8, TimestampLtz.fromEpochMicros(micros))
    assertThat(predicate).isEqualTo(expected)
  }

  test("Unknown column returns None") {
    val p = pred("=", ref("missing"), lit(Integer.valueOf(1), IntegerType))
    assert(SparkPredicateConverter.convert(rowType, p).isEmpty)
  }

  test("AlwaysTrue / AlwaysFalse return None (unsupported)") {
    assert(SparkPredicateConverter.convert(rowType, new AlwaysTrue()).isEmpty)
    assert(SparkPredicateConverter.convert(rowType, new AlwaysFalse()).isEmpty)
  }

  test("convertPredicates returns AND of all convertible and the accepted list") {
    val predicates: Seq[Predicate] = Seq(
      pred("=", ref("id"), lit(Integer.valueOf(1), IntegerType)),
      pred("IS_NOT_NULL", ref("name")),
      pred("=", ref("unknown"), lit(Integer.valueOf(1), IntegerType))
    )
    val (predicate, accepted) = SparkPredicateConverter.convertPredicates(rowType, predicates)
    assert(predicate.isDefined)
    assert(predicate.get.isInstanceOf[CompoundPredicate])
    assert(accepted == Seq(predicates(0), predicates(1)))
  }

  test("convertPredicates collapses a single predicate without wrapping in AND") {
    val predicates: Seq[Predicate] = Seq(pred("=", ref("id"), lit(Integer.valueOf(1), IntegerType)))
    val (predicate, accepted) = SparkPredicateConverter.convertPredicates(rowType, predicates)
    assert(predicate.isDefined)
    assert(predicate.get.isInstanceOf[LeafPredicate])
    assert(accepted == Seq(predicates.head))
  }

  test("convertPredicates with no convertible predicates returns empty") {
    val predicates: Seq[Predicate] =
      Seq(pred("=", ref("unknown"), lit(Integer.valueOf(1), IntegerType)), new AlwaysTrue())
    val (predicate, accepted) = SparkPredicateConverter.convertPredicates(rowType, predicates)
    assert(predicate.isEmpty)
    assert(accepted.isEmpty)
  }

  private def convert(predicate: Predicate): FlussPredicate =
    SparkPredicateConverter
      .convert(rowType, predicate)
      .getOrElse(fail(s"Expected predicate $predicate to be convertible"))

  private def ref(name: String): NamedReference = Expressions.column(name)

  private def lit[T](v: T, dt: DataType): Literal[T] = new Literal[T] {
    override def value(): T = v
    override def dataType(): DataType = dt
    override def children(): Array[Expression] = Array.empty
  }

  private def pred(name: String, children: Expression*): Predicate =
    new Predicate(name, children.toArray)

  private def rowTypeOf(fields: (String, FlussDataType)*): RowType = {
    val list = new java.util.ArrayList[DataField]()
    fields.foreach {
      case (name, tpe) =>
        list.add(new DataField(name, tpe))
    }
    new RowType(list)
  }
}
