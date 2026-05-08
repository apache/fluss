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

import org.apache.fluss.predicate.{Predicate => FlussPredicate, PredicateBuilder, UnsupportedExpression}
import org.apache.fluss.row.{BinaryString, Decimal, TimestampLtz, TimestampNtz}
import org.apache.fluss.types._

import org.apache.spark.sql.connector.expressions.{Expression, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And, Not, Or, Predicate}
import org.apache.spark.sql.types.{Decimal => SparkDecimal}
import org.apache.spark.unsafe.types.UTF8String

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.time.LocalDate

import scala.jdk.CollectionConverters._

/**
 * Unsupported predicates yield `None`; callers must re-apply all — Fluss pushdown is batch-level,
 * not row-exact.
 */
object SparkPredicateConverter {

  def convert(rowType: RowType, predicate: Predicate): Option[FlussPredicate] = {
    val builder = new PredicateBuilder(rowType)
    try {
      Some(toFluss(rowType, builder, predicate))
    } catch {
      case _: UnsupportedExpression => None
    }
  }

  /** Returns the AND of all convertible predicates, plus the subset of Spark ones accepted. */
  def convertPredicates(
      rowType: RowType,
      predicates: Seq[Predicate]): (Option[FlussPredicate], Seq[Predicate]) = {
    val (accepted, converted) =
      predicates.flatMap(p => convert(rowType, p).map((p, _))).unzip
    val combined = converted match {
      case Seq() => None
      case Seq(single) => Some(single)
      case many => Some(PredicateBuilder.and(many.asJava))
    }
    (combined, accepted)
  }

  private def toFluss(
      rowType: RowType,
      builder: PredicateBuilder,
      predicate: Predicate): FlussPredicate = predicate match {

    case and: And =>
      PredicateBuilder.and(
        toFluss(rowType, builder, and.left()),
        toFluss(rowType, builder, and.right()))

    case or: Or =>
      PredicateBuilder.or(
        toFluss(rowType, builder, or.left()),
        toFluss(rowType, builder, or.right()))

    case not: Not => negate(rowType, builder, not.child())

    case _: AlwaysTrue | _: AlwaysFalse => throw new UnsupportedExpression

    case p =>
      p.name() match {
        case "=" => compare(rowType, builder, p, builder.equal)
        case ">" => compare(rowType, builder, p, builder.greaterThan)
        case ">=" => compare(rowType, builder, p, builder.greaterOrEqual)
        case "<" => compare(rowType, builder, p, builder.lessThan)
        case "<=" => compare(rowType, builder, p, builder.lessOrEqual)
        case "<=>" => nullSafeEqual(rowType, builder, p)
        case "IS_NULL" => builder.isNull(fieldIndex(builder, p))
        case "IS_NOT_NULL" => builder.isNotNull(fieldIndex(builder, p))
        case "IN" => inPredicate(rowType, builder, p)
        case "STARTS_WITH" => stringMatch(rowType, builder, p, builder.startsWith)
        case "ENDS_WITH" => stringMatch(rowType, builder, p, builder.endsWith)
        case "CONTAINS" => stringMatch(rowType, builder, p, builder.contains)
        case _ => throw new UnsupportedExpression
      }
  }

  // PredicateBuilder has no generic negate; invert only the shapes we can by hand.
  private def negate(
      rowType: RowType,
      builder: PredicateBuilder,
      child: Predicate): FlussPredicate = child.name() match {
    case "IS_NULL" => builder.isNotNull(fieldIndex(builder, child))
    case "IS_NOT_NULL" => builder.isNull(fieldIndex(builder, child))
    case "=" if canInvertBooleanEq(rowType, builder, child) =>
      val idx = fieldIndex(builder, child)
      val bool = literalValue(child, 1).asInstanceOf[JBoolean]
      builder.equal(idx, JBoolean.valueOf(!bool))
    case _ => throw new UnsupportedExpression
  }

  private def compare(
      rowType: RowType,
      builder: PredicateBuilder,
      predicate: Predicate,
      op: (Int, Object) => FlussPredicate): FlussPredicate = {
    val idx = fieldIndex(builder, predicate)
    op(idx, literalAt(rowType, predicate, 1, idx))
  }

  private def nullSafeEqual(
      rowType: RowType,
      builder: PredicateBuilder,
      predicate: Predicate): FlussPredicate = {
    val idx = fieldIndex(builder, predicate)
    if (literalValue(predicate, 1) == null) builder.isNull(idx)
    else builder.equal(idx, literalAt(rowType, predicate, 1, idx))
  }

  private def inPredicate(
      rowType: RowType,
      builder: PredicateBuilder,
      predicate: Predicate): FlussPredicate = {
    val idx = fieldIndex(builder, predicate)
    val fieldType = rowType.getTypeAt(idx)
    val literals = predicate.children().drop(1).map(literalFrom(fieldType, _)).toSeq
    // PredicateBuilder.in rejects an empty list with IllegalArgumentException.
    if (literals.isEmpty) throw new UnsupportedExpression
    builder.in(idx, literals.asJava)
  }

  private def stringMatch(
      rowType: RowType,
      builder: PredicateBuilder,
      predicate: Predicate,
      op: (Int, Object) => FlussPredicate): FlussPredicate = {
    val idx = fieldIndex(builder, predicate)
    requireStringType(rowType.getTypeAt(idx))
    op(idx, literalAt(rowType, predicate, 1, idx))
  }

  private def fieldIndex(builder: PredicateBuilder, predicate: Predicate): Int = {
    val idx = builder.indexOf(fieldName(predicate))
    if (idx < 0) throw new UnsupportedExpression
    idx
  }

  private def fieldName(predicate: Predicate): String = predicate.children() match {
    case Array(ref: NamedReference, _*) if ref.fieldNames().length == 1 => ref.fieldNames()(0)
    case _ => throw new UnsupportedExpression
  }

  private def literalValue(predicate: Predicate, childIdx: Int): Any =
    predicate.children()(childIdx) match {
      case lit: Literal[_] => lit.value()
      case _ => throw new UnsupportedExpression
    }

  private def literalAt(
      rowType: RowType,
      predicate: Predicate,
      childIdx: Int,
      fieldIdx: Int): Object =
    literalFrom(rowType.getTypeAt(fieldIdx), predicate.children()(childIdx))

  private def literalFrom(fieldType: DataType, expr: Expression): Object = expr match {
    case lit: Literal[_] => toFlussLiteral(fieldType, lit.value())
    case _ => throw new UnsupportedExpression
  }

  // Only boolean `col = literal` is invertible: "NOT (col = true)" rewrites to "col = false".
  private def canInvertBooleanEq(
      rowType: RowType,
      builder: PredicateBuilder,
      predicate: Predicate): Boolean = {
    try {
      val idx = builder.indexOf(fieldName(predicate))
      idx >= 0 &&
      rowType.getTypeAt(idx).getTypeRoot == DataTypeRoot.BOOLEAN &&
      literalValue(predicate, 1).isInstanceOf[JBoolean]
    } catch {
      case _: UnsupportedExpression => false
    }
  }

  private def requireStringType(tpe: DataType): Unit = tpe.getTypeRoot match {
    case DataTypeRoot.STRING | DataTypeRoot.CHAR =>
    case _ => throw new UnsupportedExpression
  }

  // Catalyst internal form: UTF8String (strings), Int (date days), Long (timestamp micros),
  // Spark Decimal, boxed primitives for numerics.
  private def toFlussLiteral(tpe: DataType, value: Any): Object =
    if (value == null) null
    else
      tpe.getTypeRoot match {
        case DataTypeRoot.BOOLEAN =>
          value match {
            case b: JBoolean => b
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.TINYINT =>
          value match {
            case n: Number => JByte.valueOf(n.byteValue())
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.SMALLINT =>
          value match {
            case n: Number => JShort.valueOf(n.shortValue())
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.INTEGER =>
          value match {
            case n: Number => Integer.valueOf(n.intValue())
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.BIGINT =>
          value match {
            case n: Number => JLong.valueOf(n.longValue())
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.FLOAT =>
          value match {
            case n: Number => JFloat.valueOf(n.floatValue())
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.DOUBLE =>
          value match {
            case n: Number => JDouble.valueOf(n.doubleValue())
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.STRING | DataTypeRoot.CHAR =>
          value match {
            case s: UTF8String => BinaryString.fromString(s.toString)
            case s: String => BinaryString.fromString(s)
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.BINARY | DataTypeRoot.BYTES =>
          value match {
            case b: Array[Byte] => b
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.DECIMAL =>
          val dt = tpe.asInstanceOf[DecimalType]
          value match {
            case d: SparkDecimal =>
              Decimal.fromBigDecimal(d.toJavaBigDecimal, dt.getPrecision, dt.getScale)
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.DATE =>
          // RPC serialization (PredicateMessageUtils) expects LocalDate.
          value match {
            case d: Integer => LocalDate.ofEpochDay(d.longValue())
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
          value match {
            case l: JLong => TimestampNtz.fromMicros(l.longValue())
            case _ => throw new UnsupportedExpression
          }

        case DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
          value match {
            case l: JLong => TimestampLtz.fromEpochMicros(l.longValue())
            case _ => throw new UnsupportedExpression
          }

        case _ => throw new UnsupportedExpression
      }
}
