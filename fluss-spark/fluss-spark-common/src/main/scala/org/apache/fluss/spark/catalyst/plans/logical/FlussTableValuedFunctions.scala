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

package org.apache.fluss.spark.catalyst.plans.logical

import org.apache.fluss.spark.{SparkFlussConf, SparkTable}
import org.apache.fluss.spark.catalyst.plans.logical.FlussTableValuedFunctions._
import org.apache.fluss.spark.read.FlussOffsetInitializers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Attribute, CurrentTimestamp, Expression, ExpressionInfo, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, ShortType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.{LocalDate, LocalDateTime, ZoneId, ZoneOffset}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Fluss table-valued functions (TVFs), usable from pure SQL. A call is translated into the `scan.*`
 * options the DataFrame API accepts and resolved into a plain [[DataSourceV2Relation]], so it is
 * scoped to the single query and keeps projection, filter push down and metrics working.
 */
object FlussTableValuedFunctions {

  val INCREMENTAL_BETWEEN_TIMESTAMP = "fluss_incremental_between_timestamp"

  val supportedFnNames: Seq[String] = Seq(INCREMENTAL_BETWEEN_TIMESTAMP)

  private type TableFunctionDescription =
    (FunctionIdentifier, ExpressionInfo, TableFunctionBuilder)

  def getTableValueFunctionInjection(fnName: String): TableFunctionDescription = {
    val (info, builder) = fnName match {
      case INCREMENTAL_BETWEEN_TIMESTAMP =>
        FunctionRegistryBase.build[IncrementalBetweenTimestamp](fnName, since = None)
      case _ =>
        throw new IllegalArgumentException(
          s"Function $fnName isn't a supported Fluss table valued function.")
    }
    (FunctionIdentifier(fnName), info, builder)
  }

  /** Resolves a TVF call into a relation over the referenced Fluss table. */
  def resolveFlussTableValuedFunction(
      spark: SparkSession,
      tvf: FlussTableValueFunction): LogicalPlan = {
    val args = tvf.args
    val sessionState = spark.sessionState
    val catalogManager = sessionState.catalogManager

    if (args.isEmpty) {
      throw new IllegalArgumentException(
        s"${tvf.fnName} requires a table identifier as its first argument.")
    }

    // Parse the arguments first, so an argument error does not depend on the table being resolvable.
    val options = tvf.parseArgs(args.tail)

    val tableArg = args.head.eval()
    if (tableArg == null) {
      throw new IllegalArgumentException(
        s"The first argument of ${tvf.fnName} must be a non-null table identifier.")
    }
    val tableIdentifier = tableArg.toString

    val (catalogName, namespace, tableName) =
      sessionState.sqlParser.parseMultipartIdentifier(tableIdentifier) match {
        case Seq(table) =>
          (catalogManager.currentCatalog.name(), catalogManager.currentNamespace, table)
        case Seq(db, table) => (catalogManager.currentCatalog.name(), Array(db), table)
        case Seq(catalog, db, table) => (catalog, Array(db), table)
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid table identifier '$tableIdentifier' for ${tvf.fnName}. Expected " +
              "'table', 'database.table' or 'catalog.database.table'.")
      }
    val fullTableIdentifier = (catalogName +: namespace :+ tableName).mkString(".")

    val catalogPlugin = catalogManager.catalog(catalogName)
    if (!catalogPlugin.isInstanceOf[TableCatalog]) {
      throw new IllegalArgumentException(
        s"${tvf.fnName} requires a table catalog, but catalog '$catalogName' is " +
          s"${catalogPlugin.getClass.getName}.")
    }
    val tableCatalog = catalogPlugin.asInstanceOf[TableCatalog]
    val ident = Identifier.of(namespace, tableName)
    val table = tableCatalog.loadTable(ident)
    if (!table.isInstanceOf[SparkTable]) {
      throw new IllegalArgumentException(
        s"${tvf.fnName} only supports Fluss tables, but '$fullTableIdentifier' is " +
          s"backed by ${table.getClass.getName}.")
    }

    DataSourceV2Relation.create(
      table,
      Some(tableCatalog),
      Some(ident),
      new CaseInsensitiveStringMap(options.asJava))
  }

  /**
   * Normalizes a timestamp argument, which may be any constant expression, to the string form the
   * `scan.incremental.*` options accept: a STRING is passed through (the option layer reads both
   * epoch millis and `yyyy-MM-dd HH:mm:ss`), an integral value is epoch millis, a DATE becomes the
   * start of that day, and TIMESTAMP / TIMESTAMP_NTZ are converted from Spark's internal
   * microseconds.
   */
  private[logical] def toTimestampOptionValue(fnName: String, expr: Expression): String = {
    // RuntimeReplaceable expressions (e.g. the `-` in `now() - INTERVAL 1 HOUR`) only become
    // evaluable once the optimizer rewrites them, which has not happened yet during analysis.
    val evaluable = expr.transformUp { case r: RuntimeReplaceable => r.replacement }

    val value =
      try {
        evaluable.eval()
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"Failed to evaluate the timestamp argument '${expr.sql}' of $fnName. It must be a " +
              "constant expression; literals and datetime functions such as now() or " +
              "unix_timestamp() are supported, references to table columns are not.",
            e
          )
      }
    if (value == null) {
      throw new IllegalArgumentException(s"Timestamp arguments of $fnName must not be null.")
    }
    val normalized = evaluable.dataType match {
      case StringType => value.toString
      case ShortType | IntegerType | LongType => value.toString
      case DateType => dateDaysToEpochMillis(value.asInstanceOf[Int]).toString
      case TimestampType => (value.asInstanceOf[Long] / 1000L).toString
      case TimestampNTZType => ntzMicrosToEpochMillis(value.asInstanceOf[Long]).toString
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported timestamp argument type $other for $fnName. Use a STRING (epoch " +
            "milliseconds or 'yyyy-MM-dd HH:mm:ss'), an integral epoch milliseconds value, a " +
            "DATE or a TIMESTAMP.")
    }
    if (normalized.trim.isEmpty) {
      throw new IllegalArgumentException(
        s"Timestamp arguments of $fnName must not be blank. Provide epoch milliseconds or a " +
          "'yyyy-MM-dd HH:mm:ss' timestamp.")
    }
    normalized
  }

  private val MICROS_PER_SECOND = 1000000L

  /**
   * Converts a `DATE` argument to epoch milliseconds, at the start of that day in the Spark session
   * time zone.
   */
  private def dateDaysToEpochMillis(days: Int): Long =
    LocalDate
      .ofEpochDay(days.toLong)
      .atStartOfDay(ZoneId.of(SQLConf.get.sessionLocalTimeZone))
      .toInstant
      .toEpochMilli

  /**
   * Converts a `TIMESTAMP_NTZ` argument to epoch milliseconds. Its wall-clock microseconds are
   * re-interpreted in the Spark session time zone, the same convention the `yyyy-MM-dd HH:mm:ss`
   * string form uses, so both forms resolve to the same instant.
   */
  private def ntzMicrosToEpochMillis(micros: Long): Long = {
    val seconds = Math.floorDiv(micros, MICROS_PER_SECOND)
    val nanos = Math.floorMod(micros, MICROS_PER_SECOND) * 1000L
    LocalDateTime
      .ofEpochSecond(seconds, nanos.toInt, ZoneOffset.UTC)
      .atZone(ZoneId.of(SQLConf.get.sessionLocalTimeZone))
      .toInstant
      .toEpochMilli
  }
}

/**
 * An unresolved Fluss table-valued function.
 *
 * @param fnName
 *   one of [[FlussTableValuedFunctions.supportedFnNames]].
 */
abstract class FlussTableValueFunction(val fnName: String) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false

  val args: Seq[Expression]

  /** Translates the arguments following the table identifier into Fluss scan options. */
  def parseArgs(argsWithoutTable: Seq[Expression]): Map[String, String]
}

/**
 * Plan for `fluss_incremental_between_timestamp(table, startTimestamp[, endTimestamp])`.
 *
 * The window is left-closed and right-open, `[start, end)`, on the record commit timestamp, and
 * covers the data Fluss still retains inside it. An omitted end timestamp means "up to now".
 */
case class IncrementalBetweenTimestamp(override val args: Seq[Expression])
  extends FlussTableValueFunction(INCREMENTAL_BETWEEN_TIMESTAMP) {

  override def parseArgs(argsWithoutTable: Seq[Expression]): Map[String, String] = {
    if (argsWithoutTable.size != 1 && argsWithoutTable.size != 2) {
      throw new IllegalArgumentException(
        s"$INCREMENTAL_BETWEEN_TIMESTAMP needs a table identifier followed by a startTimestamp " +
          s"and an optional endTimestamp, e.g. " +
          s"$INCREMENTAL_BETWEEN_TIMESTAMP('db.t', '2026-01-01 00:00:00', '2026-01-01 01:00:00'). " +
          s"Got ${argsWithoutTable.size + 1} arguments.")
    }

    val start = toTimestampOptionValue(INCREMENTAL_BETWEEN_TIMESTAMP, argsWithoutTable.head)
    // Resolving the end here pins the window at analysis time, so rows committed while the query is
    // planned stay out of it and re-executing the same relation reads the same window.
    val endArg = if (argsWithoutTable.size == 2) argsWithoutTable.last else CurrentTimestamp()
    val end = toTimestampOptionValue(INCREMENTAL_BETWEEN_TIMESTAMP, endArg)
    requireValidWindow(start, end)

    Map(
      SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key() -> start,
      SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key() -> end)
  }

  /** A reversed or degenerate window fails fast instead of silently returning no rows. */
  private def requireValidWindow(start: String, end: String): Unit = {
    val startMs = FlussOffsetInitializers.parseTimestamp(
      start,
      SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key())
    val endMs = FlussOffsetInitializers.parseTimestamp(
      end,
      SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key())
    FlussOffsetInitializers.requireValidWindow(start, end, startMs, endMs)
  }
}
