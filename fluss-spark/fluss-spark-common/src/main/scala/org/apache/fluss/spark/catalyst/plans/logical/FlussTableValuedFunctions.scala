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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExpressionInfo, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, LongType, ShortType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Fluss table-valued functions (TVFs), usable from pure SQL.
 *
 * A TVF is only sugar over per-relation scan options: the function arguments are translated into
 * the same `scan.*` options the DataFrame API accepts, and the call is then resolved into a plain
 * [[DataSourceV2Relation]]. Consequently projection, filter push down and metrics keep working, and
 * the options are scoped to the single query instead of leaking through session configuration.
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

  /**
   * Resolves a Fluss TVF call into a [[DataSourceV2Relation]] over the referenced Fluss table, with
   * the function arguments translated into scan options.
   */
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

    // Parse the remaining arguments first so that an argument error is reported without depending
    // on the referenced table being resolvable.
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
          (catalogManager.currentCatalog.name(), catalogManager.currentNamespace.head, table)
        case Seq(db, table) => (catalogManager.currentCatalog.name(), db, table)
        case Seq(catalog, db, table) => (catalog, db, table)
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid table identifier '$tableIdentifier' for ${tvf.fnName}. Expected " +
              "'table', 'database.table' or 'catalog.database.table'.")
      }

    val catalogPlugin = catalogManager.catalog(catalogName)
    if (!catalogPlugin.isInstanceOf[TableCatalog]) {
      throw new IllegalArgumentException(
        s"${tvf.fnName} requires a table catalog, but catalog '$catalogName' is " +
          s"${catalogPlugin.getClass.getName}.")
    }
    val tableCatalog = catalogPlugin.asInstanceOf[TableCatalog]
    val ident = Identifier.of(Array(namespace), tableName)
    val table = tableCatalog.loadTable(ident)
    if (!table.isInstanceOf[SparkTable]) {
      throw new IllegalArgumentException(
        s"${tvf.fnName} only supports Fluss tables, but '$catalogName.$namespace.$tableName' is " +
          s"backed by ${table.getClass.getName}.")
    }

    DataSourceV2Relation.create(
      table,
      Some(tableCatalog),
      Some(ident),
      new CaseInsensitiveStringMap(options.asJava))
  }

  /**
   * Normalizes a timestamp argument to the string form accepted by the `scan.incremental.*`
   * timestamp options.
   *
   * A STRING argument is passed through untouched, so both epoch milliseconds and
   * `yyyy-MM-dd HH:mm:ss` keep being interpreted by the option layer. Integral arguments are epoch
   * milliseconds. TIMESTAMP arguments are converted from Spark's internal microseconds, otherwise a
   * `TIMESTAMP '...'` literal would silently be read as epoch milliseconds.
   *
   * Any constant expression is accepted, e.g. `CAST(unix_timestamp() * 1000 AS STRING)` or
   * `date_format(now() - INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm:ss')`.
   */
  private[logical] def toTimestampOptionValue(fnName: String, expr: Expression): String = {
    // `RuntimeReplaceable` expressions (such as the `-` in `now() - INTERVAL 1 HOUR`) only become
    // evaluable once the optimizer's ReplaceExpressions rule rewrites them, which has not happened
    // yet while the analyzer resolves this function. Apply the same rewrite bottom-up here.
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
    evaluable.dataType match {
      case StringType => value.toString
      case ShortType | IntegerType | LongType => value.toString
      case TimestampType | TimestampNTZType => (value.asInstanceOf[Long] / 1000L).toString
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported timestamp argument type $other for $fnName. Use a STRING (epoch " +
            "milliseconds or 'yyyy-MM-dd HH:mm:ss'), an integral epoch milliseconds value, or a " +
            "TIMESTAMP.")
    }
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
 * Plan for [[FlussTableValuedFunctions.INCREMENTAL_BETWEEN_TIMESTAMP]].
 *
 * Usage:
 *   - `fluss_incremental_between_timestamp(table, startTimestamp, endTimestamp)`
 *   - `fluss_incremental_between_timestamp(table, startTimestamp)` reads up to the latest data
 *
 * The window is left-closed and right-open, `[start, end)`, on the record commit timestamp.
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
    val startOptions =
      Map(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key() -> start)

    // The end bound is always written explicitly so the call stays self-contained: options take
    // precedence over session configuration, which may still hold a stale end timestamp.
    if (argsWithoutTable.size == 2) {
      val end = toTimestampOptionValue(INCREMENTAL_BETWEEN_TIMESTAMP, argsWithoutTable.last)
      startOptions + (SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key() -> end)
    } else {
      startOptions +
        (SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key() -> SparkFlussConf.END_TIMESTAMP_LATEST)
    }
  }
}
