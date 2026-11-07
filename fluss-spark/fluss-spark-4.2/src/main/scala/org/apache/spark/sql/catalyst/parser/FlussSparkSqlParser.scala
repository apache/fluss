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

package org.apache.spark.sql.catalyst.parser

import org.apache.fluss.spark.catalog.SupportsProcedures
import org.apache.fluss.spark.catalyst.plans.logical.{FlussCallStatement, FlussNamedArgument, FlussPositionalArgument}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{BindParameters, GeneralParameterizedQuery, NameParameterizedQuery, PosParameterizedQuery, UnresolvedProcedure}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedArgumentExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Call, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin => SparkCurrentOrigin}
import org.apache.spark.sql.connector.catalog.CatalogNotFoundException
import org.apache.spark.sql.types.{DataType, StructType}

/** Preserves Spark parsing and parameter context while routing Fluss procedure calls. */
class FlussSparkSqlParser(delegate: ParserInterface, spark: SparkSession) extends ParserInterface {

  override def parsePlan(sqlText: String): LogicalPlan = routeCall(delegate.parsePlan(sqlText))

  override def parsePlanWithParameters(
      sqlText: String,
      parameterContext: ParameterContext): LogicalPlan = {
    val parsed = delegate.parsePlanWithParameters(sqlText, parameterContext)
    val bound = parsed match {
      case call @ Call(UnresolvedProcedure(name), _, _)
          if spark.sessionState.conf.legacyParameterSubstitutionConstantsOnly &&
            isFlussCatalog(name) =>
        // Fluss argument wrappers are not Catalyst expressions. Bind native CALL arguments
        // before wrapping them so legacy analyzer binding can still see every marker.
        bindCallParameters(call, parameterContext)
      case other => other
    }
    routeCall(bound)
  }

  override def parseQuery(sqlText: String): LogicalPlan = delegate.parseQuery(sqlText)

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseRoutineParam(sqlText: String): StructType = delegate.parseRoutineParam(sqlText)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  private def routeCall(plan: LogicalPlan): LogicalPlan = plan match {
    case call @ Call(UnresolvedProcedure(name), args, _) if isFlussCatalog(name) =>
      SparkCurrentOrigin.withOrigin(call.origin) {
        val arguments = args.map {
          case NamedArgumentExpression(key, value) => FlussNamedArgument(key, value)
          case expression => FlussPositionalArgument(expression)
        }
        FlussCallStatement(name, arguments)
      }
    case other => other
  }

  private def bindCallParameters(call: Call, context: ParameterContext): LogicalPlan = {
    context match {
      case NamedParameterContext(params) if params.nonEmpty =>
        BindParameters(NameParameterizedQuery(call, params))
      case PositionalParameterContext(params) if params.nonEmpty =>
        BindParameters(PosParameterizedQuery(call, params))
      case HybridParameterContext(args, names) if args.nonEmpty =>
        BindParameters(GeneralParameterizedQuery(call, args, names))
      case _ => call
    }
  }

  private def isFlussCatalog(name: Seq[String]): Boolean = {
    // Existing Fluss procedure names are catalog.procedure or catalog.namespace.procedure.
    if (name.length != 2 && name.length != 3) {
      false
    } else {
      try {
        spark.sessionState.catalogManager.catalog(name.head).isInstanceOf[SupportsProcedures]
      } catch {
        // An unknown catalog belongs to Spark's native resolution/error path.
        case _: CatalogNotFoundException => false
      }
    }
  }
}
