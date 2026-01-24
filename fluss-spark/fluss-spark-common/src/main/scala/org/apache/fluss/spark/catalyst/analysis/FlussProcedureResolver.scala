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

package org.apache.fluss.spark.catalyst.analysis

import org.apache.fluss.spark.catalog.SupportsProcedures
import org.apache.fluss.spark.catalyst.plans.logical._
import org.apache.fluss.spark.procedure.ProcedureParameter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}

import java.util.Locale

/** Resolution rule for Fluss stored procedures. */
case class FlussProcedureResolver(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case FlussCallStatement(nameParts, arguments) if nameParts.nonEmpty =>
      val (catalog, identifier) = resolveCatalogAndIdentifier(nameParts)
      if (catalog == null || !catalog.isInstanceOf[SupportsProcedures]) {
        throw new RuntimeException(s"Catalog ${nameParts.head} is not a SupportsProcedures")
      }

      val procedureCatalog = catalog.asInstanceOf[SupportsProcedures]
      val procedure = procedureCatalog.loadProcedure(identifier)
      val parameters = procedure.parameters
      val normalizedParameters = normalizeParameters(parameters)
      validateParameters(normalizedParameters)
      val normalizedArguments = normalizeArguments(arguments)
      FlussCallCommand(
        procedure,
        args = buildArgumentExpressions(normalizedParameters, normalizedArguments))

    case call @ FlussCallCommand(procedure, arguments) if call.resolved =>
      val parameters = procedure.parameters
      val newArguments = arguments.zipWithIndex.map {
        case (argument, index) =>
          val parameter = parameters(index)
          val parameterType = parameter.dataType
          val argumentType = argument.dataType
          if (parameterType != argumentType && !Cast.canUpCast(argumentType, parameterType)) {
            throw new RuntimeException(
              s"Cannot cast $argumentType to $parameterType of ${parameter.name}.")
          }
          if (parameterType != argumentType) {
            Cast(argument, parameterType)
          } else {
            argument
          }
      }

      if (newArguments != arguments) {
        call.copy(args = newArguments)
      } else {
        call
      }
  }

  private def resolveCatalogAndIdentifier(nameParts: Seq[String]): (CatalogPlugin, Identifier) = {
    val catalogManager = sparkSession.sessionState.catalogManager
    if (nameParts.length == 2) {
      val catalogName = nameParts.head
      val procedureName = nameParts(1)
      val catalog = catalogManager.catalog(catalogName)
      (catalog, Identifier.of(Array("sys"), procedureName))
    } else if (nameParts.length == 3) {
      val catalogName = nameParts.head
      val namespace = nameParts(1)
      val procedureName = nameParts(2)
      val catalog = catalogManager.catalog(catalogName)
      (catalog, Identifier.of(Array(namespace), procedureName))
    } else {
      throw new RuntimeException(s"Invalid procedure name: ${nameParts.mkString(".")}")
    }
  }

  private def normalizeParameters(parameters: Seq[ProcedureParameter]): Seq[ProcedureParameter] = {
    parameters.map {
      parameter =>
        val normalizedName = parameter.name.toLowerCase(Locale.ROOT)
        if (parameter.isRequired) {
          ProcedureParameter.required(normalizedName, parameter.dataType)
        } else {
          ProcedureParameter.optional(normalizedName, parameter.dataType)
        }
    }
  }

  private def validateParameters(parameters: Seq[ProcedureParameter]): Unit = {
    val duplicateParamNames = parameters.groupBy(_.name).collect {
      case (name, matchingParams) if matchingParams.length > 1 => name
    }
    if (duplicateParamNames.nonEmpty) {
      throw new RuntimeException(
        s"Parameter names ${duplicateParamNames.mkString("[", ",", "]")} are duplicated.")
    }
    parameters.sliding(2).foreach {
      case Seq(previousParam, currentParam)
          if !previousParam.isRequired && currentParam.isRequired =>
        throw new RuntimeException(
          s"Optional parameters should be after required ones but $currentParam is after $previousParam.")
      case _ =>
    }
  }

  private def normalizeArguments(arguments: Seq[FlussCallArgument]): Seq[FlussCallArgument] = {
    arguments.map {
      case a @ FlussNamedArgument(name, _) => a.copy(name = name.toLowerCase(Locale.ROOT))
      case other => other
    }
  }

  private def buildArgumentExpressions(
      parameters: Seq[ProcedureParameter],
      arguments: Seq[FlussCallArgument]): Seq[Expression] = {
    val nameToPositionMap = parameters.map(_.name).zipWithIndex.toMap
    val nameToArgumentMap = buildNameToArgumentMap(parameters, arguments, nameToPositionMap)
    val missingParamNames = parameters.filter(_.isRequired).collect {
      case parameter if !nameToArgumentMap.contains(parameter.name) => parameter.name
    }
    if (missingParamNames.nonEmpty) {
      throw new RuntimeException(
        s"Required parameters ${missingParamNames.mkString("[", ",", "]")} are missing.")
    }
    val argumentExpressions = new Array[Expression](parameters.size)
    nameToArgumentMap.foreach {
      case (name, argument) => argumentExpressions(nameToPositionMap(name)) = argument.expr
    }
    parameters.foreach {
      case parameter if !parameter.isRequired && !nameToArgumentMap.contains(parameter.name) =>
        argumentExpressions(nameToPositionMap(parameter.name)) =
          Literal.create(null, parameter.dataType)
      case _ =>
    }
    argumentExpressions.toSeq
  }

  private def buildNameToArgumentMap(
      parameters: Seq[ProcedureParameter],
      arguments: Seq[FlussCallArgument],
      nameToPositionMap: Map[String, Int]): Map[String, FlussCallArgument] = {
    val isNamedArgument = arguments.exists(_.isInstanceOf[FlussNamedArgument])
    val isPositionalArgument = arguments.exists(_.isInstanceOf[FlussPositionalArgument])

    if (isNamedArgument && isPositionalArgument) {
      throw new RuntimeException("Cannot mix named and positional arguments.")
    }

    if (isNamedArgument) {
      val namedArguments = arguments.asInstanceOf[Seq[FlussNamedArgument]]
      val validationErrors = namedArguments.groupBy(_.name).collect {
        case (name, procedureArguments) if procedureArguments.size > 1 =>
          s"Procedure argument $name is duplicated."
        case (name, _) if !nameToPositionMap.contains(name) => s"Argument $name is unknown."
      }
      if (validationErrors.nonEmpty) {
        throw new RuntimeException(s"Invalid arguments: ${validationErrors.mkString(", ")}")
      }
      namedArguments.map(arg => arg.name -> arg).toMap
    } else {
      if (arguments.size > parameters.size) {
        throw new RuntimeException("Too many arguments for procedure")
      }
      arguments.zipWithIndex.map {
        case (argument, position) =>
          val param = parameters(position)
          param.name -> argument
      }.toMap
    }
  }
}
