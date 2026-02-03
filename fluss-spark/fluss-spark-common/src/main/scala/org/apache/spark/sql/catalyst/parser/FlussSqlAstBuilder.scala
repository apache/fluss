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

import org.apache.fluss.spark.catalyst.plans.logical._

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.FlussSqlExtensionParser._
import org.apache.spark.sql.catalyst.plans.logical._

import scala.collection.JavaConverters._

/**
 * The AST Builder for Fluss SQL extensions.
 *
 * @param delegate
 *   The main Spark SQL parser.
 */
class FlussSqlAstBuilder(delegate: ParserInterface)
  extends FlussSqlExtensionBaseVisitor[AnyRef]
  with Logging {

  /** Creates a single statement of extension statements. */
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  /** Creates a [[FlussCallStatement]] for a stored procedure call. */
  override def visitCall(ctx: CallContext): FlussCallStatement = withOrigin(ctx) {
    val name =
      ctx.multipartIdentifier.parts.asScala.map(part => cleanIdentifier(part.getText)).toSeq
    val args = ctx.callArgument.asScala.map(typedVisit[FlussCallArgument]).toSeq
    FlussCallStatement(name, args)
  }

  /** Creates a positional argument in a stored procedure call. */
  override def visitPositionalArgument(ctx: PositionalArgumentContext): FlussCallArgument =
    withOrigin(ctx) {
      val expression = typedVisit[Expression](ctx.expression)
      FlussPositionalArgument(expression)
    }

  /** Creates a named argument in a stored procedure call. */
  override def visitNamedArgument(ctx: NamedArgumentContext): FlussCallArgument = withOrigin(ctx) {
    val name = cleanIdentifier(ctx.identifier.getText)
    val expression = typedVisit[Expression](ctx.expression)
    FlussNamedArgument(name, expression)
  }

  /** Creates a [[Expression]] in a positional and named argument. */
  override def visitExpression(ctx: ExpressionContext): Expression = {
    val sqlString = reconstructSqlString(ctx)
    delegate.parseExpression(sqlString)
  }

  /** Returns a multi-part identifier as Seq[String]. */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(part => cleanIdentifier(part.getText)).toSeq
    }

  /** Remove backticks from identifier. */
  private def cleanIdentifier(ident: String): String = {
    if (ident.startsWith("`") && ident.endsWith("`")) {
      ident.substring(1, ident.length - 1)
    } else {
      ident
    }
  }

  private def reconstructSqlString(ctx: ParserRuleContext): String = {
    ctx.children.asScala
      .map {
        case c: ParserRuleContext => reconstructSqlString(c)
        case t: TerminalNode => t.getText
      }
      .mkString(" ")
  }

  private def typedVisit[T](ctx: ParseTree): T =
    ctx.accept(this).asInstanceOf[T]

  private def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  private def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }
}

case class Origin(
    line: Option[Int] = None,
    startPosition: Option[Int] = None,
    startIndex: Option[Int] = None,
    stopIndex: Option[Int] = None,
    sqlText: Option[String] = None,
    objectType: Option[String] = None,
    objectName: Option[String] = None)

object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)
  def reset(): Unit = value.set(Origin())

  def withOrigin[A](o: Origin)(f: => A): A = {
    val previous = get
    set(o)
    val ret =
      try f
      finally { set(previous) }
    ret
  }
}
