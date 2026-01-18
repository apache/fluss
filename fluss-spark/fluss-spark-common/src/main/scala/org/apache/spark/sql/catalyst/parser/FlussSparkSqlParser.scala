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

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Parser extension for Fluss SQL extensions.
 *
 * @param delegate
 *   The main Spark SQL parser.
 */
class FlussSparkSqlParser(delegate: ParserInterface) extends ParserInterface {

  private lazy val astBuilder = new FlussSqlAstBuilder(delegate)

  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      parse(sqlText)(parser => astBuilder.visitSingleStatement(parser.singleStatement()))
    } catch {
      case _: ParseException | _: ParseCancellationException =>
        delegate.parsePlan(sqlText)
    }
  }

  override def parseQuery(sqlText: String): LogicalPlan = parsePlan(sqlText)

  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  private def parse[T](sqlText: String)(
      toResult: org.apache.spark.sql.catalyst.parser.FlussSparkSqlParserParser => T): T = {
    val lexer = new FlussSparkSqlParserLexer(
      new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(FlussParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser =
      new org.apache.spark.sql.catalyst.parser.FlussSparkSqlParserParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(FlussParseErrorListener)

    try {
      try {
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case _: ParseCancellationException =>
          tokenStream.seek(0)
          parser.reset()
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(sqlText)
      case e: AnalysisException =>
        val position = org.apache.spark.sql.catalyst.trees.Origin(e.line, e.startPosition)
        throw new ParseException(Option(sqlText), e.message, position, position)
    }
  }
}

class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume()
  override def getSourceName: String = wrapped.getSourceName
  override def index(): Int = wrapped.index()
  override def mark(): Int = wrapped.mark()
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size()

  override def getText(interval: Interval): String = {
    wrapped.getText(interval)
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

object FlussParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    val position = org.apache.spark.sql.catalyst.trees.Origin(Some(line), Some(charPositionInLine))
    throw new ParseException(None, msg, position, position)
  }
}
