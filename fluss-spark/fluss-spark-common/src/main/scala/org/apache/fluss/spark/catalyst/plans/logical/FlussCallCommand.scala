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

import org.apache.fluss.spark.procedure.Procedure

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LeafCommand
import org.apache.spark.sql.catalyst.util.truncatedString

/** A CALL statement that needs to be resolved to a procedure. */
case class FlussCallStatement(name: Seq[String], args: Seq[FlussCallArgument]) extends LeafCommand {
  override def output: Seq[Attribute] = Seq.empty
}

/** Base trait for CALL statement arguments. */
sealed trait FlussCallArgument {
  def expr: Expression
}

/** A positional argument in a stored procedure call. */
case class FlussPositionalArgument(expr: Expression) extends FlussCallArgument

/** A named argument in a stored procedure call. */
case class FlussNamedArgument(name: String, expr: Expression) extends FlussCallArgument

/** A CALL command that has been resolved to a specific procedure. */
case class FlussCallCommand(procedure: Procedure, args: Seq[Expression]) extends LeafCommand {

  override lazy val output: Seq[Attribute] =
    procedure.outputType.map(
      field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())

  override def simpleString(maxFields: Int): String = {
    s"Call${truncatedString(output, "[", ", ", "]", maxFields)} ${procedure.description}"
  }
}
