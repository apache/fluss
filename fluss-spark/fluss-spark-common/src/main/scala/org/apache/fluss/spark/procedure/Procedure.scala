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

package org.apache.fluss.spark.procedure

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Interface for Fluss stored procedures that can be invoked via Spark SQL CALL statements.
 *
 * Procedures allow users to perform administrative and management operations through Spark SQL. All
 * procedures are located in the `sys` namespace.
 *
 * Example usage:
 * {{{
 * CALL catalog.sys.procedure_name(parameter => 'value')
 * }}}
 */
trait Procedure {

  /**
   * Returns the parameters accepted by this procedure.
   *
   * @return
   *   array of procedure parameters
   */
  def parameters(): Array[ProcedureParameter]

  /**
   * Returns the output schema of this procedure.
   *
   * @return
   *   the output StructType
   */
  def outputType(): StructType

  /**
   * Executes the procedure with the given arguments.
   *
   * @param args
   *   the argument values as an InternalRow
   * @return
   *   array of result rows
   */
  def call(args: InternalRow): Array[InternalRow]

  /**
   * Returns a human-readable description of this procedure.
   *
   * @return
   *   the procedure description
   */
  def description(): String = getClass.toString
}
