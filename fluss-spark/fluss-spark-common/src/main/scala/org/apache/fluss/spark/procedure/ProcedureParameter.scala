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

import org.apache.spark.sql.types.DataType

/**
 * Represents a parameter for a Fluss stored procedure.
 *
 * @param name
 *   the parameter name
 * @param dataType
 *   the Spark SQL data type of the parameter
 * @param isRequired
 *   whether this parameter is required
 */
case class ProcedureParameter(name: String, dataType: DataType, isRequired: Boolean = false)

object ProcedureParameter {

  /**
   * Creates a required procedure parameter.
   *
   * @param name
   *   the parameter name
   * @param dataType
   *   the parameter data type
   * @return
   *   a required ProcedureParameter
   */
  def required(name: String, dataType: DataType): ProcedureParameter =
    ProcedureParameter(name, dataType, isRequired = true)

  /**
   * Creates an optional procedure parameter.
   *
   * @param name
   *   the parameter name
   * @param dataType
   *   the parameter data type
   * @return
   *   an optional ProcedureParameter
   */
  def optional(name: String, dataType: DataType): ProcedureParameter =
    ProcedureParameter(name, dataType, isRequired = false)
}
