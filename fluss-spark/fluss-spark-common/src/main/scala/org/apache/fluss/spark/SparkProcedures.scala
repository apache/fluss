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

package org.apache.fluss.spark

import org.apache.fluss.spark.procedure.{GetClusterConfigsProcedure, ProcedureBuilder}

import java.util.Locale

object SparkProcedures {

  private val BUILDERS: Map[String, () => ProcedureBuilder] = initProcedureBuilders()

  def newBuilder(name: String): ProcedureBuilder = {
    val builderSupplier = BUILDERS.get(name.toLowerCase(Locale.ROOT))
    builderSupplier.map(_()).orNull
  }

  def names(): Set[String] = BUILDERS.keySet

  private def initProcedureBuilders(): Map[String, () => ProcedureBuilder] = {
    Map(
      "get_cluster_configs" -> (() => GetClusterConfigsProcedure.builder())
    )
  }
}
