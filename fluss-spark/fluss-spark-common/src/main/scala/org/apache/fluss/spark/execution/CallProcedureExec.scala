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

package org.apache.fluss.spark.execution

import org.apache.fluss.spark.procedure.Procedure

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.SparkPlan

/** Physical plan node for executing a stored procedure. */
case class CallProcedureExec(output: Seq[Attribute], procedure: Procedure, args: Seq[Expression])
  extends SparkPlan {

  override def children: Seq[SparkPlan] = Seq.empty

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    copy()
  }

  override protected def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    val argumentValues = new Array[Any](args.length)
    args.zipWithIndex.foreach {
      case (arg, index) =>
        argumentValues(index) = arg.eval(null)
    }

    val argRow = new GenericInternalRow(argumentValues)
    val resultRows = procedure.call(argRow)

    sparkContext.parallelize(resultRows)
  }
}
