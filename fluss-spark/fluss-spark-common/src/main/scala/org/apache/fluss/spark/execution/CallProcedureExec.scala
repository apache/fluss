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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow}
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for executing a stored procedure.
 *
 * This implementation extends LeafExecNode for simpler command-style execution without requiring
 * RDD-based distributed processing. This approach is more appropriate for procedures which are
 * typically synchronous administrative operations.
 *
 * Benefits over SparkPlan-based implementation:
 *   - No need to create and manage RDDs explicitly
 *   - Simpler code without parallelize() overhead
 *   - Better semantic match for command-style operations
 *   - Consistent with other catalog implementations (e.g., Apache Paimon)
 *
 * @param output
 *   the output attributes of the procedure result
 * @param procedure
 *   the procedure to execute
 * @param args
 *   the argument expressions to pass to the procedure
 */
case class CallProcedureExec(output: Seq[Attribute], procedure: Procedure, args: Seq[Expression])
  extends LeafExecNode {

  override def simpleString(maxFields: Int): String = {
    s"CallProcedure ${procedure.description()}"
  }

  /**
   * Execute the procedure and collect results efficiently.
   *
   * This is the primary execution method that directly returns results without unnecessary RDD
   * creation overhead.
   */
  override def executeCollect(): Array[InternalRow] = {
    // Evaluate all argument expressions
    val argumentValues = new Array[Any](args.length)
    args.zipWithIndex.foreach {
      case (arg, index) =>
        argumentValues(index) = arg.eval(null)
    }

    // Package arguments and invoke procedure
    val argRow = new GenericInternalRow(argumentValues)
    procedure.call(argRow)
  }

  override def executeTake(limit: Int): Array[InternalRow] = {
    executeCollect().take(limit)
  }

  override def executeTail(limit: Int): Array[InternalRow] = {
    val results = executeCollect()
    results.takeRight(limit)
  }

  /**
   * Execute the procedure and return results as an RDD.
   *
   * This method is required by the SparkPlan interface but simply wraps the executeCollect()
   * results in an RDD for compatibility with the execution framework.
   */
  override protected def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    sparkContext.parallelize(executeCollect(), 1)
  }

  override def withNewChildrenInternal(
      newChildren: IndexedSeq[org.apache.spark.sql.execution.SparkPlan]): CallProcedureExec = {
    copy()
  }
}
