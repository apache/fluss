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

package org.apache.fluss.spark.functions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

/** Registers Fluss sketch helpers without replacing Spark's native function names. */
object SketchFunctions {
  def inject(extensions: SparkSessionExtensions): Unit = {
    val builders: Seq[(String, Int, Seq[Expression] => Expression)] = Seq(
      (
        "fluss_hll_build_agg",
        1,
        args =>
          nativeOrFallback(
            "hll_sketch_agg",
            args,
            SketchAggregate(args.head, hll = true, mergeInput = false))),
      ("fluss_hll_merge_agg", 1, args => SketchAggregate(args.head, hll = true, mergeInput = true)),
      ("fluss_hll_estimate", 1, args => HllEstimate(args.head)),
      (
        "fluss_kll_double_build_agg",
        1,
        args => SketchAggregate(args.head, hll = false, mergeInput = false)),
      (
        "fluss_kll_double_merge_agg",
        1,
        args => SketchAggregate(args.head, hll = false, mergeInput = true)),
      ("fluss_kll_double_quantile", 2, args => KllDoubleQuantile(args.head, args(1)))
    )
    builders.foreach {
      case (name, arity, builder) =>
        extensions.injectFunction(
          (
            FunctionIdentifier(name),
            new ExpressionInfo(getClass.getName, name),
            args => {
              require(args.size == arity, s"$name requires $arity arguments")
              builder(args)
            }))
    }
  }

  private def nativeOrFallback(
      name: String,
      args: Seq[Expression],
      fallback: => Expression): Expression = {
    val identifier = FunctionIdentifier(name)
    if (FunctionRegistry.builtin.functionExists(identifier)) {
      FunctionRegistry.builtin.lookupFunction(identifier, args)
    } else fallback
  }
}
