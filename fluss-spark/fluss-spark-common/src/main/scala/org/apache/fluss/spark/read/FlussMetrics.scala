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

package org.apache.fluss.spark.read

import org.apache.spark.sql.connector.metric.{CustomSumMetric, CustomTaskMetric}

object FlussMetrics {
  val NUM_ROWS_READ = "numRowsRead"
}

// ---------------------------------------------------------------------------
// CustomMetric classes — driver-side aggregation (must have 0-arg constructor)
// ---------------------------------------------------------------------------

/** Base trait for Fluss sum metrics. */
sealed trait FlussSumMetric extends CustomSumMetric {
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    var sum = 0L
    for (v <- taskMetrics) { sum += v }
    String.valueOf(sum)
  }
}

/** Aggregates total rows read across all tasks. */
case class FlussNumRowsReadMetric() extends FlussSumMetric {
  override def name(): String = FlussMetrics.NUM_ROWS_READ
  override def description(): String = "number of rows read from Fluss"
}

// ---------------------------------------------------------------------------
// CustomTaskMetric classes — executor-side reporting
// ---------------------------------------------------------------------------

/** Base trait for Fluss task metrics. */
sealed trait FlussTaskMetric extends CustomTaskMetric

/** Reports the number of rows read in a single task. */
case class FlussNumRowsReadTaskMetric(override val value: Long) extends FlussTaskMetric {
  override def name(): String = FlussMetrics.NUM_ROWS_READ
}
