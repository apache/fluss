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

import org.apache.spark.util.AccumulatorV2

/** Accumulator that tracks the maximum Long value across all tasks. */
class MaxLongAccumulator extends AccumulatorV2[java.lang.Long, java.lang.Long] {
  @volatile private var _max: Long = 0L

  override def isZero: Boolean = _max == 0L

  override def copy(): MaxLongAccumulator = {
    val newAcc = new MaxLongAccumulator
    newAcc._max = this._max
    newAcc
  }

  override def reset(): Unit = { _max = 0L }

  override def add(v: java.lang.Long): Unit = {
    if (v > _max) _max = v
  }

  override def merge(other: AccumulatorV2[java.lang.Long, java.lang.Long]): Unit =
    other match {
      case o: MaxLongAccumulator => if (o._max > _max) _max = o._max
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${other.getClass.getName} into MaxLongAccumulator")
    }

  override def value: java.lang.Long = _max
}
