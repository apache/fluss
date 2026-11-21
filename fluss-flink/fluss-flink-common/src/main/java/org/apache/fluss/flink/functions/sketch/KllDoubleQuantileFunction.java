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

package org.apache.fluss.flink.functions.sketch;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Reads a quantile from serialized DataSketches KLL doubles bytes. */
public class KllDoubleQuantileFunction extends ScalarFunction {
    /** Returns null for null arguments or empty sketches; rank must be in [0, 1]. */
    public Double eval(byte[] bytes, Double rank) {
        if (bytes == null || rank == null) {
            return null;
        }
        checkArgument(rank >= 0.0 && rank <= 1.0, "Quantile rank must be in [0, 1].");
        KllDoublesSketch sketch = KllDoublesSketch.heapify(Memory.wrap(bytes));
        return sketch.isEmpty() ? null : sketch.getQuantile(rank);
    }
}
